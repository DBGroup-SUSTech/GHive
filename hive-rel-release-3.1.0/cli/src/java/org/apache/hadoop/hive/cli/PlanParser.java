package org.apache.hadoop.hive.cli;

import java.io.*;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;
import java.util.regex.*;

import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import com.alibaba.fastjson.JSON;



public class PlanParser {
    enum ParserState {
        /** The begin. */
        BEGIN,
        /** The filter operator. */
        FILTER,
        /** The file output operator. */
        FILE_OUTPUT_OPERATOR,
        /** The table scan. */
        TABLE_SCAN,
        /** The map join. */
        MAPJOIN,
        /** The merge join. */
        MERGEJOIN,
        /** The select. */
        SELECT,
        /** The groupby. */
        GROUPBY,
        /** The move. */
        MOVE,
        /** The map. */
        MAP,
        /** The reduce. */
        REDUCER,
        /** The partition list. */
        PARTITION_LIST,
        /** The partition. */
        PARTITION,
        /** CREATE TABLE if destination is a table */
        CREATE,
        /** SHUFFLE*/
        SHUFFLE
    }
    public static List<Node> getTree(List<Node> nodes) {
        Map<String, Node> nodeMap = Maps.newHashMap();
        List<Node> rootList = Lists.newArrayList();
        for (Node node : nodes) {
            nodeMap.put(node.str, node);
            String parentId = node.parentId;
            if (parentId == null) {
                rootList.add(node);
            }
        }
        for (Node node : nodes) {
            String parentId = node.parentId;
            if (parentId != null) {
                Node pnode = nodeMap.get(parentId);
                if (pnode == null) {
                    continue;
                }
                List<Node> children = pnode.children;
                if (children == null) {
                    children = Lists.newArrayList();
                    pnode.children = children;
                }
                children.add(node);
            }
        }
        return rootList;
    }
    public static List<Vertex> getDAG(List<Vertex> dag,List<Node> list) throws IOException {
        Map<String, Vertex> nodeMap = Maps.newHashMap();
        List<Vertex> rootList = Lists.newArrayList();
        for (Vertex Vertex : dag) {
            nodeMap.put(Vertex.str, Vertex);
            String parentId = Vertex.parentId;
            if (parentId == null) {
                rootList.add(Vertex);
            }
        }
        for (Vertex vertex : dag) {
            String parentId = vertex.parentId;
            vertex.subTree=getSubTree(list,vertex.str);
            if (parentId != null) {
                Vertex pnode = nodeMap.get(parentId);
                if (pnode == null) {
                    continue;
                }
                List<Vertex> children = pnode.children;
                if (children == null) {
                    children = Lists.newArrayList();
                    pnode.children = children;
                }
                children.add(vertex);
            }
        }
        return rootList;
    }

    public String parsePlan(String planString) throws IOException {
        Pattern operator = Pattern.compile("\\[[^\\[]*\\]");  //obtain name fragment in []
        Pattern info = Pattern.compile("(\\([^\\)]*\\))");   //obtain rows and width in ()
        Pattern vertex=Pattern.compile("(Map|Reducer) [0-9]+");   //obtain vertex with Map/Reducer + num
        boolean plan_start = false;
        boolean vertex_start = false;
        ParserState state = ParserState.BEGIN;
        ParserState prevState = state;
        List<Node> li = Lists.newArrayList();
        List<Vertex> dag = Lists.newArrayList();
        Map<String, Vertex> vertexMap = Maps.newHashMap();
        int indention = 0;
        String []lines = planString.split("\n");
        for (String line : lines) {
            String tr = line.trim();
            if (!tr.equals("")) {
                indention = getIndentation(line);
                if (tr.startsWith("<-")) {
                    indention = indention + 2;
                    tr = tr.substring(2);
                }
            } else {
                continue;
            }
            if (!plan_start && (tr.startsWith("Map") || tr.startsWith("Reducer"))) {
                Matcher m = vertex.matcher(tr);
                boolean fatherV = true;
                Vertex father = null;
                while (m.find()) {
                    String id = m.group();
                    Vertex v = null;
                    if (vertexMap.get(id) == null) {
                        v = new Vertex(id);
                        dag.add(v);
                        vertexMap.put(id, v);
                    } else {
                        v = vertexMap.get(id);
                    }
                    if (fatherV) {
                        father = v;
                        fatherV = false;
                    } else {
                        v.parentId = father.str;
                    }
                }
            }
            if (tr.equals("Stage-0")) {
                plan_start = true;
            }
            if (!plan_start) {
                continue;
            }
            if ((tr.startsWith("Reducer") || tr.startsWith("Map")) && !vertex_start) {
                vertex_start = true;
                String id;
                if (!tr.startsWith("Map Join") && tr.startsWith("Map")) {
                    id = "Map " + tr.split("Map")[1].split(" ")[1];
                } else {
                    id = "Reducer " + tr.split("Reducer")[1].split(" ")[1];
                }
                li.add(new Node(id, indention));
            }
            if (!vertex_start) {
                continue;
            }
            state = nextState(tr, state);
            if (state != prevState) {
                Matcher m = operator.matcher(tr);
                while (m.find()) {
                    String id;
                    if (!tr.startsWith("Map Join") && tr.startsWith("Map")) {
                        id = "Map " + tr.split("Map")[1].split(" ")[1];
                    } else if (tr.startsWith("Reducer")) {
                        id = "Reducer " + tr.split("Reducer")[1].split(" ")[1];
                    } else {
                        id = m.group();
                    }
                    String cur_info = null;
                    Matcher m2 = info.matcher(tr);
                    while (m2.find()) {
                        cur_info = m2.group();
                    }
                    if (li.size() == 0) {
                        li.add(new Node(id, indention));
                    } else if (state == ParserState.FILE_OUTPUT_OPERATOR || state == ParserState.SHUFFLE) {
                        li.add(new Node(id, indention + 1));
                        li.get(li.size() - 1).parentId = li.get(li.size() - 2).str;
                    } else if (indention > li.get(li.size() - 1).ind) {
                        li.add(new Node(id, indention));
                        li.get(li.size() - 1).parentId = li.get(li.size() - 2).str;
                    } else if (indention == li.get(li.size() - 1).ind) {
                        li.add(new Node(id, indention));
                        li.get(li.size() - 1).parentId = li.get(li.size() - 2).parentId;
                    } else if (indention < li.get(li.size() - 1).ind) {
                        int pointer = li.size() - 1;
                        while (pointer >= 0 && indention < li.get(pointer).ind) {
                            pointer--;
                        }
                        li.add(new Node(id, indention));
                        li.get(li.size() - 1).parentId = li.get(pointer).parentId;
                    }
                    if (cur_info != null) {
                        li.get(li.size() - 1).rows = Integer.parseInt(cur_info.split(" ")[0].split("=")[1]);
                        int pointer=li.size() - 2;
                        while(pointer>=0&&li.get(pointer).rows==0) {
                            li.get(pointer).rows = li.get(li.size() - 1).rows;
                            pointer--;
                        }
                        li.get(li.size() - 1).width = Integer.parseInt(cur_info.split(" ")[1].split("=")[1].split("\\)")[0]);
                    }
                }
            }
            prevState = state;
        }
        List<Node> root = getTree(li);
        dag = getDAG(dag,root);
        return JSON.toJSONString(dag, true);
    }
    public static List<Node> getSubTree(List<Node> li, String require) throws IOException {
        Queue<Node> queue = new LinkedList<>();
        List<Node> subTree=Lists.newArrayList();
        queue.offer(li.get(0));
        Node current=queue.peek();
        assert current != null;
        while (current!=null&&!current.str.equals(require)){
            if (current.children!=null) {
                for (int i = 0; i < current.children.size(); i++) {
                    queue.offer(current.children.get(i));
                }
            }
            current=queue.poll();
        }
        if(current == null){
            return subTree;
        }
        Stack<Node> stack = new Stack<>();
        stack.push(current.children.get(0));
        while (!stack.isEmpty()){
            current=stack.pop();
            if(subTree.size()==0) {
                subTree.add(new Node(current.str, current.ind));
                subTree.get(subTree.size()-1).rows=current.rows;
                subTree.get(subTree.size()-1).width=current.width;
            }else{
                subTree.add(new Node(current.str, current.ind));
                subTree.get(subTree.size()-1).parentId=current.parentId;
                subTree.get(subTree.size()-1).rows=current.rows;
                subTree.get(subTree.size()-1).width=current.width;

            }
            if (current.children!=null) {
                for (int i = 0; i < current.children.size(); i++) {
                    if (!current.children.get(i).str.startsWith("Reducer")&&!current.children.get(i).str.startsWith("Map")) {
                        stack.push(current.children.get(i));
                    }else{
                        Node temp=new Node(current.children.get(i).str, current.children.get(i).ind);
                        temp.parentId=current.children.get(i).parentId;
                        temp.rows=current.children.get(i).rows;
                        temp.width=current.children.get(i).width;
                        stack.push(temp);
                    }
                }
            }
        }
        List<Node> temp=getTree(subTree);
        return temp;
    }

    private int getIndentation(String s) {
        for(int i = 0; i < s.length(); i++) {
            if (s.charAt(i) != ' ') {
                return i - 1;
            }
        }
        return s.length();
    }

    private ParserState nextState(String tr, ParserState state) {
        if (tr.startsWith("File Output Operator")) {
            return ParserState.FILE_OUTPUT_OPERATOR;
        } else if (tr.startsWith("Reducer")) {
            return ParserState.REDUCER;
        } else if (tr.startsWith("Move Operator")) {
            return ParserState.MOVE;
        } else if (tr.startsWith("TableScan")) {
            return ParserState.TABLE_SCAN;
        } else if (tr.startsWith("Filter Operator")) {
            return ParserState.FILTER;
        } else if (tr.startsWith("Map Join Operator")) {
            return ParserState.MAPJOIN;
        } else if (tr.startsWith("Map")) {
            return ParserState.MAP;
        } else if (tr.startsWith("Merge Join Operator")) {
            return ParserState.MERGEJOIN;
        } else if (tr.startsWith("Select Operator")) {
            return ParserState.SELECT;
        } else if (tr.startsWith("Group By Operator")) {
            return ParserState.GROUPBY;
        } else if (tr.startsWith("Partition") && state == ParserState.PARTITION_LIST) {
            return ParserState.PARTITION;
        } else if (tr.startsWith("Create Table Operator")) {
            return ParserState.CREATE;
        }else if (tr.startsWith("SHUFFLE")||tr.startsWith("PARTITION_ONLY_SHUFFLE")||tr.startsWith("BROADCAST")) {
            return ParserState.SHUFFLE;
        }
        return state;
    }
}

class Vertex {
    @JSONField(ordinal = 1)
    String str;
    @JSONField(ordinal = 2)
    String parentId;
    @JSONField(ordinal = 3)
    List<Vertex> children;
    @JSONField(ordinal = 4)
    List<Node> subTree;
    public Vertex(String str) {
        this.str = str;
    }

    public String getStr() {
        return str;
    }

    public String getParentId() {
        return parentId;
    }

    public List<Node> getSubTree() {
        return subTree;
    }

    public List<Vertex> getChildren() {
        return children;
    }
}
class Node {
    public Node(String str, int ind) {
        this.str = str;
        this.ind = ind;
    }

    public Node(String str, String parentId, int ind, int rows, int width) {
        this.str = str;
        this.parentId = parentId;
        this.ind = ind;
        this.rows = rows;
        this.width = width;
    }

    @JSONField(ordinal = 1)
    String str;
    @JSONField(ordinal = 2)
    String parentId;
    @JSONField(ordinal = 3)
    int ind;
    @JSONField(ordinal = 4)
    int rows;
    @JSONField(ordinal = 5)
    int width;
    @JSONField(ordinal = 6)
    List<Node> children;
    public String getStr() {
        return str;
    }

    public int getRows() {
        return rows;
    }

    public int getWidth() {
        return width;
    }

    public int getInd() {
        return ind;
    }

    public String getParentId() {
        return parentId;
    }

    public List<Node> getChildren() {
        return children;
    }

}

