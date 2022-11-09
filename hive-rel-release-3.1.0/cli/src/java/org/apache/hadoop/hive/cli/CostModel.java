package org.apache.hadoop.hive.cli;


public class CostModel {


    static long getVertexInputCount(String vertexName) {
        if (!CliDriver.dependencyMap.containsKey(vertexName)) {
            return 0;
        }
        long count = 0;
        for (String child: CliDriver.dependencyMap.get(vertexName)) {
//            System.out.println(child + ", " +Main.vertexMap.get(child).subTree.get(0).rows);
            count += CliDriver.vertexMap.get(child).subTree.get(0).rows;
        }
        return count;
    }


    static void getRowStatics(Node node, long[] rowNums) {
        if (node.str.startsWith("[MAPJOIN")) {
            rowNums[0] += node.rows; // rowNums[0]: MAPJOIN
//            rowNums[0] += node.children.get(0).rows+node.children.get(1).rows;
        } else if (node.str.startsWith("[MERGEJOIN")) {
            rowNums[1] += node.rows; //rowNums[1]: MERGEJOIN
            rowNums[6] += node.children.get(0).rows + node.children.get(1).rows; //rowNums[6]: SORT
        } else if (node.str.startsWith("[SEL")) {
            rowNums[2] += node.rows; //rowNums[2]: SEL
//            rowNums[2] += node.children.get(0).rows; //rowNums[2]: SEL
        } else if (node.str.startsWith("[GBY")) { //rowNums[3]: GBY
            rowNums[3] += node.rows; //rowNums[2]: SEL
//            rowNums[3] += node.children.get(0).rows;
        } else if (node.str.startsWith("[FIL")) { //rowNums[4]: FIL
            rowNums[4] += node.rows; //rowNums[2]: SEL
//            rowNums[4] += node.children.get(0).rows;
        } else if (node.str.startsWith("[RS") | node.str.startsWith("[FS")) { //rowNums[4]: FIL
            rowNums[5] += node.rows; //rowNums[2]: SEL
//            rowNums[5] += node.rows;
        } else if (node.str.startsWith("[TS")) { //rowNums[4]: FIL
            rowNums[8] += node.rows;
        }
    }

    static double estimateCPUTime(long[] rowNums){
        double[] ks = new double[9];
        double[] bs = new double[9];
        ks[0] = 0.0005;
        ks[1] = 0.0007;
        ks[2] = 0.0001;
        ks[3] = 0.00065;
        ks[4] = 0.0004;
        ks[5] = 0;
        ks[6] = 0.00145;
        ks[7] = 0;
        ks[8] = 0.00075;


        bs[0] = 600;
        bs[1] = 400;
        bs[2] = 0;
        bs[3] = 0;
        bs[4] = 0;
        bs[5] = 0;
        bs[6] = 0;
        bs[7] = 0;
        bs[8] = 0;


        double cost = 2000;
        for (int i = 0; i < rowNums.length; i++) {
            cost += ks[i] * rowNums[i] + bs[i];
        }
        return cost;
    }


    static double estimateGPUTime(long[] rowNums){
        double[] ks = new double[9];
        double[] bs = new double[9];
        ks[0] = 0.0004;
        ks[1] = 0.0004;
        ks[2] = 0.00001;
        ks[3] = 0.00045;
        ks[4] = 0.0002;
        ks[5] = 0.0001;
        ks[6] = 0;
        ks[7] = 0.0007;
        ks[8] = 0.00075;


        bs[0] = 0;
        bs[1] = 0;
        bs[2] = 0;
        bs[3] = 0;
        bs[4] = 0;
        bs[5] = 0;
        bs[6] = 0;
        bs[7] = 0;
        bs[8] = 0;


        double cost = 4900;
        for (int i = 0; i < rowNums.length; i++) {
            cost += ks[i] * rowNums[i] + bs[i];
        }
        return cost;
    }


    static void traverseAndGetStat(Node root, long[]rowNums){
        getRowStatics(root, rowNums);
        if (root.children!=null) {
            for (Node node : root.children) {
                traverseAndGetStat(node, rowNums);
            }
        }
    }


    static double traverseAndEstimate(Node root, String vertexName, boolean isGPU){
        long[] rowNums = new long[9];
        rowNums[7] = getVertexInputCount(vertexName);
        traverseAndGetStat(root, rowNums);
//        System.out.println(rowNums[0] + "," + rowNums[1] + "," + rowNums[2] + "," + rowNums[3] + "," + rowNums[4] + "," + rowNums[5] + "," + rowNums[6] + "," + rowNums[7] + "," + rowNums[8]);
        if (isGPU) {
            return estimateGPUTime(rowNums);
        } else {
            return estimateCPUTime(rowNums);
        }
    }

}
