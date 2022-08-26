package org.apache.hadoop.hive.ql.exec.ghive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InfoCollector {

    public static Map<String, Map<Byte, List<Integer>>> retainListMap = new HashMap<>();

    public static Map<String, Map<Byte, List<String>>> maintainValuesMap = new HashMap<>();

    public static org.apache.hadoop.hive.ql.exec.ghive.DataFlow[] recordProcessorDataFlows;

    public static GTable[] recordProcessorInput;
    public static ArrayList<GTable> allInputs;

    public static boolean hasData = false;

    private static String vertexName;
    private static ArrayList<org.apache.hadoop.hive.ql.exec.ghive.DataFlow> allDataFlows;
    public static boolean isGPU = true;


    public static void setVertexName(String vertexName) {
        InfoCollector.vertexName = vertexName;
    }

    public static String getVertexName() {
        return vertexName;
    }

    public static ArrayList<org.apache.hadoop.hive.ql.exec.ghive.DataFlow> getAllDataFlows() {
        return allDataFlows;
    }

    public static void addDataFlow(org.apache.hadoop.hive.ql.exec.ghive.DataFlow dataFlow) {
        allDataFlows.add(dataFlow);
    }

    public static void fresh(String vName) {
        vertexName = vName;
        hasData = false;
        recordProcessorDataFlows = null;
        recordProcessorInput = null;
        allDataFlows = new ArrayList<>();
        allInputs = new ArrayList<>();
    }
}
