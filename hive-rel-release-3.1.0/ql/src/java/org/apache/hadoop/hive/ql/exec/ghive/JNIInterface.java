package org.apache.hadoop.hive.ql.exec.ghive;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JNIInterface {

    static {
        if (InfoCollector.isGPU) {
            try {
                System.load("/tmp/org_apache_hadoop_hive_ql_exec_Operator.so");
                // invoke dynamic link library
            } catch (Exception e) {
                System.err.println("load native library [operator] failed.");
            }
            System.out.println("load native library [operator] succeed.");
        }
    }

    public static GTables input;

    public static native GPUResult GPUProcess(GTables gpuInput);

    public static void readyInputs() {
        input = new GTables(InfoCollector.allInputs);
        Gson gson = new Gson();
        Type gsonType = new TypeToken<HashMap<String, Map<Byte, List<String>>>>() {}.getType();
        input.maintainValuesJsonString = gson.toJson(InfoCollector.maintainValuesMap, gsonType);
        input.thisVertexName = InfoCollector.getVertexName();
    }

    public static GPUResult process() {
        return GPUProcess(input);
    }
}
