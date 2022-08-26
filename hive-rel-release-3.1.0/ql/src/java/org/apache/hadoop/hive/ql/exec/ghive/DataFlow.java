package org.apache.hadoop.hive.ql.exec.ghive;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.compress.ZStandardCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.*;


@Deprecated
public class DataFlow {

    private static final Logger LOG = LoggerFactory.getLogger(DataFlow.class.getName());

    private static DataTransAdapter dataTransAdapter;

    public String vertexName;
    protected ArrayList<Long>[] longColumns;
    protected ArrayList<Double>[] doubleColumns;
    protected ArrayList<String>[] stringColumns;
    protected ArrayList<Integer>[] intColumns;
    protected int longColumnCnt = 0;
    protected int doubleColumnCnt = 0;

    protected int stringColumnCnt = 0;
    protected int intColumnCnt = 0;

    private int longColIdx;
    private int doubleColIdx;
    private int intColIdx;
    private int stringColIdx;

    protected ArrayList<Integer> type = new ArrayList<>(); // 0: long, 1: double, 2: bytes
    protected int keyCnt;
    protected boolean initialized;
    private boolean setKeyCnt;
    protected int rowCnt;

    public static void readyTrans() {
        dataTransAdapter = new DataTransAdapter(InfoCollector.getAllDataFlows());
    }
    public DataFlow(String vertexName) {
        initialized = false;
        this.vertexName = vertexName;
        LOG.info("Initialing the DataFlow for the vertex: " + vertexName);
    }

    // get number of long / double columns.
    public void init(ObjectInspector oi) {
        switch (oi.getCategory()) {
            case PRIMITIVE: {
                PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
                switch (poi.getPrimitiveCategory()) {
                    case LONG: {
                        longColumnCnt++;
                        type.add(0);
                        break;
                    }
                    case DOUBLE: {
                        doubleColumnCnt++;
                        type.add(1);
                        break;
                    }
                    case INT: {
                        intColumnCnt++;
                        type.add(2);
                        break;
                    }
                    case STRING: {
                        stringColumnCnt++;
                        type.add(3);
                    }
                }
                break;
            }
            case STRUCT: {
                StructObjectInspector soi = (StructObjectInspector) oi;
                List<? extends StructField> structFields = soi.getAllStructFieldRefs();
                for (StructField sf : structFields) {
                    init(sf.getFieldObjectInspector());
                }
                break;
            }
        }
    }

    public void init(VectorizedRowBatch batch) {
        for (int i = 0; i < batch.projectionSize; i++) {
            ColumnVector column = batch.cols[batch.projectedColumns[i]];
            if (column.type == ColumnVector.Type.LONG) {
                longColumnCnt++;
                type.add(0);
            } else if (column.type == ColumnVector.Type.DOUBLE) {
                doubleColumnCnt++;
                type.add(1);
            }else if (column.type==ColumnVector.Type.BYTES){
                stringColumnCnt++;
                type.add(3);
            }

            else {
                System.out.println("GHive ERROR: unrecognized column instance of " + column.type);
            }
        }
    }

    @SuppressWarnings("unchecked")
    void postInit() {
        longColumns = new ArrayList[longColumnCnt];
        doubleColumns = new ArrayList[doubleColumnCnt];
        intColumns = new ArrayList[intColumnCnt];
        stringColumns = new ArrayList[stringColumnCnt];
        for (int i = 0; i < doubleColumns.length; i++) {
            doubleColumns[i] = new ArrayList<>();
        }
        for (int i = 0; i < longColumns.length; i++) {
            longColumns[i] = new ArrayList<>();
        }
        for (int i = 0; i < intColumns.length; i++) {
            intColumns[i] = new ArrayList<>();
        }
        for (int i = 0; i < stringColumns.length; i++) {
            stringColumns[i] = new ArrayList<>();
        }
        System.out.println("haotian-type-length: " + type.size());
    }

    public void feedRow(Object row, ObjectInspector oi) {
        //myLogA(row.toString() + " " + oi.getTypeName());
        if (!initialized) {
            init(oi);
            postInit();
            initialized = true;
        }
        longColIdx = 0;
        doubleColIdx = 0;
        intColIdx = 0;
        stringColIdx = 0;
        feedRowInternal(row, oi);
        rowCnt++;
    }

    private void feedRowInternal(Object o, ObjectInspector oi) {
        switch (oi.getCategory()) {
            case PRIMITIVE: {
                PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
                switch (poi.getPrimitiveCategory()) {
                    case LONG: {
                        if (o==null){
                            longColumns[longColIdx++].add(null);
                        }
                        else
                            longColumns[longColIdx++].add(((LongObjectInspector) poi).get(o));
                        break;
                    }
                    case DOUBLE: {
                        doubleColumns[doubleColIdx++].add(((DoubleObjectInspector) poi).get(o));
                        break;
                    }
                    case INT: {
                        intColumns[intColIdx++].add(((IntObjectInspector) poi).get(o));
                        break;
                    }
                    case STRING: {
                        stringColumns[stringColIdx++].add(((StringObjectInspector) poi).getPrimitiveJavaObject(o));
                    }
                }
                break;
            }
            case STRUCT: {
                StructObjectInspector soi = (StructObjectInspector) oi;
                List<? extends StructField> structFields = soi.getAllStructFieldRefs();
                for (StructField structField : structFields) {
                    feedRowInternal(soi.getStructFieldData(o, structField), structField.getFieldObjectInspector());
                }
                break;
            }
        }
    }

    public void feedRow(VectorizedRowBatch batch) {
        //myLogB(batch.toString());
        if (!initialized) {
            init(batch);
            postInit();
            initialized = true;
        }
        int longColIdx = 0;
        int doubleColIdx = 0;
        int stringColIdx = 0;
        for (int i = 0; i < batch.projectionSize; i++) {
            ColumnVector column = batch.cols[batch.projectedColumns[i]];
            switch (column.type) {
                case LONG: {
                    LongColumnVector longColumnVector = (LongColumnVector) column;
                    long[] longCol = longColumnVector.vector;
                    for (int j = 0; j < batch.size; j++) {
                        if (!longColumnVector.isRepeating) {
                            longColumns[longColIdx].add(longCol[j]);
                        } else {
                            longColumns[longColIdx].add(longCol[0]);
                        }
                    }
                    longColIdx++;
                    break;
                }
                case DOUBLE: {
                    DoubleColumnVector doubleColumnVector = (DoubleColumnVector) column;
                    double[] doubleCol = doubleColumnVector.vector;
                    for (int j = 0; j < batch.size; j++) {
                        if (!doubleColumnVector.isRepeating) {
                            doubleColumns[doubleColIdx].add(doubleCol[j]);
                        } else {
                            doubleColumns[doubleColIdx].add(doubleCol[0]);
                        }
                    }
                    doubleColIdx++;
                    break;
                }

                case  BYTES: {
                    BytesColumnVector bytesColumnVector = (BytesColumnVector) column;
                    byte[][] bytesCol = bytesColumnVector.vector;

                    int[] length=bytesColumnVector.length;
                    byte[][] bytesCol_length=new byte[bytesCol.length][];
                    //batch.size ==> number of rows
                    for (int j = 0; j < batch.size; j++) {
                        int each_length=length[j];
                        bytesCol_length[j]=new byte[each_length];
                        for (int k=0;k<each_length;k++){
                            bytesCol_length[j][k]=bytesCol[j][bytesColumnVector.start[j]+k];
                        }
                        if (!bytesColumnVector.isRepeating) {
                            String s = new String(bytesCol_length[j]);
                            stringColumns[stringColIdx].add(s);
                        } else {
                            String s = new String(bytesCol_length[0]);
                            stringColumns[stringColIdx].add(s);
                        }
                    }
                    stringColIdx++;
                    break;
                }

            }
        }
        rowCnt += batch.size;
    }

    public static GPUResult process() {
        DataTransAdapter dataTransAdapter = new DataTransAdapter(InfoCollector.getAllDataFlows());
        Gson gson = new Gson();
//            Type gsonType = new TypeToken<HashMap<String, Map<Byte, List<Integer>>>>(){}.getType();
//            gsonString = gson.toJson(InfoCollector.retainListMap, gsonType);
        Type gsonType = new TypeToken<HashMap<String, Map<Byte, List<String>>>>() {
        }.getType();
        String gsonString = gson.toJson(InfoCollector.maintainValuesMap, gsonType);
        System.out.println("haotian-maintainValueJsonString: " + gsonString);
        System.out.println("Profiling: GHive 'JNI' starting at time " + System.currentTimeMillis() + " ms");
        System.out.println(("datatranstest: " + dataTransAdapter));
        System.out.println("java end!");
        return Operator.GPUProcess(InfoCollector.getVertexName(), dataTransAdapter.longCols,
                dataTransAdapter.doubleCols, dataTransAdapter.intCols, dataTransAdapter.stringCols,
                dataTransAdapter.vertexName, dataTransAdapter.sequence,
                dataTransAdapter.longColumnCnt, dataTransAdapter.doubleColumnCnt,
                dataTransAdapter.intColumnCnt, dataTransAdapter.stringColumnCnt,
                dataTransAdapter.keyCnt, dataTransAdapter.rowCnt, gsonString);
//    return Operator.GPUProcess(InfoCollector.getVertexName(), dataTransAdapter.longCols,
//        dataTransAdapter.doubleCols, dataTransAdapter.vertexName, dataTransAdapter.sequence,
//        dataTransAdapter.longColumnCnt, dataTransAdapter.doubleColumnCnt,
//        dataTransAdapter.keyCnt, dataTransAdapter.rowCnt);
    }

    public static class DataTransAdapter {
        private final long[][] longCols;
        private final double[][] doubleCols;
        private final int[][] intCols;
        private final String[][] stringCols;
        private final String[] vertexName;
        private final int[][] sequence;
        int[] longColumnCnt;
        int[] doubleColumnCnt;
        int[] intColumnCnt;
        int[] stringColumnCnt;
        int[] keyCnt;
        int[] rowCnt;

        DataTransAdapter(ArrayList<DataFlow> dataFlows) {
            int totalLongColCnt = 0;
            int totalDoubleColCnt = 0;
            int totalIntColCnt = 0;
            int totalStringColCnt = 0;
            for (DataFlow df : dataFlows) {
                totalLongColCnt += df.longColumnCnt;
                totalDoubleColCnt += df.doubleColumnCnt;
                totalIntColCnt += df.intColumnCnt;
                totalStringColCnt += df.stringColumnCnt;
            }

            // init each
            longCols = new long[totalLongColCnt][];
            doubleCols = new double[totalDoubleColCnt][];
            intCols = new int[totalIntColCnt][];
            stringCols = new String[totalStringColCnt][];
            vertexName = new String[dataFlows.size()];
            sequence = new int[dataFlows.size()][];
            keyCnt = new int[dataFlows.size()];
            rowCnt = new int[dataFlows.size()];
            longColumnCnt = new int[dataFlows.size()];
            doubleColumnCnt = new int[dataFlows.size()];
            intColumnCnt = new int[dataFlows.size()];
            stringColumnCnt = new int[dataFlows.size()];

            int longCopyIdx = 0;
            int doubleCopyIdx = 0;
            int intCopyIdx = 0;
            int stringCopyIdx = 0;

            for (int i = 0; i < dataFlows.size(); i++) {
                DataFlow df = dataFlows.get(i);
//                System.out.println("haotian-rowCnt: " + df.rowCnt);
//                System.out.println("haotian-thisVertex: " + InfoCollector.getVertexName());
//                System.out.println("haotian-doubleColumnCnt: " + Arrays.toString(doubleColumnCnt));
//                System.out.println("haotian-df" + df.toString());
                if (df.rowCnt == 0) {
                    continue;
                }
                InfoCollector.hasData = true;
                if (df.longColumnCnt != 0) {
                    for (ArrayList<Long> eachLongCols : df.longColumns) {
                        longCols[longCopyIdx] = new long[eachLongCols.size()];
                        for (int j = 0; j < eachLongCols.size(); j++) {
                            //todo: null!
                            longCols[longCopyIdx][j] = eachLongCols.get(j);
                        }
                        longCopyIdx++;
                    }
                }


                if (df.doubleColumnCnt != 0) {
                    for (ArrayList<Double> eachDoubleCols : df.doubleColumns) {
                        doubleCols[doubleCopyIdx] = new double[eachDoubleCols.size()];
                        for (int j = 0; j < eachDoubleCols.size(); j++) {
                            doubleCols[doubleCopyIdx][j] = eachDoubleCols.get(j);
                        }
                        doubleCopyIdx++;
                    }
                }

                if (df.intColumnCnt != 0) {
                    for (ArrayList<Integer> eachIntCols : df.intColumns) {
                        intCols[intCopyIdx] = new int[eachIntCols.size()];
                        for (int j = 0; j < eachIntCols.size(); j++) {
                            intCols[intCopyIdx][j] = eachIntCols.get(j);
                        }
                        intCopyIdx++;
                    }
                }

                if (df.stringColumnCnt != 0) {
                    for (ArrayList<String> eachStringCols : df.stringColumns) {
                        stringCols[stringCopyIdx] = new String[eachStringCols.size()];
                        for (int j = 0; j < eachStringCols.size(); j++) {
                            stringCols[stringCopyIdx][j] = eachStringCols.get(j);
                        }
                        stringCopyIdx++;
                    }
                }


                vertexName[i] = df.vertexName;

                int longSeq = 0;
                int doubleSeq = df.longColumnCnt;
                int intSeq = doubleSeq + df.doubleColumnCnt;
                int stringSeq = intSeq + df.intColumnCnt;
                int[] eachSeq = new int[df.type.size()];
                for (int j = 0; j < df.type.size(); j++) {
                    switch (df.type.get(j)) {
                        case 0: {
                            eachSeq[j] = longSeq++;
                            break;
                        }
                        case 1: {
                            eachSeq[j] = doubleSeq++;
                            break;
                        }
                        case 2: {
                            eachSeq[j] = intSeq++;
                            break;
                        }
                        case 3: {
                            eachSeq[j] = stringSeq++;
                            break;
                        }
                    }
                }
                sequence[i] = eachSeq;
                rowCnt[i] = df.rowCnt;
                keyCnt[i] = df.keyCnt;
                longColumnCnt[i] = df.longColumnCnt;
                doubleColumnCnt[i] = df.doubleColumnCnt;
                intColumnCnt[i] = df.intColumnCnt;
                stringColumnCnt[i] = df.stringColumnCnt;
                System.out.println("haotian-dataAdapter: ");
                System.out.println("vertex: " + df.vertexName);
                System.out.println("eachSeqLen: " + df.type.size());
            }
        }

        public String toString() {
            return //"dataTransAdapter.longCols: " + Arrays.deepToString(dataTransAdapter.longCols) + "\n" +
                    //"dataTransAdapter.doubleCols: " + Arrays.deepToString(dataTransAdapter.doubleCols) + "\n" +
                    "vertex name: " + Arrays.toString(vertexName) + " " +
                            "sequence: " + Arrays.deepToString(sequence) + " " +
                            "long column cnt: " + Arrays.toString(longColumnCnt) + " " +
                            "double column cnt: " + Arrays.toString(doubleColumnCnt) + " " +
                            "int column cnt: " + Arrays.toString(intColumnCnt) + " " +
                            "string column cnt: " + Arrays.toString(stringColumnCnt) + " " +
                            "key cnt: " + Arrays.toString(keyCnt) + " " +
                            "row cnt: " + Arrays.toString(rowCnt) + " ";
        }

    }

    public ArrayList<Integer> getType() {
        return type;
    }

    public boolean isSetKeyCnt() {
        return setKeyCnt;
    }

    public void setKeyCnt(int keyCnt) {
        this.keyCnt = keyCnt;
        setKeyCnt = true;
    }

    public boolean isInitialized() {
        return initialized;
    }

    @Override
    @SuppressWarnings("all")
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int longIdx = 0;
        int doubleIdx = 0;

        sb.append("vertex name: ").append(vertexName).append("\n");
        if (longColumnCnt > 0) {
            for (int i = 0; i < longColumns.length; i++) {
                sb.append("Long " + i + ": ");
                sb.append(longColumns[i].toString());
                sb.append("\n");
            }
        } else {
            sb.append("no long column");
        }

        if (doubleColumnCnt > 0) {
            for (int i = 0; i < doubleColumns.length; i++) {
                sb.append("Double " + i + ": ");
                sb.append(doubleColumns[i].toString());
                sb.append("\n");
            }
        } else {
            sb.append("no double column");
        }

        if (type == null) {
            sb.append("type: null");
        } else {
            for (int i = 0; i < type.size(); i++) {
                sb.append("type [" + i + "]: " + type.get(i) + "; ");
            }
        }
        sb.append("\n");
        return sb.toString();
    }

    //    private static int logNumA = 0;
    //    private static int logNumB = 0;
    //
    //    private void myLogA(String s) {
    //        if (logNumA++ < 25) LOG.info("Dfloga:" + s);
    //    }
    //
    //    private void myLogB(String s) {
    //        if (logNumB++ < 25) LOG.info("Dflogb:" + s);
    //    }
}