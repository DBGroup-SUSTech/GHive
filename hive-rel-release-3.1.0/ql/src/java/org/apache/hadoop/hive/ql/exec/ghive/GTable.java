package org.apache.hadoop.hive.ql.exec.ghive;

import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


/**
 * GPUInput for each vertex.
 */
public class GTable {

    private static final Logger LOG = LoggerFactory.getLogger(GTable.class.getName());
    public String vertexName;
    public ByteBuffer[] longCols; // 0
    public ByteBuffer[] doubleCols; // 1
    public ByteBuffer[] intCols; // 2
    public ByteBuffer[] stringCols; // 3
    public ByteBuffer[] stringIdxCols;

    // Bitmap -> preserved value as an optimization for data transferring and calculation.
    // It is not safe to use in production currently if such preserved values are meaningful.
    // The bitmap implementation can refer to the stringIdxCols implemented for string.
    private static final long LONG_NULL = Long.MAX_VALUE;
    private static final int INT_NULL = Integer.MAX_VALUE;
    private static final double DOUBLE_NULL = Double.NaN;

    public int[] columnCnts = new int[4];
    public ArrayList<Integer> types;

    public int keyCnt;
    public int rowCnt;

    public boolean initialized;
    public boolean setKeyCnt;

    private int longIdx;
    private int doubleIdx;
    private int intIdx;
    private int stringIdx;

    public GTable(String vertexName) {
        initialized = false;
        this.vertexName = vertexName;
        types = new ArrayList<>();
        LOG.info("Initializing GPUInput for the vertex: " + vertexName);
    }

    // get number of long / double / int / String Columns.
    // works for both key and value.
    public void getMeta(ObjectInspector oi) {
        switch (oi.getCategory()) {
            case PRIMITIVE: {
                PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
                switch (poi.getPrimitiveCategory()) {
                    case LONG: {
                        columnCnts[0]++;
                        types.add(0);
                        break;
                    }
                    case DOUBLE: {
                        columnCnts[1]++;
                        types.add(1);
                        break;
                    }
                    case INT: {
                        columnCnts[2]++;
                        types.add(2);
                        break;
                    }
                    case STRING: {
                        columnCnts[3]++;
                        types.add(3);
                        break;
                    }
                    default: {
                        LOG.error("GHive Error: Only support long / double / string / int currently");
                        break;
                    }
                }
                break;
            }
            case STRUCT: {
                StructObjectInspector soi = (StructObjectInspector) oi;
                List<? extends StructField> structFields = soi.getAllStructFieldRefs();
                for (StructField sf : structFields) {
                    getMeta(sf.getFieldObjectInspector());
                }
                break;
            }
        }
    }



    public void getMeta(VectorizedRowBatch batch) {
        for (int i = 0; i < batch.projectionSize; i++) {
            ColumnVector column = batch.cols[batch.projectedColumns[i]];
            if (column == null) {
                continue;
            }
            if (column.type == ColumnVector.Type.LONG) {
                columnCnts[0]++;
                types.add(0);
            } else if (column.type == ColumnVector.Type.DOUBLE) {
                columnCnts[1]++;
                types.add(1);
            } else if (column.type == ColumnVector.Type.BYTES) {
                columnCnts[3]++;
                types.add(3);
            } else {
                LOG.error("GHive Error: Only support long / double / string / int currently");
                LOG.error("GHive Error: unrecognized column instance of " + column.type);
            }
        }
    }


    // allocate space to store columns.
    void init() {
        longCols = new ByteBuffer[columnCnts[0]];
        doubleCols = new ByteBuffer[columnCnts[1]];
        intCols = new ByteBuffer[columnCnts[2]];
        stringCols = new ByteBuffer[columnCnts[3]];
        stringIdxCols = new ByteBuffer[columnCnts[3]];
        for (int i = 0; i < longCols.length; i ++) {
            longCols[i] = ByteBuffer.allocateDirect(1024);
            longCols[i].order(ByteOrder.LITTLE_ENDIAN);
        }
        for (int i = 0; i < doubleCols.length; i ++) {
            doubleCols[i] = ByteBuffer.allocateDirect(1024);
            doubleCols[i].order(ByteOrder.LITTLE_ENDIAN);
        }
        for (int i = 0; i < intCols.length; i ++) {
            intCols[i] = ByteBuffer.allocateDirect(1024);
            intCols[i].order(ByteOrder.LITTLE_ENDIAN);
        }
        for (int i = 0; i < stringCols.length; i ++) {
            stringCols[i] = ByteBuffer.allocateDirect(1024);
            stringIdxCols[i] = ByteBuffer.allocateDirect(1024);
            stringCols[i].order(ByteOrder.LITTLE_ENDIAN);
            stringIdxCols[i].order(ByteOrder.LITTLE_ENDIAN);
        }
        LOG.info("GHive Info: GPUInput for " + vertexName +
                " initialized with " + types.size() + " columns");
    }


    public void feedRow(Object row, ObjectInspector oi) {
        if (!initialized) {
            getMeta(oi);
            init();
            initialized = true;
        }
        longIdx = 0;
        doubleIdx = 0;
        intIdx = 0;
        stringIdx = 0;
        feedRowInternal(row, oi);
        rowCnt++;
    }


    private void feedRowInternal(Object o, ObjectInspector oi) {
        switch (oi.getCategory()) {
            case PRIMITIVE: {
                PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
                switch (poi.getPrimitiveCategory()) {
                    case LONG: {
                        if (longCols[longIdx].capacity() - longCols[longIdx].position() < 8) {
                            ByteBuffer newBuf = ByteBuffer.allocateDirect(2 * longCols[longIdx].capacity());
                            newBuf.order(ByteOrder.LITTLE_ENDIAN);
                            longCols[longIdx].flip();
                            newBuf.put(longCols[longIdx]);
                            SpaceManager.clean(longCols[longIdx]);
                            longCols[longIdx] = newBuf;
                        }
                        if (o == null) {
                            longCols[longIdx++].putLong(LONG_NULL);
                        } else {
                            longCols[longIdx++].putLong(((LongObjectInspector) poi).get(o));
                        }
                        break;
                    }
                    case DOUBLE: {
                        if (doubleCols[doubleIdx].capacity() - doubleCols[doubleIdx].position() < 8) {
                            ByteBuffer newBuf = ByteBuffer.allocateDirect(2 * doubleCols[doubleIdx].capacity());
                            newBuf.order(ByteOrder.LITTLE_ENDIAN);
                            doubleCols[doubleIdx].flip();
                            newBuf.put(doubleCols[doubleIdx]);
                            SpaceManager.clean(doubleCols[doubleIdx]);
                            doubleCols[doubleIdx] = newBuf;
                        }
                        if (o == null) {
                            doubleCols[doubleIdx++].putDouble(DOUBLE_NULL);
                        } else {
                            doubleCols[doubleIdx++].putDouble(((DoubleObjectInspector) poi).get(o));
                        }
                        break;
                    }
                    case INT: {
                        if (intCols[intIdx].capacity() - intCols[intIdx].position() < 4) {
                            ByteBuffer newBuf = ByteBuffer.allocateDirect(2 * intCols[intIdx].capacity());
                            newBuf.order(ByteOrder.LITTLE_ENDIAN);
                            intCols[intIdx].flip();
                            newBuf.put(intCols[intIdx]);
                            SpaceManager.clean(intCols[intIdx]);
                            intCols[intIdx] = newBuf;
                        }
                        if (o == null) {
                            intCols[intIdx++].putInt(INT_NULL);
                        } else {
                            intCols[intIdx++].putInt(((IntObjectInspector) poi).get(o));
                        }
                        break;
                    }
                    case STRING: {
                        if (stringIdxCols[stringIdx].capacity() - stringIdxCols[stringIdx].position() < 4) {
                            ByteBuffer newBuf = ByteBuffer.allocateDirect(2 * stringIdxCols[stringIdx].capacity());
                            newBuf.order(ByteOrder.LITTLE_ENDIAN);
                            stringIdxCols[stringIdx].flip();
                            newBuf.put(stringIdxCols[stringIdx]);
                            SpaceManager.clean(stringIdxCols[stringIdx]);
                            stringIdxCols[stringIdx] = newBuf;
                        }
                        if (o == null) {
                            stringIdxCols[stringIdx].putInt(-1);
                            stringIdxCols[stringIdx].putInt(-1);
                        } else {
                            int stringLength = ((StringObjectInspector) poi).getPrimitiveJavaObject(o).length();
                            if (stringCols[stringIdx].capacity() - stringCols[stringIdx].position() < stringLength) {
                                ByteBuffer newBuf = ByteBuffer.allocateDirect(2 * stringCols[stringIdx].capacity());
                                newBuf.order(ByteOrder.LITTLE_ENDIAN);
                                stringCols[stringIdx].flip();
                                newBuf.put(stringCols[stringIdx]);
                                SpaceManager.clean(stringCols[stringIdx]);
                                stringCols[stringIdx] = newBuf;
                            }
                            stringIdxCols[stringIdx].putInt(stringCols[stringIdx].position());
                            stringCols[stringIdx].put(((StringObjectInspector) poi).
                                    getPrimitiveJavaObject(o).getBytes(StandardCharsets.UTF_8));
                            stringIdxCols[stringIdx].putInt(stringCols[stringIdx].position());
                        }
                        stringIdx++;
                        break;
                    }
                    default: {
                        LOG.error("GHive Error: Only support long / double / string / int currently");
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

    public void feedBatch(VectorizedRowBatch batch) {
        if (!initialized) {
            getMeta(batch);
            init();
            initialized = true;
        }
        longIdx = 0;
        doubleIdx = 0;
        stringIdx = 0;

        for (int i = 0; i < batch.projectionSize; i++) {
            ColumnVector column = batch.cols[batch.projectedColumns[i]];
            if (column == null) {
                continue;
            }
            switch (column.type) {
                case LONG: {
                    LongColumnVector longColumnVector = (LongColumnVector) column;
                    long[] longCol = longColumnVector.vector;
                    for (int j = 0; j < batch.size; j++) {
                        if (longCols[longIdx].capacity() - longCols[longIdx].position() < 8) {
                            ByteBuffer newBuf = ByteBuffer.allocateDirect(2 * longCols[longIdx].capacity());
                            newBuf.order(ByteOrder.LITTLE_ENDIAN);
                            longCols[longIdx].flip();
                            newBuf.put(longCols[longIdx]);
                            SpaceManager.clean(longCols[longIdx]);
                            longCols[longIdx] = newBuf;
                        }
                        // TODO: How to distinguish null and 0?
                        if (longColumnVector.isNull[j]) {
                            longCols[longIdx].putLong(LONG_NULL);
//                            System.out.println("GHive-Warning: longCols[" + longIdx + "] is null");
                        }
                        else if (!longColumnVector.isRepeating) {
                            longCols[longIdx].putLong(longCol[j]);
                        } else {
                            longCols[longIdx].putLong(longCol[0]);
                        }
                    }
                    longIdx++;
                    break;
                }
                case DOUBLE: {
                    DoubleColumnVector doubleColumnVector = (DoubleColumnVector) column;
                    double[] doubleCol = doubleColumnVector.vector;
                    for (int j = 0; j < batch.size; j++) {
                        if (doubleCols[doubleIdx].capacity() - doubleCols[doubleIdx].position() < 8) {
                            ByteBuffer newBuf = ByteBuffer.allocateDirect(2 * doubleCols[doubleIdx].capacity());
                            newBuf.order(ByteOrder.LITTLE_ENDIAN);
                            doubleCols[doubleIdx].flip();
                            newBuf.put(doubleCols[doubleIdx]);
                            SpaceManager.clean(doubleCols[doubleIdx]);
                            doubleCols[doubleIdx] = newBuf;
                        }
                        // TODO: How to distinguish null and 0?
                        if (doubleColumnVector.isNull[j]) {
                            doubleCols[doubleIdx].putDouble(DOUBLE_NULL);
//                            System.out.println("GHive-Warning: doubleCols[" + doubleIdx + "] is null");
                        }
                        else if (!doubleColumnVector.isRepeating) {
                            doubleCols[doubleIdx].putDouble(doubleCol[j]);
                        } else {
                            doubleCols[doubleIdx].putDouble(doubleCol[0]);
                        }
                    }
                    doubleIdx ++;
                    break;
                }
                case BYTES: {
                    BytesColumnVector bytesColumnVector = (BytesColumnVector) column;
                    byte[][] bytesCol = bytesColumnVector.vector;
                    int[] length = bytesColumnVector.length;
                    byte[][] bytesColLength = new byte[bytesCol.length][];

                    for (int j = 0; j < batch.size; j++) {
                        int eachLength = length[j];
                        bytesColLength[j] = new byte[eachLength];
                        if (eachLength > 0) {
                            System.arraycopy(bytesCol[j], bytesColumnVector.start[j],
                                    bytesColLength[j], 0, eachLength);
                        }
                        if (stringIdxCols[stringIdx].capacity() - stringIdxCols[stringIdx].position() < 4) {
                            ByteBuffer newBuf = ByteBuffer.allocateDirect(2 * stringIdxCols[stringIdx].capacity());
                            newBuf.order(ByteOrder.LITTLE_ENDIAN);
                            stringIdxCols[stringIdx].flip();
                            newBuf.put(stringIdxCols[stringIdx]);
                            SpaceManager.clean(stringIdxCols[stringIdx]);
                            stringIdxCols[stringIdx] = newBuf;
                        }
                        if (bytesColumnVector.isNull[j]) {
//                            System.out.println("GHive Warning: string column with null item.");
                            stringIdxCols[stringIdx].putInt(-1);
                            stringIdxCols[stringIdx].putInt(-1);
                        }
                        else if (!bytesColumnVector.isRepeating) {
                            int stringLength = bytesColLength[j].length;
                            if (stringCols[stringIdx].capacity() - stringCols[stringIdx].position() < stringLength) {
                                ByteBuffer newBuf = ByteBuffer.allocateDirect(2 * stringCols[stringIdx].capacity());
                                newBuf.order(ByteOrder.LITTLE_ENDIAN);
                                stringCols[stringIdx].flip();
                                newBuf.put(stringCols[stringIdx]);
                                SpaceManager.clean(stringCols[stringIdx]);
                                stringCols[stringIdx] = newBuf;
                            }
                            stringIdxCols[stringIdx].putInt(stringCols[stringIdx].position());
                            stringCols[stringIdx].put(bytesColLength[j]);
                            stringIdxCols[stringIdx].putInt(stringCols[stringIdx].position());
                        } else {
                            int stringLength = bytesColLength[0].length;
                            if (stringCols[stringIdx].capacity() - stringCols[stringIdx].position() < stringLength) {
                                ByteBuffer newBuf = ByteBuffer.allocateDirect(2 * stringCols[stringIdx].capacity());
                                newBuf.order(ByteOrder.LITTLE_ENDIAN);
                                stringCols[stringIdx].flip();
                                newBuf.put(stringCols[stringIdx]);
                                SpaceManager.clean(stringCols[stringIdx]);
                                stringCols[stringIdx] = newBuf;
                            }
                            stringIdxCols[stringIdx].putInt(stringCols[stringIdx].position());
                            stringCols[stringIdx].put(bytesColLength[0]);
                            stringIdxCols[stringIdx].putInt(stringCols[stringIdx].position());
                        }
                    }
                    stringIdx ++;
                    break;
                }
            }
        }
        rowCnt += batch.size;
    }

    public boolean isSetKeyCnt() {
        return setKeyCnt;
    }

    public void setKeyCnt(int keyCnt) {
        this.keyCnt = keyCnt;
        setKeyCnt = true;
    }


    public String toString(int rowNumber) {
        StringBuilder sb = new StringBuilder();
        int longIdx = 0;
        int doubleIdx = 0;

        sb.append("vertex name: ").append(vertexName).append("\n");
        if (columnCnts[0] > 0) {
            for (int i = 0; i < longCols.length; i++) {
                sb.append("Long ").append(i).append("th : ");

                int rowNum=Math.min(rowCnt,rowNumber);
                sb.append("[");
                for (int j = 0; j < rowNum; j++) {
                    sb.append(longCols[i].getLong(j*8));
                    sb.append(", ");
                }
                sb.append("]");
                sb.append("\n");
            }
        } else {
            sb.append("no long column");
        }

        if (columnCnts[1] > 0) {
            for (int i = 0; i < doubleCols.length; i++) {
                sb.append("Double ").append(i).append("th : ");

                int rowNum=Math.min(rowCnt,rowNumber);
                sb.append("[");
                for (int j = 0; j < rowNum; j++) {
                    sb.append(doubleCols[i].getDouble(j*8));
                    sb.append(", ");
                }
                sb.append("]");
                sb.append("\n");
            }
        } else {
            sb.append("no double column");
        }

        if (columnCnts[2] > 0) {
            for (int i = 0; i < intCols.length; i++) {
                sb.append("Int ").append(i).append("th : ");

                int rowNum=Math.min(rowCnt,rowNumber);
                sb.append("[");
                for (int j = 0; j < rowNum; j++) {
                    sb.append(intCols[i].getInt(j*4));
                    sb.append(", ");
                }
                sb.append("]");
                sb.append("\n");
            }
        } else {
            sb.append("no int column");
        }
        if (columnCnts[3] > 0) {
            //Not implemented yet
            for (int i = 0; i < stringCols.length; i++) {
                sb.append("String ").append(i).append("th : ");
                int rowNum=Math.min(rowCnt,rowNumber);
                sb.append("[");
                for (int j = 0; j < rowNum; j++) {
                    sb.append("str");
                    sb.append(", ");
                }
                sb.append("]");
                sb.append("\n");
            }
        } else {
            sb.append("no int column");
        }
        sb.append("\n");
        sb.append("Column types:").append(types.toString());
        sb.append("\n");
        return sb.toString();
    }
}
