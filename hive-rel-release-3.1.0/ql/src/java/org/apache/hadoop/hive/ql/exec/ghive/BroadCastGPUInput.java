package org.apache.hadoop.hive.ql.exec.ghive;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class BroadCastGPUInput extends GTable {

    private static final Logger LOG = LoggerFactory.getLogger(BroadCastGPUInput.class.getName());

    private int typeIdx;
    private int longIdx;
    private int doubleIdx;
    private int intIdx;
    private int stringIdx;

    public BroadCastGPUInput(String vertexName) {
        super(vertexName);
    }

    public void broadCastInit(ObjectInspector keyInspector, ObjectInspector valueInspector) {
        assert keyInspector != null;
        getMeta(keyInspector);
        keyCnt = types.size();
        if (valueInspector != null) {
            getMeta(valueInspector);
        }
        init();
        initialized = true;
        LOG.info("BroadCastGPUInput initialized successfully!");
    }

    public void feedRow(ArrayList<Object> keyArr, ArrayList<Object> valueArr) {
        assert keyArr != null;
        assert keyArr.size() == keyCnt;
        if (valueArr != null) {
            assert types.size() == keyArr.size() + valueArr.size();
        }
        typeIdx = 0;
        longIdx = 0;
        doubleIdx = 0;
        intIdx = 0;
        stringIdx = 0;

        for (Object o: keyArr) {
            feedRowInternal(o);
        }
        if (valueArr != null) {
            for (Object o: valueArr) {
                feedRowInternal(o);
            }
        }
        rowCnt++;
    }

    private void feedRowInternal(Object o) {
        switch (types.get(typeIdx++)) {
            case 0: {
                if (longCols[longIdx].capacity() - longCols[longIdx].position() < 8) {
                    ByteBuffer newBuf = ByteBuffer.allocateDirect(2 * longCols[longIdx].capacity());
                    newBuf.order(ByteOrder.LITTLE_ENDIAN);
                    longCols[longIdx].flip();
                    newBuf.put(longCols[longIdx]);
                    SpaceManager.clean(longCols[longIdx]);
                    longCols[longIdx] = newBuf;
                }
                longCols[longIdx++].putLong(((LongWritable) o).get());
                break;
            }
            case 1: {
                if (doubleCols[doubleIdx].capacity() - doubleCols[doubleIdx].position() < 8) {
                    ByteBuffer newBuf = ByteBuffer.allocateDirect(2 * doubleCols[doubleIdx].capacity());
                    newBuf.order(ByteOrder.LITTLE_ENDIAN);
                    doubleCols[doubleIdx].flip();
                    newBuf.put(doubleCols[doubleIdx]);
                    SpaceManager.clean(doubleCols[doubleIdx]);
                    doubleCols[doubleIdx] = newBuf;
                }
                doubleCols[doubleIdx++].putDouble(((DoubleWritable) o).get());
                break;
            }
            case 2: {
                if (intCols[intIdx].capacity() - intCols[intIdx].position() < 4) {
                    ByteBuffer newBuf = ByteBuffer.allocateDirect(2 * intCols[intIdx].capacity());
                    newBuf.order(ByteOrder.LITTLE_ENDIAN);
                    intCols[intIdx].flip();
                    newBuf.put(intCols[intIdx]);
                    SpaceManager.clean(intCols[intIdx]);
                    intCols[intIdx] = newBuf;
                }
                intCols[intIdx++].putInt(((IntWritable) o).get());
                break;
            }
            case 3: {
                if (stringIdxCols[stringIdx].capacity() - stringIdxCols[stringIdx].position() < 4) {
                    ByteBuffer newBuf = ByteBuffer.allocateDirect(2 * stringIdxCols[stringIdx].capacity());
                    newBuf.order(ByteOrder.LITTLE_ENDIAN);
                    stringIdxCols[stringIdx].flip();
                    newBuf.put(stringIdxCols[stringIdx]);
                    SpaceManager.clean(stringIdxCols[stringIdx]);
                    stringIdxCols[stringIdx] = newBuf;
                }

                int stringLength = o.toString().length();

                if (stringCols[stringIdx].capacity() - stringCols[stringIdx].position() < stringLength) {
                    ByteBuffer newBuf = ByteBuffer.allocateDirect(2 * stringCols[stringIdx].capacity());
                    newBuf.order(ByteOrder.LITTLE_ENDIAN);
                    stringCols[stringIdx].flip();
                    newBuf.put(stringCols[stringIdx]);
                    SpaceManager.clean(stringCols[stringIdx]);
                    stringCols[stringIdx] = newBuf;
                }
                stringIdxCols[stringIdx].putInt(stringCols[stringIdx].position());
                stringCols[stringIdx].put(o.toString().getBytes(StandardCharsets.UTF_8));
                stringIdxCols[stringIdx].putInt(stringCols[stringIdx].position());
                break;
            }
        }
    }
}
