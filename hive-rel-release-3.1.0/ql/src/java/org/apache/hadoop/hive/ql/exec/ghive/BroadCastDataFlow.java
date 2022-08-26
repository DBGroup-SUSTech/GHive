package org.apache.hadoop.hive.ql.exec.ghive;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BroadCastDataFlow extends DataFlow {

    private static final Logger LOG = LoggerFactory.getLogger(DataFlow.class.getName());

    private int longColIdx = 0;
    private int doubleColIdx = 0;
    private int stringColIdx = 0;
    private int intColIdx = 0;
    private int tmpIdx = 0;

    public BroadCastDataFlow(String vertexName) {
        super(vertexName);
    }

    public void broadCastInit(ObjectInspector keyInspector, ObjectInspector valueInspector) {
        assert keyInspector != null;
        init(keyInspector);
        keyCnt = type.size();
        if (valueInspector != null) {
            init(valueInspector);
        }
        postInit();
        initialized = true;
        LOG.info("Broadcast_xyk: " + super.toString() + " " + getInfo());
    }


    public void feedRow(ArrayList<Object> keyArr, ArrayList<Object> valueArr) {
        assert keyArr != null;
        assert keyArr.size() == keyCnt;
        if (valueArr != null) {
            assert type.size() == keyArr.size() + valueArr.size();
        }
        tmpIdx = 0;
        longColIdx = 0;
        doubleColIdx = 0;
        intColIdx = 0;
        stringColIdx = 0;

        LOG.info("xyk_keyArr");
        LOG.info(keyArr.toString());
        for (Object o : keyArr) {
            writeColumn(o);
        }
        if (valueArr != null) {
            for (Object o : valueArr) {
                writeColumn(o);
            }
        }
        rowCnt++;
    }

    private void writeColumn(Object o) {
        switch (type.get(tmpIdx++)) {
            case 0: {
                longColumns[longColIdx++].add(((LongWritable) o).get());
                return;
            }
            case 1: {
                doubleColumns[doubleColIdx++].add(((DoubleWritable) o).get());
                return;
            }
            case 2: {
                intColumns[intColIdx++].add(((IntWritable) o).get());
                return;
            }
            case 3: {
                stringColumns[stringColIdx++].add(o.toString());
                return;
            }
        }
        tmpIdx--;
    }


    public String getInfo() {
        return " Long Column number: " + longColumnCnt +
                " Double Column number: " + doubleColumnCnt +
                " type: " + type.toString();
    }
}