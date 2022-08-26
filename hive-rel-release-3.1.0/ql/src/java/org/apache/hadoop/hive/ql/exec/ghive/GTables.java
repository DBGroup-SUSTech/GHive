package org.apache.hadoop.hive.ql.exec.ghive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class GTables {

    private static final Logger LOG = LoggerFactory.getLogger(GTables.class.getName());

    public ByteBuffer[] longCols;
    public ByteBuffer[] doubleCols;
    public ByteBuffer[] intCols;
    public ByteBuffer[] stringCols;
    public ByteBuffer[] stringIdxCols;

    public int[][] sequences;
    public int[][] types;

    public String thisVertexName;
    public String maintainValuesJsonString;
    public String[] vertexName;

    public int[] longColumnCnt;
    public int[] doubleColumnCnt;
    public int[] intColumnCnt;
    public int[] stringColumnCnt;
    public int[] keyCnt;
    public int[] rowCnt;


    GTables(ArrayList<GTable> inputs) {
        // 0: Long, 1: Double, 2: Int, 3: String
        int[] totalColumnCnt = new int[4];
        for (GTable input: inputs) {
            for (int i = 0; i < totalColumnCnt.length; i ++) {
                totalColumnCnt[i] += input.columnCnts[i];
            }
        }
        longCols = new ByteBuffer[totalColumnCnt[0]];
        doubleCols = new ByteBuffer[totalColumnCnt[1]];
        intCols = new ByteBuffer[totalColumnCnt[2]];
        stringCols = new ByteBuffer[totalColumnCnt[3]];
        stringIdxCols = new ByteBuffer[totalColumnCnt[3]];

        longColumnCnt = new int[inputs.size()];
        doubleColumnCnt = new int[inputs.size()];
        intColumnCnt = new int[inputs.size()];
        stringColumnCnt = new int[inputs.size()];

        vertexName = new String[inputs.size()];
        sequences = new int[inputs.size()][];
        types = new int[inputs.size()][];
        keyCnt = new int[inputs.size()];
        rowCnt = new int[inputs.size()];

        int longCopyIdx = 0;
        int doubleCopyIdx = 0;
        int intCopyIdx = 0;
        int stringCopyIdx = 0;

        for (int i = 0; i < inputs.size(); i ++) {
            GTable input = inputs.get(i);
            if (input.rowCnt == 0) {
                LOG.info(vertexName[i] + " has rowCnt 0 (no data)");
                continue;
            }
            InfoCollector.hasData = true;
            if (input.columnCnts[0] != 0) {
                for (int j = 0; j < input.columnCnts[0]; j ++) {
                    longCols[longCopyIdx++] = input.longCols[j];
                }
            }

            if (input.columnCnts[1] != 0) {
                for (int j = 0; j < input.columnCnts[1]; j ++) {
                    doubleCols[doubleCopyIdx++] = input.doubleCols[j];
                }
            }

            if (input.columnCnts[2] != 0) {
                for (int j = 0; j < input.columnCnts[2]; j ++) {
                    intCols[intCopyIdx++] = input.intCols[j];
                }
            }

            if (input.columnCnts[3] != 0) {
                for (int j = 0; j < input.columnCnts[3]; j ++) {
                    stringCols[stringCopyIdx] = input.stringCols[j];
                    stringIdxCols[stringCopyIdx++] = input.stringIdxCols[j];
                }
            }

            vertexName[i] = input.vertexName;

            int longSeq = 0;
            int doubleSeq = input.columnCnts[0];
            int intSeq = doubleSeq + input.columnCnts[1];
            int stringSeq = intSeq + input.columnCnts[2];
            int[] eachSeq = new int[input.types.size()];
            int[] eachType = new int[input.types.size()];
            for (int j = 0; j < input.types.size(); j ++) {
                eachType[j] = input.types.get(j);
                switch (input.types.get(j)) {
                    case 0: {
                        eachSeq[j] = longSeq ++;
                        break;
                    }
                    case 1: {
                        eachSeq[j] = doubleSeq ++;
                        break;
                    }
                    case 2: {
                        eachSeq[j] = intSeq ++;
                        break;
                    }
                    case 3: {
                        eachSeq[j] = stringSeq ++;
                        break;
                    }
                }
            }
            sequences[i] = eachSeq;
            types[i] = eachType;
            rowCnt[i] = input.rowCnt;
            keyCnt[i] = input.keyCnt;
            longColumnCnt[i] = input.columnCnts[0];
            doubleColumnCnt[i] = input.columnCnts[1];
            intColumnCnt[i] = input.columnCnts[2];
            stringColumnCnt[i] = input.columnCnts[3];
        }
    }
}
