package org.apache.hadoop.hive.ql.exec.ghive;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GPUResult {
    private final long[][] longCols;
    private final double[][] doubleCols;
    private final int[][] intCols;
    private final String[][] stringCols;
    private final int dataLength;
    private final int[] sequence;
    private transient final Logger LOG = LoggerFactory.getLogger(getClass().getName());

    private final int[] type;

    public GPUResult(long[][] longCols, double[][] doubleCols, int[][] intCols,
                     String[][] stringCols, int dataLength, int[] sequence) {

        this.longCols = (longCols == null) ? new long[0][] : longCols;
        this.doubleCols = (doubleCols == null) ? new double[0][] : doubleCols;
        this.intCols = (intCols == null) ? new int[0][] : intCols;
        this.stringCols = (stringCols == null) ? new String[0][] : stringCols;
        this.dataLength = dataLength;
        this.sequence = sequence;
        type = new int[sequence.length];
    }

    public void setInspectorType(String inspectorString) {
        Pattern pattern = Pattern.compile(".*<(.*)>");
        Matcher matcher = pattern.matcher(inspectorString);
        if (matcher.find()) {
            System.out.println("GHive: matcher-group[0]: " + matcher.group(0));
            System.out.println("GHive: matcher-group[1]: " + matcher.group(1));
            String[] str = matcher.group(1).split(",");
            //assert str.length == type.length;
            System.out.println(str.length);
            System.out.println(type.length);
            int range= Math.min(str.length, type.length);
            for (int i = 0; i < range; i++) {
                System.out.println(str[i]);
                if (str[i].matches(".*Long.*")) {
                    type[i] = 0;
                } else if (str[i].matches(".*Int.*")) {
                    type[i] = 2;
                } else if (str[i].matches(".*Double.*")) {
                    type[i] = 1;
                } else if (str[i].matches(".*String.*")) {
                    type[i] = 3;
                }
            }
        }
    }

    public VectorizedRowBatch generateBatch() {
        int longColNum = 0;
        int doubleColNum = 0;
        int intColNum = 0;
        int stringColNum = 0;
        if (longCols != null) {
            longColNum = longCols.length;
        }
        if (doubleCols != null) {
            doubleColNum = doubleCols.length;
        }
        if (intCols != null) {
            intColNum = intCols.length;
        }
        if (stringCols != null) {
            stringColNum = stringCols.length;
        }
        int totalColNum = longColNum + doubleColNum + intColNum + stringColNum;
        if (totalColNum == 0) {
            LOG.info("GHive: processing result contains nothing (when generating the result batch).");
            return null;
        }
        VectorizedRowBatch batch = new VectorizedRowBatch(totalColNum);
        //int ithCol = 0;
        for (int i = 0; i < sequence.length; i++) {
            if (sequence[i] < longColNum) {
                LongColumnVector longColumnVector = new LongColumnVector(dataLength);
                longColumnVector.vector = longCols[sequence[i]];
                batch.cols[i] = longColumnVector;
            }
            else if (sequence[i] < longColNum + doubleColNum){
                DoubleColumnVector doubleColumnVector = new DoubleColumnVector(dataLength);
                doubleColumnVector.vector = doubleCols[sequence[i] - longColNum];
                batch.cols[i] = doubleColumnVector;
            }
            else if (sequence[i] < longColNum + doubleColNum + intColNum) {
                LongColumnVector longColumnVector = new LongColumnVector(dataLength);
                long[] tmp = new long[intCols[sequence[i] - longColNum - doubleColNum].length];
                for (int j = 0; j < intCols[sequence[i] - longColNum - doubleColNum].length; j++) {
                    tmp[j] = intCols[sequence[i]- longColNum - doubleColNum][j];
                }
                longColumnVector.vector = tmp;
                batch.cols[i] = longColumnVector;
            }
            else {
                BytesColumnVector bytesColumnVector = new BytesColumnVector(dataLength);
                String[] t = stringCols[sequence[i] - (longColNum + doubleColNum + intColNum)];
                byte[][] tmp = new byte[t.length][];
                for (int j = 0; j < t.length; j++) {
                    tmp[j] = t[j].getBytes();
                }
                bytesColumnVector.vector = tmp;
                bytesColumnVector.start = new int[tmp.length];
                bytesColumnVector.length = new int[tmp.length];
                for (int j = 0; j < tmp.length; j++) {
                    bytesColumnVector.length[j] = tmp[j].length;
                }
                batch.cols[i] = bytesColumnVector;
            }
        }
        batch.size = dataLength;
        return batch;
    }

    private int currentIdx = 0;
    public boolean hasNext() {
        return currentIdx < dataLength;
    }

    public List<Object> next() {
        List<Object> ret = new ArrayList<>();
        int longSeq = longCols.length;
        int doubleSeq = longSeq + doubleCols.length;
        int intSeq = doubleSeq + intCols.length;
        int stringSeq = intSeq + stringCols.length;
        for (int idx = 0; idx < sequence.length; idx++) {
            int seq = sequence[idx];
            if (seq < longSeq) {
                if (type[idx] == 0) {
                    ret.add(new LongWritable(longCols[seq][currentIdx]));
                } else if (type[idx] == 2) {
                    ret.add(new IntWritable((int)longCols[seq][currentIdx]));
                }
            } else if (seq < doubleSeq) {
                ret.add(new DoubleWritable(doubleCols[seq - longSeq][currentIdx]));
            } else if (seq < intSeq) {
                ret.add(new IntWritable(intCols[seq - doubleSeq][currentIdx]));
            } else {
                ret.add(new Text(stringCols[seq - intSeq][currentIdx]));
            }
        }
        currentIdx ++;
        return ret;
    }

    @Override
    public String toString() {
        return "GPUResult{" +
                "longCols=" + Arrays.deepToString(longCols) + "\n" +
                ", doubleCols=" + Arrays.deepToString(doubleCols) + "\n" +
                ", intCols=" + Arrays.deepToString(intCols) + "\n" +
                ", stringCols=" + Arrays.deepToString(stringCols) + "\n" +
                ", dataLength=" + dataLength + "\n" +
                ", sequence=" + Arrays.toString(sequence) + "\n" +
                ", type=" + Arrays.toString(type) + "\n" +
                '}';
    }
}