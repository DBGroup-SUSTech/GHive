package org.apache.hadoop.hive.ql.exec.ghive;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@Deprecated
public class SpaceManager {


    public static void clean(final ByteBuffer byteBuffer) {
        if (byteBuffer.isDirect()) {
            ((DirectBuffer)byteBuffer).cleaner().clean();
        }
    }

    public static ByteBuffer[] longColsSpace; // 0
    public static ByteBuffer[] doubleColsSpace; // 1
    public static ByteBuffer[] intColsSpace; // 2
    public static ByteBuffer[] stringColsSpace; // 3
    public static ByteBuffer[] stringIdxColsSpace;

    public static int tblLongColsIdx;
    public static int tblDoubleColsIdx;
    public static int tblIntColsIdx;
    public static int tblStringColsIdx;
    public static int tblStringIdxColsIdx;

    static {
        tblLongColsIdx = 0;
        tblDoubleColsIdx = 0;
        tblIntColsIdx = 0;
        tblStringColsIdx = 0;
        tblStringIdxColsIdx = 0;
    }


//    static {
//        longColsSpace = new ByteBuffer[5];
//        doubleColsSpace = new ByteBuffer[5];
//        intColsSpace = new ByteBuffer[5];
//        stringColsSpace = new ByteBuffer[5];
//        stringIdxColsSpace = new ByteBuffer[5];
//
//        for (int i = 0; i < 5; i ++) {
//            longColsSpace[i] = ByteBuffer.allocateDirect(1024 * 1024 * 1024);
//            longColsSpace[i].order(ByteOrder.LITTLE_ENDIAN);
//        }
//        for (int i = 0; i < 5; i ++) {
//            doubleColsSpace[i] = ByteBuffer.allocateDirect(1024 * 1024 * 1024);
//            doubleColsSpace[i].order(ByteOrder.LITTLE_ENDIAN);
//        }
//        for (int i = 0; i < 5; i ++) {
//            intColsSpace[i] = ByteBuffer.allocateDirect(1024 * 1024 * 1024);
//            intColsSpace[i].order(ByteOrder.LITTLE_ENDIAN);
//        }
//        for (int i = 0; i < 5; i ++) {
//            stringColsSpace[i] = ByteBuffer.allocateDirect(1024 * 1024 * 1024);
//            stringIdxColsSpace[i] = ByteBuffer.allocateDirect(1024 * 1024 * 1024);
//            stringColsSpace[i].order(ByteOrder.LITTLE_ENDIAN);
//            stringIdxColsSpace[i].order(ByteOrder.LITTLE_ENDIAN);
//        }
//        System.out.println("Direct ByteBuffer allocated successfully!");
//    }

}
