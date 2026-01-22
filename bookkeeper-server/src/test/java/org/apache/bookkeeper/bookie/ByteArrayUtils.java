package org.apache.bookkeeper.bookie;

import java.util.Arrays;

public class ByteArrayUtils {
    private ByteArrayUtils() {}

    public static byte[] newArray(byte b, int n) {
        byte[] bytes = new byte[n];
        Arrays.fill(bytes, b);
        return bytes;
    }
}
