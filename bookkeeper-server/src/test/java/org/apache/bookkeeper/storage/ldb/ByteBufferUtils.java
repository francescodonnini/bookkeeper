package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;

class ByteBufferUtils {
    private ByteBufferUtils() {}

    public static void populate(ByteBuf buf, int n) {
        for (int i = 0; i < n; i++) {
            buf.writeByte((byte) 42);
        }
        buf.resetReaderIndex();
    }
}
