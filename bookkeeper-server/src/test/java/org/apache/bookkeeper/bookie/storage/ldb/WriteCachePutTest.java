package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.nio.charset.Charset;
import java.util.Arrays;

public class WriteCachePutTest {
    private static class WriteCacheVisibleState {
        private final long size;
        private final long count;
        private final boolean empty;

        public WriteCacheVisibleState(WriteCache cache) {
            this(cache.size(), cache.count(), cache.isEmpty());
        }

        public WriteCacheVisibleState(long size, long count, boolean empty) {
            this.size = size;
            this.count = count;
            this.empty = empty;
        }

        public void assertEquals(WriteCache actual) {
            Assertions.assertEquals(size, actual.size());
            Assertions.assertEquals(count, actual.count());
            Assertions.assertEquals(empty, actual.isEmpty());
        }

        public void assertSuccessfulInsertion(WriteCache cache, long entrySize) {
            Assertions.assertEquals(size + entrySize, cache.size());
            Assertions.assertEquals(count + 1, cache.count());
            Assertions.assertFalse(cache.isEmpty());
        }
    }

    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    /**
     * test0 - ((-1, 0, bytes(n)), IllegalArgumentException)
     * This test should fail because the cache doesn't accept an entry whose key contains a negative number.
     */
    @Test
    public void test0() {
        WriteCache sut = new WriteCache(allocator, 8192, 2048);
        assertEmptyCache(sut);
        ByteBuf buf = Unpooled.buffer(1024);
        buf.writeCharSequence("This test should fail because ledgerId < 0!", Charset.forName("UTF-8"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> sut.put(-1, 0, buf));
        assertEmptyCache(sut);
    }

    /**
     * test1 - ((42, -1, bytes(n)), IllegalArgumentException)
     * This test should fail because the cache doesn't accept an entry whose key contains a negative number
     */
    @Test
    public void test1() {
        WriteCache sut = new WriteCache(allocator, 8192, 2048);
        assertEmptyCache(sut);
        ByteBuf buf = Unpooled.buffer(1024);
        buf.writeCharSequence("This test should fail because entry is null!", Charset.forName("UTF-8"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> sut.put(42, -1, buf));
        assertEmptyCache(sut);
    }

    /**
     * test2 - ((0, 1, null), IllegalArgumentException)
     * This test should fail because the cache doesn't accept a null entry
     */
    @Test
    public void test2() {
        WriteCache sut = new WriteCache(allocator, 8192, 2048);
        assertEmptyCache(sut);
        Assertions.assertThrows(IllegalArgumentException.class, () -> sut.put(0, 1, null));
        assertEmptyCache(sut);
    }

    /**
     *
     */
    @Test
    public void test3() {
        int maxCacheSize = 8192;
        int maxSegmentSize = 2048;
        int fillSize = 1536;
        WriteCache sut = new WriteCache(allocator, maxCacheSize, maxSegmentSize);
        int ledgerId = 0;
        int entryId = 0;
        createNonEmptyCache(sut, 2048, ledgerId, entryId, fillSize);
        WriteCacheVisibleState state = new WriteCacheVisibleState(sut);

        int n = 1024;
        byte[] data = new byte[n];
        Arrays.fill(data, (byte) 42);
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        Assertions.assertTrue(sut.put(ledgerId, entryId, buf));
        assertSuccessfulInsertion(sut, state, ledgerId, entryId, data);
    }

    private void test4() {
        int maxCacheSize = 4 * 1024 * 1024 * 1024;
        int maxSegmentSize = 1024 * 1024 * 1024;
        int fillSize = 805306368;
        WriteCache sut = new WriteCache(allocator, maxCacheSize, maxSegmentSize);
        createNonEmptyCache(sut, maxSegmentSize, 0, 0, fillSize);
        WriteCacheVisibleState initialState = new WriteCacheVisibleState(sut);
        int n = 512 * 1024 * 1024;
        byte[] data = new byte[n];
        for (int i = 0; i < n; i++) {
            data[i] = (byte) 42;
        }
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        int ledgerId = Integer.MAX_VALUE;
        int entryId = Integer.MAX_VALUE;
        Assertions.assertTrue(sut.put(ledgerId, entryId, buf));
        assertSuccessfulInsertion(sut, initialState, ledgerId, entryId, data);
    }

    private int createNonEmptyCache(WriteCache cache, int maxSegmentSize, int ledgerId, int startEntryId, int fillSize) {
        ByteBuf buf = Unpooled.buffer(Math.min(maxSegmentSize, fillSize));
        int i = 0;
        while (fillSize > 0) {
            int n = Math.min(maxSegmentSize, fillSize);
            for (int j = 0; j < n; ++j) {
                buf.writeByte((byte) 42);
            }
            cache.put(ledgerId, startEntryId + i, buf);
            fillSize -= n;
            buf.resetReaderIndex();
            i++;
        }
        return i;
    }

    private void assertDuplicatedInsertion(WriteCache cache, WriteCacheVisibleState old, int ledgerId, int entryId) {
        old.assertEquals(cache);

    }

    private void assertSuccessfulInsertion(WriteCache cache, WriteCacheVisibleState old, int ledgerId, int entryId, byte[] data) {
        old.assertSuccessfulInsertion(cache, ledgerId);
        ByteBuf expected = Unpooled.wrappedBuffer(data);
        ByteBuf lastEntry = cache.getLastEntry(ledgerId);
        Assertions.assertEquals(expected, lastEntry);
        Assertions.assertEquals(expected, cache.get(ledgerId, entryId));
    }

    @Test
    public void test() {
        WriteCache sut = new WriteCache(allocator, 4096);
        assertEmptyCache(sut);
        final long ledgerId = 0;
        final long entryId = 0;
        ByteBuf buf = Unpooled.buffer(1024);
        String entryData = "Hello World!";
        int n = buf.writeCharSequence(entryData, Charset.forName("UTF-8"));
        Assertions.assertTrue(sut.put(ledgerId, entryId, buf));
        Assertions.assertEquals(1, sut.count());
        Assertions.assertEquals(n, sut.size());
        ByteBuf lastEntry = sut.getLastEntry(ledgerId);
        Assertions.assertEquals(entryData, lastEntry.toString(Charset.forName("UTF-8")));
        ByteBuf actual = sut.get(ledgerId, entryId);
        Assertions.assertEquals(entryData, actual.toString(Charset.forName("UTF-8")));
    }

    private void assertEmptyCache(WriteCache sut) {
        Assertions.assertTrue(sut.isEmpty());
        Assertions.assertEquals(0, sut.count());
        Assertions.assertEquals(0, sut.size());
    }
}
