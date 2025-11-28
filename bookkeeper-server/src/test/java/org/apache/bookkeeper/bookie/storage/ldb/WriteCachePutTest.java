package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class WriteCachePutTest {
    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    @Test
    public void test1() {
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(8 * 1024)
                .maxSegmentSize(2 * 1024)
                .currentSegmentNumber(1)
                .currentSegmentSize(0)
                .build();
        WriteCacheAssertionUtils.assertEmptyCache(sut);
        ByteBuf buf = Unpooled.buffer(27);
        ByteBufferUtils.populate(buf, 27);
        Assertions.assertThrows(IllegalArgumentException.class, () -> sut.put(-1, 0, buf));
        WriteCacheAssertionUtils.assertEmptyCache(sut);
    }

    @Test
    public void test2() {
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(4 * 1024 * 1024)
                .maxSegmentSize(2 * 1024)
                .currentSegmentNumber(1)
                .currentSegmentSize(512)
                .build();
        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        ByteBuf buf = Unpooled.buffer(8);
        ByteBufferUtils.populate(buf, 8);
        Assertions.assertThrows(IllegalArgumentException.class, () -> sut.put(0, -1, buf));
        oldState.assertEquals(sut);
    }

    @Test
    public void test3() {
        WriteCache sut = WriteCacheBuilder.builder()
                    .allocator(allocator)
                    .maxCacheSize(2 * 1024 * 1024)
                    .maxSegmentSize(1024)
                    .currentSegmentNumber(3)
                    .currentSegmentSize(0)
                    .build();
        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        Assertions.assertThrows(NullPointerException.class, () -> sut.put(0, 0, null));
        oldState.assertEquals(sut);
    }

    @Test
    public void test4() {
        final long ledgerId = 0;
        final long entryId = 0;
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(2 * 1024 * 1024)
                .maxSegmentSize(1024)
                .currentSegmentNumber(1)
                .currentSegmentSize(0)
                .exclude(ledgerId, entryId)
                .build();
        WriteCacheAssertionUtils.assertEmptyCache(sut);
        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        ByteBuf buf = Unpooled.buffer(64);
        ByteBufferUtils.populate(buf, 64);
        Assertions.assertTrue(sut.put(ledgerId, entryId, buf));
        assertSuccessfulInsertion(sut, oldState, ledgerId, entryId, buf.array());
    }

    @Test
    public void test5() {
        final long ledgerId = 194;
        final long entryId = 123;
        final int maxSegmentSize = 2 * 1024;
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(4 * 1024 * 1024)
                .maxSegmentSize(maxSegmentSize)
                .currentSegmentNumber(1)
                .currentSegmentSize(0)
                .exclude(ledgerId, entryId)
                .build();
        WriteCacheAssertionUtils.assertEmptyCache(sut);
        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        ByteBuf buf = Unpooled.buffer(maxSegmentSize);
        ByteBufferUtils.populate(buf, maxSegmentSize);
        Assertions.assertTrue(sut.put(ledgerId, entryId, buf));
        assertSuccessfulInsertion(sut, oldState, ledgerId, entryId, buf.array());
    }

    @Test
    public void test6() {
        final long ledgerId = Long.MAX_VALUE;
        final long entryId = 42;
        final int n = 1024 + 1;
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(8 * 1024 * 1024)
                .maxSegmentSize(1024)
                .currentSegmentNumber(1)
                .currentSegmentSize(0)
                .exclude(ledgerId, entryId)
                .build();
        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        ByteBuf buf = Unpooled.buffer(n);
        ByteBufferUtils.populate(buf, n);
        Assertions.assertFalse(sut.put(ledgerId, entryId, buf));
        assertUnsuccessfulInsertion(sut, oldState);
    }

    @Test
    public void test7() {
        final long ledgerId = 1;
        final long entryId = 1;
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(22 * 1024 * 1024)
                .maxSegmentSize(4 * 1024)
                .currentSegmentNumber(2)
                .currentSegmentSize(0)
                .include(ledgerId, entryId)
                .build();

        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        ByteBuf buf = Unpooled.buffer(4 * 1024);
        ByteBufferUtils.populate(buf, 4 * 1024);
        Assertions.assertTrue(sut.put(ledgerId, entryId, buf));
        assertSuccessfulInsertion(sut, oldState, ledgerId, entryId, buf.array());
    }

    @Test
    public void test8() {
        final long ledgerId = 13;
        final long entryId = 17;
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(8 * 1024 * 1024)
                .maxSegmentSize(1024 * 1024)
                .currentSegmentNumber(1)
                .currentSegmentSize(512)
                .include(ledgerId, entryId)
                .build();
        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        ByteBuf buf = Unpooled.buffer(64);
        ByteBufferUtils.populate(buf, 64);
        Assertions.assertTrue(sut.put(ledgerId, entryId, buf));
        assertSuccessfulInsertion(sut, oldState, ledgerId, entryId, buf.array());
    }

    @Test
    public void test9() {
        final long ledgerId = 33;
        final long entryId = Long.MAX_VALUE;
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(16 * 1024)
                .maxSegmentSize(1024)
                .currentSegmentNumber(3)
                .currentSegmentSize(512)
                .include(ledgerId, entryId)
                .build();
        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        ByteBuf buf = Unpooled.buffer(512);
        ByteBufferUtils.populate(buf, 512);
        Assertions.assertTrue(sut.put(ledgerId, entryId, buf));
        assertSuccessfulInsertion(sut, oldState, ledgerId, entryId, buf.array());
    }

    @Test
    public void test10() {
        final long ledgerId = 5;
        final long entryId = 5;
        final int n = 1024 * 1024 + 1;
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(8 * 1024 * 1024)
                .maxSegmentSize(1024 * 1024)
                .currentSegmentNumber(8)
                .currentSegmentSize(0)
                .include(ledgerId, entryId)
                .build();
        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        ByteBuf buf = Unpooled.buffer(n);
        ByteBufferUtils.populate(buf, n);
        Assertions.assertFalse(sut.put(ledgerId, entryId, buf));
        assertUnsuccessfulInsertion(sut, oldState);
    }

    @Test
    public void test11() {
        final long ledgerId = 51;
        final long entryId = 15;
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(8 * 1024 * 1024)
                .maxSegmentSize(1024 * 1024)
                .currentSegmentNumber(1)
                .currentSegmentSize(128)
                .exclude(51, 15)
                .build();
        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        ByteBuf buf = Unpooled.buffer(64);
        ByteBufferUtils.populate(buf, 64);
        Assertions.assertTrue(sut.put(ledgerId, entryId, buf));
        assertSuccessfulInsertion(sut, oldState, ledgerId, entryId, buf.array());
    }

    @Test
    public void test12() {
        final long ledgerId = 500;
        final long entryId = 2345;
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(16 * 1024)
                .maxSegmentSize(1024)
                .currentSegmentNumber(8)
                .currentSegmentSize(128)
                .exclude(ledgerId, entryId)
                .build();
        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        ByteBuf buf = Unpooled.buffer(896);
        ByteBufferUtils.populate(buf, 896);
        Assertions.assertTrue(sut.put(ledgerId, entryId, buf));
        assertSuccessfulInsertion(sut, oldState, ledgerId, entryId, buf.array());
    }

    @Test
    public void test13() {
        final long ledgerId = 0;
        final long entryId = Long.MAX_VALUE;
        final int n = 519 * 1024 + 1;
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(8 * 1024 * 1024)
                .maxSegmentSize(1024 * 1024)
                .currentSegmentNumber(8)
                .currentSegmentSize(517 * 1024)
                .exclude(ledgerId, entryId)
                .build();
        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        ByteBuf buf = Unpooled.buffer(n);
        ByteBufferUtils.populate(buf, n);
        Assertions.assertFalse(sut.put(ledgerId, entryId, buf));
        assertUnsuccessfulInsertion(sut, oldState);
    }

    @Test
    public void test14() {
        final long ledgerId = 0;
        final long entryId = 1;
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(8 * 1024 * 1024)
                .maxSegmentSize(1024 * 1024)
                .currentSegmentNumber(1)
                .currentSegmentSize(0)
                .build();
        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        ByteBuf buf = Unpooled.buffer(1024);
        ByteBufferUtils.populate(buf, 1024);
        Assertions.assertTrue(sut.put(ledgerId, entryId + 1, buf));
        assertSuccessfulInsertion(sut, oldState, ledgerId, entryId + 1, buf.array());
        ByteBuf buf2 = Unpooled.buffer(1024);
        ByteBufferUtils.populate(buf2, 1024);
        WriteCacheVisibleState oldState2 = new WriteCacheVisibleState(sut);
        Assertions.assertTrue(sut.put(ledgerId, entryId, buf2));
        assertSuccessfulInsertion(sut, oldState2, ledgerId, entryId, buf2.array());
    }

    @Test
    public void test15() {
        final long ledgerId = 0;
        final long entryId = 1;
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(1024)
                .maxSegmentSize(2 * 1024)
                .currentSegmentNumber(1)
                .currentSegmentSize(0)
                .build();
        WriteCacheAssertionUtils.assertEmptyCache(sut);
        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        ByteBuf buf = Unpooled.buffer(512);
        ByteBufferUtils.populate(buf, 512);
        Assertions.assertTrue(sut.put(ledgerId, entryId, buf));
        assertSuccessfulInsertion(sut, oldState, ledgerId, entryId, buf.array());
    }

    @Test
    public void test16() {
        final long ledgerId = Long.MAX_VALUE - 1;
        final long entryId = 12;
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(1024)
                .maxSegmentSize(2 * 1024)
                .currentSegmentNumber(1)
                .currentSegmentSize(0)
                .build();
        WriteCacheAssertionUtils.assertEmptyCache(sut);
        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        ByteBuf buf = Unpooled.buffer(1024);
        ByteBufferUtils.populate(buf, 1024);
        Assertions.assertTrue(sut.put(ledgerId, entryId, buf));
        assertSuccessfulInsertion(sut, oldState, ledgerId, entryId, buf.array());
    }

    @Test
    public void test17() {
        final long ledgerId = 13;
        final long entryId = Long.MAX_VALUE - 1;
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(1024)
                .maxSegmentSize(2 * 1024)
                .currentSegmentNumber(1)
                .currentSegmentSize(0)
                .exclude(ledgerId, entryId)
                .build();
        WriteCacheAssertionUtils.assertEmptyCache(sut);
        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        ByteBuf buf = Unpooled.buffer(1025);
        ByteBufferUtils.populate(buf, 1025);
        Assertions.assertFalse(sut.put(ledgerId, entryId, buf));
        assertUnsuccessfulInsertion(sut, oldState);
    }

    @Test
    public void test18() {
        final long ledgerId = 13;
        final long entryId = Long.MAX_VALUE - 1;
        final int n = 82;
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(1024)
                .maxSegmentSize(2 * 1024)
                .currentSegmentNumber(1)
                .currentSegmentSize(64)
                .exclude(ledgerId, entryId)
                .build();
        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        ByteBuf buf = Unpooled.buffer(n);
        ByteBufferUtils.populate(buf, n);
        Assertions.assertTrue(sut.put(ledgerId, entryId, buf));
        assertSuccessfulInsertion(sut, oldState, ledgerId, entryId, buf.array());
    }

    @Test
    public void test19() {
        final long ledgerId = 13;
        final long entryId = Long.MAX_VALUE - 1;
        final int n = 960;
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(1024)
                .maxSegmentSize(2 * 1024)
                .currentSegmentNumber(1)
                .currentSegmentSize(64)
                .exclude(ledgerId, entryId)
                .build();
        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        ByteBuf buf = Unpooled.buffer(n);
        ByteBufferUtils.populate(buf, n);
        Assertions.assertTrue(sut.put(ledgerId, entryId, buf));
        assertSuccessfulInsertion(sut, oldState, ledgerId, entryId, buf.array());
    }

    @Test
    public void test20() {
        final long ledgerId = 131313;
        final long entryId = 0;
        final int n = 960;
        WriteCache sut = WriteCacheBuilder.builder()
                .allocator(allocator)
                .maxCacheSize(1024)
                .maxSegmentSize(2 * 1024)
                .currentSegmentNumber(1)
                .currentSegmentSize(64)
                .exclude(ledgerId, entryId)
                .build();
        WriteCacheVisibleState oldState = new WriteCacheVisibleState(sut);
        ByteBuf buf = Unpooled.buffer(n + 1);
        ByteBufferUtils.populate(buf, n + 1);
        Assertions.assertTrue(sut.put(ledgerId, entryId, buf));
        assertSuccessfulInsertion(sut, oldState, ledgerId, entryId, buf.array());
    }

    private void assertUnsuccessfulInsertion(WriteCache cache, WriteCacheVisibleState old) {
        old.assertEquals(cache);
    }

    private void assertSuccessfulInsertion(WriteCache cache, WriteCacheVisibleState old, long ledgerId, long entryId, byte[] data) {
        old.assertSuccessfulInsertion(cache, data.length);
        ByteBuf expected = Unpooled.wrappedBuffer(data);
        ByteBuf lastEntry = cache.getLastEntry(ledgerId);
        Assertions.assertEquals(expected, lastEntry);
        Assertions.assertEquals(expected, cache.get(ledgerId, entryId));
    }
}
