package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

public class WriteCacheForEachOrderedTest {
    private static class MyConsumer implements WriteCache.EntryConsumer {
        private final int expectedCount;
        private final Map<Long, Map<Long, ByteBuf>> entries;
        private final Set<LongPair> alreadySeen = new HashSet<>();
        private int count = 0;
        private long previousLedgerId = -1;
        private long previousEntryId;

        private MyConsumer(int expectedCount, Map<Long, Map<Long, ByteBuf>> entries) {
            this.expectedCount = expectedCount;
            this.entries = entries;
        }

        @Override
        public void accept(long ledgerId, long entryId, ByteBuf entry) {
            if (ledgerId >= 0) {
                Assertions.assertTrue(alreadySeen.add(new LongPair(ledgerId, entryId)));
                Assertions.assertTrue(previousLedgerId < ledgerId || (previousLedgerId == ledgerId && previousEntryId <= entryId));
                Map<Long, ByteBuf> m = entries.get(ledgerId);
                Assertions.assertNotNull(m);
                ByteBuf expected = m.get(entryId);
                Assertions.assertNotNull(expected);
                Assertions.assertEquals(expected, entry);
            }
            previousLedgerId = ledgerId;
            previousEntryId = entryId;
            ++count;
        }

        public void assertConsistency() {
            Assertions.assertEquals(expectedCount, count);
        }
    }

    @Test
    public void testEmptyCache() throws IOException {
        final int expectedCount = 0;
        WriteCache sut = new WriteCache(ByteBufAllocator.DEFAULT, 8 * 1024, 1024);
        WriteCacheAssertionUtils.assertEmptyCache(sut);
        MyConsumer consumer = new MyConsumer(expectedCount, new HashMap<>());
        sut.forEach(consumer);
        consumer.assertConsistency();
    }

    @Test
    public void testSingleEntrySequence() throws IOException {
        final int expectedCount = 1;
        WriteCache sut = new WriteCache(ByteBufAllocator.DEFAULT, 8 * 1024, 1024);
        ByteBuf buf = Unpooled.wrappedBuffer(new byte[]{1, 2, 3});
        Map<Long, Map<Long, ByteBuf>> entries = new HashMap<>();
        entries.computeIfAbsent(0L, k -> new HashMap<>()).put(1L, buf);
        sut.put(0, 1, buf);
        MyConsumer consumer = new MyConsumer(expectedCount, entries);
        sut.forEach(consumer);
        consumer.assertConsistency();
    }

    @Test
    public void testSingleEntryRepeatedSequence() throws IOException {
        final int expectedCount = 1;
        final long ledgerId = Long.MAX_VALUE;
        final long entryId = 1;
        WriteCache sut = new WriteCache(ByteBufAllocator.DEFAULT, 8 * 1024, 1024);
        ByteBuf b1 = Unpooled.buffer(3);
        b1.writeBytes(new byte[]{1, 2, 3});
        ByteBuf b2 = Unpooled.buffer(7);
        b2.writeBytes(new byte[]{42, 42, 42, 42, 42, 42, 42});
        ByteBuf b3 = Unpooled.buffer(32);
        b3.writeBytes("Hello, I am the last entry!".getBytes("UTF-8"));
        Map<Long, Map<Long, ByteBuf>> entries = new HashMap<>();
        entries.computeIfAbsent(ledgerId, k -> new HashMap<>()).put(entryId, b1);
        entries.computeIfAbsent(ledgerId, k -> new HashMap<>()).put(entryId, b2);
        entries.computeIfAbsent(ledgerId, k -> new HashMap<>()).put(entryId, b3);
        sut.put(ledgerId, entryId, b1);
        sut.put(ledgerId, entryId, b2);
        sut.put(ledgerId, entryId, b3);
        MyConsumer consumer = new MyConsumer(expectedCount, entries);
        sut.forEach(consumer);
        consumer.assertConsistency();
    }

    @Test
    public void testNonRepeatedAscendingOrderedSequence() throws IOException {
        final int expectedCount = 3;
        final long ledgerId = Long.MAX_VALUE;
        final long entryId = 1;
        WriteCache sut = new WriteCache(ByteBufAllocator.DEFAULT, 8 * 1024, 1024);
        ByteBuf b1 = Unpooled.buffer(3);
        b1.writeBytes(new byte[]{1, 2, 3});
        ByteBuf b2 = Unpooled.buffer(7);
        b2.writeBytes(new byte[]{42, 42, 42, 42, 42, 42, 42});
        ByteBuf b3 = Unpooled.buffer(32);
        b3.writeBytes("Hello, I am the last entry!".getBytes("UTF-8"));
        Map<Long, Map<Long, ByteBuf>> entries = new HashMap<>();
        entries.computeIfAbsent(ledgerId, k -> new HashMap<>()).put(entryId, b1);
        entries.computeIfAbsent(ledgerId, k -> new HashMap<>()).put(entryId + 1, b2);
        entries.computeIfAbsent(ledgerId, k -> new HashMap<>()).put(entryId + 2, b3);
        sut.put(ledgerId, entryId, b1);
        sut.put(ledgerId, entryId + 1, b2);
        sut.put(ledgerId, entryId + 2, b3);
        MyConsumer consumer = new MyConsumer(expectedCount, entries);
        sut.forEach(consumer);
        consumer.assertConsistency();
    }

    @Test
    public void testRepeateadAscendingOrderedSequence() throws IOException {
        final int expectedCount = 3;
        final long ledgerId = Long.MAX_VALUE;
        final long entryId = 1;
        WriteCache sut = new WriteCache(ByteBufAllocator.DEFAULT, 8 * 1024, 1024);
        ByteBuf b1 = Unpooled.buffer(3);
        b1.writeBytes(new byte[]{1, 2, 3});
        ByteBuf b2 = Unpooled.buffer(7);
        b2.writeBytes(new byte[]{42, 42, 42, 42, 42, 42, 42});
        ByteBuf b3 = Unpooled.buffer(32);
        b3.writeBytes("Hello, I am the last entry!".getBytes("UTF-8"));
        Map<Long, Map<Long, ByteBuf>> entries = new HashMap<>();
        entries.computeIfAbsent(ledgerId, k -> new HashMap<>()).put(entryId, b1);
        entries.computeIfAbsent(ledgerId, k -> new HashMap<>()).put(entryId + 1, b2);
        entries.computeIfAbsent(ledgerId, k -> new HashMap<>()).put(entryId + 2, b3);
        entries.computeIfAbsent(ledgerId, k -> new HashMap<>()).put(entryId + 2, b3);
        sut.put(ledgerId, entryId, b1);
        sut.put(ledgerId, entryId + 1, b2);
        sut.put(ledgerId, entryId + 2, b3);
        MyConsumer consumer = new MyConsumer(expectedCount, entries);
        sut.forEach(consumer);
        consumer.assertConsistency();
    }
}
