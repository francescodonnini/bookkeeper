package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the forEach method of WriteCache.
 */
public class WriteCacheForEachGeminiTest {

    private WriteCache writeCache;
    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private static final long MAX_CACHE_SIZE = 1024 * 1024; // 1MB

    @BeforeEach
    public void setup() {
        writeCache = new WriteCache(allocator, MAX_CACHE_SIZE);
    }

    @AfterEach
    public void teardown() {
        writeCache.close();
    }

    /**
     * Entry representation for verification.
     */
    private static class TestEntry {
        long ledgerId;
        long entryId;
        String content;

        TestEntry(long ledgerId, long entryId, String content) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.content = content;
        }
    }

    @Test
    public void testForEachEmptyCache() throws IOException {
        List<Long> entries = new ArrayList<>();
        writeCache.forEach((ledgerId, entryId, entry) -> {
            entries.add(ledgerId);
        });
        assertTrue(entries.isEmpty(), "Callback should not be triggered for empty cache");
    }

    @Test
    public void testForEachOrdering() throws IOException {
        // Add entries out of order
        addEntry(10L, 2L, "L10-E2");
        addEntry(10L, 1L, "L10-E1");
        addEntry(5L, 100L, "L5-E100");
        addEntry(100L, 1L, "L100-E1");

        List<TestEntry> results = new ArrayList<>();
        writeCache.forEach((ledgerId, entryId, entry) -> {
            byte[] bytes = new byte[entry.readableBytes()];
            entry.readBytes(bytes);
            results.add(new TestEntry(ledgerId, entryId, new String(bytes)));
        });

        // Verify sorted order: (5, 100) -> (10, 1) -> (10, 2) -> (100, 1)
        assertEquals(4, results.size());

        assertEquals(5L, results.get(0).ledgerId);
        assertEquals(100L, results.get(0).entryId);

        assertEquals(10L, results.get(1).ledgerId);
        assertEquals(1L, results.get(1).entryId);

        assertEquals(10L, results.get(2).ledgerId);
        assertEquals(2L, results.get(2).entryId);

        assertEquals(100L, results.get(3).ledgerId);
        assertEquals(1L, results.get(3).entryId);
    }

    @Test
    public void testForEachWithDeletedLedger() throws IOException {
        addEntry(1L, 1L, "data1");
        addEntry(2L, 1L, "data2");
        addEntry(3L, 1L, "data3");

        // Mark ledger 2 as deleted
        writeCache.deleteLedger(2L);

        List<Long> ledgerIds = new ArrayList<>();
        writeCache.forEach((ledgerId, entryId, entry) -> {
            ledgerIds.add(ledgerId);
        });

        // Ledger 2 should be skipped
        assertEquals(2, ledgerIds.size());
        assertTrue(ledgerIds.contains(1L));
        assertTrue(ledgerIds.contains(3L));
        assertTrue(!ledgerIds.contains(2L));
    }

    @Test
    public void testForEachWithLargeNumberOfEntries() throws IOException {
        int numEntries = 5000; // Trigger potential array resizing in WriteCache
        for (int i = 0; i < numEntries; i++) {
            addEntry(1L, i, "data" + i);
        }

        int[] count = {0};
        writeCache.forEach((ledgerId, entryId, entry) -> {
            assertEquals(1L, ledgerId);
            assertEquals(count[0], entryId);
            count[0]++;
        });

        assertEquals(numEntries, count[0]);
    }

    @Test
    public void testForEachBufferSlicing() throws IOException {
        String data = "some-reusable-data";
        addEntry(1L, 1L, data);

        writeCache.forEach((ledgerId, entryId, entry) -> {
            // Verify that the ByteBuf passed to consumer has correct indexes
            assertEquals(0, entry.readerIndex());
            assertEquals(data.getBytes().length, entry.readableBytes());

            byte[] bytes = new byte[entry.readableBytes()];
            entry.getBytes(entry.readerIndex(), bytes);
            assertEquals(data, new String(bytes));
        });
    }

    private void addEntry(long ledgerId, long entryId, String data) {
        byte[] bytes = data.getBytes();
        ByteBuf buffer = Unpooled.wrappedBuffer(bytes);
        writeCache.put(ledgerId, entryId, buffer);
    }
}
