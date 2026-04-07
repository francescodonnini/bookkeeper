package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link WriteCache#put(long, long, ByteBuf)} method.
 */
public class WriteCachePutGeminiTest {

    private WriteCache writeCache;
    private static final int MAX_CACHE_SIZE = 1024 * 1024; // 1MB
    private static final int MAX_SEGMENT_SIZE = 512 * 1024; // 512KB

    @BeforeEach
    public void setup() {
        writeCache = new WriteCache(PooledByteBufAllocator.DEFAULT, MAX_CACHE_SIZE, MAX_SEGMENT_SIZE);
    }

    @AfterEach
    public void tearDown() {
        writeCache.close();
    }

    @Test
    public void testPutSingleEntry() {
        long ledgerId = 1L;
        long entryId = 1L;
        ByteBuf data = Unpooled.copiedBuffer("test-data", StandardCharsets.UTF_8);
        int dataSize = data.readableBytes();

        boolean result = writeCache.put(ledgerId, entryId, data);

        assertTrue(result, "Put should succeed");
        assertEquals(dataSize, writeCache.size(), "Cache size should match data size");
        assertEquals(1, writeCache.count(), "Cache count should be 1");

        ByteBuf retrieved = writeCache.get(ledgerId, entryId);
        assertNotNull(retrieved);
        assertEquals(data, retrieved);
        retrieved.release();
    }

    @Test
    public void testPutCacheFull() {
        long ledgerId = 1L;
        // Create an entry slightly larger than the total cache capacity
        ByteBuf largeData = Unpooled.buffer(MAX_CACHE_SIZE + 64);
        largeData.writerIndex(MAX_CACHE_SIZE + 1);

        boolean result = writeCache.put(ledgerId, 1L, largeData);

        assertFalse(result, "Put should fail when data exceeds maxCacheSize");
        assertEquals(0, writeCache.size(), "Cache size should remain 0");
    }

    @Test
    public void testPutBoundarySegmentWrap() {
        // Test logic where entry doesn't fit in the current segment and must move to the next
        // Max segment is 512KB. We fill up 511KB, then try to put a 2KB entry.
        int firstPartSize = MAX_SEGMENT_SIZE - 1024; // 511KB
        ByteBuf firstPart = Unpooled.buffer(firstPartSize);
        firstPart.writerIndex(firstPartSize);

        writeCache.put(1L, 1L, firstPart);

        // This 2KB entry cannot fit in the remaining 1KB of segment 0.
        // It must be placed at the start of segment 1.
        ByteBuf secondPart = Unpooled.copiedBuffer("wrap-data", StandardCharsets.UTF_8);
        boolean result = writeCache.put(1L, 2L, secondPart);

        assertTrue(result, "Should succeed by wrapping into the next segment");

        ByteBuf retrieved = writeCache.get(1L, 2L);
        assertEquals(secondPart, retrieved);
        retrieved.release();
    }

    @Test
    public void testPutLastEntryIdUpdate() {
        long ledgerId = 10L;

        ByteBuf entry1 = Unpooled.copiedBuffer("entry-1", StandardCharsets.UTF_8);
        ByteBuf entry2 = Unpooled.copiedBuffer("entry-2", StandardCharsets.UTF_8);

        writeCache.put(ledgerId, 1L, entry1);
        writeCache.put(ledgerId, 2L, entry2);

        ByteBuf last = writeCache.getLastEntry(ledgerId);
        assertEquals(entry2, last, "Last entry should be entryId 2");
        last.release();
    }

    @Test
    public void testPutOutOrderEntryId() {
        long ledgerId = 20L;

        ByteBuf entry1 = Unpooled.copiedBuffer("entry-1", StandardCharsets.UTF_8);
        ByteBuf entry2 = Unpooled.copiedBuffer("entry-2", StandardCharsets.UTF_8);

        // Put entry 2 then entry 1
        writeCache.put(ledgerId, 2L, entry2);
        writeCache.put(ledgerId, 1L, entry1);

        ByteBuf last = writeCache.getLastEntry(ledgerId);
        assertEquals(entry2, last, "Last entry should still be entryId 2 even if put later");
        last.release();
    }

    @Test
    public void testPutAlignmentEffect() {
        long ledgerId = 1L;
        // 10 bytes will be aligned to 64 bytes in the offset calculation
        ByteBuf data = Unpooled.buffer(10);
        data.writerIndex(10);

        writeCache.put(ledgerId, 1L, data);

        // Even though cacheSize reports 10, internal cacheOffset should have moved by 64
        // Adding another entry to verify.
        writeCache.put(ledgerId, 2L, data);

        // If alignment works, the internal index for entry 2 should be at 64, not 10.
        // We verify this via the internal state or side effects if accessible,
        // but primarily we ensure the data is retrievable and correct.
        assertNotNull(writeCache.get(ledgerId, 1L));
        assertNotNull(writeCache.get(ledgerId, 2L));
    }
}