package org.apache.bookkeeper.bookie.storage.ldb;

import org.junit.jupiter.api.Assertions;

public class WriteCacheVisibleState {
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
