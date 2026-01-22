package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

class WriteCacheBuilder {
    private ByteBufAllocator allocator;
    private long maxCacheSize;
    private int maxSegmentSize;
    private int currentSegmentNumber = 1;
    private int currentSegmentSize = 0;
    private HashSet<LongPair> include = new HashSet<>();
    private HashSet<LongPair> exclude = new HashSet<>();

    public static WriteCacheBuilder builder() {
        return new WriteCacheBuilder();
    }

    public WriteCacheBuilder allocator(ByteBufAllocator allocator) {
        this.allocator = Objects.requireNonNull(allocator);
        return this;
    }

    public WriteCacheBuilder maxCacheSize(long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
        return this;
    }

    public WriteCacheBuilder maxSegmentSize(int maxSegmentSize) {
        this.maxSegmentSize = maxSegmentSize;
        return this;
    }

    public WriteCacheBuilder currentSegmentNumber(int currentSegmentNumber) {
        this.currentSegmentNumber = currentSegmentNumber;
        return this;
    }

    public WriteCacheBuilder currentSegmentSize(int currentSegmentSize) {
        this.currentSegmentSize = currentSegmentSize;
        return this;
    }

    public WriteCacheBuilder include(long ledgerId, long entryId) {
        this.include.add(new LongPair(ledgerId, entryId));
        return this;
    }

    public WriteCacheBuilder include(Set<LongPair> include) {
        this.include.addAll(include);
        return this;
    }

    public WriteCacheBuilder exclude(long ledgerId, long entryId) {
        this.exclude.add(new LongPair(ledgerId, entryId));
        return this;
    }

    public WriteCacheBuilder exclude(Set<LongPair> exclude) {
        this.exclude.addAll(exclude);
        return this;
    }

    public WriteCache build() {
        if (allocator == null) {
            throw new IllegalStateException("allocator not set");
        }
        WriteCache cache = new WriteCache(allocator, maxCacheSize, maxSegmentSize);
        fillCache(cache, maxCacheSize, maxSegmentSize, currentSegmentNumber, currentSegmentSize, exclude, include);
        return cache;
    }

    private void fillCache(
            WriteCache cache,
            long maxCacheSize,
            int maxSegmentSize,
            int numOfFullSegments,
            int currentSegmentSize,
            Set<LongPair> exclude,
            Set<LongPair> include) {
        int capacity = Math.toIntExact(Math.min(maxCacheSize, maxSegmentSize));
        if (currentSegmentSize >= capacity) {
            throw new IllegalArgumentException("currentSegmentSize cannot be greater than a segment");
        }
        ByteBuf buf = Unpooled.buffer(capacity);
        while (numOfFullSegments-- > 1) {
            LongPair pair;
            if (include.isEmpty()) {
                pair = randomPair(exclude);
            } else {
                Iterator<LongPair> iterator = include.iterator();
                pair = iterator.next();
                iterator.remove();
                exclude.add(pair);
            }
            ByteBufferUtils.populate(buf, capacity);
            cache.put(pair.getX(), pair.getY(), buf);
            buf.resetWriterIndex();
        }
        if (currentSegmentSize > 0) {
            ByteBufferUtils.populate(buf, currentSegmentSize);
            LongPair pair = randomPair(exclude);
            cache.put(pair.getX(), pair.getY(), buf);
        }
    }

    private static LongPair randomPair(Set<LongPair> exclude) {
        while (true) {
            long x = Math.abs(ThreadLocalRandom.current().nextLong());
            long y = Math.abs(ThreadLocalRandom.current().nextLong());
            LongPair pair = new LongPair(x, y);
            if (!exclude.contains(pair)) {
                exclude.add(pair);
                return pair;
            }
        }
    }
}
