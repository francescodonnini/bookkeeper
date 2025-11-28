package org.apache.bookkeeper.bookie.storage.ldb;

import java.util.Objects;

class LongPair {
    private final long x;
    private final long y;

    public LongPair(long x, long y) {
        this.x = x;
        this.y = y;
    }

    public long getX() {
        return x;
    }

    public long getY() {
        return y;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LongPair)) return false;
        LongPair longPair = (LongPair) o;
        return x == longPair.x && y == longPair.y;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y);
    }
}
