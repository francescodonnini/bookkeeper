package org.apache.bookkeeper.bookie.storage.ldb;

import org.junit.jupiter.api.Assertions;

public class WriteCacheAssertionUtils {
    public static void assertEmptyCache(WriteCache sut) {
        Assertions.assertTrue(sut.isEmpty());
        Assertions.assertEquals(0, sut.count());
        Assertions.assertEquals(0, sut.size());
    }
}
