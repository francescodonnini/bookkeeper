package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

public class BufferedChannelTest {
    private static Path createTempFile(String name, String permissions, byte[] bytes) throws IOException {
        Path path = createTempFile(name, permissions);
        try (FileWriter writer = new FileWriter(path.toFile())) {
            for (int i = 0; i < bytes.length; i++) {
                writer.write(bytes[i]);
            }
        }
        System.out.println("Created temp file: " + path);
        return path;
    }

    private static Path createTempFile(String name, String permissions) throws IOException {
        return Files.createTempFile(name, null, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(permissions)));
    }

    private static byte[] bytes(byte b, int n) {
        byte[] bytes = new byte[n];
        Arrays.fill(bytes, b);
        return bytes;
    }

    private static Stream<Arguments> inputs() {
        return Stream.of(
                Arguments.of(0, 1023, 1024, "rw-rw-r--", setOf(StandardOpenOption.WRITE)),
                Arguments.of(0, 1024, 1024, "rw-rw-r--", setOf(StandardOpenOption.WRITE)),
                Arguments.of(0, 1025, 1024, "rw-rw-r--", setOf(StandardOpenOption.WRITE)),
                Arguments.of(1023, 1024, 1024, "rw-rw-r--", setOf(StandardOpenOption.WRITE)),
                Arguments.of(1023, 1025, 1024, "rw-rw-r--", setOf(StandardOpenOption.WRITE)),
                Arguments.of(1024, 1025, 1024, "rw-rw-r--", setOf(StandardOpenOption.WRITE)),
                Arguments.of(1024, 1023, 1024, "rw-rw-r--", setOf(StandardOpenOption.WRITE)),
                Arguments.of(1023, 1024, 1024, "rw-rw-r--", setOf(StandardOpenOption.WRITE))
        );
    }

    private static Stream<Arguments> newInputs() {
        return Stream.of(
                Arguments.of(0, 1, 0, "rw-rw-r--", setOf(StandardOpenOption.WRITE)),
                Arguments.of(0, 1, 2, "rw-rw-r--", setOf(StandardOpenOption.WRITE)),
                Arguments.of(0, 1, 1024, "rw-rw-r--", setOf(StandardOpenOption.WRITE)),
                Arguments.of(0, 666, 667, "rw-rw-r--", setOf(StandardOpenOption.WRITE)),
                Arguments.of(0, 1, 2, "rw-rw-r--", setOf(StandardOpenOption.WRITE)),
                Arguments.of(0, 0, 1, "rw-rw-r--", setOf(StandardOpenOption.WRITE)),
                Arguments.of(0, 0, 1125, "rw-rw-r--", setOf(StandardOpenOption.WRITE)),
                // This test seems to make BufferedChannel stuck on an infinite loop
                Arguments.of(666, 0, 667, "rw-rw-r--", setOf(StandardOpenOption.WRITE))
        );
    }

    @ParameterizedTest
    @MethodSource("newInputs")
    public void parametricSingleWriteTest(int unpersistedByteBound, int writeCapacity, int writableBytes, String permissions, Set<OpenOption> options) throws IOException {
        Path path = createTempFile("file", permissions, "Hello, World!".getBytes());
        ByteBuf src = ByteBufAllocator.DEFAULT.buffer(writeCapacity);
        if (writableBytes > 0) {
            src.writeBytes(bytes((byte)50, writableBytes));
        }
        parametricSingleWriteTest(unpersistedByteBound, writeCapacity, src, writableBytes, path, options);
        Files.delete(path);
    }

    private static Set<OpenOption> setOf(OpenOption ...options) {
        Set<OpenOption> set = new HashSet<>();
        Collections.addAll(set, options);
        return set;
    }

    private void parametricSingleWriteTest(int unpersistedByteBound, int writeCapacity, ByteBuf src, int writableBytes, Path path, Set<OpenOption> options) throws IOException {
        String summary = "unpersistedByteBound=" + unpersistedByteBound + ", writeCapacity=" + writeCapacity + ", n=" + writableBytes;
        Integer flushedBytes = null;
        Long oldFileChannelPosition = null;
        try (FileChannel channel = FileChannel.open(path, options);
             BufferedChannel b = new BufferedChannel(ByteBufAllocator.DEFAULT, channel, writeCapacity, unpersistedByteBound)) {
            BufferedChannelState old = BufferedChannelState.of(b);

            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> b.write(src), testFailed(summary));

            Assertions.assertEquals(old.getPosition() + writableBytes, b.position(), testFailed(summary));
            if (shouldFlush(writableBytes, writeCapacity, unpersistedByteBound)) {
                int bufferedBytes;
                if (unpersistedByteBound > 0) {
                    bufferedBytes = writableBytes % Math.min(writeCapacity, unpersistedByteBound);
                } else {
                    bufferedBytes = writableBytes % writeCapacity;
                }
                flushedBytes = writableBytes - bufferedBytes;
                Assertions.assertEquals(
                        old.getFileChannelPosition() + flushedBytes,
                        b.getFileChannelPosition(),
                        testFailed(summary));
                oldFileChannelPosition = old.getFileChannelPosition();
            } else {
                Assertions.assertEquals(old.getFileChannelPosition(), b.getFileChannelPosition(), testFailed(summary));
                Assertions.assertEquals(old.getNumOfBytesInWriteBuffer() + writableBytes, b.getNumOfBytesInWriteBuffer(), testFailed(summary));
                Assertions.assertEquals(old.getUnpersistedBytes() + writableBytes, b.getUnpersistedBytes(), testFailed(summary));
            }
        }

        if (shouldFlush(writableBytes, writeCapacity, unpersistedByteBound)) {
            Assertions.assertNotNull(oldFileChannelPosition);
            Assertions.assertNotNull(flushedBytes);
            checkUnderlyingFile(path, oldFileChannelPosition.intValue(), flushedBytes, src);
        }
    }

    private static String testFailed(String summary) {
        return String.format("Test %s failed", summary);
    }

    private boolean shouldFlush(int writeBytes, int writeCapacity, int unpersistedByteBound) {
        if (unpersistedByteBound > 0) {
            return writeBytes >= Math.min(writeCapacity, unpersistedByteBound);
        } else {
            return writeBytes > writeCapacity;
        }
    }

    private void checkUnderlyingFile(Path path, int position, int n, ByteBuf expected) {
        try (FileInputStream reader = new FileInputStream(path.toFile())) {
            byte[] actual = new byte[n];
            int bytesRead = reader.read(actual, position, n);
            Assertions.assertEquals(n, bytesRead);
            byte[] expectedByteArray = new byte[n];
            expected.getBytes(0, expectedByteArray);
            Assertions.assertArrayEquals(expectedByteArray, actual);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}