package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

public class BufferedChannelWriteTest {
    @Test
    public void nullSourceTest() throws IOException {
        Path path = TemporaryFile.create("file", "rw-rw-r--");
        try (FileChannel fc = FileChannel.open(path, setOf(StandardOpenOption.WRITE));
             BufferedChannel b = new BufferedChannel(ByteBufAllocator.DEFAULT, fc, 100, 1)) {
            Assertions.assertThrows(NullPointerException.class, () -> b.write(null));
        } finally {
            Files.delete(path);
        }
    }

    @Test
    public void emptySourceTest() throws IOException {
        Path path = TemporaryFile.create("file", "rw-rw-r--");
        try (FileChannel fc = FileChannel.open(path, setOf(StandardOpenOption.WRITE));
             BufferedChannel b = new BufferedChannel(ByteBufAllocator.DEFAULT, fc, 1)) {
            ByteBuf empty = ByteBufAllocator.DEFAULT.buffer(0);
            b.write(empty);
            Assertions.assertEquals(0, b.getUnpersistedBytes());
            Assertions.assertEquals(0, b.getNumOfBytesInWriteBuffer());
            Assertions.assertEquals(0, b.getFileChannelPosition());
            Assertions.assertEquals(0, b.position());
            Assertions.assertEquals(0, b.size());
        } finally {
            Files.delete(path);
        }
    }

    private static Stream<Arguments> readOnlyFileTestInputs() {
        return Stream.of(
                Arguments.of(3, 2, 2),
                Arguments.of(3, 2, 4)
        );
    }

    @ParameterizedTest
    @MethodSource("readOnlyFileTestInputs")
    public void readOnlyFileTest(int inputSize, int unpersistedBytesBound, int writeCapacity) throws IOException {
        Path path = TemporaryFile.create("ro", "r--r--r--");
        try (FileChannel fc = FileChannel.open(path, setOf(StandardOpenOption.READ));
             BufferedChannel b = new BufferedChannel(ByteBufAllocator.DEFAULT, fc, writeCapacity, unpersistedBytesBound)) {
            ByteBuf src = ByteBufAllocator.DEFAULT.buffer(8);
            src.writeBytes(randomBytes(inputSize));
            Assertions.assertThrows(NonWritableChannelException.class, () -> b.write(src));
        } finally {
            Files.delete(path);
        }
    }

    private static Stream<Arguments> singleWriteInput() {
        return Stream.of(
                Arguments.of(2, 3, 1),
                Arguments.of(2, 1, 3),
                Arguments.of(1, 2, 3),
                Arguments.of(1, 3, 2),
                Arguments.of(4, 3, 2),
                Arguments.of(3, 1, 2),
                Arguments.of(2, 0, 3),
                Arguments.of(3, 0, 2),
                Arguments.of(3, 0, 3)
        );
    }

    @ParameterizedTest
    @MethodSource("singleWriteInput")
    public void singleWriteTest(int inputSize, int unpersistedBytesBound, int writeCapacity) throws IOException {
        Path path = TemporaryFile.create("file", "rw-rw-r--");
        try (FileChannel fc = FileChannel.open(path, setOf(StandardOpenOption.WRITE, StandardOpenOption.READ));
             BufferedChannel bc = new BufferedChannel(ByteBufAllocator.DEFAULT, fc, writeCapacity, unpersistedBytesBound)) {
            byte[] data = randomBytes(inputSize);
            ByteBuf src = ByteBufAllocator.DEFAULT.buffer(data.length);

            long filePosition = bc.getFileChannelPosition();
            long position = bc.position();

            src.writeBytes(data);
            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> bc.write(src));

            if (unpersistedBytesBound > 0 && data.length > unpersistedBytesBound) {
                ByteBuffer fileBuffer = ByteBuffer.allocate(data.length);
                int br = fc.read(fileBuffer, filePosition);
                Assertions.assertArrayEquals(Arrays.copyOfRange(data, 0, br), Arrays.copyOfRange(fileBuffer.array(), 0, br));
            }

            ByteBuf actual = ByteBufAllocator.DEFAULT.buffer(data.length);
            bc.read(actual, position);
            Assertions.assertEquals(data.length, actual.readableBytes());
            for (int i = 0; i < data.length; i++) {
                Assertions.assertEquals(data[i], actual.getByte(i));
            }

            Assertions.assertEquals(position + data.length, bc.position());
        } finally {
            Files.delete(path);
        }
    }

    private static Stream<Arguments> multipleWritesTestInput() {
        return Stream.of(
                Arguments.of(new int[] {4, 4}, 9, 7),
                Arguments.of(new int[] {1, 1, 1}, 2, 4),
                Arguments.of(new int[] {2, 3, 2}, 8, 9),
                Arguments.of(new int[] {3, 4, 5, 3}, 17, 16),
                Arguments.of(new int[] {128, 357, 180, 180, 180}, 1024, 1023),
                Arguments.of(new int[] {6553, 6554, 6553, 8190, 12457, 7643, 17586}, 65534, 65535),
                Arguments.of(new int[] {3, 2, 2}, 8, 5),
                Arguments.of(new int[] {3, 2, 2}, 5, 8)
        );
    }

    @ParameterizedTest
    @MethodSource("multipleWritesTestInput")
    public void multipleWritesTest(int[] inputsSize, int unpersistedBytesBound, int writeCapacity) throws IOException {
        Path path = TemporaryFile.create("file", "rw-rw-r--");
        try (FileChannel fc = FileChannel.open(path, setOf(StandardOpenOption.WRITE, StandardOpenOption.READ));
             BufferedChannel bc = new BufferedChannel(ByteBufAllocator.DEFAULT, fc, writeCapacity, unpersistedBytesBound)) {
            for (int inputSize : inputsSize) {
                long p = bc.position();
                ByteBuf expected = ByteBufAllocator.DEFAULT.buffer(inputSize);
                byte[] data = randomBytes(inputSize);
                expected.writeBytes(data);
                bc.write(expected);

                ByteBuf actual = ByteBufAllocator.DEFAULT.buffer(inputSize);
                bc.read(actual, p);
                Assertions.assertEquals(inputSize, actual.readableBytes());
                for (int i = 0; i < inputSize; ++i) {
                    Assertions.assertEquals(data[i], actual.getByte(i));
                }

                Assertions.assertEquals(p + inputSize, bc.position());
                expected.release();
            }
        } finally {
            Files.delete(path);
        }
    }

    private static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        new Random().nextBytes(bytes);
        return bytes;
    }

    private static Set<OpenOption> setOf(OpenOption ...options) {
        Set<OpenOption> set = new HashSet<>();
        Collections.addAll(set, options);
        return set;
    }
}