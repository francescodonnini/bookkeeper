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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

import static org.mockito.Mockito.*;

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
            empty.release();
        } finally {
            Files.delete(path);
        }
    }

    @Test
    public void closedChannelTest() throws IOException {
        Path path = TemporaryFile.create("file", "rw-rw-r--");
        FileChannel fc = FileChannel.open(path, setOf(StandardOpenOption.READ, StandardOpenOption.WRITE));
        try (BufferedChannel b = new BufferedChannel(ByteBufAllocator.DEFAULT, fc, 1, 1)) {
            ByteBuf data = ByteBufAllocator.DEFAULT.buffer(8);
            data.writeBytes(randomBytes(8));
            fc.close();
            Assertions.assertThrows(ClosedChannelException.class, () -> b.write(data));
            data.release();
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
            src.release();
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
            src.release();
            actual.release();
        } finally {
            Files.delete(path);
        }
    }

    private static Stream<Arguments> multipleWritesTestInput() {
        return Stream.of(
                Arguments.of(ints(4, 4), 0, 9),
                Arguments.of(ints(1, 2, 2), 0, 4),
                Arguments.of(ints(5, 6, 7), 0, 6),
                Arguments.of(ints(0, 0, 0, 0), 1, 2),
                Arguments.of(ints(1, 0, 1), 3, 4),
                Arguments.of(ints(12, 3, 24), 48, 64),
                Arguments.of(ints(8, 9, 10), 27, 28),
                Arguments.of(ints(3, 5, 8, 13), 6, 28),
                Arguments.of(ints(4, 4, 2), 9, 8)
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
                actual.release();
            }
        } finally {
            Files.delete(path);
        }
    }

    private static int[] ints(Integer ...ints) {
        int[] result = new int[ints.length];
        for (int i = 0; i < ints.length; i++) {
            result[i] = ints[i];
        }
        return result;
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

    private static Stream<Arguments> ioForceTestInputs() {
        return Stream.of(
                Arguments.of(11, 10, ints(4, 4, 2)),
                Arguments.of(19, 20, ints(8, 8, 5)),
                Arguments.of(19, 20, ints(8, 8, 3)),
                Arguments.of(9, 10, ints(4, 4, 1))
        );
    }

    @ParameterizedTest
    @MethodSource("ioForceTestInputs")
    public void ioForceTest(int writeCapacity, int unpersistedBytesBound, int[] inputSize) throws IOException {
        Path path = TemporaryFile.create("file", "rw-rw-r--");
        FileChannel fc = FileChannel.open(path, setOf(StandardOpenOption.WRITE, StandardOpenOption.READ));
        int totalSize = Arrays.stream(inputSize).sum();
        byte[] data = new byte[totalSize];
        try (FileChannel mock = spy(fc);
             BufferedChannel bc = new BufferedChannel(ByteBufAllocator.DEFAULT, mock, writeCapacity, unpersistedBytesBound)) {

            int forceCount = 0;
            int bytesWritten = 0;
            int flushedBytes = 0;
            int unpersistedBytes = 0;
            for (int size : inputSize) {
                ByteBuf src = ByteBufAllocator.DEFAULT.buffer(size);
                byte[] bytes = randomBytes(size);
                System.arraycopy(bytes, 0, data, bytesWritten, size);
                src.writeBytes(bytes);
                bc.write(src);
                unpersistedBytes += size;
                bytesWritten += size;

                if (unpersistedBytes < unpersistedBytesBound) {
                    Assertions.assertTrue(bc.getUnpersistedBytes() < unpersistedBytesBound);
                } else {
                    forceCount++;
                    unpersistedBytes -= unpersistedBytesBound;
                    flushedBytes = bytesWritten;
                }
                verify(mock, times(forceCount)).force(anyBoolean());
                src.release();
            }
            if (forceCount > 0) {
                byte[] actual = Files.readAllBytes(path);
                Assertions.assertArrayEquals(data, Arrays.copyOfRange(actual, 0, flushedBytes));
            }
        } finally {
            Files.delete(path);
        }
    }
}