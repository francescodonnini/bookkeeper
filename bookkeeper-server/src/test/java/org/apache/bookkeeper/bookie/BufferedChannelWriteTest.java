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
import java.util.*;
import java.util.stream.Stream;

public class BufferedChannelWriteTest {
    private static final byte[] EMPTY = new byte[0];

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
                Arguments.of(3, 0, 2),
                Arguments.of(3, 0, 3),
                Arguments.of(20, 10, 5)
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

            int writeBufferSize = bc.getNumOfBytesInWriteBuffer();
            long unpersistedBytes = bc.getUnpersistedBytes();
            long filePosition = bc.getFileChannelPosition();
            long position = bc.position();

            src.writeBytes(data);
            bc.write(src);

            if (writeBufferSize + data.length >= writeCapacity || (unpersistedBytesBound > 0 && unpersistedBytes + data.length >= unpersistedBytesBound)) {
                ByteBuffer actual = ByteBuffer.allocate(data.length);
                int br = fc.read(actual, filePosition);
                Assertions.assertArrayEquals(Arrays.copyOfRange(data, 0, br), Arrays.copyOfRange(actual.array(), 0, br));
            } else {
                ByteBuf actual = ByteBufAllocator.DEFAULT.buffer(data.length);
                bc.read(actual, position);
                Assertions.assertEquals(data.length, actual.readableBytes());
                for (int i = 0; i < data.length; i++) {
                    Assertions.assertEquals(data[i], actual.getByte(i));
                }
            }
            Assertions.assertEquals(position + data.length, bc.position());
        } finally {
            Files.delete(path);
        }
    }

    private static Stream<Arguments> multipleWritesInput() {
        return Stream.of(
                Arguments.of(2, 3, 1),
                Arguments.of(2, 1, 3),
                Arguments.of(1, 2, 3),
                Arguments.of(1, 3, 2),
                Arguments.of(4, 3, 2),
                Arguments.of(3, 1, 2),
                Arguments.of(3, 0, 2),
                Arguments.of(3, 0, 3),
                Arguments.of(20, 10, 5)
        );
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