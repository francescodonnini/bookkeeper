package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

public class BufferedChannelTest {
    private static class BufferedChannelState {
        private final long position;
        private final long fileChannelPosition;
        private final long numOfBytesInWriteBuffer;
        private final long unpersistedBytes;

        public static BufferedChannelState of(BufferedChannel channel) {
            return new BufferedChannelState(channel.position(), channel.getFileChannelPosition(), channel.getNumOfBytesInWriteBuffer(), channel.getUnpersistedBytes());
        }

        private BufferedChannelState(long position, long fileChannelPosition, long numOfBytesInWriteBuffer, long unpersistedBytes) {
            this.position = position;
            this.fileChannelPosition = fileChannelPosition;
            this.numOfBytesInWriteBuffer = numOfBytesInWriteBuffer;
            this.unpersistedBytes = unpersistedBytes;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            BufferedChannelState that = (BufferedChannelState) o;
            return position == that.position && fileChannelPosition == that.fileChannelPosition && numOfBytesInWriteBuffer == that.numOfBytesInWriteBuffer && unpersistedBytes == that.unpersistedBytes;
        }

        @Override
        public int hashCode() {
            return Objects.hash(position, fileChannelPosition, numOfBytesInWriteBuffer, unpersistedBytes);
        }

        public long getPosition() {
            return position;
        }

        public long getFileChannelPosition() {
            return fileChannelPosition;
        }

        public long getNumOfBytesInWriteBuffer() {
            return numOfBytesInWriteBuffer;
        }

        public long getUnpersistedBytes() {
            return unpersistedBytes;
        }
    }

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
                Arguments.of("rw-rw-r--", 0, 1023, 1024, setOf(StandardOpenOption.WRITE)),
                Arguments.of("rw-rw-r--", 0, 1024, 1024, setOf(StandardOpenOption.WRITE)),
                Arguments.of("rw-rw-r--", 0, 1025, 1024, setOf(StandardOpenOption.WRITE)),
                Arguments.of("rw-rw-r--", 1023, 1024, 1024, setOf(StandardOpenOption.WRITE)),
                Arguments.of("rw-rw-r--", 1023, 1025, 1024, setOf(StandardOpenOption.WRITE)),
                Arguments.of("rw-rw-r--", 1024, 1025, 1024, setOf(StandardOpenOption.WRITE)),
                Arguments.of("rw-rw-r--", 1024, 1023, 1024, setOf(StandardOpenOption.WRITE)),
                Arguments.of("rw-rw-r--", 1023, 1024, 1024, setOf(StandardOpenOption.WRITE))
        );
    }

    @ParameterizedTest
    @MethodSource("inputs")
    public void parametricSingleWriteTest(String permissions, int unpersistedByteBound, int writeCapacity, int writableBytes, Set<OpenOption> options) throws IOException {
        Path path = createTempFile("file", permissions);
        ByteBuf src = ByteBufAllocator.DEFAULT.buffer(writeCapacity);
        if (writableBytes > 0) {
            src.writeBytes(bytes((byte)50, writableBytes));
        }
        parametricSingleWriteTest(unpersistedByteBound, writeCapacity, src, writableBytes, path, options);
        Files.delete(path);
    }

    private static Set<OpenOption> setOf(OpenOption ...options) {
        Set<OpenOption> set = new HashSet<>();
        for (OpenOption option : options) {
            set.add(option);
        }
        return set;
    }

    private void parametricSingleWriteTest(int unpersistedByteBound, int writeCapacity, ByteBuf src, int writableBytes, Path path, Set<OpenOption> options) throws IOException {
        try (FileChannel channel = FileChannel.open(path, options);
             BufferedChannel b = new BufferedChannel(ByteBufAllocator.DEFAULT, channel, writeCapacity, unpersistedByteBound)) {
            boolean appendMode = options.contains(StandardOpenOption.APPEND);
            BufferedChannelState old = BufferedChannelState.of(b);
            b.write(src);
            Assertions.assertEquals(old.getPosition() + writableBytes, b.position());
            if (shouldFlush(writableBytes, writeCapacity, unpersistedByteBound)) {
                if (appendMode) {
                    Assertions.assertEquals(old.getFileChannelPosition() + writableBytes, b.getFileChannelPosition());
                }
                checkUnderlyingFile(path, (int) old.fileChannelPosition, Math.min(writeCapacity, unpersistedByteBound), src);
            } else {
                Assertions.assertEquals(old.getFileChannelPosition(), b.getFileChannelPosition());
                Assertions.assertEquals(old.getNumOfBytesInWriteBuffer() + writableBytes, b.getNumOfBytesInWriteBuffer());
                Assertions.assertEquals(old.getUnpersistedBytes() + writableBytes, b.getUnpersistedBytes());
            }
        }
    }

    private boolean shouldFlush(int writeBytes, int writeCapacity, int unpersistedByteBound) {
        if (unpersistedByteBound > 0) {
            return writeBytes >= Math.min(writeCapacity, unpersistedByteBound);
        }
        return unpersistedByteBound == 0;
    }

    private void checkUnderlyingFile(Path path, int position, int n, ByteBuf expected) {
        try (FileInputStream reader = new FileInputStream(path.toFile())) {
            byte[] actual = new byte[n];
            reader.read(actual, position, n);
            byte[] expectedByteArray = new byte[n];
            expected.getBytes(0, expectedByteArray);
            Assertions.assertArrayEquals(expectedByteArray, actual);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}