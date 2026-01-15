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
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

public class BufferedChannelTest {
    private static Path createTempFile(String name, String permissions, byte[] bytes) throws IOException {
        Path path = createTempFile(name, permissions);
        try (FileWriter writer = new FileWriter(path.toFile())) {
            for (byte b : bytes) {
                writer.write(b);
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

    private static byte[] EMPTY = new byte[0];

    // c_W, U_B, |src|, |ω| perm, opts
    private static Stream<Arguments> newInputs() {
        return Stream.of(
                Arguments.of(0, 0, 1, 0, "rw-rw-r--", setOf(StandardOpenOption.WRITE), EMPTY),
                Arguments.of(1, 1, 2, 0, "rw-rw-r--", setOf(StandardOpenOption.WRITE), EMPTY),
                Arguments.of(2047, 2046, 1024, 0, "rw-rw-r--", setOf(StandardOpenOption.WRITE), "Hello, World!".getBytes()),
                Arguments.of(2048, 2049, 667, 0, "rw-rw-r--", setOf(StandardOpenOption.WRITE), "Hello, World!".getBytes()),
                Arguments.of(511, 510, 512, 0, "rw-rw-r--", setOf(StandardOpenOption.WRITE), "Hello, World!".getBytes()),
                Arguments.of(411, 412, 413, 0, "rw-rw-r--", setOf(StandardOpenOption.WRITE), "Hello, World!".getBytes()),
                Arguments.of(0, 0, 1125, 0, "rw-rw-r--", setOf(StandardOpenOption.WRITE), EMPTY),
                Arguments.of(1233, 0, 1, 1232, "rw-rw-r--", setOf(StandardOpenOption.WRITE), EMPTY),
                Arguments.of(1233, 0, 2, 1232, "rw-rw-r--", setOf(StandardOpenOption.WRITE), "Hello, World!".getBytes()),
                Arguments.of(1233, 665, 1, 664, "rw-rw-r--", setOf(StandardOpenOption.WRITE), "Hello, World!".getBytes()),
                Arguments.of(1233, 665, 2, 664, "rw-rw-r--", setOf(StandardOpenOption.WRITE), "Hello, World!".getBytes())
        );
    }

    @ParameterizedTest
    @MethodSource("newInputs")
    public void parametricSingleWriteTest(
            int writeCapacity,
            int unpersistedByteBound,
            int writableBytes,
            int initialBufferSize,
            String permissions,
            Set<OpenOption> options,
            byte[] initialFileContent) throws IOException {
        String summary = "writeCapacity=" + writeCapacity
                + ", unpersistedByteBound=" + unpersistedByteBound
                + ", n=" + writableBytes
                + ", ω=" + initialBufferSize
                + ", |f|=" + initialFileContent.length;

        Path path;
        if (initialFileContent.length == 0) {
            path = createTempFile("file", permissions);
        } else {
            path = createTempFile("file", permissions, initialFileContent);
        }

        ByteBuf src = ByteBufAllocator.DEFAULT.buffer(writeCapacity);
        Integer flushedBytes;
        Long oldFileChannelPosition;
        try (FileChannel channel = FileChannel.open(path, options);
             BufferedChannel b = new BufferedChannel(ByteBufAllocator.DEFAULT, channel, writeCapacity, unpersistedByteBound)) {
            // Riempi il buffer di scrittura di esattamente initialBufferSize byte(s)
            if (initialBufferSize > 0) {
                ByteBuf initialContent = ByteBufAllocator.DEFAULT.buffer(initialBufferSize);
                b.write(initialContent);
                initialContent.release();
            }
            BufferedChannelState old = BufferedChannelState.of(b);

            // Crea il buffer sorgente di dimensione writableBytes
            if (writableBytes > 0) {
                src.writeBytes(bytes((byte)50, writableBytes));
            }

            // Timeout di 10 s perché ci sono dei casi di test che rimangono incastrati in un loop infinito
            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> b.write(src), testFailed(summary));

            // In seguito alla scrittura isolata di writableBytes byte(s) la posizione assoluta della prossima scrittura deve essere
            // avanzata di tanti byte quanti sono quelli che sono stati scritti!
            Assertions.assertEquals(old.getPosition() + writableBytes, b.position(), testFailed(summary));

            if (shouldFlush(writableBytes, writeCapacity, unpersistedByteBound)) {
                // Se è necessario un flush bisogna distinguere i due casi in cui il flush avviene a causa dello scavalcamento
                // della soglia oppure per il riempimento del buffer di scrittura
                if (unpersistedByteBound > 0) {
                    flushedBytes = writableBytes;
                } else {
                    flushedBytes = writeCapacity;
                }
                Assertions.assertEquals(
                        old.getFileChannelPosition() + flushedBytes,
                        b.getFileChannelPosition(),
                        testFailed(summary));
                // Si controlla se il contenuto del buffer è stato riversato nel file sottostante
                oldFileChannelPosition = old.getFileChannelPosition();
                Assertions.assertNotNull(oldFileChannelPosition);
                Assertions.assertNotNull(flushedBytes);
                checkUnderlyingFile(path, oldFileChannelPosition.intValue(), flushedBytes, src);
            } else {
                Assertions.assertEquals(old.getFileChannelPosition(), b.getFileChannelPosition(), testFailed(summary));
                Assertions.assertEquals(old.getNumOfBytesInWriteBuffer() + writableBytes, b.getNumOfBytesInWriteBuffer(), testFailed(summary));
                // se unpersistedByteBound > 0 ma non è stato effettuato alcun flush allora bisogna verificare che il numero di bytes accumulati è stato
                // aggiornato correttamente
                if (unpersistedByteBound > 0) {
                    Assertions.assertEquals(old.getUnpersistedBytes() + writableBytes, b.getUnpersistedBytes(), testFailed(summary));
                }
            }
        } finally {
            src.release();
        }
        Files.delete(path);
    }

    private static Set<OpenOption> setOf(OpenOption ...options) {
        Set<OpenOption> set = new HashSet<>();
        Collections.addAll(set, options);
        return set;
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