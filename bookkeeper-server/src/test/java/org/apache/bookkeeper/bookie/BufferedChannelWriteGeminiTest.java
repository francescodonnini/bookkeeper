package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class BufferedChannelWriteGeminiTest {
    @TempDir
    Path tempDir;

    private FileChannel createTempFileChannel() throws IOException {
        Path file = tempDir.resolve("test-file-" + System.currentTimeMillis() + ".log");
        return FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    @Test
    public void testWriteExactBufferCapacity() throws IOException {
        FileChannel fc = createTempFileChannel();
        try (FileChannel spyFc = spy(fc);
             BufferedChannel bc = new BufferedChannel(ByteBufAllocator.DEFAULT, spyFc, 10, 0L)) {

            ByteBuf data = ByteBufAllocator.DEFAULT.buffer(10);
            data.writeBytes(new byte[10]);

            bc.write(data);

            // Because we wrote exactly the capacity, writeBuffer.isWritable() becomes false,
            // triggering an immediate flush() inside the write loop.
            // Therefore, the write buffer should be empty, and the FileChannel should have been written to.
            Assertions.assertEquals(0, bc.getNumOfBytesInWriteBuffer());
            verify(spyFc, atLeastOnce()).write(any(ByteBuffer.class));

            data.release();
        }
    }

    @Test
    public void testWriteExceedingBufferCapacity() throws IOException {
        FileChannel fc = createTempFileChannel();
        try (FileChannel spyFc = spy(fc);
             BufferedChannel bc = new BufferedChannel(ByteBufAllocator.DEFAULT, spyFc, 10, 0L)) {

            ByteBuf data = ByteBufAllocator.DEFAULT.buffer(15);
            data.writeBytes(new byte[15]);

            bc.write(data);

            // It should write 10 bytes, hit capacity, flush, and write the remaining 5 bytes to the buffer
            Assertions.assertEquals(5, bc.getNumOfBytesInWriteBuffer());
            verify(spyFc, atLeastOnce()).write(any(ByteBuffer.class));

            data.release();
        }
    }

    @Test
    public void testNoForceWriteWhenBoundIsZero() throws IOException {
        FileChannel fc = createTempFileChannel();
        try (FileChannel spyFc = spy(fc);
             BufferedChannel bc = new BufferedChannel(ByteBufAllocator.DEFAULT, spyFc, 10, 0L)) {

            ByteBuf data = ByteBufAllocator.DEFAULT.buffer(50);
            data.writeBytes(new byte[50]);

            bc.write(data);

            // Flushes should happen strictly due to buffer capacity (10)
            verify(spyFc, atLeastOnce()).write(any(ByteBuffer.class));

            // Unpersisted bytes bound is 0, so forceWrite should NEVER be called
            verify(spyFc, never()).force(anyBoolean());

            data.release();
        }
    }

    @Test
    public void testWriteExactUnpersistedBytesBound() throws IOException {
        FileChannel fc = createTempFileChannel();
        try (FileChannel spyFc = spy(fc);
             BufferedChannel bc = new BufferedChannel(ByteBufAllocator.DEFAULT, spyFc, 50, 10L)) {

            ByteBuf data = ByteBufAllocator.DEFAULT.buffer(10);
            data.writeBytes(new byte[10]);

            bc.write(data);

            // Unpersisted bound is 10 and we wrote exactly 10.
            // This should trigger doRegularFlushes check and invoke forceWrite(false).
            verify(spyFc, times(1)).force(false);

            data.release();
        }
    }

    @Test
    public void testWriteLargePayloadTriggersOneForceWrite() throws IOException {
        FileChannel fc = createTempFileChannel();
        try (FileChannel spyFc = spy(fc);
             BufferedChannel bc = new BufferedChannel(ByteBufAllocator.DEFAULT, spyFc, 10, 20L)) {

            ByteBuf data = ByteBufAllocator.DEFAULT.buffer(55);
            data.writeBytes(new byte[55]);

            bc.write(data);

            // 55 bytes total:
            // - Capacity flushes inside the loop at 10, 20, 30, 40, 50 (5 flushes due to buffer size)
            // - After the loop, unpersistedBytes (55) >= unpersistedBytesBound (20).
            //   This triggers a final flush() for the remaining 5 bytes and sets shouldForceWrite = true.
            // - Therefore, spyFc.write() is called 6 times, but spyFc.force() is called exactly 1 time.
            verify(spyFc, times(6)).write(any(ByteBuffer.class));
            verify(spyFc, times(1)).force(false);

            // The buffer should be completely empty because the final bound check triggers a flush()
            Assertions.assertEquals(0, bc.getNumOfBytesInWriteBuffer());

            data.release();
        }
    }

    @Test
    public void testConcurrentWrites() throws IOException, InterruptedException {
        try (FileChannel fc = createTempFileChannel();
             BufferedChannel bc = new BufferedChannel(ByteBufAllocator.DEFAULT, fc, 50, 0L)) {

            int numThreads = 10;
            int writesPerThread = 100;
            int bytesPerWrite = 5;

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            CountDownLatch latch = new CountDownLatch(numThreads);

            for (int i = 0; i < numThreads; i++) {
                executor.submit(() -> {
                    try {
                        for (int j = 0; j < writesPerThread; j++) {
                            ByteBuf data = ByteBufAllocator.DEFAULT.buffer(bytesPerWrite);
                            data.writeBytes(new byte[bytesPerWrite]);
                            bc.write(data);
                            data.release();
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        latch.countDown();
                    }
                });
            }

            Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS), "Threads did not complete in time");
            executor.shutdown();

            bc.flush();
            long expectedTotalBytes = (long) numThreads * writesPerThread * bytesPerWrite;

            // Verify that all bytes were properly synchronized and written to the file channel
            Assertions.assertEquals(expectedTotalBytes, fc.size());
        }
    }
}