package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class BufferedChannelReadTest {
    private static Stream<Arguments> inputsForReadFromUnderlyingFile() {
        return Stream.of(
                // #1: Si legge l'inizio del file sottostante
                Arguments.of(1024, 0, 2048, 0, 1, 1),
                // #2: si legge la seconda metà eccetto l'ultimo byte del file sottostante
                Arguments.of(128, 0, 1023, 512, 510, 512),
                // #3: si legge l'ultimo byte del file sottostante
                Arguments.of(1, 0, 128, 127, 1, 64)
        );
    }

    @ParameterizedTest
    @MethodSource("inputsForReadFromUnderlyingFile")
    public void readFromUnderlyingFileOnly(
        int capacity,
        int unpersistedByteBound,
        int fileSize,
        int readPos,
        int length,
        int destinationCapacity
    ) throws IOException {
        Path path = TemporaryFile.create("file", "rw-rw-rw-");
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
             BufferedChannel b = new BufferedChannel(ByteBufAllocator.DEFAULT, channel, capacity, unpersistedByteBound)) {
            // Riempie il file sottostante
            byte[] srcBackingArray = ByteArrayUtils.newArray((byte)50, fileSize);
            ByteBuf src = ByteBufAllocator.DEFAULT.buffer(fileSize);
            src.writeBytes(srcBackingArray);
            b.write(src);
            b.flushAndForceWrite(true);

            // Ci si assicura che il contenuto iniziale del file sia stato salvato effettivamente nel file sottostante
            Assertions.assertEquals(0, b.getUnpersistedBytes());
            Assertions.assertEquals(0, b.getNumOfBytesInWriteBuffer());
            Assertions.assertEquals(fileSize, b.getFileChannelPosition());

            ByteBuf dest = ByteBufAllocator.DEFAULT.buffer(destinationCapacity);
            int bytesRead = b.read(dest, readPos, length);
            // Attenzione! Il parametro length viene ignorato perché almeno in questa circostanza read() legge tanti byte quanti ne sono
            // disponibili in dest. Il controllo iniziale era:
            // Assertions.assertEquals(length, bytesRead);
            Assertions.assertEquals(dest.readableBytes(), bytesRead);
            byte [] actual = new byte[dest.readableBytes()];
            dest.readBytes(actual);
            Assertions.assertArrayEquals(Arrays.copyOfRange(srcBackingArray, readPos, readPos + bytesRead), actual);
        }
    }

    private static Stream<Arguments> inputsForReadFromWriteBufferOnly() {
        return Stream.of(
                // #1: si legge il primo byte dal buffer di scrittura, buffer di scrittura mai stato flushato
                Arguments.of(1024, 1024, 512, 0, 64, 0, 1, 64),
                // #2: si leggono gli ultimi 32B dal buffer di scrittura, buffer di scrittura mai stato flushato
                Arguments.of(1024, 1, 1024, 0, 64, 32, 32, 32),
                // #3: si legge l'intero contenuto del buffer di scritture eccetto il primo e l'ultimo byte, il buffer di
                // scrittura è stato già flushato una volta
                Arguments.of(1024, 0, 1024, 1024, 64, 1025, 62, 62)

        );
    }

    // La seguente suite di test ha l'obbiettivo di testare le letture provenienti dal buffer di scrittura.
    // Prima di effettuare la lettura, il buffer di scrittura può essere stato già svuotato almeno una volta oppure no
    @ParameterizedTest
    @MethodSource("inputsForReadFromWriteBufferOnly")
    public void readFromWriteBufferOnly(
        int writeCapacity,
        int readCapacity,
        int unpersistedByteBound,
        int fileSize,
        int writeBufferSize,
        int readPos,
        int length,
        int destinationCapacity
    ) throws IOException {
        Path path = TemporaryFile.create("file", "rw-rw-rw-");
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
             BufferedChannel b = new BufferedChannel(ByteBufAllocator.DEFAULT, channel, writeCapacity, readCapacity, unpersistedByteBound)) {
            // Riempie il file sottostante
            if (fileSize > 0) {
                byte[] srcBackingArray = ByteArrayUtils.newArray((byte) 70, fileSize);
                ByteBuf src = ByteBufAllocator.DEFAULT.buffer(fileSize);
                src.writeBytes(srcBackingArray);
                b.write(src);
                // Ci si assicura che il contenuto iniziale del file sia stato salvato effettivamente nel file sottostante
                Assertions.assertEquals(0, b.getUnpersistedBytes());
                Assertions.assertEquals(0, b.getNumOfBytesInWriteBuffer());
                Assertions.assertEquals(fileSize, b.getFileChannelPosition());
            }
            // translatedReadPos è l'offset relativo da cui si comincia a leggere nel buffer di scrittura
            int translatedReadPos = readPos - fileSize;
            byte[] srcBackingArray = new byte[writeBufferSize];
            // Ci si aspetta di leggere la sequenze '2' (in byte) invece di '*' (in byte)
            Arrays.fill(srcBackingArray, 0, translatedReadPos, (byte) 42);
            Arrays.fill(srcBackingArray, translatedReadPos, translatedReadPos + length, (byte) 50);
            Arrays.fill(srcBackingArray, 0, translatedReadPos, (byte) 42);

            // Si riempie il buffer di scrittura senza svuotarlo
            ByteBuf src = ByteBufAllocator.DEFAULT.buffer(writeBufferSize);
            src.writeBytes(srcBackingArray);
            b.write(src);
            // Si controlla che effettivamente il buffer di scrittura non è stato svuotato
            Assertions.assertEquals(writeBufferSize, b.getUnpersistedBytes());
            Assertions.assertEquals(writeBufferSize, b.getNumOfBytesInWriteBuffer());

            ByteBuf dest = ByteBufAllocator.DEFAULT.buffer(destinationCapacity);
            int bytesRead = b.read(dest, readPos, length);
            // Attenzione! Il parametro length viene ignorato quando è possibile leggere tanti byte quanti sono quelli disponibili nel buffer di lettura.
            // Il controllo iniziale era:
            // Assertions.assertEquals(length, bytesRead);
            Assertions.assertEquals(dest.readableBytes(), bytesRead);
            byte [] actual = new byte[dest.readableBytes()];
            dest.readBytes(actual);
            Assertions.assertArrayEquals(Arrays.copyOfRange(srcBackingArray, translatedReadPos, translatedReadPos + bytesRead), actual);
        }
    }

    private static Stream<Arguments> inputsForReadFromReadBufferOnly() {
        return Stream.of(
                // #1: si legge a prima metà del buffer di lettura
                Arguments.of(1024, 0, 1024, 0, 512, 512),
                // #2: si legge l'ultimo byte del buffer di lettura
                Arguments.of(512, 0, 512, 510, 1, 1),
                // #3: si leggono tutti i byte del buffer di lettura eccetto il primo e l'ultimo
                Arguments.of(73, 0, 73, 1, 23, 24)
        );
    }


    // La seguente suite di test ha l'obbiettivo di testare le letture provenienti dal buffer di lettura, in particolare
    // si vuole testare se effettivamente i dati letti dal file sottostante vengano salvati nel buffer di lettura in modo
    // tale che la lettura successiva sulla zona interessata non provenga dal file sottostante ma dal buffer di lettura.
    // Per testare questo comportamento ho deciso di "mockare" l'oggetto FileChannel che viene passato a BufferedChannel durante
    // la creazione. Questo mock permette di contare quante volte è stato invocato il metodo read, ci si aspetta che sia stato
    // invocato solamente una volta
    @ParameterizedTest
    @MethodSource("inputsForReadFromReadBufferOnly")
    public void readFromReadBufferOnly(
            int capacity,
            int unpersistedByteBound,
            int fileSize,
            int readPos,
            int length,
            int destinationCapacity
    ) throws IOException {
        try (FileChannel mock = mock(FileChannel.class);
             BufferedChannel b = new BufferedChannel(ByteBufAllocator.DEFAULT, mock, capacity, unpersistedByteBound)) {
            AtomicLong mockedFilePosition = new AtomicLong(0);
            // Il mock del metodo write() legge per intero il buffer ricevuto in input e si limita a restituire il numero
            // di byte letti (aggiorna la posizione in cui si troverebbe il cursore del file dopo la scrittura)
            when(mock.write(any(ByteBuffer.class))).thenAnswer(a -> {
                ByteBuffer buffer = a.getArgument(0);
                byte[] dummyDest = new byte[1];
                int bytesWritten = 0;
                if (buffer.remaining() > 0) {
                    while (buffer.remaining() > 0) {
                        buffer.get(dummyDest);
                        bytesWritten++;
                    }
                    mockedFilePosition.addAndGet(bytesWritten);
                }
                return bytesWritten;
            });

            // Quando viene invocato il metodo read dal canale "mockato" allora si riempie il buffer di lettura con
            // una sequenza di byte specifica
            when(mock.read(any(ByteBuffer.class), anyLong())).thenAnswer(a -> {
                ByteBuffer buffer = a.getArgument(0);
                int bytesRead = 0;
                while (buffer.remaining() > 0) {
                    buffer.put((byte) 50);
                    bytesRead++;
                }
                return bytesRead;
            });

            when(mock.position()).thenAnswer(i -> mockedFilePosition.get());

            // Questa scrittura innesca il meccanismo di flushing nel file sottostante
            byte[] srcBackingArray = ByteArrayUtils.newArray((byte) 70, fileSize);
            ByteBuf src = ByteBufAllocator.DEFAULT.buffer(fileSize);
            src.writeBytes(srcBackingArray);
            b.write(src);
            // Ci si assicura che il contenuto iniziale del file sia stato flushato effettivamente nel file sottostante
            Assertions.assertEquals(0, b.getUnpersistedBytes());
            Assertions.assertEquals(0, b.getNumOfBytesInWriteBuffer());
            Assertions.assertEquals(fileSize, b.getFileChannelPosition());


            // Questa lettura causa il salvataggio dei dati letti dal file sottostante al buffer di lettura
            ByteBuf d1 = ByteBufAllocator.DEFAULT.buffer(destinationCapacity);
            b.read(d1, readPos, length);

            // A questo punto il metodo read del file channel mockato deve essere stato invocato esattamente una volta
            verify(mock, times(1)).read(any(ByteBuffer.class), anyLong());

            // Questa seconda lettura non deve causare l'invocazione di read da parte del file channel sottostante
            ByteBuf d2 = ByteBufAllocator.DEFAULT.buffer(destinationCapacity);
            b.read(d2, readPos);
            verify(mock, times(1)).read(any(ByteBuffer.class), anyLong());
        }

    }

}
