package org.apache.bookkeeper.bookie;

import java.util.Objects;

class BufferedChannelState {
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