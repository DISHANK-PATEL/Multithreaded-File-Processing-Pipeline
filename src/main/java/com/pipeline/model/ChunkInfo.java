package com.pipeline.model;

public class ChunkInfo {

    private final int chunkId;
    private final long startByte;
    private final long endByte;
    private final int totalChunks;

    public ChunkInfo(int chunkId, long startByte, long endByte, int totalChunks) {
        if (startByte < 0)
            throw new IllegalArgumentException("startByte must be >= 0, got: " + startByte);
        if (endByte < startByte)
            throw new IllegalArgumentException(
                    "endByte must be >= startByte, got: endByte=" + endByte + " startByte=" + startByte);

        this.chunkId     = chunkId;
        this.startByte   = startByte;
        this.endByte     = endByte;
        this.totalChunks = totalChunks;
    }

    public boolean isFirstChunk() {
        return chunkId == 0;
    }

    public boolean isLastChunk() {
        return chunkId == totalChunks - 1;
    }

    public long chunkSizeBytes() {
        return endByte - startByte;
    }

    public int  getChunkId()     { return chunkId; }
    public long getStartByte()   { return startByte; }
    public long getEndByte()     { return endByte; }
    public int  getTotalChunks() { return totalChunks; }

    @Override
    public String toString() {
        return String.format(
                "ChunkInfo{id=%d/%d, start=%d, end=%d, size=%d bytes, first=%b, last=%b}",
                chunkId, totalChunks - 1,
                startByte, endByte, chunkSizeBytes(),
                isFirstChunk(), isLastChunk()
        );
    }
}