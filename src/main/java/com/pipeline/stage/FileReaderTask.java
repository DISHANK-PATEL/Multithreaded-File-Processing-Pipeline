package com.pipeline.stage;

import com.pipeline.core.MetricsCollector;
import com.pipeline.model.ChunkInfo;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class FileReaderTask implements Runnable {

    public static final String POISON_PILL = "__READER_POISON_PILL__";
    private final String filePath;
    private final ChunkInfo chunk;
    private final BlockingQueue<String> readerQueue;
    private final AtomicInteger activeReaders;
    private final int parserThreadCount;
    private final MetricsCollector metrics;

    public FileReaderTask(String filePath,
                          ChunkInfo chunk,
                          BlockingQueue<String> readerQueue,
                          AtomicInteger activeReaders,
                          int parserThreadCount,
                          MetricsCollector metrics) {
        this.filePath          = filePath;
        this.chunk             = chunk;
        this.readerQueue       = readerQueue;
        this.activeReaders     = activeReaders;
        this.parserThreadCount = parserThreadCount;
        this.metrics           = metrics;
    }

    @Override
    public void run() {
        System.out.printf("[Reader-%d] Starting. Chunk: %s%n",
                chunk.getChunkId(), chunk);

        try {
            readChunk();
        } catch (IOException e) {
            System.err.printf("[Reader-%d] IOException in chunk %d: %s%n",
                    chunk.getChunkId(), chunk.getChunkId(), e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.printf("[Reader-%d] Interrupted. Shutting down cleanly.%n",
                    chunk.getChunkId());
        } finally {
            signalDownstreamIfLast();
        }
    }

    private void readChunk() throws IOException, InterruptedException {
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {

            // Jump to this chunk's start position
            raf.seek(chunk.getStartByte());

            if (!chunk.isFirstChunk()) {
                String skippedLine = raf.readLine();
                if (skippedLine == null) {
                    return;
                }
            }
            long lineNumber = estimateStartLine();
            String line;

            while ((line = raf.readLine()) != null) {
                if (!chunk.isLastChunk() && raf.getFilePointer() > chunk.getEndByte()) {
                    pushLine(line, lineNumber++);
                    break;
                }

                // Skip blank lines at reader level to reduce queue pressure
                if (line.trim().isEmpty()) {
                    metrics.incrementSkippedLines();
                    lineNumber++;
                    continue;
                }
                pushLine(line, lineNumber++);
            }

            System.out.printf("[Reader-%d] Finished. Lines read from chunk.%n",
                    chunk.getChunkId());
        }
    }

    private void pushLine(String line, long lineNumber) throws InterruptedException {
        String message = lineNumber + "\t" + line;
        readerQueue.put(message);
        metrics.incrementLinesRead();
    }

    private void signalDownstreamIfLast() {
        int remaining = activeReaders.decrementAndGet();
        System.out.printf("[Reader-%d] Decremented activeReaders → %d%n",
                chunk.getChunkId(), remaining);

        if (remaining == 0) {
            System.out.printf("[Reader-%d] Last reader. Injecting %d poison pills.%n",
                    chunk.getChunkId(), parserThreadCount);

            for (int i = 0; i < parserThreadCount; i++) {
                try {
                    readerQueue.put(POISON_PILL);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.printf(
                            "[Reader-%d] Interrupted while injecting pill %d/%d%n",
                            chunk.getChunkId(), i + 1, parserThreadCount);
                }
            }
        }
    }

    private long estimateStartLine() {
        final long AVG_LINE_BYTES = 80L;
        return chunk.getStartByte() / AVG_LINE_BYTES;
    }
}