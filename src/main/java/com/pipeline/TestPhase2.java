package com.pipeline;

import com.pipeline.core.MetricsCollector;
import com.pipeline.model.ChunkInfo;
import com.pipeline.model.LogRecord;
import com.pipeline.stage.FileReaderTask;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestPhase2 {

    public static void main(String[] args) throws Exception {

        System.out.println("========================================");
        System.out.println("  PHASE 1 + 2 TEST");
        System.out.println("========================================");

        // ── PHASE 1: Test LogRecord ──
        System.out.println("\n--- LogRecord Tests ---");

        // Normal record
        LogRecord r = new LogRecord(
                "2024-01-15 10:00:01 INFO Hello world",
                "INFO", 1L, false,
                "2024-01-15 10:00:01",
                List.of("hello", "world")
        );
        System.out.println(r);
        System.out.println("isPoisonPill : " + r.isPoisonPill());  // false
        System.out.println("isError      : " + r.isError());       // false
        System.out.println("words        : " + r.getWords());      // [hello, world]
        System.out.println("timestamp    : " + r.getTimestamp());  // 2024-01-15 10:00:01

        // Poison pill
        LogRecord pill = LogRecord.POISON_PILL;
        System.out.println("\n" + pill);
        System.out.println("isPoisonPill : " + pill.isPoisonPill()); // true

        // Error record
        LogRecord err = new LogRecord("!!!malformed%%%", 42L);
        System.out.println("\n" + err);
        System.out.println("isError      : " + err.isError());     // true

        // ── PHASE 1: Test ChunkInfo ──
        System.out.println("\n--- ChunkInfo Tests ---");

        ChunkInfo first = new ChunkInfo(0, 0, 1000, 4);
        System.out.println(first);
        System.out.println("isFirstChunk : " + first.isFirstChunk()); // true
        System.out.println("isLastChunk  : " + first.isLastChunk());  // false

        ChunkInfo last = new ChunkInfo(3, 3000, 4000, 4);
        System.out.println(last);
        System.out.println("isFirstChunk : " + last.isFirstChunk());  // false
        System.out.println("isLastChunk  : " + last.isLastChunk());   // true

        // ── PHASE 1: Test MetricsCollector ──
        System.out.println("\n--- MetricsCollector Tests ---");

        MetricsCollector metrics = new MetricsCollector();
        metrics.start();
        metrics.incrementLinesRead();
        metrics.incrementLinesRead();
        metrics.incrementLinesParsed();
        metrics.incrementErrorLines();
        metrics.incrementSkippedLines();
        metrics.stop();

        System.out.println("linesRead   : " + metrics.getTotalLinesRead());   // 2
        System.out.println("linesParsed : " + metrics.getTotalLinesParsed()); // 1
        System.out.println("errorLines  : " + metrics.getErrorLines());       // 1
        System.out.println("skipped     : " + metrics.getSkippedLines());     // 1
        System.out.println("elapsedMs   : " + metrics.getElapsedMs() + "ms");

        // ── PHASE 2: Test FileReaderTask ──
        System.out.println("\n--- FileReaderTask Test ---");

        String filePath = "src/main/resources/input.txt";
        File file = new File(filePath);

        if (!file.exists()) {
            System.err.println("input.txt not found at: " + file.getAbsolutePath());
            System.err.println("Place your input.txt in src/main/resources/ and retry.");
            return;
        }

        System.out.println("File size    : " + file.length() + " bytes");

        int readerThreads = 2;
        int parserThreads = 2;

        MetricsCollector readerMetrics = new MetricsCollector();
        readerMetrics.start();

        BlockingQueue<String> readerQueue = new LinkedBlockingQueue<>();
        AtomicInteger activeReaders = new AtomicInteger(readerThreads);

        long fileSize  = file.length();
        long chunkSize = fileSize / readerThreads;

        ExecutorService exec = Executors.newFixedThreadPool(4);

        for (int i = 0; i < readerThreads; i++) {
            long start = (long) i * chunkSize;
            long end   = (i == readerThreads - 1)
                    ? fileSize
                    : (long)(i + 1) * chunkSize;

            ChunkInfo chunk = new ChunkInfo(i, start, end, readerThreads);
            exec.submit(new FileReaderTask(
                    filePath, chunk, readerQueue,
                    activeReaders, parserThreads, readerMetrics
            ));
        }

        exec.shutdown();
        exec.awaitTermination(30, TimeUnit.SECONDS);
        readerMetrics.stop();

        // Count real lines vs poison pills in queue
        int lineCount = 0;
        int pillCount = 0;
        while (!readerQueue.isEmpty()) {
            String item = readerQueue.poll();
            if (FileReaderTask.POISON_PILL.equals(item)) pillCount++;
            else lineCount++;
        }

        System.out.println("\n--- FileReaderTask Results ---");
        System.out.println("Lines in queue  : " + lineCount);
        System.out.println("Poison pills    : " + pillCount
                + " (expected: " + parserThreads + ")");
        System.out.println("Metrics linesRead: " + readerMetrics.getTotalLinesRead());

        readerMetrics.printReport(readerThreads);

        System.out.println("\n========================================");
        System.out.println("  ALL PHASE 1 + 2 TESTS COMPLETE");
        System.out.println("========================================");
    }
}