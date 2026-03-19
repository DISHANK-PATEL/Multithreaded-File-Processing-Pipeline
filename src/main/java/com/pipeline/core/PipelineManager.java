package com.pipeline.core;

import com.pipeline.model.ChunkInfo;
import com.pipeline.model.LogRecord;
import com.pipeline.stage.DataParserTask;
import com.pipeline.stage.FileReaderTask;
import com.pipeline.stage.ResultAggregator;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class PipelineManager {

    private final String           filePath;
    private final int              readerThreadCount;
    private final int              parserThreadCount;
    private final int              aggregatorThreadCount;
    private final int              queueCapacity;
    private final MetricsCollector metrics;

    private ExecutorService                  executor;
    private BlockingQueue<String>            readerQueue;
    private BlockingQueue<LogRecord>         parserQueue;
    private ConcurrentHashMap<String, Long>  wordFrequencyMap;

    public PipelineManager(String filePath,
                           int readerThreadCount,
                           int parserThreadCount,
                           int aggregatorThreadCount,
                           int queueCapacity,
                           MetricsCollector metrics) {
        this.filePath               = filePath;
        this.readerThreadCount      = readerThreadCount;
        this.parserThreadCount      = parserThreadCount;
        this.aggregatorThreadCount  = aggregatorThreadCount;
        this.queueCapacity          = queueCapacity;
        this.metrics                = metrics;
    }

    public void run() throws InterruptedException {

        readerQueue      = new LinkedBlockingQueue<>(queueCapacity);
        parserQueue      = new LinkedBlockingQueue<>(queueCapacity);
        wordFrequencyMap = new ConcurrentHashMap<>();

        List<ChunkInfo> chunks = buildChunks();
        if (chunks.isEmpty()) {
            System.err.println("[PipelineManager] No chunks built — file may be empty.");
            metrics.stop();
            return;
        }

        AtomicInteger activeReaders     = new AtomicInteger(readerThreadCount);
        AtomicInteger activeAggregators = new AtomicInteger(aggregatorThreadCount);
        Object        completionLock    = new Object();
        AtomicBoolean streamComparisonDone = new AtomicBoolean(false);

        int cores      = Runtime.getRuntime().availableProcessors();
        int totalTasks = readerThreadCount + parserThreadCount + aggregatorThreadCount;
        int poolSize   = totalTasks + (cores * 2);
        executor = Executors.newWorkStealingPool(poolSize);


        System.out.printf(
                "[PipelineManager] Pool=%d  readers=%d  parsers=%d  aggregators=%d  cores=%d%n",
                poolSize, readerThreadCount, parserThreadCount, aggregatorThreadCount, cores);
        System.out.printf(
                "[PipelineManager] ReaderQueue=%d  ParserQueue=%d%n",
                queueCapacity, queueCapacity);

        for (int i = 0; i < aggregatorThreadCount; i++) {
            executor.submit(new ResultAggregator(
                    parserQueue,
                    wordFrequencyMap,
                    activeAggregators,
                    completionLock,
                    streamComparisonDone,
                    metrics
            ));
        }

        for (int i = 0; i < parserThreadCount; i++) {
            executor.submit(new DataParserTask(
                    readerQueue,
                    parserQueue,
                    activeAggregators,
                    aggregatorThreadCount,
                    metrics
            ));
        }

        Thread.sleep(200);
        metrics.start();

        for (int i = 0; i < readerThreadCount; i++) {
            executor.submit(new FileReaderTask(
                    filePath,
                    chunks.get(i),
                    readerQueue,
                    activeReaders,
                    parserThreadCount,
                    metrics
            ));
        }

        synchronized (completionLock) {
            while (activeAggregators.get() > 0) {
                completionLock.wait(5000);
            }
        }

        executor.shutdown();
        boolean terminated = executor.awaitTermination(60, TimeUnit.SECONDS);
        if (!terminated) {
            System.err.println("[PipelineManager] WARNING: forcing shutdownNow()");
            executor.shutdownNow();
        }

        metrics.stop();

        System.out.printf("[PipelineManager] Complete. WordMap=%d unique terms.%n",
                wordFrequencyMap.size());

        printTopWords(10);
    }
    private List<ChunkInfo> buildChunks() {
        File file     = new File(filePath);
        long fileSize = file.length();

        if (fileSize == 0) return new ArrayList<>();

        List<ChunkInfo> chunks    = new ArrayList<>(readerThreadCount);
        long            chunkSize = fileSize / readerThreadCount;

        System.out.printf("[PipelineManager] File=%d bytes  chunkSize=%d bytes%n",
                fileSize, chunkSize);

        for (int i = 0; i < readerThreadCount; i++) {
            long start = (long) i * chunkSize;
            long end   = (i == readerThreadCount - 1)
                    ? fileSize
                    : (long)(i + 1) * chunkSize;

            ChunkInfo chunk = new ChunkInfo(i, start, end, readerThreadCount);
            chunks.add(chunk);
            System.out.printf("[PipelineManager] %s%n", chunk);
        }

        return chunks;
    }

    public ConcurrentHashMap<String, Long> getWordFrequencyMap() {
        return wordFrequencyMap;
    }

    private void printTopWords(int n) {
        if (wordFrequencyMap == null || wordFrequencyMap.isEmpty()) {
            System.out.println("[PipelineManager] Word frequency map is empty.");
            return;
        }
        System.out.println("\n[PipelineManager] Top " + n + " most frequent terms:");
        wordFrequencyMap.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                .limit(n)
                .forEach(e -> System.out.printf("  %-20s : %d%n",
                        e.getKey(), e.getValue()));
    }
}