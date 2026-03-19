package com.pipeline.stage;

import com.pipeline.core.MetricsCollector;
import com.pipeline.model.LogRecord;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

public class ResultAggregator implements Runnable {

    private final BlockingQueue<LogRecord> parserQueue;
    private final ConcurrentHashMap<String, Long> wordFrequencyMap;
    private final AtomicInteger activeAggregators;
    private final Object completionLock;
    private final MetricsCollector metrics;
    private final AtomicBoolean streamComparisonDone;

    public ResultAggregator(BlockingQueue<LogRecord>          parserQueue,
                            ConcurrentHashMap<String, Long>   wordFrequencyMap,
                            AtomicInteger                     activeAggregators,
                            Object                            completionLock,
                            AtomicBoolean                 streamComparisonDone,
                            MetricsCollector                  metrics) {
        this.parserQueue       = parserQueue;
        this.wordFrequencyMap  = wordFrequencyMap;
        this.activeAggregators = activeAggregators;
        this.completionLock    = completionLock;
        this.streamComparisonDone = streamComparisonDone;
        this.metrics           = metrics;
    }

    @Override
    public void run() {
        System.out.printf("[Aggregator-%s] Started.%n",
                Thread.currentThread().getName());

        try {
            aggregateLoop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.printf("[Aggregator-%s] Interrupted. Shutting down.%n",
                    Thread.currentThread().getName());
        } finally {
            signalCompletion();
        }
    }

    private void aggregateLoop() throws InterruptedException {
        while (true) {
            LogRecord record = parserQueue.take();

            if (record.isPoisonPill()) {
                System.out.printf("[Aggregator-%s] Received POISON_PILL. Exiting loop.%n",
                        Thread.currentThread().getName());
                return;
            }

            if (record.isError()) {
                continue;
            }

            for (String word : record.getWords()) {
                wordFrequencyMap.merge(word, 1L, Long::sum);
            }
            String levelKey = "LEVEL_" + record.getLevel().toUpperCase();
            wordFrequencyMap.merge(levelKey, 1L, Long::sum);
        }
    }

    private void runStreamComparison() {
        if (wordFrequencyMap.isEmpty()) {
            System.out.println("[Aggregator] Map empty — skipping stream comparison.");
            return;
        }

        long seqStart = System.nanoTime();
        wordFrequencyMap.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                .limit(10)
                .forEach(e -> {});
        long seqUs = (System.nanoTime() - seqStart) / 1_000;
        metrics.setSeqStreamUs(seqUs);

        long parStart = System.nanoTime();
        wordFrequencyMap.entrySet().parallelStream()
                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                .limit(10)
                .forEach(e -> {});
        long parUs = (System.nanoTime() - parStart) / 1_000;
        metrics.setParStreamUs(parUs);

        System.out.printf("[Aggregator-%s] Stream comparison done. seq=%dµs par=%dµs%n",
                Thread.currentThread().getName(), seqUs, parUs);
    }

    private void signalCompletion() {
        int remaining = activeAggregators.decrementAndGet();
        System.out.printf("[Aggregator-%s] Decremented activeAggregators → %d%n",
                Thread.currentThread().getName(), remaining);

        if (remaining == 0) {
            if (streamComparisonDone.compareAndSet(false, true)) {
                runStreamComparison();
            }
            synchronized (completionLock) {
                System.out.printf("[Aggregator-%s] Last aggregator. " +
                                "Signalling PipelineManager.%n",
                        Thread.currentThread().getName());
                completionLock.notifyAll();
            }
        }
    }
}