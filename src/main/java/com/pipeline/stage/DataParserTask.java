package com.pipeline.stage;

import com.pipeline.core.MetricsCollector;
import com.pipeline.model.LogRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataParserTask implements Runnable {

    private static final Pattern LOG_PATTERN = Pattern.compile(
            "^(\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2})?\\s*" +
                    "(INFO|ERROR|WARN|DEBUG|TRACE)?\\s*(.*)$",
            Pattern.CASE_INSENSITIVE
    );

    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
            "a", "an", "the", "is", "it", "in", "on", "at", "to", "for",
            "of", "and", "or", "but", "not", "with", "from", "by", "as",
            "be", "was", "are", "has", "have", "had", "do", "did", "will",
            "this", "that", "its", "into", "via", "after", "after", "per"
    ));

    private static final int MIN_WORD_LENGTH = 2;

    private final BlockingQueue<String>    readerQueue;

    private final BlockingQueue<LogRecord> parserQueue;

    private final AtomicInteger activeAggregators;

    private final int aggregatorThreadCount;

    private final MetricsCollector metrics;

    public DataParserTask(BlockingQueue<String>    readerQueue,
                          BlockingQueue<LogRecord> parserQueue,
                          AtomicInteger            activeAggregators,
                          int                      aggregatorThreadCount,
                          MetricsCollector         metrics) {
        this.readerQueue          = readerQueue;
        this.parserQueue          = parserQueue;
        this.activeAggregators    = activeAggregators;
        this.aggregatorThreadCount = aggregatorThreadCount;
        this.metrics              = metrics;
    }

    @Override
    public void run() {
        System.out.printf("[Parser-%s] Started.%n",
                Thread.currentThread().getName());

        try {
            parseLoop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.printf("[Parser-%s] Interrupted. Shutting down.%n",
                    Thread.currentThread().getName());
        } finally {
            injectDownstreamPills();
        }
    }

    private void parseLoop() throws InterruptedException {
        while (true) {
            String message = readerQueue.take();

            if (FileReaderTask.POISON_PILL.equals(message)) {
                System.out.printf("[Parser-%s] Received POISON_PILL. Exiting loop.%n",
                        Thread.currentThread().getName());
                return;
            }

            LogRecord record = parseLine(message);

            parserQueue.put(record);

            if (record.isError()) {
                metrics.incrementErrorLines();
            } else {
                metrics.incrementLinesParsed();
            }
        }
    }
    private LogRecord parseLine(String message) {
        long lineNumber = -1;
        String rawLine  = message;

        try {

            int tabIndex = message.indexOf('\t');
            if (tabIndex > 0) {
                lineNumber = Long.parseLong(message.substring(0, tabIndex));
                rawLine    = message.substring(tabIndex + 1);
            }
            Matcher matcher = LOG_PATTERN.matcher(rawLine);

            String timestamp   = "";
            String level       = "UNKNOWN";
            String messageBody = rawLine;

            if (matcher.matches()) {
                if (matcher.group(1) != null) {
                    timestamp = matcher.group(1).trim();
                }
                if (matcher.group(2) != null) {
                    level = matcher.group(2).toUpperCase().trim();
                }

                if (matcher.group(3) != null) {
                    messageBody = matcher.group(3).trim();
                }
            }

            List<String> words = tokenise(messageBody);

            return new LogRecord(rawLine, level, lineNumber, false, timestamp, words);

        } catch (Exception e) {
            System.err.printf("[Parser-%s] Failed to parse line %d: %s%n",
                    Thread.currentThread().getName(), lineNumber, e.getMessage());
            return new LogRecord(rawLine, lineNumber);
        }
    }

    private List<String> tokenise(String text) {
        if (text == null || text.trim().isEmpty()) {
            return new ArrayList<>();
        }

        List<String> words = new ArrayList<>();

        String[] tokens = text.split("\\s+");
        for (String token : tokens) {
            String clean = token.replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
            if (clean.length() < MIN_WORD_LENGTH) continue;
            if (STOP_WORDS.contains(clean)) continue;
            if (clean.matches("\\d+")) continue;

            words.add(clean);
        }

        return words;
    }

    private void injectDownstreamPills() {
        System.out.printf("[Parser-%s] Injecting %d downstream pills.%n",
                Thread.currentThread().getName(), aggregatorThreadCount);

        for (int i = 0; i < aggregatorThreadCount; i++) {
            try {
                parserQueue.put(LogRecord.POISON_PILL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.printf("[Parser-%s] Interrupted injecting pill %d/%d%n",
                        Thread.currentThread().getName(), i + 1, aggregatorThreadCount);
            }
        }
    }
}