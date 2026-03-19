package com;

import com.pipeline.core.FileValidator;
import com.pipeline.core.MetricsCollector;
import com.pipeline.core.PipelineManager;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Main {

    private static final String INPUT_FILE_PATH = "pipeline-clean/src/main/resources/input3.txt";
    private static final int[] THREAD_COUNTS = {1, 2, 4, 8};
    private static final int QUEUE_CAPACITY = 8000;
    private static final int PARSER_THREADS = 2;
    private static final int AGGREGATOR_THREADS = 2;

    public static void main(String[] args) {
        System.out.println("═".repeat(54));
        System.out.println("  Multi-Threaded File Processing Pipeline");
        System.out.println("  JDK: " + System.getProperty("java.version") +
                "  Cores: " + Runtime.getRuntime().availableProcessors());
        System.out.println("═".repeat(54));

        String filePath = resolveFilePath(args);

        System.out.println("\n[Startup] Validating input file: " + filePath);
        FileValidator.ValidationResult validation = FileValidator.validate(filePath);

        if (!validation.isValid()) {
            System.err.println("[Startup] VALIDATION FAILED: " + validation.getReason());
            System.err.println("[Startup] Aborting. Fix the input file and retry.");
            System.exit(1);
        }

        System.out.println("[Startup] Validation passed. " + validation.getReason());

        List<MetricsCollector> results = new ArrayList<>();

        for (int threadCount : THREAD_COUNTS) {
            System.out.println("\n" + "─".repeat(54));
            System.out.println("  Starting run with " + threadCount + " reader thread(s)");
            System.out.println("─".repeat(54));

            MetricsCollector metrics = new MetricsCollector();

            try {
                PipelineManager manager = new PipelineManager(
                        filePath,
                        threadCount,
                        PARSER_THREADS,
                        AGGREGATOR_THREADS,
                        QUEUE_CAPACITY,
                        metrics
                );

                manager.run();

            } catch (Exception e) {
                System.err.println("[Run-" + threadCount + "] Pipeline failed: " + e.getMessage());
                e.printStackTrace();
                continue;
            }

            metrics.printReport(threadCount);
            results.add(metrics);
        }

        printComparisonTable(results);
        System.out.println("\n[App] All runs complete. Exiting.");
    }

    private static void printComparisonTable(List<MetricsCollector> results) {
        if (results.isEmpty()) {
            System.out.println("\n[App] No successful runs to compare.");
            return;
        }

        System.out.println("\n\n" + "═".repeat(92));
        System.out.println("  PERFORMANCE COMPARISON TABLE");
        System.out.println("═".repeat(92));

        MetricsCollector.printTableHeader();

        for (int i = 0; i < results.size(); i++) {
            int tc = THREAD_COUNTS[i];
            System.out.println(results.get(i).toTableRow(tc));
        }

        System.out.println("─".repeat(92));

        if (results.size() >= 2) {
            MetricsCollector first = results.get(0);
            MetricsCollector best = results.stream()
                    .max((a, b) -> Long.compare(a.getThroughput(), b.getThroughput()))
                    .orElse(first);

            System.out.println("\n  Scaling analysis:");
            System.out.printf("  Baseline (1 thread)  : %d lines/sec%n", first.getThroughput());
            System.out.printf("  Best throughput      : %d lines/sec%n", best.getThroughput());

            if (first.getThroughput() > 0) {
                double speedup = (double) best.getThroughput() / first.getThroughput();
                System.out.printf("  Speedup              : %.2fx%n", speedup);
            }
        }

    }

    private static String resolveFilePath(String[] args) {
        if (args != null && args.length > 0) {
            String argPath = args[0].trim();
            if (!argPath.isEmpty()) {
                System.out.println("[Startup] Using file path from argument: " + argPath);
                return argPath;
            }
        }

        File defaultFile = new File(INPUT_FILE_PATH);
        if (!defaultFile.exists()) {
            System.err.println("[Startup] Default input file not found at: " +
                    new File(INPUT_FILE_PATH).getAbsolutePath());
            System.err.println("[Startup] Either:");
            System.err.println("  1. Place your input1.txt at src/main/resources/input1.txt");
            System.err.println("  2. Pass the file path as a command-line argument");
            System.exit(1);
        }

        return INPUT_FILE_PATH;
    }
}