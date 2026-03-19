package com;

import com.pipeline.core.FileValidator;
import com.pipeline.core.MetricsCollector;
import com.pipeline.core.PipelineManager;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Main {
    private static final String INPUT_FILE_PATH =
            "pipeline-clean/src/main/resources/input3.txt";

    private static final int QUEUE_CAPACITY = 50000;

    static class TestConfig {
        final String label;
        final int    readerThreads;
        final int    parserThreads;
        final int    aggregatorThreads;

        TestConfig(String label, int readers, int parsers, int aggregators) {
            this.label             = label;
            this.readerThreads     = readers;
            this.parserThreads     = parsers;
            this.aggregatorThreads = aggregators;
        }

        @Override
        public String toString() {
            return String.format("R=%d P=%d A=%d", readerThreads,
                    parserThreads, aggregatorThreads);
        }
    }

    static class TestResult {
        final TestConfig     config;
        final MetricsCollector metrics;
        final boolean        success;

        TestResult(TestConfig config, MetricsCollector metrics, boolean success) {
            this.config  = config;
            this.metrics = metrics;
            this.success = success;
        }
    }

    public static void main(String[] args) {

        printBanner();

        String filePath = resolveFilePath(args);
        System.out.println("\n[Startup] Validating: " + filePath);
        FileValidator.ValidationResult validation = FileValidator.validate(filePath);
        if (!validation.isValid()) {
            System.err.println("[Startup] VALIDATION FAILED: " + validation.getReason());
            System.exit(1);
        }
        System.out.println("[Startup] " + validation.getReason());

        List<TestConfig> configs = buildTestConfigs();
        List<TestResult> results = new ArrayList<>();
        for (TestConfig config : configs) {
            TestResult result = runSingle(filePath, config);
            results.add(result);
        }

        printAllTables(results);
        System.out.println("\n[Main] All scenarios complete.");
    }

    private static List<TestConfig> buildTestConfigs() {
        List<TestConfig> configs = new ArrayList<>();

        configs.add(new TestConfig("[S1] Readers=1  Parsers=2  Agg=2", 1,  2, 2));
        configs.add(new TestConfig("[S1] Readers=2  Parsers=2  Agg=2", 2,  2, 2));
        configs.add(new TestConfig("[S1] Readers=4  Parsers=2  Agg=2", 4,  2, 2));
        configs.add(new TestConfig("[S1] Readers=8  Parsers=2  Agg=2", 8,  2, 2));

        configs.add(new TestConfig("[S2] Readers=4  Parsers=1  Agg=2", 4,  1, 2));
        configs.add(new TestConfig("[S2] Readers=4  Parsers=2  Agg=2", 4,  2, 2));
        configs.add(new TestConfig("[S2] Readers=4  Parsers=4  Agg=2", 4,  4, 2));
        configs.add(new TestConfig("[S2] Readers=4  Parsers=8  Agg=2", 4,  8, 2));


        configs.add(new TestConfig("[S3] Readers=4  Parsers=2  Agg=1", 4,  2, 1));
        configs.add(new TestConfig("[S3] Readers=4  Parsers=2  Agg=2", 4,  2, 2));
        configs.add(new TestConfig("[S3] Readers=4  Parsers=2  Agg=4", 4,  2, 4));
        configs.add(new TestConfig("[S3] Readers=4  Parsers=2  Agg=8", 4,  2, 8));

        configs.add(new TestConfig("[S4] Balanced  R=1  P=1  A=1",    1,  1, 1));
        configs.add(new TestConfig("[S4] Balanced  R=2  P=2  A=2",    2,  2, 2));
        configs.add(new TestConfig("[S4] Balanced  R=4  P=4  A=4",    4,  4, 4));
        configs.add(new TestConfig("[S4] Balanced  R=8  P=8  A=8",    8,  8, 8));

        configs.add(new TestConfig("[S5] Stress  R=16  P=8   A=4",   16,  8, 4));
        configs.add(new TestConfig("[S5] Stress  R=8   P=16  A=4",    8, 16, 4));
        configs.add(new TestConfig("[S5] Stress  R=4   P=8   A=16",   4,  8, 16));

        configs.add(new TestConfig("[S6] FastRead   R=8  P=1  A=1",   8,  1, 1));
        configs.add(new TestConfig("[S6] SlowRead   R=1  P=8  A=4",   1,  8, 4));
        configs.add(new TestConfig("[S6] FastAgg    R=2  P=2  A=8",   2,  2, 8));

        return configs;
    }
    private static TestResult runSingle(String filePath, TestConfig config) {
        System.out.println("\n" + "─".repeat(60));
        System.out.printf("  Running: %s%n", config.label);
        System.out.printf("  Config : readers=%-2d  parsers=%-2d  aggregators=%-2d%n",
                config.readerThreads, config.parserThreads, config.aggregatorThreads);
        System.out.println("─".repeat(60));

        MetricsCollector metrics = new MetricsCollector();

        try {
            PipelineManager manager = new PipelineManager(
                    filePath,
                    config.readerThreads,
                    config.parserThreads,
                    config.aggregatorThreads,
                    QUEUE_CAPACITY,
                    metrics
            );
            manager.run();
            return new TestResult(config, metrics, true);

        } catch (Exception e) {
            System.err.printf("[Run] FAILED: %s — %s%n", config.label, e.getMessage());
            e.printStackTrace();
            return new TestResult(config, metrics, false);
        }
    }

    private static void printAllTables(List<TestResult> results) {

        // ── Master table — all runs ──
        System.out.println("\n\n" + "═".repeat(110));
        System.out.println("  FULL RESULTS — ALL SCENARIOS");
        System.out.println("═".repeat(110));
        printFullHeader();

        for (TestResult r : results) {
            if (r.success) {
                System.out.println(toFullRow(r));
            } else {
                System.out.printf("| %-40s | %-6s | %-6s | %-6s | FAILED%n",
                        r.config.label, r.config.readerThreads,
                        r.config.parserThreads, r.config.aggregatorThreads);
            }
        }
        System.out.println("─".repeat(110));

        printScenarioTable(results, "[S1]", "SCENARIO 1 — Varying Reader Threads");

        printScenarioTable(results, "[S2]", "SCENARIO 2 — Varying Parser Threads");

        printScenarioTable(results, "[S3]", "SCENARIO 3 — Varying Aggregator Threads");

        printScenarioTable(results, "[S4]", "SCENARIO 4 — Balanced Scaling");

        printScenarioTable(results, "[S5]", "SCENARIO 5 — Stress Test");

        printScenarioTable(results, "[S6]", "SCENARIO 6 — Imbalanced Pipeline");

        printBestRun(results);
    }

    private static void printScenarioTable(List<TestResult> all,
                                           String prefix, String title) {
        List<TestResult> scenario = new ArrayList<>();
        for (TestResult r : all) {
            if (r.config.label.startsWith(prefix) && r.success) {
                scenario.add(r);
            }
        }
        if (scenario.isEmpty()) return;

        System.out.println("\n" + "═".repeat(110));
        System.out.println("  " + title);
        System.out.println("═".repeat(110));
        printFullHeader();

        for (TestResult r : scenario) {
            System.out.println(toFullRow(r));
        }

        TestResult best = scenario.stream()
                .max((a, b) -> Long.compare(
                        a.metrics.getThroughput(),
                        b.metrics.getThroughput()))
                .orElse(scenario.get(0));

        System.out.println("─".repeat(110));
        System.out.printf("  Best: %s → %d lines/sec%n",
                best.config.label, best.metrics.getThroughput());

        long baseline = scenario.get(0).metrics.getThroughput();
        if (baseline > 0) {
            double speedup = (double) best.metrics.getThroughput() / baseline;
            System.out.printf("  Speedup vs baseline: %.2fx%n", speedup);
        }
    }

    private static void printBestRun(List<TestResult> results) {
        System.out.println("\n" + "═".repeat(110));
        System.out.println("  OVERALL ANALYSIS");
        System.out.println("═".repeat(110));

        TestResult best = results.stream()
                .filter(r -> r.success)
                .max((a, b) -> Long.compare(
                        a.metrics.getThroughput(),
                        b.metrics.getThroughput()))
                .orElse(null);

        TestResult worst = results.stream()
                .filter(r -> r.success)
                .min((a, b) -> Long.compare(
                        a.metrics.getThroughput(),
                        b.metrics.getThroughput()))
                .orElse(null);

        if (best != null) {
            System.out.printf("  Best config  : %s%n", best.config.label);
            System.out.printf("  Throughput   : %d lines/sec%n",
                    best.metrics.getThroughput());
            System.out.printf("  Config       : readers=%-2d parsers=%-2d aggregators=%-2d%n",
                    best.config.readerThreads,
                    best.config.parserThreads,
                    best.config.aggregatorThreads);
        }

        if (worst != null) {
            System.out.printf("%n  Worst config : %s%n", worst.config.label);
            System.out.printf("  Throughput   : %d lines/sec%n",
                    worst.metrics.getThroughput());
        }

        if (best != null && worst != null) {
            double ratio = (double) best.metrics.getThroughput()
                    / worst.metrics.getThroughput();
            System.out.printf("%n  Best/Worst ratio : %.2fx%n", ratio);
            System.out.println(
                    "  This shows the cost of wrong thread configuration.");
        }

        System.out.println("═".repeat(110));
    }

    private static void printFullHeader() {
        System.out.println("─".repeat(110));
        System.out.printf("| %-40s | %-6s | %-6s | %-6s | %-12s | %-14s | %-10s | %-10s |%n",
                "Label", "R", "P", "A",
                "Elapsed(ms)", "Throughput(l/s)", "Seq(µs)", "Par(µs)");
        System.out.println("─".repeat(110));
    }

    private static String toFullRow(TestResult r) {
        return String.format(
                "| %-40s | %-6d | %-6d | %-6d | %-12d | %-14d | %-10d | %-10d |",
                r.config.label,
                r.config.readerThreads,
                r.config.parserThreads,
                r.config.aggregatorThreads,
                r.metrics.getElapsedMs(),
                r.metrics.getThroughput(),
                r.metrics.getSeqStreamMs(),
                r.metrics.getParStreamMs()
        );
    }

    private static void printBanner() {
        System.out.println("═".repeat(60));
        System.out.println("  Multi-Threaded File Processing Pipeline");
        System.out.println("  Rigorous Multi-Dimensional Benchmark");
        System.out.println("  JDK: " + System.getProperty("java.version") +
                "  Cores: " + Runtime.getRuntime().availableProcessors());
        System.out.println("═".repeat(60));
    }

    private static String resolveFilePath(String[] args) {
        if (args != null && args.length > 0) {
            String argPath = args[0].trim();
            if (!argPath.isEmpty()) {
                System.out.println("[Startup] Using argument path: " + argPath);
                return argPath;
            }
        }

        File defaultFile = new File(INPUT_FILE_PATH);
        if (!defaultFile.exists()) {
            System.err.println("[Startup] File not found: " +
                    defaultFile.getAbsolutePath());
            System.err.println("[Startup] Pass file path as argument or update INPUT_FILE_PATH");
            System.exit(1);
        }
        return INPUT_FILE_PATH;
    }
}