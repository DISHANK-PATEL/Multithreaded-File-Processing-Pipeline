package com.pipeline.core;
import java.util.concurrent.atomic.AtomicLong;

public class MetricsCollector {

    private final AtomicLong totalLinesRead   = new AtomicLong(0);
    private final AtomicLong totalLinesParsed = new AtomicLong(0);
    private final AtomicLong errorLines       = new AtomicLong(0);
    private final AtomicLong skippedLines     = new AtomicLong(0);
    private volatile long startTimeNanos;
    private volatile long endTimeNanos;
    private volatile long seqStreamMs;
    private volatile long parStreamMs;
    public void start() {
        totalLinesRead.set(0);
        totalLinesParsed.set(0);
        errorLines.set(0);
        skippedLines.set(0);
        seqStreamMs  = 0;
        parStreamMs  = 0;
        startTimeNanos = System.nanoTime();
    }

    public void stop() {
        endTimeNanos = System.nanoTime();
    }

    public void incrementLinesRead()    { totalLinesRead.incrementAndGet(); }
    public void addLinesRead(long n)    { totalLinesRead.addAndGet(n); }
    public void incrementLinesParsed()  { totalLinesParsed.incrementAndGet(); }
    public void incrementErrorLines()   { errorLines.incrementAndGet(); }
    public void incrementSkippedLines() { skippedLines.incrementAndGet(); }
    public void setSeqStreamMs(long ms) { this.seqStreamMs = ms; }
    public void setParStreamMs(long ms) { this.parStreamMs = ms; }
    
    public long getTotalLinesRead()   { return totalLinesRead.get(); }
    public long getTotalLinesParsed() { return totalLinesParsed.get(); }
    public long getErrorLines()       { return errorLines.get(); }
    public long getSkippedLines()     { return skippedLines.get(); }
    public long getSeqStreamMs()      { return seqStreamMs; }
    public long getParStreamMs()      { return parStreamMs; }
    
    public long getElapsedMs() {
        if (endTimeNanos == 0) return 0;
        return (endTimeNanos - startTimeNanos) / 1_000_000;
    }
    
    public long getThroughput() {
        long elapsedMs = getElapsedMs();
        if (elapsedMs == 0) return 0;
        return (totalLinesParsed.get() * 1000L) / elapsedMs;
    }
    
    public void printReport(int threadCount) {
        System.out.println("\n╔══════════════════════════════════════════════╗");
        System.out.printf( "║   PIPELINE METRICS  [reader threads = %2d]    ║%n", threadCount);
        System.out.println("╠══════════════════════════════════════════════╣");
        System.out.printf( "║  Lines Read       : %-25d ║%n", totalLinesRead.get());
        System.out.printf( "║  Lines Parsed     : %-25d ║%n", totalLinesParsed.get());
        System.out.printf( "║  Error Lines      : %-25d ║%n", errorLines.get());
        System.out.printf( "║  Skipped Lines    : %-25d ║%n", skippedLines.get());
        System.out.println("╠══════════════════════════════════════════════╣");
        System.out.printf( "║  Elapsed Time     : %-20d ms   ║%n", getElapsedMs());
        System.out.printf( "║  Throughput       : %-17d lines/s ║%n", getThroughput());
        System.out.println("╠══════════════════════════════════════════════╣");
        System.out.printf( "║  Sequential Stream: %-20d ms   ║%n", seqStreamMs);
        System.out.printf( "║  Parallel Stream  : %-20d ms   ║%n", parStreamMs);
        
        if (seqStreamMs > 0 && parStreamMs > 0) {
            if (parStreamMs < seqStreamMs) {
                long gain = seqStreamMs - parStreamMs;
                System.out.printf("║  Parallel faster by %-19d ms   ║%n", gain);
            } else {
                long overhead = parStreamMs - seqStreamMs;
                System.out.printf("║  Sequential faster by %-18d ms   ║%n", overhead);
            }
        }
        System.out.println("╚══════════════════════════════════════════════╝");
    }
    
    public String toTableRow(int threadCount) {
        return String.format("| %-12d | %-18d | %-20d | %-12d | %-14d |",
                threadCount, getElapsedMs(), getThroughput(),
                seqStreamMs, parStreamMs);
    }
    
    public static void printTableHeader() {
        System.out.println("\n" + "─".repeat(90));
        System.out.printf("| %-12s | %-18s | %-20s | %-12s | %-14s |%n",
                "Threads", "Elapsed (ms)", "Throughput (l/s)", "Seq (ms)", "Parallel (ms)");
        System.out.println("─".repeat(90));
    }

    @Override
    public String toString() {
        return String.format(
                "MetricsCollector{read=%d, parsed=%d, errors=%d, skipped=%d, " +
                        "elapsed=%dms, seq=%dms, par=%dms}",
                totalLinesRead.get(), totalLinesParsed.get(),
                errorLines.get(), skippedLines.get(),
                getElapsedMs(), seqStreamMs, parStreamMs
        );
    }
}