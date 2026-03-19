package com.pipeline.core;
import java.util.concurrent.atomic.AtomicLong;
public class MetricsCollector {

    private final AtomicLong totalLinesRead   = new AtomicLong(0);
    private final AtomicLong totalLinesParsed = new AtomicLong(0);
    private final AtomicLong errorLines       = new AtomicLong(0);
    private final AtomicLong skippedLines     = new AtomicLong(0);

    private volatile long startTimeNanos;
    private volatile long endTimeNanos;

    private volatile long seqStreamUs;
    private volatile long parStreamUs;

    public void start() {
        totalLinesRead.set(0);
        totalLinesParsed.set(0);
        errorLines.set(0);
        skippedLines.set(0);
        seqStreamUs    = 0;
        parStreamUs    = 0;
        startTimeNanos = System.nanoTime();
        endTimeNanos   = 0;
    }

    public void stop() {
        endTimeNanos = System.nanoTime();
    }

    public void incrementLinesRead()    { totalLinesRead.incrementAndGet(); }
    public void addLinesRead(long n)    { totalLinesRead.addAndGet(n); }
    public void incrementLinesParsed()  { totalLinesParsed.incrementAndGet(); }
    public void incrementErrorLines()   { errorLines.incrementAndGet(); }
    public void incrementSkippedLines() { skippedLines.incrementAndGet(); }

    public void setSeqStreamUs(long us) { this.seqStreamUs = us; }
    public void setParStreamUs(long us) { this.parStreamUs = us; }

    public void setSeqStreamMs(long val) { this.seqStreamUs = val; }
    public void setParStreamMs(long val) { this.parStreamUs = val; }

    public long getTotalLinesRead()   { return totalLinesRead.get(); }
    public long getTotalLinesParsed() { return totalLinesParsed.get(); }
    public long getErrorLines()       { return errorLines.get(); }
    public long getSkippedLines()     { return skippedLines.get(); }
    public long getSeqStreamUs()      { return seqStreamUs; }
    public long getParStreamUs()      { return parStreamUs; }

    public long getWallClockMs() {
        if (endTimeNanos == 0) return 0;
        return (endTimeNanos - startTimeNanos) / 1_000_000;
    }

    public long getElapsedMs()    { return getWallClockMs(); }

    public long getSeqStreamMs()  { return seqStreamUs; }

    public long getParStreamMs()  { return parStreamUs; }

    public long getThroughput() {
        long ms = getWallClockMs();
        return (ms == 0) ? 0 : (totalLinesParsed.get() * 1000L) / ms;
    }

    public double getParallelOverhead() {
        if (seqStreamUs == 0) return 0;
        return (double) parStreamUs / seqStreamUs;
    }

    public void printReport(int threadCount) {
        double overhead = getParallelOverhead();

        System.out.println("\n╔══════════════════════════════════════════════════════╗");
        System.out.printf( "║  PERFORMANCE ANALYSIS [threads: %2d]                 ║%n", threadCount);
        System.out.println("╠══════════════════════════════════════════════════════╣");
        System.out.printf( "║  Lines Read       : %-15d                ║%n", totalLinesRead.get());
        System.out.printf( "║  Lines Parsed     : %-15d                ║%n", totalLinesParsed.get());
        System.out.printf( "║  Error Lines      : %-15d                ║%n", errorLines.get());
        System.out.printf( "║  Skipped Lines    : %-15d                ║%n", skippedLines.get());
        System.out.println("╟──────────────────────────────────────────────────────╢");
        System.out.printf( "║  Wall-Clock Time  : %-15d ms             ║%n", getWallClockMs());
        System.out.printf( "║  Throughput       : %-15d l/s            ║%n", getThroughput());
        System.out.println("╟──────────────────────────────────────────────────────╢");
        System.out.printf( "║  CPU Effort (Seq) : %-15d µs             ║%n", seqStreamUs);
        System.out.printf( "║  CPU Effort (Par) : %-15d µs             ║%n", parStreamUs);

        if (overhead > 1.0) {
            System.out.printf(
                    "║  Parallel Tax     : %-15.2fx more effort    ║%n", overhead);
        } else if (overhead > 0) {
            System.out.printf(
                    "║  Efficiency Gain  : %-15.2fx faster effort  ║%n", (1.0 / overhead));
        }
        System.out.println("╚══════════════════════════════════════════════════════╝");
    }

    public static void printTableHeader() {
        System.out.println("\n" + "─".repeat(105));
        System.out.printf("| %-8s | %-15s | %-18s | %-12s | %-12s | %-15s |%n",
                "Threads", "Wall-Clock(ms)", "Throughput(l/s)",
                "Seq Effort", "Par Effort", "Parallel Tax");
        System.out.println("─".repeat(105));
    }

    public String toTableRow(int threadCount) {
        return String.format(
                "| %-8d | %-15d | %-18d | %-12d | %-12d | %-15.2fx |",
                threadCount,
                getWallClockMs(),
                getThroughput(),
                seqStreamUs,
                parStreamUs,
                getParallelOverhead()
        );
    }

    @Override
    public String toString() {
        return String.format(
                "MetricsCollector{read=%d, parsed=%d, errors=%d, skipped=%d, " +
                        "elapsed=%dms, seq=%dµs, par=%dµs}",
                totalLinesRead.get(), totalLinesParsed.get(),
                errorLines.get(), skippedLines.get(),
                getWallClockMs(), seqStreamUs, parStreamUs
        );
    }
}