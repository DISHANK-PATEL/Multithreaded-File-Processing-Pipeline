# Multi-Threaded File Processing Pipeline
<img width="1009" height="918" alt="image" src="https://github.com/user-attachments/assets/388034d0-0715-4fea-b65b-7568217eb930" />
A high-performance Java pipeline that reads large text files using parallel chunk-based I/O, processes data through a three-stage concurrent pipeline, and aggregates word frequencies 
using thread-safe data structures. 


## Overview

The goal is to process large-scale text files (logs, novels, plain text) as fast as possible by parallelising every stage of the work — reading, parsing, and 
aggregating — using Java's built-in concurrency primitives.

**What it produces:** A word frequency map showing how often every meaningful word appears in the file, alongside log level category counts if the file is a structured log.

**What it demonstrates:**
- Partition-based parallel file reading using `RandomAccessFile.seek()`
- Producer-Consumer pipeline using `BlockingQueue` with automatic backpressure
- Thread-safe aggregation using `ConcurrentHashMap.merge()` with bucket-level locking
- Lock-free counters using `AtomicLong` (CAS — Compare and Swap)
- Graceful shutdown using the Poison Pill pattern
- Parallel vs sequential stream performance comparison
- Performance profiling across thread count configurations



## Project Structure

```
src/main/java/com/pipeline/
│
├── model/
│   ├── LogRecord.java          ← Immutable DTO — carries parsed line data
│   └── ChunkInfo.java          ← Byte-range descriptor for each reader thread
│
├── core/
│   ├── MetricsCollector.java   ← Thread-safe performance tracker (AtomicLong)
│   ├── FileValidator.java      ← Pre-flight file check before threads launch
│   └── PipelineManager.java    ← wires pool, queues, tasks
│
├── stage/
│   ├── FileReaderTask.java     ← Stage 1: parallel chunk reading
│   ├── DataParserTask.java     ← Stage 2: parsing 
│   └── ResultAggregator.java   ← Stage 3: ConcurrentHashMap aggregation
│
├── Main.java                    

src/main/resources/
└── input.txt                   ← input files
```

---

## How It Works

### Stage 1 — Parallel File Reading

The file is divided into equal byte-range chunks, one per reader thread. Each `FileReaderTask` calls `RandomAccessFile.seek(startByte)` to jump directly to its assigned position instead of reading sequentially from the top.

**Partial line edge case:** Byte boundaries almost never land on a newline character. To handle this, every chunk except the first skips its first line (which the previous chunk already read in full). The last chunk always reads to true EOF regardless of its assigned end byte, absorbing any rounding remainder from integer division.

```
File:     [─────────────────────────────────────────]
4 threads:[──chunk0──][──chunk1──][──chunk2──][──chunk3──]

chunk0: starts at byte 0         → always clean line start
chunk1: seeks to byte X          → skips first partial line
chunk2: seeks to byte Y          → skips first partial line
chunk3: seeks to byte Z, reads → EOF
```

### Stage 2 — Parsing 

`DataParserTask` threads consume raw strings from `readerQueue`. Each line is matched against a pattern that optionally extracts a timestamp and log level, then tokenises the message body:

- Punctuation stripped via `replaceAll("[^a-zA-Z0-9]", "")`
- Lowercased for case-insensitive counting
- Stop words filtered (the, is, at, in, of, and...)
- Pure numbers filtered
- Minimum word length of 2 characters enforced

The parser handles both structured log files and plain text files. If no timestamp or level is found, the entire line is tokenised as plain text.

### Stage 3 — Thread-Safe Aggregation

`ResultAggregator` threads consume `LogRecord` objects from `parserQueue` and update a shared `ConcurrentHashMap` using `.merge()`:

```java
wordFrequencyMap.merge(word, 1L, Long::sum);
```

This single call is atomic at the bucket level — `ConcurrentHashMap` hashes each key to a specific bucket and locks only that bucket during the update. Multiple threads writing to different keys run truly in parallel.
After all records are processed, the last aggregator thread runs the sequential vs parallel stream comparison and stores the timings in `MetricsCollector`.

---

## Concurrency Model

Six distinct concurrency mechanisms are used across the pipeline:

**1. Bucket-level locking — `ConcurrentHashMap.merge()`**
The map is internally divided into buckets. Each key hashes to one bucket. Only that bucket is locked during a write — other buckets remain free for concurrent updates. No `synchronized` keyword anywhere in aggregation code.

**2. Compare-And-Swap — `AtomicLong` / `AtomicInteger`**
All counters (`totalLinesRead`, `errorLines`, `activeReaders`, `activeAggregators`) use CAS operations. Threads never block — they retry on conflict. No lock contention, no waiting.

**3. ReentrantLock + Condition — `BlockingQueue`**
`LinkedBlockingQueue.put()` blocks when the queue is full (backpressure). `take()` blocks when empty. This handles the producer-consumer synchronisation automatically. If parsers are slow, readers pause — preventing `OutOfMemoryError` from queue saturation.

**4. Object Monitor — `synchronized` + `wait()` / `notifyAll()`**
`PipelineManager.run()` parks on `completionLock.wait()` until all aggregators finish. The last aggregator calls `completionLock.notifyAll()` to unblock the manager.

**5. Thread Interrupt — `InterruptedException`**
Every stage catches `InterruptedException`, restores the interrupt flag with `Thread.currentThread().interrupt()`, and exits via its `finally` block. `executor.shutdownNow()` triggers this for forced shutdown.

**6. Volatile visibility**
Timing fields (`startTimeNanos`, `endTimeNanos`, `seqStreamUs`, `parStreamUs`) are `volatile` — writes go directly to main memory, reads always see the latest value. No full synchronisation needed for single-writer, multi-reader fields.

---

## Graceful Shutdown

Shutdown flows through the pipeline as a Poison Pill signal — a special sentinel value each stage recognises as the shutdown command.

```
Readers finish reading
       ↓
Last reader injects N pills into readerQueue
(one per parser thread — each parser needs its own pill)
       ↓
Each parser receives pill → exits loop
       ↓
Each parser injects K pills into parserQueue (finally block)
(one per aggregator thread)
       ↓
Each aggregator receives pill → decrements activeAggregators
       ↓
Last aggregator (counter hits 0):
  → runs stream comparison
  → calls completionLock.notifyAll()
       ↓
PipelineManager.run() unblocks
  → executor.shutdown()
  → executor.awaitTermination(60s)
  → metrics.stop()
  → run() returns
```
## Performance Results

This evaluation is based on a file of ~1.4 million lines under **I/O-intensive conditions** (minimal CPU computation).

---

### Observations

- The workload is dominated by disk I/O.
- Increasing parallelism improves performance only up to a limit.
- Beyond that limit, performance degrades due to contention and disk inefficiencies.

---

### Experimental Outputs

<p align="center">
  <img src="https://github.com/user-attachments/assets/0d258f2d-b702-4d68-a9b0-5311fe7a2e96" width="80%">
  <img src="https://github.com/user-attachments/assets/a7496589-7249-4c1e-9a69-f5ad529c47c8" width="40%">
  <img src="https://github.com/user-attachments/assets/a84a8961-5a38-476d-83b3-1f80da47982d" width="40%">
</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/e2eaacaa-c5bd-43c5-8250-b462d438f7c9" width="80%">
  <img src="https://github.com/user-attachments/assets/cda7fde7-91ae-429b-8f16-6a8d107d5b81" width="80%">
  <img src="https://github.com/user-attachments/assets/17bae05f-f385-42d6-a4bf-3c74f74f71d1" width="80%">
  <img src="https://github.com/user-attachments/assets/66dc53f8-82f9-42fc-a7ef-6a74f9c2ddae" width="80%">
  <img src="https://github.com/user-attachments/assets/05f773c0-180c-41ad-845a-edc8bdd5fbec" width="80%">
<img width="1468" height="574" alt="image" src="https://github.com/user-attachments/assets/4ab7ac75-5032-4198-ba23-4e53f7c4d56c" />
Since file is encrypted the pipeline doesnt start and is caught in pre-flight checks 
</p>

---

### System Context

- Machine: Mac M4 (10 CPU cores)
- Storage: NVMe SSD with an effective parallel I/O queue depth of ~4

---

### Parallel vs Sequential

**Parallel streams perform better when:**
- Word frequency map exceeds ~50,000 unique entries
- Input data has high vocabulary diversity (e.g., large text corpora)

**Sequential processing performs better when:**
- Vocabulary is limited (e.g., log files)
- Word map size is small (hundreds to a few thousand entries)
- Contention on shared keys is high

---

### Thread Configuration (R)

- **R = 4**
  - Optimal for the given system
  - Matches NVMe SSD parallel I/O capability (queue depth ≈ 4)
  - Provides best throughput

- **R = 8**
  - Leads to disk thrashing
  - Increased seek overhead
  - Reduced effective bandwidth per thread

---

### Contention Analysis

- Small word maps lead to high contention
- Multiple threads frequently update the same keys
- `ConcurrentHashMap` introduces bucket-level locking
- Operations serialize under contention

Result:
- Synchronization overhead outweighs parallel gains

---

### Conclusion

- Performance depends on input characteristics and hardware limits
- Increasing threads beyond optimal levels reduces efficiency
- Parallelism is beneficial only when contention is low and workload justifies it

**Key takeaway:** optimal performance requires balancing I/O capacity, thread count, and data characteristics
## Running the Project

### Prerequisites

- JDK 21
- IntelliJ IDEA (or any Java IDE)
- No external dependencies — pure JDK

### Setup

Clone or copy the project into IntelliJ. Mark directories:

```
git clone https://github.com/DISHANK-PATEL/Multithreaded-File-Processing-Pipeline.git
src/main/java      → Sources Root
src/main/resources → Resources Root
Place your input file
run Main.java
```
---
