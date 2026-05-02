# HisBench

`HisBench` is a high-throughput benchmark harness for `HistoryDB` focused on enqueue-to-completion latency, queue pressure, rollover behavior, and restart warmup characteristics.

## Key Capabilities

- Measures **end-to-end insert latency** and splits it into:
  - caller-observed wall latency
  - shard-queue wait latency
  - shard writer execution latency
- Supports phased and mixed workloads with configurable concurrency.
- Supports burst traffic models: `spike`, `ramp`, and `wave`.
- Captures queue/failure correlation samples with timestamps.
- Provides restart warmup timeline and steady-state detection.
- Emits process resource telemetry (CPU, memory, GC, and process I/O where available).

## Usage

```bash
dotnet run --project HisBench -- --help
```

### Common Options

- `--add-count N`, `--add-forks N`
- `--lookup-count N`, `--lookup-forks N`
- `--history-dir DIR`
- `--duration SEC`
- `--target-generations N`, `--fill-factor F`
- `--mix I:L:E`
- `--dataset-size N`
- `--zipfian`
- `--restart-test`, `--cold-restart`
- `--burst-mode`, `--burst-profile spike|ramp|wave`, `--burst-duty-pct N`, `--burst-sleep-ms N`
- `--correlation-ms N`
- `--saturation-queue-depth N`
- `--degraded-p99-us N`
- `--shards N`, `--slots-per-shard N` (slots must be a power of two; defaults: 128 shards, ~128 MiB mmap per shard)
- `--load-factor N`, `--queue-capacity N`, `--retained-generations N`
- `--bloom-checkpoint-interval N`, `--bloom-persist throughput|durability`, `--enqueue-dup-check full|active-generation-only`
- `--insert-batch [N]` — sets `HistoryDatabase.SetWriterCoalesceBatchSize(N)` as the **ceiling** for writer-side coalescing (default **N=128** when omitted; `N` in 1..128; `0` means ceiling 1). When `N` &gt; 1, the library **adaptively** tunes the effective burst per shard up to `N` unless you call `SetWriterCoalesceAdaptive(false)` in custom code. Inserts still use `HistoryAddForBenchmarkAsync` per key (same dedupe/hash as unbatched); only mmap/Bloom work is amortized in the writer. Use a **fresh** `--history-dir` when comparing runs; an existing dataset will make inserts mostly duplicates.
- `--scrub-samples N`, `--scrub-interval-ms N`
- `--queue-threshold N`, `--probe-threshold N` (rollover tuning when `--target-generations` is used)
- `--auto` (runs repeated 4M insert + 4M lookup trials and recommends tuned settings)

## Example Runs

### Sustained mixed load with burst profile

```bash
dotnet run --project HisBench -- \
  --history-dir ./hisbench-data \
  --duration 120 \
  --mix 70:20:10 \
  --add-forks 16 \
  --lookup-forks 16 \
  --burst-mode \
  --burst-profile wave \
  --burst-duty-pct 20 \
  --correlation-ms 200 \
  --saturation-queue-depth 100000 \
  --degraded-p99-us 10000
```

### Restart warmup analysis

```bash
dotnet run --project HisBench -- \
  --history-dir ./hisbench-data \
  --add-count 5000000 \
  --lookup-count 1000000 \
  --restart-test \
  --degraded-p99-us 5000
```

## Interpreting the Report

- `Insert queue wait` rising while `Insert writer execution` is stable indicates queueing/backpressure saturation.
- `Status: saturated=true` indicates queue depth crossed `--saturation-queue-depth`.
- `Status: degraded=true` indicates insert p99 crossed `--degraded-p99-us`.
- `Restart warmup timeline` shows latency convergence after reopen.
- `Correlation samples (tail)` provides time-correlated queue depth and failure deltas.

## Estimated Storage Results

These are directional estimates for a modern 8-16 core host with moderate memory pressure and default shard topology. Actual results depend on filesystem, CPU topology, background load, and tuning options.

### SATA SSD (typical)

- Insert throughput: **0.5M - 1.5M ops/s**
- Insert p99 wall latency: **2ms - 15ms**
- Queue wait becomes dominant earlier under bursty load.
- Restart steady-state (lookup p99 normalization): **2s - 12s**

### NVMe SSD (typical)

- Insert throughput: **1.5M - 4.0M ops/s**
- Insert p99 wall latency: **0.5ms - 6ms**
- Lower queue pressure for equivalent concurrency and duty cycle.
- Restart steady-state (lookup p99 normalization): **0.5s - 4s**

## Notes

- On Linux, process I/O counters are collected from `/proc/self/io`.
- On Windows, process I/O counters depend on runtime API availability.
- `--cold-restart` is best effort; permissions and platform tools may limit cache-drop behavior.
