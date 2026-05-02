# HisBench tuning guide

This document explains how to tune **HisBench** and the underlying **HistoryDB** settings for the outcome you care about: peak insert throughput, stable tail latency, sustainable disk use, or realistic mixed load. Defaults skew toward a **moderate** topology; large shard counts and huge mmap tables can saturate a single NVMe without improving useful throughput.

For the full flag list and defaults, run:

```bash
dotnet run --project HisBench -- --help
```

---

## 0. Measurement API and timers (insert throughput)

**Which API:** HisBench’s insert and duplicate phases call `HistoryAddForBenchmarkAsync`, which hashes message and server IDs, enqueues to the shard writer, and **does not** append the walk/journal (`RecordInserted`). Throughput numbers are **not** comparable to `HistoryAdd` / `HistoryAddAsync`, which perform metadata and optional journal work on every successful insert.

**When `HistorySync` runs:** The benchmark harness calls `HistorySync()` in `finally` after the workload phases complete, before the final `GetPerformanceSnapshot()`. The reported **total elapsed** and **insert throughput (full run clock)** therefore include that sync (and, for phased runs, duplicate and lookup passes). With default large mmap tables, **HistorySync** can take seconds, and **engine cold open** (storage sizing, telemetry read, `HistoryDatabase` construction including mmap/Bloom, then `ConfigureDatabase`) can take many more on a fresh or large tree; the report’s **Clock:** line breaks those out with phased insert/duplicate/lookup wall times so the numbers add up. Use **insert throughput (insert phase only)** for engine-limited insert rate; use the histograms for latency shape.

**Bloom checkpoints:** Default engine behavior uses **throughput** Bloom persistence (`TryWrite` to the persist channel; drops counted as `BloomCheckpointsDropped` in `GetPerformanceSnapshot()`). Use `--bloom-persist durability` to block shard writers until checkpoints are accepted (stronger on-disk Bloom cadence under load).

**Cross-generation duplicate pre-check:** Default `--enqueue-dup-check full` matches production semantics (scan retained generations before enqueue). For synthetic **unique-key** insert-only experiments, `--enqueue-dup-check active-generation-only` removes that scan and can raise peak inserts/sec at the cost of incorrect cross-generation duplicate semantics if the same hash could appear in an older generation.

---

## Profiling the insert hot path

Use a short run and CPU sampling focused on `HistoryDB`:

```bash
dotnet tool install -g dotnet-trace
dotnet trace collect --format speedscope --providers Microsoft-DotNETCore-SampleProfiler -- dotnet run -c Release --project HisBench -- --add-count 500000 --add-forks 8
```

Open the generated `.speedscope.json` in [Speedscope](https://www.speedscope.app/) and search for frames such as `ExistsInGenerationsBeforeOrEqual`, `TryEnqueueBloomPersist`, `CloneWordsLocked`, `BloomFilter64`, `WriteAsync`, and `RunShardWriterAsync`.

---

## 1. Decide what “maximum performance” means

Before changing knobs, pick one primary goal:

| Goal | What to optimize | HisBench signals |
|------|------------------|------------------|
| **Peak inserts/sec** | Offer enough concurrency without drowning the disk or causing probe storms | **Insert throughput (insert phase only)** on phased runs; insert wall p99; `Insert writer execution` vs `Insert queue wait` (the full-run insert/sec line divides by total wall time including duplicate + lookup passes and shutdown) |
| **Stable tail latency** | Avoid queue saturation and excessive table load | Queue wait p99; `PendingQueueItemsApprox`; verdict line |
| **Disk sustainability** | Fewer large parallel write streams, less mmap working set | Process read/write bytes; storage delta; host disk queue depth (outside HisBench) |
| **Lookup-heavy** | Bloom trust, generations retained, skew model | Lookup throughput; restart warmup if you use `--restart-test` |

You often cannot maximize all of these at once. **Throughput runs** may use more forks and aggressive retention; **latency runs** may reduce forks or shard parallelism until queue wait stops dominating.

---

## 2. Capacity: `shards`, `slots-per-shard`, and `--load-factor`

Each shard is a fixed-size mmap table: **32 bytes per slot** (plus a small header). `slots-per-shard` must be a **power of two**.

Approximate **logical insert budget per generation** (before rollover or full-table behavior):

```text
capacityPerGeneration ≈ shards × slotsPerShard × (loadFactorPercent / 100)
```

Tune `shards` and `slots-per-shard` so this matches your **intended dataset size per generation** with headroom, not the largest values that fit on disk. Empty address space still costs **virtual size**, **file creation**, **Bloom sizing**, and **writer thread count** (see below).

---

## 3. Shards vs file size (disk and CPU)

HistoryDB uses **roughly one writer pipeline per shard**. More shards means **more parallel writers** and **more mmap files** touched over time.

- **Disk already saturated** (NVMe pegged, high write bytes, spiky latency): prefer **fewer shards** and **larger `slots-per-shard`** for the same logical capacity, so you reduce **competing write streams** and filesystem metadata pressure. Then adjust `--add-forks` downward if the disk is still the bottleneck.
- **Disk underutilized, CPU hot on writers**: modestly **more shards** can raise aggregate insert rate until the disk or probe limits cap you.
- **Memory pressure**: RSS follows **pages you touch**, not only committed size. Uniform hashing over many large shards tends to **spread** the working set; **fewer, smaller** shards can reduce resident pages for the same insert count if your workload is skewed.

There is no universal optimum: **sweep** `shards` and `slots-per-shard` while watching insert throughput, insert p99, queue depth, and process I/O in the report.

---

## 4. Client concurrency: `--add-forks` and `--lookup-forks`

These control how many concurrent workers run inserts, duplicate passes, lookups, or mixed work.

- **Too low**: underutilizes CPU and may under-drive the storage device.
- **Too high**: floods per-shard bounded queues, raises **queue wait**, increases contention, and can **max out** a single disk without increasing useful inserts/sec.

Increase forks in **small steps** (for example 8 → 12 → 16). If **queue wait** rises while **writer execution** stays flat, you are **queue-bound**; more forks usually **hurt**. If writer time dominates and disk is quiet, you may need **more engine parallelism** (shards) or faster storage, not only more forks.

---

## 5. Queue depth and backpressure: `--queue-capacity`

Each shard has a **bounded channel** sized by `--queue-capacity` (default large). This caps how far producers can run ahead of shard writers.

- **Very large** capacity: absorbs bursts but can **inflate memory** for queued work and hides sustained overload until latency explodes.
- **Smaller** capacity: **tighter backpressure** and earlier visibility of saturation in benchmarks (more representative of production guardrails).

Tune together with `--add-forks`: capacity is not a substitute for matching producer count to what the engine can drain.

---

## 6. Generations and disk footprint: `--retained-generations`

Each retained generation keeps its own shard set on disk and in the engine’s lifecycle. **Lower** `--retained-generations` for throughput experiments where you do not need a long multi-generation lookup tail; this reduces **background retirement I/O** and **Bloom** work across generations.

**Higher** retention is useful when measuring **lookup** behavior across older data or restart warmup with realistic depth.

---

## 7. Bloom checkpoints: `--bloom-checkpoint-interval` and `--bloom-persist`

Bloom filters are periodically serialized to sidecar files. A **smaller** interval increases **disk write** traffic and can cap insert throughput during sustained load. A **larger** interval reduces that overhead (at the cost of less frequent on-disk Bloom snapshots—acceptable for many synthetic runs). **`--bloom-checkpoint-interval 0` disables** periodic checkpoints entirely.

`--bloom-persist throughput` (default) enqueues checkpoints with `TryWrite` and counts drops in the final `BloomCheckpointsDropped` field. `--bloom-persist durability` blocks shard writers until each checkpoint is accepted when the persist channel applies backpressure.

For **pure insert throughput**, try **increasing** the interval (or `0`) after you have a stable baseline; use **durability** only when you need strict checkpoint queuing behavior under overload.

---

## 8. Slot scrubber: `--scrub-samples` and `--scrub-interval-ms`

The scrubber walks mmap slots on a timer to detect inconsistencies and repair Bloom trust. **Higher** sample rates or **shorter** intervals add **background CPU and memory traffic**.

For **insert-only throughput**, use a **longer** interval and/or **fewer** samples. Use more aggressive scrubbing when debugging **correctness**, Bloom mismatch rates, or **lookup** anomalies (watch probe-related counters in the report).

---

## 9. Forcing rollover: `--target-generations`, `--fill-factor`, `--queue-threshold`, `--probe-threshold`

When `--target-generations` is greater than zero, HisBench configures rollover thresholds so the run can drive multiple generations.

- **`--fill-factor`**: scales how full each generation becomes before rollover pressure (used in the internal per-generation insert target). Keep within a sensible band (defaults are conservative).
- **`--queue-threshold` / `--probe-threshold`**: proactive rollover signals forwarded to HistoryDB when target generations are enabled. **Zero** disables queue-based or aggregate probe-based proactive rollover for those dimensions (see `--help` semantics). Set explicit values only when you are **reproducing** a specific rollover policy.

If you only care about **single-generation insert speed**, leave `--target-generations` at **0** unless you explicitly need multi-generation behavior.

---

## 10. Workload shape

- **`--duration SEC`**: when set, the run stops when this time budget elapses (combined with any external cancellation). In **phased** mode with a positive duration, the insert-phase target is **`--dataset-size`** when that is positive; otherwise inserts are driven until the clock stops (very large logical target). For predictable time-boxed phased runs, set an explicit **`--dataset-size`**, or use **`--add-count`** with duration at zero. In **`--mix`** mode, workers loop until cancellation; worker count is **`max(add-forks, lookup-forks)`**.
- **`--mix I:L:E`**: mixed insert / lookup / expire ratios. Realistic for services; harder to interpret than phased mode—tune one dimension at a time.
- **`--zipfian`**: skewed lookups toward hot keys; stresses cache and Bloom negatives differently than uniform access.
- **`--dataset-size`**: caps logical key space; useful to create **contention** or **duplicate-heavy** phases without growing the dataset forever.
- **`--burst-mode`** and **`--burst-profile`**: shape arrival rate; use to see latency and queue behavior under **duty cycles** rather than flat max rate.
- **`--duration`**: sustained runs; combine with mixed or burst settings to observe **steady state** vs warmup.

---

## 11. Measurement overhead and reporting thresholds

- **`--correlation-ms`**: background sampling interval for queue and failure correlation. For **minimal overhead**, increase this interval or set it to **0** (sampler disabled—check current parser behavior for zero; if supported, use it for pure noise reduction).
- **`--saturation-queue-depth` / `--degraded-p99-us`**: affect **verdict / status labeling** in the report when non-zero; they do not change engine throughput by themselves.

Use non-zero thresholds when you want automated **PASS/FAIL style** interpretation against SLOs.

---

## 12. Restart and cold cache: `--restart-test`, `--cold-restart`

These modes measure **reopen and lookup warmup**. They are valuable for **operational** tuning but are not the primary path for **maximum insert** tuning. Expect different I/O patterns (more read traffic, different caching behavior).

---

## 13. Environment checklist

- Put `--history-dir` on the **device under test**; avoid sharing the disk with noisy neighbors during sweeps.
- Use a **fresh directory** when comparing configurations so generation layout and file growth are comparable.
- Close **antivirus** exclusions or indexing on the benchmark path on Windows if policy allows; they distort write latency.
- Compare runs with the **same** `--random-msgids`, mix, and counts when possible so insert and lookup paths stay comparable.

---

## 14. Suggested tuning workflow

1. Fix **target capacity** with `shards` × `slots-per-shard` × load factor; avoid oversizing tables.
2. Set **`--retained-generations`** and **`--bloom-checkpoint-interval`** appropriate to the experiment (throughput vs durability vs lookup depth).
3. Sweep **`--add-forks`** to find the knee where throughput stops rising or queue wait dominates.
4. If disk-saturated, **reduce shards** or forks before growing shards.
5. If CPU-saturated and disk is quiet, consider **more shards** (bounded by hardware) or host-level CPU affinity (outside HisBench).
6. Record **report** sections: throughput, insert histogram split (queue vs writer), engine health counters, process I/O, storage delta.

For HistoryDB internals (Bloom budget, rollover policy, writer model), see `HistoryDB/README.md` in this repository.

---

## 15. Automatic sweep mode (`--auto`)

Use `--auto` when you want HisBench to run a built-in configuration sweep and recommend a best setup for the current host.

- Auto mode forces `--add-count 4000000` and `--lookup-count 4000000`.
- It runs multiple trials across key performance knobs (`--add-forks`, `--lookup-forks`, `--shards`, `--slots-per-shard`, `--queue-capacity`).
- Each trial writes to a unique subdirectory under `<history-dir>/auto-tune/` so runs are isolated.
- At the end, it prints a recommended flag set for future manual runs.

Example:

```bash
dotnet run --project HisBench -- \
  --history-dir ./hisbench-data \
  --auto
```
