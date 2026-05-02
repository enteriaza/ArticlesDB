# ArticlesDB

`ArticleDatabase` is a high-performance, lock-efficient NNTP history storage project focused on very fast deduplication and lookup of article/message identifiers. Built from the ground up for implementation in next generation NNTP servers.

- 🚀 ~300K–500K inserts/sec sustained
- ⚡ ~3–10 µs lookup latency (p99)
- 🧠 Optimized for duplicate-heavy workloads
- 📦 Memory-mapped, shard-based architecture

This repository currently contains:

- `HistoryDB`: the core sharded, generation-based history library (optional `HistoryDatabaseHostedService` for Generic Host shutdown ordering)
- `HisBench`: a benchmark and tuning harness for measuring throughput, latency, queue behavior, and restart warmup
- `HisCTL`: operator CLI for **scan**, **repair**, **defrag**, and **bulk expire** (time- and LRU-style recycle policies)

## Project goals

- Provide low-latency article history membership checks.
- Sustain high insert rates with bounded contention.
- Keep storage layout operationally simple (memory-mapped shard files + Bloom sidecars).
- Offer practical benchmarking and tuning workflows for real hardware.
- Provide **operator-grade** tooling for **consistency checks**, **repair**, **defrag**, and **policy-driven expiration** without ad-hoc scripts.

## History lifecycle: checks, repair, defrag, and expiration

ArticlesDB is not only a fast duplicate filter: **HistoryDB** now supports a full **on-disk lifecycle** — diagnostics, safe repair, compaction, and **bulk expiry** — surfaced for operators through **HisCTL** and through public APIs on `HistoryDatabase`.

| Concern | Mechanism | Notes |
|--------|-----------|--------|
| **Consistency check** | `HistoryDatabase.ScanAsync` | Default **exclusive** probe opens (strongest view). **`HistoryScanOptions.UseSharedRead`** (HisCTL: `scan --shared`) allows read-shared opens while a server keeps the tree mapped; findings are best-effort under load. |
| **Repair** | `HistoryDatabase.RepairAsync` | **Exclusive**; quarantine or fail-fast policies; Bloom / header / expired-set hygiene. Run with the history **closed** by other processes. |
| **Defrag** | `HistoryDatabase.HistoryDefragAsync` | Online compaction across retained generations; coordinate with other writers per ops guidance. |
| **Single-message expire** | `HistoryExpire` / `HistoryExpire(string[])` | Existing API; tombstones + `expired-md5.bin`. |
| **Bulk expire by hash** | `HistoryExpireByHashesAsync` + slot harvest | Adds many hashes, batched tombstones, persists expired set; HisCTL wraps harvest + selection. |
| **Time-based bulk expire** | HisCTL `expire time` + `INTERVAL …` | Cutoff = `UtcNow - interval`; compare **obtained** or **accessed** slot ticks (MySQL-style units; see `MySqlIntervalExpression`). |
| **LRU-style bulk expire** | HisCTL `expire recycle --limit N` | Ranks occupied slots by **last access**, then **access counter**, then **insert time**, then hash — **stale-first** with **LFU-style** tie-break; default **dry-run**, **`--apply`** to mutate. |

> **Milestone — first NNTP history store here with LRU-style expiration policies.**  
> **ArticlesDB HistoryDB is the first NNTP-oriented history database in this codebase to ship first-class, operator-controlled bulk expiration under LRU-style ordering** (`expire recycle`) **alongside TTL-style age expiry** (`expire time` / `INTERVAL`). Traditional NNTP histories emphasized duplicate suppression and growth; **HistoryDB + HisCTL** add a **policy-driven reclamation path** at the shard layer (tombstones, persisted expired hashes, generation-aware fan-out) so capacity planning can target **least-recently / least-frequently used** article hashes, not only message-at-a-time expiry.

Operator-focused documentation: [HisCTL/README.md](HisCTL/README.md). Library APIs and scan/repair/defrag semantics: [HistoryDB/README.md](HistoryDB/README.md).

## Performance Characteristics

- Single-reader shard queues reduce lock contention on write paths.
- SIMD probing is used in shard probing hot paths when supported.
- Generation retention avoids full-table rewrites while allowing bounded historical lookup scope.
- Optional affinity pinning can improve locality for shard writer and maintenance workers.

## Comparison

ArticlesDB is designed as a modern alternative to traditional NNTP history systems:

| Feature | ArticlesDB | INN | Diablo |
|--------|-----------|-----|--------|
| Inserts/sec | 300K+ | 10K–100K | 150K–300K |
| Duplicate handling | O(1)-ish | slower | optimized |
| Memory model | modern | legacy | mmap-heavy |

### Observed benchmark results (HisBench)

The following results were observed on the current project configuration using `HisBench` with:

- `shards=128`
- `slots-per-shard=4194304` (~128 MiB per shard data file)
- `queue-capacity=1000000`
- `add-forks=8`, `lookup-forks=8`

These are empirical runs from this repository and are intended as directional reference points, not hard guarantees.

#### Sustained random-ID ingest + lookup

- Command profile: `--add-count 50000000 --lookup-count 50000000 --random-msgids`
- Insert throughput: ~**361k inserts/sec**
- Lookup throughput: ~**361k lookups/sec**
- Insert latency: p99 ~**17-21 us**
- Lookup latency: p99 ~**3.5-6.9 us**
- Health counters: `FullFails=0`, `ProbeFails=0`, `QueueApprox=0`

#### Restart warmup run (same load + `--restart-test`)

- Command profile: above + `--restart-test`
- Effective throughput over full run:
  - ~**290k inserts/sec**
  - ~**435k lookups/sec** (includes restart lookup phase)
- Insert latency: p99 ~**17.7 us**
- Lookup latency: p99 ~**3.5 us**
- Restart warmup timeline stabilized in low-microsecond range quickly after reopen.

#### Duplicate-dominated profile (small logical keyspace)

- Command profile: `--dataset-size 1000000` (non-random IDs)
- Representative observed run:
  - `Duplicates detected=50000000`
  - `Inserted=0` (duplicate-path dominated by design for this scenario)
  - Lookup throughput: ~**991k lookups/sec** (near **1M ops/sec**)
  - Lookup latency: p99 ~**2.6 us**
  - Engine health counters remained clean (`FullFails=0`, `ProbeFails=0`)
- Results show high duplicate detection and low successful inserts by design.
- This profile measures duplicate-path behavior and lookup speed, not sustained unique-ingest capacity.

### Interpreting these numbers

- `failed` insert counts in bench output can represent duplicate outcomes, not only capacity failures.
- Capacity pressure should be inferred primarily from engine counters (`FullFails`, `ProbeFails`) and queue pressure, not from duplicates alone.
- For comparable runs, use a fresh history directory per benchmark scenario.

## Repository layout

- `HistoryDB/`
  - Core library implementation and policies.
  - Public API centered on `HistoryDatabase`.
  - Detailed docs: `HistoryDB/README.md`.
- `HisBench/`
  - CLI benchmark app for phased and mixed workloads.
  - Includes auto-tuning mode and restart analysis.
  - Detailed docs: `HisBench/README.md`, `HisBench/TUNING.md`.
- `HisCTL/`
  - Operator CLI: `scan`, `repair`, `defrag`, `expire` (time + recycle).
  - Detailed docs: `HisCTL/README.md`.

## Quick start

Build everything:

```bash
dotnet build ArticleDatabase.sln -c Release
```

Run **HisCTL** help (scan / repair / defrag / expire):

```bash
dotnet run --project HisCTL -- --help
```

Run benchmark help:

```bash
dotnet run --project HisBench -- --help
```

Run a baseline benchmark:

```bash
dotnet run --project HisBench -- \
  --history-dir ./hisbench-data \
  --add-count 5000000 \
  --lookup-count 1000000 \
  --add-forks 8 \
  --lookup-forks 8
```

Run automatic sweep mode:

```bash
dotnet run --project HisBench -- \
  --history-dir ./hisbench-data \
  --auto
```

## Current default topology

Current defaults in this repo are tuned for moderate single-host performance:

- `shards=128`
- `slotsPerShard=1 << 22` (4,194,304 slots, ~128 MiB data file per shard)
- `maxLoadFactorPercent=75`

See `HistoryDB/README.md` for performance characteristics and observed benchmark results.

## Notes for contributors

- Keep benchmark comparisons on fresh data directories to avoid cross-run contamination.
- Prefer recording both throughput and tail latency (`p95`/`p99`) when sharing results.
- For large restart runs, review restart warmup summaries in HisBench output.
