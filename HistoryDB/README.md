# HistoryDB

`HistoryDB` is a high-performance, generation-based, sharded history library for deduplicating 128-bit content hashes at very high write rates.

It is optimized for backend workloads where low-latency membership checks and sustained append-heavy ingestion are critical.

## What the library does

- Stores hashes in memory-mapped shard files using open addressing.
- Routes writes to shard-specific single-reader queues for concurrency safety and throughput.
- Uses Bloom sidecars to accelerate negative lookups across retained generations.
- Supports proactive generation rollover to avoid long probe paths under pressure.
- Includes optional background scrub and warm-up workflows for data hygiene and Bloom trust recovery.
- Supports optional thread affinity / NUMA-aware pinning for locality-focused deployments.

## Core Concepts

### Generation

A generation is a set of shard files (`0000.dat`..`00ff.dat` for 128 shards) plus Bloom sidecars (`0000.idx`..`00ff.idx`) under one generation folder named as 5-digit lowercase hexadecimal (`00000`..`fffff`, for example `00001`).

New writes target the active generation. Older generations remain queryable until retention retires them.

### Shard

A shard is a fixed-size memory-mapped hash table (`IShardStore`) with:

- 128-bit hash key (`hashHi`, `hashLo`)
- optional payload fields (`serverHi`, `serverLo`)
- probe-limit and load-factor protections

**On-disk format:** header `version` is always **1**. Table stride follows file length: **56 bytes** per slot (payload only) or **60 bytes** (payload plus little-endian **CRC32**, IEEE, over the first 56 bytes). New shard files use the CRC layout; older trees without the suffix remain readable. `HistorySync` best-effort **flushes** mapped shard views after header updates.

**`expired-md5.bin`:** new deployments write a **versioned header + payload CRC + hash list** and replace the file **atomically** (temp + flush + move). Legacy raw files (length multiple of 16) still load; malformed lengths fail startup with a clear error.

### Bloom sidecar

Each shard can have a Bloom sidecar that accelerates `Exists(...)` by skipping shard probes when membership is impossible.

## Public API

The public API is intentionally small:

- `HistoryDatabase(...)` (initialization)
- `bool HistoryLookup(string messageId)`
- `ValueTask<bool> HistoryLookupAsync(string messageId, CancellationToken)`
- `bool HistoryAdd(string messageId, string serverId)`
- `ValueTask<bool> HistoryAddAsync(string messageId, string serverId, CancellationToken)`
- `bool HistoryExpire(string messageId)`
- `int HistoryExpire(string[] messageIds)`
- `Task<int> HistoryExpireByHashesAsync(IReadOnlyList<Hash128> hashes, int parallelTombstones = 64, CancellationToken cancellationToken = default)` (bulk tombstone + persist)
- `IAsyncEnumerable<StoredArticleMetadata> EnumerateStoredArticleMetadataAsync(bool skipIfExpired = true, CancellationToken cancellationToken = default)` (read-only slot harvest)
- `bool IsMessageHashExpired(Hash128 messageHash)`
- `void HistorySync()`
- `HistoryHealthSnapshot GetHealthSnapshot()` (journal / bloom circuit flags and journal dirty marker)
- `IReadOnlyList<Exception> GetShutdownFaults()` (writer background task faults observed during shutdown)
- `int HistoryWalk(ref long position, out HistoryWalkEntry entry, int flags = 0)`
- `Task<HistoryDefragReport> HistoryDefragAsync(HistoryDefragOptions? options = null, CancellationToken cancellationToken = default)` (online defrag)
- `static Task<HistoryRepairReport> HistoryDatabase.ScanAsync(string rootPath, ...)` (offline diagnostic)
- `static Task<HistoryRepairReport> HistoryDatabase.RepairAsync(string rootPath, HistoryRepairOptions options, ...)` (offline repair)

Performance tuning methods:

- `SetBloomCheckpointInsertInterval(ulong inserts)`
- `SetRolloverThresholds(ulong usedSlotsThreshold, int queueDepthThreshold, long aggregateProbeFailuresThreshold)`
- `SetSlotScrubberTuning(int samplesPerTick, int intervalMilliseconds)`

## Configuration

Use `HistoryWriterOptions` with nested option groups:

- `BloomOptions`
- `RolloverOptions`
- `SlotScrubberOptions`
- `AffinityOptions`

Minimum required setting:

- `RootPath`

## Quick Start

```csharp
using Microsoft.Extensions.Logging;

ILogger? logger = null; // Provide ILogger in real host apps.
HistoryDatabase db = new(
    rootPath: @"C:\historydb",
    shardCount: 128,
    slotsPerShard: 1UL << 22,
    maxLoadFactorPercent: 75,
    logger: logger);

bool inserted = db.HistoryAdd("<message-id@host>", "7b136f3d-eeb7-4777-95f4-b9e6eeec40ea");
bool exists = db.HistoryLookup("<message-id@host>");
db.HistoryExpire("<message-id@host>");
db.HistorySync();
```

## Generic Host shutdown

For **ASP.NET Core** or `Microsoft.Extensions.Hosting` hosts, register `HistoryDatabase` as a singleton and add `HistoryDatabaseHostedService` so `StopAsync` flushes the journal and disposes mmap-backed writers **before** the process exits:

```csharp
using HistoryDB;
using HistoryDB.Application;

services.AddSingleton(sp => new HistoryDatabase(rootPath, logger: sp.GetRequiredService<ILogger<HistoryDatabase>>()));
services.AddHostedService(sp => new HistoryDatabaseHostedService(sp.GetRequiredService<HistoryDatabase>()));
```

## Architecture Overview

```mermaid
flowchart LR
    host[HostApplication] --> api[HistoryDatabase]
    api --> writer[ShardedHistoryWriter]
    writer --> policy1[RolloverPolicy]
    writer --> policy2[BloomSizingPolicy]
    writer --> shard[IShardStore]
    writer --> sidecar[IBloomSidecarStore]
    writer --> memory[ISystemMemoryInfo]
    writer --> affinity[IThreadAffinityService]
    writer --> numa[INumaTopologyService]
```

## Scan and repair

`HistoryDB` ships a scan / repair engine for the on-disk artifacts: shard data files (`.dat`), Bloom sidecars (`.idx`), the expired-hash set (`expired-md5.bin`), and the durable journal (`history-journal.log`). **`RepairAsync`** is **offline**: no `HistoryDatabase` instance may be open on that root; the engine probes with `FileShare.None` first and aborts with `LockedByOtherProcess` if anything is locked.

**`ScanAsync`** is always read-only. By default it uses the same exclusive probe semantics as repair (strongest consistency). For **live diagnostics**, pass `HistoryScanOptions { UseSharedRead = true }` so scan-only opens use shared read access while a running `HistoryDatabase` holds the tree; results are best-effort under concurrent writers (see [HisCTL/README.md](../HisCTL/README.md)).

- `HistoryDatabase.ScanAsync(rootPath, ...)` &mdash; read-only diagnostic; produces a structured `HistoryRepairReport` and never writes.
- `HistoryDatabase.RepairAsync(rootPath, options, ...)` &mdash; applies safe fixes per `HistoryRepairOptions`. Non-destructive recomputations (header watermarks, Bloom sidecar bits, expired-set tail truncation, optional journal rewrite) run under both policies. Unrecoverable shard `.dat` data is only renamed (`{name}.corrupt-{utcTicks:x}`) when `Policy = QuarantineAndRecreate`; otherwise it is reported and left untouched (`Policy = FailFast`, the default).

```csharp
using HistoryDB;
using HistoryDB.Repair;

// Diagnostic scan. Safe to invoke against any directory; never mutates files.
HistoryRepairReport scan = await HistoryDatabase.ScanAsync(@"C:\historydb");
foreach (RepairFinding finding in scan.Findings)
{
    Console.WriteLine($"{finding.Severity} {finding.Code}: {finding.Path} -- {finding.Detail}");
}

// Apply repairs. Default policy leaves unrecoverable shard data untouched.
HistoryRepairReport repair = await HistoryDatabase.RepairAsync(
    @"C:\historydb",
    new HistoryRepairOptions
    {
        Policy = RepairPolicy.QuarantineAndRecreate,
        RewriteJournalDroppingInvalidLines = true,
    });

Console.WriteLine($"Healthy={repair.IsHealthy}, headersRewritten={repair.HeadersRewritten}, bloomsRewritten={repair.BloomSidecarsRewritten}, quarantined={repair.FilesQuarantined}");
```

## Online defrag

Expiration alone records MD5 hashes in `expired-md5.bin` and filters `HistoryLookup` / `HistoryWalk`; slots stay occupied on disk until something physically removes them. **`HistoryExpire` also issues a best-effort tombstone broadcast** (fire-and-forget): the matching slot in each retained generation is rewritten to a reserved sentinel (`hash_hi == 1`, `hash_lo == 0`) so probes treat it like a gap for new inserts (reclamation) and defrag can drop it without shifting slot data in place.

**`HistoryDefragAsync`** compacts **non-active** retained generations: it rebuilds each candidate into a sibling directory under `{root}/{gen:x5}-defrag-tmp/`, copies only survivors (not expired, not tombstoned), rebuilds Bloom sidecars, then atomically swaps the generation under the writer lifecycle lock. The old directory is renamed to `{token}.compact-old-{utcTicks:x}/` and deleted asynchronously. With **`ForceRolloverActiveGeneration`** (default `true`), the writer rolls over first so the previously active generation becomes a compaction target; concurrent inserts therefore land on the new active generation.

**Disk:** expect roughly **2×** a generation’s footprint until the swap finishes. Tune **`MinExpiredFractionToCompact`** to skip generations with little reclaimable data, and **`MaxParallelGenerations`** to cap how many generations compact at once.

Tombstones are **slot payload** values (`hash_hi == 1`, `hash_lo == 0`), not a separate on-disk header version.

```csharp
using HistoryDB;
using HistoryDB.Defrag;

HistoryDefragReport defrag = await db.HistoryDefragAsync(new HistoryDefragOptions
{
    ForceRolloverActiveGeneration = true,
    MinExpiredFractionToCompact = 0.05,
    MaxParallelGenerations = 1,
    PruneExpiredSetAfterCompaction = true,
});

Console.WriteLine(
    $"Compacted={defrag.GenerationsCompacted}, survivors={defrag.SurvivorsCopied}, " +
    $"dropped expired={defrag.ExpiredEntriesDropped}, dropped tombstones={defrag.TombstonesDropped}, pruned expired-file={defrag.ExpiredEntriesPruned}");
```

## Operational Notes

- **Durability model:** hash state lives in memory-mapped shard files; Bloom sidecars are best-effort accelerators.
- **Hashing model:** public `messageId` and `serverId` strings are internally hashed with MD5 and stored as 128-bit values.
- **Expire model:** expiration is managed explicitly by API calls and applied by lookup/walk logic; tombstones plus `HistoryDefragAsync` reclaim disk and reduce effective load in older generations.
- **Backpressure:** Bloom checkpoint persistence queue is bounded; drops are logged.
- **Logging:** structured logs use `Microsoft.Extensions.Logging` with source-generated `[LoggerMessage]` methods.
- **Fallback logger:** when no `ILogger` is provided, logging falls back to `System.Diagnostics.Trace`.

## Performance Characteristics

- Single-reader shard queues reduce lock contention on write paths.
- SIMD probing is used in shard probing hot paths when supported.
- Generation retention avoids full-table rewrites while allowing bounded historical lookup scope.
- Optional affinity pinning can improve locality for shard writer and maintenance workers.

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

## Build

```bash
dotnet build HistoryDB.csproj -c Release
```
