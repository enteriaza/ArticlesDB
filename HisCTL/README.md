# HisCTL

**HisCTL** is the command-line operator utility for **HistoryDB**: consistency **scan**, filesystem **repair**, online **defrag**, and **bulk expiration** (time-based or recycle-ranked), with the same hand-rolled flag style as [HisBench](../HisBench/README.md).

Build (from repo root):

```bash
dotnet build HisCTL/HisCTL.csproj -c Release
```

Run help:

```bash
dotnet run --project HisCTL -- --help
```

## Milestone: NNTP history and LRU-style expiration

**ArticlesDB HistoryDB is the first NNTP-oriented history database to support operator-driven bulk expiration under LRU-style policies** — not only single-message `HistoryExpire`, but **ranked eviction** over occupied slots (`expire recycle`) using last access time, access counters, insert time, and hash tie-breaks, plus **age-based** expiry (`expire time`) against MySQL-style `INTERVAL` cutoffs on `date obtained` or `date accessed` ticks. That combination (mmap sharded generations, Bloom sidecars, tombstones, persisted `expired-md5.bin`, and **policy-driven bulk selection**) is not something traditional NNTP history backends have typically exposed as a first-class, automatable path.

Details of the on-disk layout and APIs live in [HistoryDB/README.md](../HistoryDB/README.md).

## Verbs

| Verb | Purpose |
|------|---------|
| `scan` | Read-only `HistoryDatabase.ScanAsync`. Default opens are **exclusive** (strongest guarantees). With `--shared`, uses shared read opens so a **live** `HistoryDatabase` can keep the tree open; findings are **best-effort** under concurrent writers (transient torn slots may appear). |
| `repair` | `HistoryDatabase.RepairAsync` with exclusive locks. **Stop the server** (or any writer) on that root before running. |
| `defrag` | Opens `HistoryDatabase`, runs `HistoryDefragAsync`. Prefer **no other writers**; defrag competes for the same mmap/queue resources. |
| `expire` | Harvests slot metadata, selects hashes (**time** or **recycle**), default **dry-run**; `--apply` writes `expired-md5.bin`, enqueues tombstones in batches, persists, then `HistorySync()` is invoked from the tool. |

## Examples

Shared scan while a server holds the directory (diagnostics only):

```bash
dotnet run --project HisCTL -- scan --shared --history-dir /data/history
```

Offline repair (exclusive):

```bash
dotnet run --project HisCTL -- repair --history-dir /data/history --policy quarantine
```

Defrag with defaults tuned in code / CLI flags:

```bash
dotnet run --project HisCTL -- defrag --history-dir /data/history
```

Expire articles older than one day by **obtained** time (dry-run — prints counts and sample hashes):

```bash
dotnet run --project HisCTL -- expire time --older-than "INTERVAL 1 DAY" --history-dir /data/history
```

Same, but using **last accessed** ticks, and actually mutating:

```bash
dotnet run --project HisCTL -- expire time --older-than "INTERVAL 1 DAY" --by accessed --history-dir /data/history --apply
```

**Recycle** mode: sort by `(date_accessed_ticks asc, accessed_counter asc, date_obtained_ticks asc, hash)` — stale, low-touch rows first — then expire the first `--limit` hashes (hybrid **stale-first + LFU-style** tie-break, not a pure single-LRU list):

```bash
dotnet run --project HisCTL -- expire recycle --limit 10000 --history-dir /data/history
dotnet run --project HisCTL -- expire recycle --limit 10000 --history-dir /data/history --apply
```

## Flags

Full flag reference is emitted by `--help` on the executable. Common knobs match a running server: `--history-dir`, `--shards`, `--slots-per-shard`, `--load-factor`, `--queue-capacity`, `--retained-generations`.

- **`--expire-parallel`** — tombstone fan-out parallelism per internal batch (default 64).
- **`INTERVAL`** grammar — see `HistoryDB/Utilities/MySqlIntervalExpression.cs`; `MONTH` / `YEAR` follow `DateTime` calendar rules from an anchor `UtcNow`, not every MySQL calendar edge case.

## Exit codes

- `0` — success  
- `1` — usage / parse error  
- `2` — operational failure (for example, scan reported unhealthy / errors)

## See also

- [HistoryDB/README.md](../HistoryDB/README.md) — scan (including shared read), repair, defrag, and programmatic `HistoryExpireByHashesAsync` / metadata enumeration.  
- [HisBench/README.md](../HisBench/README.md) — throughput and latency benchmarking.
