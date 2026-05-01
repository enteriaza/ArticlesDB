// ShardContracts.cs -- public contracts and DTOs for HistoryDB request/response payloads and diagnostics snapshots.

namespace HistoryDB.Contracts;

/// <summary>
/// Immutable request payload for a single shard write operation.
/// </summary>
/// <param name="HashHi">High 64 bits of content hash.</param>
/// <param name="HashLo">Low 64 bits of content hash.</param>
/// <param name="ServerHi">Opaque server payload high 64 bits.</param>
/// <param name="ServerLo">Opaque server payload low 64 bits.</param>
internal readonly record struct ShardInsertRequest(ulong HashHi, ulong HashLo, ulong ServerHi, ulong ServerLo);

/// <summary>
/// Point-in-time pressure metrics for one shard.
/// </summary>
internal readonly record struct ShardPressureSnapshot(
    int ShardId,
    int PendingInQueue,
    ulong UsedSlotsHeader,
    long FullInsertFailures,
    long ProbeLimitFailures);

/// <summary>
/// Monotonic counters from background slot scrubber.
/// </summary>
internal readonly record struct SlotScrubberCounters(
    long SamplesScrubbed,
    long VolatileLayoutMismatches,
    long BloomMismatchRepairs,
    long OccupiedBeyondWatermarkHints);
