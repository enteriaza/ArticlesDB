// HistoryThroughputTuning.cs -- public knobs for insert throughput vs durability trade-offs.

namespace HistoryDB;

/// <summary>
/// Controls whether Bloom checkpoint enqueue blocks shard writers when the persist channel is full.
/// </summary>
public enum BloomCheckpointPersistMode
{
    /// <summary>
    /// Best-effort: enqueue via <c>TryWrite</c>; when the channel is full the checkpoint is dropped and <see cref="HistoryPerformanceSnapshot.BloomCheckpointsDropped"/> increments.
    /// </summary>
    Throughput = 0,

    /// <summary>
    /// Shard writers block until each checkpoint is accepted by the persist channel (stronger durability of on-disk Bloom snapshots under sustained load).
    /// </summary>
    Durability = 1
}

/// <summary>
/// Controls pre-enqueue scanning of older generations for duplicate message hashes.
/// </summary>
public enum HistoryCrossGenerationDuplicateCheck
{
    /// <summary>
    /// Consult all retained generations before enqueue (correct cross-generation duplicate semantics).
    /// </summary>
    Full = 0,

    /// <summary>
    /// Skip cross-generation pre-check; duplicates are still detected within the active shard table.
    /// Unsafe if the same logical message can legitimately appear in multiple generations—intended for synthetic unique-key benchmarks.
    /// </summary>
    ActiveGenerationOnly = 1
}
