// HistoryDefragOptions.cs -- caller-tunable knobs for HistoryDatabase.HistoryDefragAsync online compaction.
// All defaults are safe for production: small candidate threshold, single-generation parallelism, expired-set pruning enabled.

namespace HistoryDB.Defrag;

/// <summary>
/// Options controlling <see cref="HistoryDB.HistoryDatabase.HistoryDefragAsync(HistoryDefragOptions?, System.Threading.CancellationToken)"/>.
/// </summary>
public sealed record HistoryDefragOptions
{
    /// <summary>
    /// When <see langword="true"/> (default), forces a generation rollover before selecting compaction candidates so the previously-active
    /// generation becomes a sealed candidate. Without this, the active generation is excluded from compaction (it still receives inserts).
    /// </summary>
    public bool ForceRolloverActiveGeneration { get; init; } = true;

    /// <summary>
    /// Skip generations whose expired-and-tombstoned fraction is below this fraction (0.0..1.0). The pre-scan computes the fraction in
    /// O(slots) reading just <c>hash_hi</c>/<c>hash_lo</c> per slot; defaults to 0.05 (skip if &lt; 5% reclaimable).
    /// </summary>
    public double MinExpiredFractionToCompact { get; init; } = 0.05;

    /// <summary>Cap on concurrent per-generation compactions. Defaults to <c>1</c> (sequential) to bound transient disk pressure.</summary>
    public int MaxParallelGenerations { get; init; } = 1;

    /// <summary>
    /// When <see langword="true"/> (default), prunes hashes from the in-memory expired set and <c>expired-md5.bin</c> sidecar that no longer
    /// occur in any retained generation after compaction. Disabling this is useful for tests that want to verify the expired set independently.
    /// </summary>
    public bool PruneExpiredSetAfterCompaction { get; init; } = true;
}
