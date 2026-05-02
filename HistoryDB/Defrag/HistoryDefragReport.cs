// HistoryDefragReport.cs -- structured outcome of HistoryDatabase.HistoryDefragAsync.
// Aggregate counters describe the operation's effect; per-generation outcomes record skips / failures / counts.

using System.Collections.ObjectModel;

namespace HistoryDB.Defrag;

/// <summary>
/// Per-generation result reported by <see cref="HistoryDB.HistoryDatabase.HistoryDefragAsync(HistoryDefragOptions?, System.Threading.CancellationToken)"/>.
/// </summary>
/// <param name="GenerationId">Generation id this outcome is reporting on.</param>
/// <param name="Compacted"><see langword="true"/> when the source generation was successfully replaced with the compacted sibling.</param>
/// <param name="SlotsScanned">Number of slots traversed in the source generation (including empty/tombstone slots).</param>
/// <param name="SurvivorsCopied">Number of non-expired non-tombstone slots copied into the sibling.</param>
/// <param name="ExpiredEntriesDropped">Number of slots whose hash was found in the expired filter and therefore not copied.</param>
/// <param name="TombstonesDropped">Number of tombstone-marker slots encountered and not copied.</param>
/// <param name="BytesReclaimed">Approximate disk bytes returned to the filesystem (source directory size minus sibling directory size).</param>
/// <param name="SkipReason">Non-null when the generation was skipped (e.g. below MinExpiredFractionToCompact, active generation, etc.).</param>
public readonly record struct GenerationDefragOutcome(
    int GenerationId,
    bool Compacted,
    long SlotsScanned,
    long SurvivorsCopied,
    long ExpiredEntriesDropped,
    long TombstonesDropped,
    long BytesReclaimed,
    string? SkipReason);

/// <summary>
/// Aggregate report returned by <see cref="HistoryDB.HistoryDatabase.HistoryDefragAsync(HistoryDefragOptions?, System.Threading.CancellationToken)"/>.
/// </summary>
public sealed class HistoryDefragReport
{
    private readonly List<GenerationDefragOutcome> _generationOutcomes = [];

    /// <summary>Total number of retained generations considered (including those skipped).</summary>
    public int GenerationsConsidered { get; internal set; }

    /// <summary>Number of generations actually compacted (subset of <see cref="GenerationsConsidered"/>).</summary>
    public int GenerationsCompacted { get; internal set; }

    /// <summary>Sum of <see cref="GenerationDefragOutcome.SlotsScanned"/> across all considered generations.</summary>
    public long SlotsScanned { get; internal set; }

    /// <summary>Sum of <see cref="GenerationDefragOutcome.SurvivorsCopied"/> across all compacted generations.</summary>
    public long SurvivorsCopied { get; internal set; }

    /// <summary>Sum of <see cref="GenerationDefragOutcome.ExpiredEntriesDropped"/> across all compacted generations.</summary>
    public long ExpiredEntriesDropped { get; internal set; }

    /// <summary>Sum of <see cref="GenerationDefragOutcome.TombstonesDropped"/> across all compacted generations.</summary>
    public long TombstonesDropped { get; internal set; }

    /// <summary>Sum of <see cref="GenerationDefragOutcome.BytesReclaimed"/> across all compacted generations.</summary>
    public long BytesReclaimed { get; internal set; }

    /// <summary>Number of hashes pruned from the in-memory expired filter and <c>expired-md5.bin</c>.</summary>
    public long ExpiredEntriesPruned { get; internal set; }

    /// <summary>Per-generation outcomes (in the order they were processed).</summary>
    public ReadOnlyCollection<GenerationDefragOutcome> Generations => _generationOutcomes.AsReadOnly();

    internal void AddGeneration(GenerationDefragOutcome outcome)
    {
        _generationOutcomes.Add(outcome);
    }
}
