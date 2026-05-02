// HistoryDefragEngine.Logging.cs -- source-generated [LoggerMessage] definitions for the online defrag pipeline.
// EventIds 6000-6099 are reserved for HistoryDB.Defrag to avoid collisions with ShardedHistoryWriter (1000-1999) and HistoryRepairEngine (5000-5999).

using Microsoft.Extensions.Logging;

namespace HistoryDB.Defrag;

internal static partial class HistoryDefragEngine
{
    [LoggerMessage(EventId = 6000, Level = LogLevel.Information, Message = "HistoryDB defrag started: expiredSnapshot={ExpiredCount}, forceRollover={ForceRollover}, minExpiredFraction={MinFraction}, maxParallel={MaxParallel}.")]
    private static partial void LogDefragStarted(ILogger logger, int expiredCount, bool forceRollover, double minFraction, int maxParallel);

    [LoggerMessage(EventId = 6001, Level = LogLevel.Warning, Message = "HistoryDB defrag forced rollover failed; continuing with the current generation set.")]
    private static partial void LogDefragRolloverFailed(ILogger logger, Exception ex);

    [LoggerMessage(EventId = 6002, Level = LogLevel.Information, Message = "HistoryDB defrag found no compaction candidates (only the active generation is retained).")]
    private static partial void LogDefragNoCandidates(ILogger logger);

    [LoggerMessage(EventId = 6003, Level = LogLevel.Debug, Message = "HistoryDB defrag skipping generation {GenerationId}: reclaimable fraction {Fraction} below threshold {Threshold} (would reclaim {ReclaimableSlots} slots).")]
    private static partial void LogDefragGenerationSkipped(ILogger logger, int generationId, double fraction, double threshold, long reclaimableSlots);

    [LoggerMessage(EventId = 6004, Level = LogLevel.Information, Message = "HistoryDB defrag skipped all {CandidateCount} candidates; no generations met MinExpiredFractionToCompact.")]
    private static partial void LogDefragAllSkipped(ILogger logger, int candidateCount);

    [LoggerMessage(EventId = 6005, Level = LogLevel.Warning, Message = "HistoryDB defrag generation {GenerationId} was not compacted: {Reason}.")]
    private static partial void LogDefragGenerationSkippedDuringCompaction(ILogger logger, int generationId, string reason);

    [LoggerMessage(EventId = 6007, Level = LogLevel.Error, Message = "HistoryDB defrag parallel compaction batch failed unexpectedly.")]
    private static partial void LogDefragCompactionBatchFailed(ILogger logger, Exception ex);

    [LoggerMessage(EventId = 6006, Level = LogLevel.Information, Message = "HistoryDB defrag completed: considered={Considered}, compacted={Compacted}, survivors={Survivors}, expiredDropped={ExpiredDropped}, tombstonesDropped={TombstonesDropped}, bytesReclaimed={BytesReclaimed}, expiredSetPruned={ExpiredPruned}.")]
    private static partial void LogDefragCompleted(
        ILogger logger,
        int considered,
        int compacted,
        long survivors,
        long expiredDropped,
        long tombstonesDropped,
        long bytesReclaimed,
        long expiredPruned);
}
