// ShardedHistoryWriter.Logging.cs -- source-generated [LoggerMessage] definitions for structured lifecycle, throughput, and fault logging in ShardedHistoryWriter.
// Keeps logging allocation-free on hot paths and centralizes event IDs/messages to satisfy CA1848 and CONTRIBUTING logging standards.

using Microsoft.Extensions.Logging;

namespace HistoryDB;

internal sealed partial class ShardedHistoryWriter
{
    [LoggerMessage(EventId = 1000, Level = LogLevel.Information, Message = "ShardedHistoryWriter initialized at {RootPath} with {ShardCount} shards, {SlotsPerShard} slots/shard, max load {MaxLoadFactorPercent}%.")]
    private partial void LogInitialized(string rootPath, int shardCount, ulong slotsPerShard, ulong maxLoadFactorPercent);

    [LoggerMessage(EventId = 1001, Level = LogLevel.Warning, Message = "Shard writer CPU affinity pin failed (example shard={ShardId}, processor={Processor}). On Windows, >64 logical processors in one group are not supported by current pin API.")]
    private partial void LogShardPinFailed(int shardId, int processor);

    [LoggerMessage(EventId = 1002, Level = LogLevel.Information, Message = "HistoryDB started; active generation is {GenerationId:X5}.")]
    private partial void LogStarted(int generationId);

    [LoggerMessage(EventId = 1003, Level = LogLevel.Debug, Message = "Background slot scrubber started.")]
    private partial void LogSlotScrubberStarted();

    [LoggerMessage(EventId = 1004, Level = LogLevel.Critical, Message = "EnqueueAsync failed with an unexpected exception (not I/O or disposal). Returning Full.")]
    private partial void LogEnqueueFailedCritical(Exception ex);

    [LoggerMessage(EventId = 1026, Level = LogLevel.Error, Message = "EnqueueAsync failed with I/O or access error; rethrowing.")]
    private partial void LogEnqueueIoFailed(Exception ex);

    [LoggerMessage(EventId = 1036, Level = LogLevel.Error, Message = "Exists failed with an unexpected non-I/O exception; returning false for safety.")]
    private partial void LogExistsUnexpectedException(Exception ex);

    [LoggerMessage(EventId = 1027, Level = LogLevel.Error, Message = "Exists failed with I/O or access error; rethrowing.")]
    private partial void LogExistsIoFailed(Exception ex);

    [LoggerMessage(EventId = 1006, Level = LogLevel.Information, Message = "HistoryDB stop requested.")]
    private partial void LogStopRequested();

    [LoggerMessage(EventId = 1007, Level = LogLevel.Error, Message = "Shutdown wait completed with unexpected error.")]
    private partial void LogShutdownWaitFailed(Exception ex);

    [LoggerMessage(EventId = 1030, Level = LogLevel.Error, Message = "Shutdown: background task faulted (taskIndex={TaskIndex}).")]
    private partial void LogShutdownWriterTaskFaulted(int taskIndex, Exception ex);

    [LoggerMessage(EventId = 1008, Level = LogLevel.Error, Message = "Shard dispose failed during shutdown (generation={GenerationId}, shard={ShardId}).")]
    private partial void LogShardDisposeFailedDuringShutdown(Exception ex, int generationId, int shardId);

    [LoggerMessage(EventId = 1009, Level = LogLevel.Information, Message = "HistoryDB stopped.")]
    private partial void LogStopped();

    [LoggerMessage(EventId = 1010, Level = LogLevel.Warning, Message = "Slot scrub volatile layout mismatch (generation={GenerationId}, shard={ShardId}, index={Index}).")]
    private partial void LogSlotScrubMismatch(int generationId, int shardId, ulong index);

    [LoggerMessage(EventId = 1011, Level = LogLevel.Warning, Message = "Trusted Bloom missed occupied slot (generation={GenerationId}, shard={ShardId}); scheduling shard Bloom re-warm.")]
    private partial void LogTrustedBloomMissed(int generationId, int shardId);

    [LoggerMessage(EventId = 1012, Level = LogLevel.Error, Message = "Bloom re-warm after scrub failed (generation={GenerationId}, shard={ShardId}).")]
    private partial void LogBloomRewarmFailed(Exception ex, int generationId, int shardId);

    [LoggerMessage(EventId = 1013, Level = LogLevel.Information, Message = "Activated generation {GenerationId:X5}.")]
    private partial void LogGenerationActivated(int generationId);

    [LoggerMessage(EventId = 1014, Level = LogLevel.Debug, Message = "Scheduled retire for generation {GenerationId:X5}.")]
    private partial void LogRetireScheduled(int generationId);

    [LoggerMessage(EventId = 1015, Level = LogLevel.Warning, Message = "Bloom checkpoint write failed for {Path}.")]
    private partial void LogBloomCheckpointWriteFailed(Exception ex, string path);

    [LoggerMessage(EventId = 1016, Level = LogLevel.Warning, Message = "Bloom periodic checkpoint skipped (snapshot exceeds max {MaxSnapshotBytes} bytes): {Path}")]
    private partial void LogBloomCheckpointSkipped(ulong maxSnapshotBytes, string path);

    [LoggerMessage(EventId = 1017, Level = LogLevel.Error, Message = "Bloom persist loop task faulted.")]
    private partial void LogBloomPersistLoopFaulted(Exception ex);

    [LoggerMessage(EventId = 1018, Level = LogLevel.Error, Message = "Bloom warm-up save failed (generation={GenerationId}, shard={ShardId}).")]
    private partial void LogBloomWarmupSaveFailed(Exception ex, int generationId, int shardId);

    [LoggerMessage(EventId = 1019, Level = LogLevel.Error, Message = "Retire generation wait failed (generation={GenerationId}).")]
    private partial void LogRetireGenerationWaitFailed(Exception ex, int generationId);

    [LoggerMessage(EventId = 1020, Level = LogLevel.Error, Message = "Shard dispose failed during retire (generation={GenerationId}, shard={ShardId}).")]
    private partial void LogShardDisposeFailedDuringRetire(Exception ex, int generationId, int shardId);

    [LoggerMessage(EventId = 1021, Level = LogLevel.Error, Message = "Bloom save failed (generation={GenerationId}, shard={ShardId}).")]
    private partial void LogBloomSaveFailed(Exception ex, int generationId, int shardId);

    [LoggerMessage(EventId = 1022, Level = LogLevel.Warning, Message = "Bloom sidecar rejected ({DiagnosticsTag}): {Path}.")]
    private static partial void LogBloomSidecarRejected(ILogger logger, Exception ex, string diagnosticsTag, string path);

    [LoggerMessage(EventId = 1023, Level = LogLevel.Error, Message = "Enqueue attempt failed (generation={GenerationId}, shard={ShardId}, attempt={Attempt}).")]
    private partial void LogEnqueueAttemptFailed(Exception ex, int generationId, int shardId, int attempt);

    [LoggerMessage(EventId = 1024, Level = LogLevel.Critical, Message = "Shard writer loop crashed (generation={GenerationId}, shard={ShardId}). Pending requests are faulted.")]
    private partial void LogShardWriterCrashed(Exception ex, int generationId, int shardId);

    [LoggerMessage(EventId = 1025, Level = LogLevel.Information, Message = "Bloom checkpoint queue full; wrote synchronously: {Path}.")]
    private partial void LogBloomCheckpointSyncFallback(string path);

    [LoggerMessage(EventId = 1100, Level = LogLevel.Error, Message = "Compaction sibling preparation failed (generation={GenerationId}, sibling={SiblingDir}).")]
    private partial void LogCompactSiblingPrepareFailed(Exception ex, int generationId, string siblingDir);

    [LoggerMessage(EventId = 1101, Level = LogLevel.Error, Message = "Compaction build failed (generation={GenerationId}).")]
    private partial void LogCompactBuildFailed(Exception ex, int generationId);

    [LoggerMessage(EventId = 1102, Level = LogLevel.Warning, Message = "Compaction sibling cleanup failed (generation={GenerationId}, sibling={SiblingDir}).")]
    private partial void LogCompactSiblingCleanupFailed(Exception ex, int generationId, string siblingDir);

    [LoggerMessage(EventId = 1103, Level = LogLevel.Warning, Message = "Compaction drain of source generation {GenerationId} writers reported failure.")]
    private partial void LogCompactDrainFailed(Exception ex, int generationId);

    [LoggerMessage(EventId = 1104, Level = LogLevel.Error, Message = "Compaction atomic swap failed (generation={GenerationId}).")]
    private partial void LogCompactSwapFailed(Exception ex, int generationId);

    [LoggerMessage(EventId = 1105, Level = LogLevel.Error, Message = "Compaction re-open failed (generation={GenerationId}).")]
    private partial void LogCompactReopenFailed(Exception ex, int generationId);

    [LoggerMessage(EventId = 1106, Level = LogLevel.Warning, Message = "Compaction retired-source cleanup failed: {Path}.")]
    private partial void LogCompactRetiredCleanupFailed(Exception ex, string path);

    [LoggerMessage(EventId = 1107, Level = LogLevel.Information, Message = "Compaction completed (generation={GenerationId}, slotsScanned={SlotsScanned}, survivors={Survivors}, expiredDropped={ExpiredDropped}, tombstonesDropped={TombstonesDropped}, bytesReclaimed={BytesReclaimed}).")]
    private partial void LogCompactCompleted(int generationId, long slotsScanned, long survivors, long expiredDropped, long tombstonesDropped, long bytesReclaimed);

    [LoggerMessage(EventId = 1108, Level = LogLevel.Warning, Message = "Compaction skipped: insufficient disk space (generation={GenerationId}, availableBytes={AvailableBytes}, minimumRequired={MinimumRequired}).")]
    private partial void LogCompactSkippedLowDisk(int generationId, long availableBytes, ulong minimumRequired);

    [LoggerMessage(EventId = 1032, Level = LogLevel.Warning, Message = "Directory size probe failed for {Path}: {Message}. Directory reclaim metrics may be inaccurate.")]
    private partial void LogDirectorySizeProbeAccessDenied(string path, string message);

    [LoggerMessage(EventId = 1033, Level = LogLevel.Warning, Message = "Bloom persistence skipped: circuit breaker is open.")]
    private partial void LogBloomPersistSkippedOpenCircuit();

    [LoggerMessage(EventId = 1034, Level = LogLevel.Warning, Message = "Bloom checkpoint skipped for low disk space at {Path} (available={AvailableBytes}, minimum={MinimumRequired}).")]
    private partial void LogBloomCheckpointSkippedLowDisk(string path, long availableBytes, ulong minimumRequired);
}
