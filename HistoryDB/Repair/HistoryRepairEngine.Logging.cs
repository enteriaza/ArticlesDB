// HistoryRepairEngine.Logging.cs -- source-generated [LoggerMessage] partials for the offline scan / repair engine.
// Centralizes EventIds and messages so the engine logs cleanly under the project's CA1848 policy.

using Microsoft.Extensions.Logging;

namespace HistoryDB.Repair;

internal sealed partial class HistoryRepairEngine
{
    [LoggerMessage(EventId = 5000, Level = LogLevel.Information, Message = "HistoryDB repair engine starting at {RootPath} (scanOnly={ScanOnly}, policy={Policy}).")]
    private static partial void LogEngineStarting(ILogger logger, string rootPath, bool scanOnly, RepairPolicy policy);

    [LoggerMessage(EventId = 5001, Level = LogLevel.Information, Message = "HistoryDB repair engine completed: generations={GenerationsScanned}, shards={ShardsScanned}, slotsScanned={SlotsScanned}, occupied={OccupiedSlotsObserved}, headersRewritten={HeadersRewritten}, bloomsRewritten={BloomSidecarsRewritten}, quarantined={FilesQuarantined}, healthy={IsHealthy}.")]
    private static partial void LogEngineCompleted(
        ILogger logger,
        int generationsScanned,
        int shardsScanned,
        ulong slotsScanned,
        ulong occupiedSlotsObserved,
        int headersRewritten,
        int bloomSidecarsRewritten,
        int filesQuarantined,
        bool isHealthy);

    [LoggerMessage(EventId = 5002, Level = LogLevel.Warning, Message = "HistoryDB repair aborted because {Path} could not be opened exclusively; another process likely has the directory open.")]
    private static partial void LogAbortLocked(ILogger logger, string path);

    [LoggerMessage(EventId = 5003, Level = LogLevel.Information, Message = "Quarantined corrupt file {Source} -> {Destination}.")]
    private static partial void LogQuarantined(ILogger logger, string source, string destination);

    [LoggerMessage(EventId = 5004, Level = LogLevel.Information, Message = "Rewrote shard header at {Path} (usedSlots {OldUsedSlots} -> {NewUsedSlots}, watermark {OldWatermark} -> {NewWatermark}).")]
    private static partial void LogHeaderRewritten(ILogger logger, string path, ulong oldUsedSlots, ulong newUsedSlots, ulong oldWatermark, ulong newWatermark);

    [LoggerMessage(EventId = 5005, Level = LogLevel.Information, Message = "Rewrote Bloom sidecar at {Path} ({OccupiedSlotsObserved} occupied slots, {BitCount} bits, {HashCount} hashes).")]
    private static partial void LogBloomRewritten(ILogger logger, string path, ulong occupiedSlotsObserved, int bitCount, int hashCount);

    [LoggerMessage(EventId = 5006, Level = LogLevel.Information, Message = "Truncated {Path} to drop {DroppedBytes} trailing bytes.")]
    private static partial void LogTruncatedTail(ILogger logger, string path, int droppedBytes);

    [LoggerMessage(EventId = 5007, Level = LogLevel.Information, Message = "Rewrote {Path} dropping {DroppedLines} unparseable journal lines (kept {KeptLines}).")]
    private static partial void LogJournalRewritten(ILogger logger, string path, int droppedLines, int keptLines);

    [LoggerMessage(EventId = 5008, Level = LogLevel.Warning, Message = "Encountered unrecoverable shard data file at {Path}: {Detail}.")]
    private static partial void LogUnrecoverableShard(ILogger logger, string path, string detail);

    [LoggerMessage(EventId = 5009, Level = LogLevel.Warning, Message = "Zeroed {ZeroedSlots} torn slot(s) in {Path}.")]
    private static partial void LogTornSlotsZeroed(ILogger logger, string path, int zeroedSlots);

    [LoggerMessage(EventId = 5010, Level = LogLevel.Warning, Message = "Repair engine encountered an I/O error processing {Path}: {Detail}")]
    private static partial void LogIoError(ILogger logger, Exception ex, string path, string detail);
}
