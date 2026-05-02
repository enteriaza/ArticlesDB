using Microsoft.Extensions.Logging;

namespace HistoryDB;

public sealed partial class HistoryDatabase
{
    [LoggerMessage(EventId = 2001, Level = LogLevel.Error, Message = "Tombstone broadcast task faulted.")]
    private partial void LogTombstoneBroadcastTaskFaulted(Exception ex);

    [LoggerMessage(EventId = 2002, Level = LogLevel.Error, Message = "Tombstone broadcast failed synchronously (enqueue).")]
    private partial void LogTombstoneBroadcastSyncFailed(Exception ex);

    [LoggerMessage(EventId = 2003, Level = LogLevel.Error, Message = "Journal writer flush failed during dispose.")]
    private partial void LogJournalWriterFlushDisposeFailed(Exception ex);

    [LoggerMessage(EventId = 2004, Level = LogLevel.Error, Message = "Journal stream flush failed during dispose.")]
    private partial void LogJournalStreamFlushDisposeFailed(Exception ex);

    [LoggerMessage(EventId = 2005, Level = LogLevel.Error, Message = "PersistExpiredSet failed after pruning; will retry on next HistorySync.")]
    private partial void LogPersistExpiredAfterPruneFailed(Exception ex);

    [LoggerMessage(EventId = 2006, Level = LogLevel.Warning, Message = "Invalid history journal line {LineNumber} at byte {LineStart}: {Reason}")]
    private partial void LogJournalLineInvalid(int lineNumber, long lineStart, string reason);

    [LoggerMessage(EventId = 2007, Level = LogLevel.Information, Message = "Journal trimmed: previous length {PreviousLength} bytes; retained tail {KeptBytes} bytes.")]
    private partial void LogJournalTrimmed(long previousLength, long keptBytes);

    [LoggerMessage(EventId = 2008, Level = LogLevel.Warning, Message = "Journal flush skipped: circuit open in cooldown.")]
    private partial void LogJournalCircuitOpenSkipFlush();

    [LoggerMessage(EventId = 2009, Level = LogLevel.Error, Message = "Journal flush failed.")]
    private partial void LogJournalFlushFailed(Exception ex);

    [LoggerMessage(EventId = 2010, Level = LogLevel.Warning, Message = "Failed to write journal dirty marker.")]
    private partial void LogJournalDirtyMarkerFailed(Exception ex);

    [LoggerMessage(EventId = 2011, Level = LogLevel.Critical, Message = "Journal append failed after successful shard insert (data may be ahead of journal).")]
    private partial void LogJournalAppendAfterInsertFailed(Exception ex);
}
