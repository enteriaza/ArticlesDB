// HistoryMetadataStore.Logging.cs -- source-generated logging for HistoryMetadataStore.

using Microsoft.Extensions.Logging;

namespace HistoryDB.Application;

internal sealed partial class HistoryMetadataStore
{
    [LoggerMessage(EventId = 2001, Level = LogLevel.Information, Message = "HistoryDB journal replay for {JournalPath} skipped {InvalidCount} invalid line(s).")]
    private partial void LogJournalReplayInvalidSummary(string journalPath, int invalidCount);
}
