// HistoryDatabaseContracts.cs -- application contracts for the public HistoryDatabase facade.

namespace HistoryDB.Contracts;

internal interface IMessageHashProvider
{
    Hash128 ComputeHash(string text);
}

internal interface IHistoryMetadataStore : IAsyncDisposable
{
    bool IsExpired(Hash128 messageHash);

    void RecordInserted(string messageId, string serverId, Hash128 messageHash, DateTimeOffset createdAtUtc, bool persistJournalAndWalk = true);

    bool Expire(Hash128 messageHash);

    int Expire(ReadOnlySpan<Hash128> messageHashes);

    void PersistExpiredSet();

    int Walk(ref long position, out HistoryWalkEntry entry);

    long GetEvictedWalkEntryCount();

    ValueTask FlushJournalAsync(CancellationToken cancellationToken = default);
}

internal readonly record struct Hash128(ulong Hi, ulong Lo);

internal readonly record struct HistoryWriterMetrics(
    int ActiveGenerationId,
    int RetainedGenerations,
    long FullInsertFailures,
    long ProbeLimitFailures,
    long PendingQueueItemsApprox,
    ulong UsedSlotsApprox);
