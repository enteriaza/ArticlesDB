// IHistoryWriter.cs -- public abstraction for enqueueing deduplicated inserts and membership checks in HistoryDB.

namespace HistoryDB.Contracts;

/// <summary>
/// Public HistoryDB writer contract for queueing shard writes and querying hash existence.
/// </summary>
internal interface IHistoryWriter : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Enqueues a deduplicated insert request into the active generation.
    /// </summary>
    ValueTask<ShardInsertResult> EnqueueAsync(ShardInsertRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enqueues a deduplicated insert request into the active generation.
    /// </summary>
    ValueTask<ShardInsertResult> EnqueueAsync(ulong hashHi, ulong hashLo, ulong serverHi, ulong serverLo, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns whether the hash exists in any retained generation.
    /// </summary>
    /// <remarks>Throws when storage is unhealthy or disposed; does not mask errors as non-existence.</remarks>
    bool Exists(ulong hashHi, ulong hashLo);

    /// <summary>
    /// Requests cooperative shutdown and awaits worker completion.
    /// </summary>
    Task StopAsync();

    /// <summary>
    /// Returns a pressure snapshot for the active generation.
    /// </summary>
    IReadOnlyList<ShardPressureSnapshot> GetActiveGenerationShardPressure();

    /// <summary>
    /// Returns scrubber counters.
    /// </summary>
    SlotScrubberCounters GetSlotScrubberCounters();
}

/// <summary>
/// Factory contract for creating <see cref="IHistoryWriter"/> instances.
/// </summary>
internal interface IHistoryWriterFactory
{
    IHistoryWriter Create(HistoryWriterOptions options, Microsoft.Extensions.Logging.ILogger? logger = null);
}
