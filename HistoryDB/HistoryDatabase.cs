// HistoryDatabase.cs -- public HistoryDB API using string message IDs and server UUIDs, internally hashed to MD5 for storage.

using HistoryDB.Application;
using HistoryDB.Contracts;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HistoryDB;

/// <summary>
/// Public HistoryDB entry point. Accepts string message IDs and server IDs, hashes them to MD5 internally, and delegates to the internal shard engine.
/// </summary>
/// <remarks>
/// <para>
/// The walk/journal subsystem is opt-in for long-running deployments. When enabled, every successful insert appends a UTF-8 line
/// to <c>history-journal.log</c> and a record to an in-memory ring buffer that backs <see cref="HistoryWalk(ref long, out HistoryWalkEntry, int)"/>.
/// Disable the journal (<c>enableWalkJournal: false</c>) for high-throughput servers that do not consume the walk API; otherwise tune
/// <c>maxWalkEntriesInMemory</c> to bound resident memory growth.
/// </para>
/// <para>
/// Synchronous <see cref="HistoryAdd"/> blocks the caller; prefer <see cref="HistoryAddAsync"/> under load. Synchronous <see cref="Dispose"/> blocks until shutdown completes; prefer <see cref="DisposeAsync"/>.
/// </para>
/// </remarks>
public sealed class HistoryDatabase : IAsyncDisposable, IDisposable
{
    /// <summary>Default in-memory walk window. Bounded so very long-running processes do not accumulate unbounded RAM.</summary>
    public const int DefaultMaxWalkEntriesInMemory = 1_048_576;

    private readonly ShardedHistoryWriter _writer;
    private readonly IHistoryMetadataStore _metadata;
    private readonly IMessageHashProvider _hashProvider;
    private readonly bool _enableWalkJournal;

    /// <summary>
    /// Initializes a HistoryDB instance and activates internal generation processing.
    /// </summary>
    /// <param name="rootPath">Root directory for shard data, Bloom sidecars, and metadata.</param>
    /// <param name="shardCount">Shard count.</param>
    /// <param name="slotsPerShard">Slots per shard (power of two).</param>
    /// <param name="maxLoadFactorPercent">Maximum load factor percent for inserts.</param>
    /// <param name="queueCapacityPerShard">Bounded channel capacity per shard writer.</param>
    /// <param name="maxRetainedGenerations">Generations retained before retiring oldest.</param>
    /// <param name="enableWalkJournal">When <see langword="true"/> (default), persists the journal and tracks an in-memory walk buffer; disable for high-throughput servers that do not use <see cref="HistoryWalk(ref long, out HistoryWalkEntry, int)"/>.</param>
    /// <param name="maxWalkEntriesInMemory">FIFO bound on resident walk entries. Use <c>0</c> for unbounded (legacy behavior); legacy mode is only safe for short-lived processes because the list grows with every insert.</param>
    /// <param name="logger">Optional structured logger.</param>
    public HistoryDatabase(
        string rootPath,
        int shardCount = 128,
        ulong slotsPerShard = 1UL << 22,
        ulong maxLoadFactorPercent = 75,
        int queueCapacityPerShard = 1_000_000,
        int maxRetainedGenerations = 8,
        bool enableWalkJournal = true,
        int maxWalkEntriesInMemory = DefaultMaxWalkEntriesInMemory,
        ILogger? logger = null)
    {
        if (maxWalkEntriesInMemory < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxWalkEntriesInMemory), "maxWalkEntriesInMemory must be >= 0 (0 = unbounded).");
        }

        ILogger resolvedLogger = logger ?? NullLogger<HistoryDatabase>.Instance;
        string normalizedRoot = Path.GetFullPath(rootPath);
        Directory.CreateDirectory(normalizedRoot);
        _writer = new ShardedHistoryWriter(
            rootPath: normalizedRoot,
            shardCount: shardCount,
            slotsPerShard: slotsPerShard,
            maxLoadFactorPercent: maxLoadFactorPercent,
            queueCapacityPerShard: queueCapacityPerShard,
            maxRetainedGenerations: maxRetainedGenerations,
            logger: logger);

        _hashProvider = new Md5MessageHashProvider();
        _metadata = new HistoryMetadataStore(
            normalizedRoot,
            _hashProvider,
            resolvedLogger,
            maxWalkEntriesInMemory,
            loadJournal: enableWalkJournal);
        _enableWalkJournal = enableWalkJournal;
    }

    /// <summary>
    /// Checks if a message ID exists and has not been expired.
    /// </summary>
    public bool HistoryLookup(string messageId)
    {
        Hash128 messageHash = _hashProvider.ComputeHash(messageId);
        if (_metadata.IsExpired(messageHash))
        {
            return false;
        }

        return _writer.Exists(messageHash.Hi, messageHash.Lo);
    }

    /// <summary>
    /// Adds a message ID with server UUID. Returns true when inserted, false when duplicate/full/probe-limit.
    /// </summary>
    /// <remarks>Blocks the calling thread; prefer <see cref="HistoryAddAsync"/> in server code.</remarks>
    public bool HistoryAdd(string messageId, string serverId) =>
        HistoryAddAsync(messageId, serverId).AsTask().ConfigureAwait(false).GetAwaiter().GetResult();

    /// <summary>
    /// Adds a message ID with server UUID. Returns true when inserted, false when duplicate/full/probe-limit.
    /// </summary>
    public async ValueTask<bool> HistoryAddAsync(string messageId, string serverId, CancellationToken cancellationToken = default)
    {
        if (!Guid.TryParse(serverId, out _))
        {
            throw new ArgumentException("serverId must be a valid UUID string.", nameof(serverId));
        }

        Hash128 messageHash = _hashProvider.ComputeHash(messageId);
        Hash128 serverHash = _hashProvider.ComputeHash(serverId);
        ShardInsertResult result = await _writer
            .EnqueueAsync(messageHash.Hi, messageHash.Lo, serverHash.Hi, serverHash.Lo, cancellationToken)
            .ConfigureAwait(false);
        if (result == ShardInsertResult.Inserted)
        {
            _metadata.RecordInserted(
                messageId,
                serverId,
                messageHash,
                DateTimeOffset.UtcNow,
                persistJournalAndWalk: _enableWalkJournal);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Benchmark-only async insert that measures end-to-end enqueue completion without blocking a thread pool thread.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is internal to the assembly and intended for <c>HisBench</c> via <c>InternalsVisibleTo</c>. Public callers should use <see cref="HistoryAddAsync"/>.
    /// To avoid benchmark contamination, this path intentionally skips walk/journal bookkeeping that can serialize high-concurrency runs.
    /// </para>
    /// </remarks>
    internal async ValueTask<ShardInsertBenchmarkResult> HistoryAddForBenchmarkAsync(
        string messageId,
        string serverId,
        CancellationToken cancellationToken = default)
    {
        if (!Guid.TryParse(serverId, out _))
        {
            throw new ArgumentException("serverId must be a valid UUID string.", nameof(serverId));
        }

        Hash128 messageHash = _hashProvider.ComputeHash(messageId);
        Hash128 serverHash = _hashProvider.ComputeHash(serverId);
        ShardInsertBenchmarkResult bench =
            await _writer.EnqueueForBenchmarkAsync(messageHash.Hi, messageHash.Lo, serverHash.Hi, serverHash.Lo, cancellationToken).ConfigureAwait(false);
        return bench;
    }

    /// <summary>
    /// Expires one message ID. Expired entries are skipped by lookup/walk.
    /// </summary>
    public bool HistoryExpire(string messageId) => _metadata.Expire(_hashProvider.ComputeHash(messageId));

    /// <summary>
    /// Expires many message IDs. Returns number of new expirations applied.
    /// </summary>
    public int HistoryExpire(string[] messageIds)
    {
        if (messageIds is null)
        {
            throw new ArgumentNullException(nameof(messageIds));
        }

        Hash128[] hashes = new Hash128[messageIds.Length];
        for (int i = 0; i < messageIds.Length; i++)
        {
            hashes[i] = _hashProvider.ComputeHash(messageIds[i]);
        }

        return _metadata.Expire(hashes);
    }

    /// <summary>
    /// Synchronizes internal history metadata, shard headers, and bloom files to disk.
    /// </summary>
    public void HistorySync()
    {
        _writer.Sync();
        _metadata.PersistExpiredSet();
        _metadata.FlushJournalAsync().AsTask().ConfigureAwait(false).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Procedural walk API. Returns 1 when an entry is produced; returns 0 when end is reached.
    /// </summary>
    public int HistoryWalk(ref long position, out HistoryWalkEntry entry, int flags = 0) => _metadata.Walk(ref position, out entry);

    /// <summary>
    /// Returns the number of walk entries evicted from the in-memory window since process start (FIFO retention).
    /// </summary>
    public long GetEvictedWalkEntryCount() => _metadata.GetEvictedWalkEntryCount();

    /// <summary>
    /// Tunes bloom checkpoint frequency in inserts (0 disables periodic checkpoints).
    /// </summary>
    public void SetBloomCheckpointInsertInterval(ulong inserts) => _writer.SetBloomCheckpointInsertInterval(inserts);

    /// <summary>
    /// Tunes proactive rollover thresholds.
    /// </summary>
    public void SetRolloverThresholds(ulong usedSlotsThreshold, int queueDepthThreshold, long aggregateProbeFailuresThreshold) =>
        _writer.SetRolloverThresholds(usedSlotsThreshold, queueDepthThreshold, aggregateProbeFailuresThreshold);

    /// <summary>
    /// Tunes slot scrubber pacing.
    /// </summary>
    public void SetSlotScrubberTuning(int samplesPerTick, int intervalMilliseconds) =>
        _writer.SetSlotScrubberTuning(samplesPerTick, intervalMilliseconds);

    /// <summary>
    /// Returns internal pressure and failure counters useful for benchmark instrumentation.
    /// </summary>
    public HistoryPerformanceSnapshot GetPerformanceSnapshot()
    {
        ShardedHistoryWriter.WriterInternalMetrics m = _writer.GetInternalMetrics();
        return new HistoryPerformanceSnapshot(
            m.ActiveGenerationId,
            m.RetainedGenerations,
            m.PendingQueueItemsApprox,
            m.FullInsertFailures,
            m.ProbeLimitFailures,
            m.UsedSlotsApprox);
    }

    /// <summary>
    /// Flushes metadata and shuts down the shard writer.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        HistorySync();
        await _writer.DisposeAsync().ConfigureAwait(false);
        await _metadata.DisposeAsync().ConfigureAwait(false);
    }

    /// <inheritdoc cref="DisposeAsync"/>
    /// <remarks>Blocks the caller; do not invoke from a synchronization context that forbids blocking.</remarks>
    public void Dispose() => DisposeAsync().AsTask().ConfigureAwait(false).GetAwaiter().GetResult();
}

/// <summary>
/// A walked history record returned by <see cref="HistoryDatabase.HistoryWalk(ref long, out HistoryWalkEntry, int)"/>.
/// </summary>
public readonly record struct HistoryWalkEntry(string MessageId, string ServerId, DateTimeOffset CreatedAtUtc);

/// <summary>
/// Snapshot of internal pressure and failure counters.
/// </summary>
public readonly record struct HistoryPerformanceSnapshot(
    int ActiveGenerationId,
    int RetainedGenerations,
    long PendingQueueItemsApprox,
    long FullInsertFailures,
    long ProbeLimitFailures,
    ulong UsedSlotsApprox);
