// HistoryDatabase.cs -- public HistoryDB API using string message IDs and server UUIDs, internally hashed to MD5 for storage.

using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using HistoryDB.Contracts;
using HistoryDB.Defrag;
using HistoryDB.Repair;
using HistoryDB.Utilities;

using Microsoft.Extensions.Logging;

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
/// </remarks>
public sealed partial class HistoryDatabase : IAsyncDisposable
{
    /// <summary>Default in-memory walk window. Bounded so very long-running processes do not accumulate unbounded RAM.</summary>
    public const int DefaultMaxWalkEntriesInMemory = 1_048_576;

    private readonly ShardedHistoryWriter _writer;
    private readonly string _expiredPath;
    private readonly string _journalPath;
    private readonly bool _enableWalkJournal;
    private readonly int _maxWalkEntriesInMemory;
    private readonly object _stateLock = new();
    private readonly object _journalWriteLock = new();
    private FileStream? _journalStream;
    private StreamWriter? _journalWriter;
    private readonly HashSet<Hash128> _expired = [];
    private long _walkEntriesEvicted;
    private readonly ILogger _logger;

    /// <summary>
    /// Initializes a HistoryDB instance and activates internal generation processing.
    /// </summary>
    /// <param name="journalMaxFileBytesBeforeTrim">When &gt; 0 and journal length exceeds this, <see cref="HistorySync"/> may atomically rewrite the journal retaining only the tail (see <paramref name="journalTailRetainBytes"/>).</param>
    /// <param name="journalTailRetainBytes">Bytes to keep when trimming (cap applied to current file size).</param>
    /// <param name="journalBufferFlushThresholdBytes">Coalesce journal lines until this many UTF-8 bytes are buffered (then flush).</param>
    /// <param name="journalCheckpointEveryNLines">Persist replay checkpoint every N successfully parsed journal lines during startup replay.</param>
    /// <param name="minimumFreeDiskBytes">When non-zero, compaction and bloom snapshot paths skip work if free space on the data volume is below this threshold.</param>
    public HistoryDatabase(
        string rootPath,
        int shardCount = 128,
        ulong slotsPerShard = 1UL << 22,
        ulong maxLoadFactorPercent = 75,
        int queueCapacityPerShard = 1_000_000,
        int maxRetainedGenerations = 8,
        bool enableWalkJournal = true,
        int maxWalkEntriesInMemory = DefaultMaxWalkEntriesInMemory,
        ILogger? logger = null,
        long journalMaxFileBytesBeforeTrim = 0,
        long journalTailRetainBytes = DefaultJournalTailRetainBytes,
        int journalBufferFlushThresholdBytes = DefaultJournalBufferFlushThresholdBytes,
        int journalCheckpointEveryNLines = DefaultJournalCheckpointLineInterval,
        ulong minimumFreeDiskBytes = 0)
    {
        if (maxWalkEntriesInMemory < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxWalkEntriesInMemory), "maxWalkEntriesInMemory must be >= 0 (0 = unbounded).");
        }

        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
        string normalizedRoot = Path.GetFullPath(rootPath);
        Directory.CreateDirectory(normalizedRoot);
        _expiredPath = Path.Combine(normalizedRoot, "expired-md5.bin");
        _journalPath = Path.Combine(normalizedRoot, "history-journal.log");
        _journalCheckpointPath = Path.Combine(normalizedRoot, "history-journal.replay.chk");
        _journalDirtyMarkerPath = Path.Combine(normalizedRoot, "history-journal.dirty");
        _journalMaxFileBytesBeforeTrim = journalMaxFileBytesBeforeTrim;
        _journalTailRetainBytes = journalTailRetainBytes > 0 ? journalTailRetainBytes : DefaultJournalTailRetainBytes;
        _journalBufferFlushThreshold = journalBufferFlushThresholdBytes > 0 ? journalBufferFlushThresholdBytes : DefaultJournalBufferFlushThresholdBytes;
        _journalCheckpointEveryNLines = journalCheckpointEveryNLines > 0 ? journalCheckpointEveryNLines : DefaultJournalCheckpointLineInterval;
        _journalCircuit = new ConsecutiveFailureCircuit(5, TimeSpan.FromSeconds(60));
        _enableWalkJournal = enableWalkJournal;
        _maxWalkEntriesInMemory = maxWalkEntriesInMemory;

        InitWalkStorage();

        _writer = new ShardedHistoryWriter(
            rootPath: rootPath,
            shardCount: shardCount,
            slotsPerShard: slotsPerShard,
            maxLoadFactorPercent: maxLoadFactorPercent,
            queueCapacityPerShard: queueCapacityPerShard,
            maxRetainedGenerations: maxRetainedGenerations,
            minimumFreeDiskBytes: minimumFreeDiskBytes,
            logger: logger);

        LoadExpiredSet();
        if (_enableWalkJournal)
        {
            LoadJournal();
        }

        TryDeleteJournalDirtyMarker();
    }

    /// <summary>Whether repeated journal flush failures have opened the short-term circuit (writes may be skipped until cooldown).</summary>
    public bool IsJournalCircuitOpen => _journalCircuit.IsOpen;

    /// <summary>Whether Bloom checkpoint persistence circuit is open on the writer.</summary>
    public bool IsBloomPersistCircuitOpen => _writer.IsBloomPersistCircuitOpen;

    /// <summary>Captured non-fault-free shutdown task exceptions from writer/scrub threads (empty when clean).</summary>
    public IReadOnlyList<Exception> GetShutdownFaults() => _writer.GetShutdownFaults();

    /// <summary>Operational health flags for hosts (liveness dashboards).</summary>
    public HistoryHealthSnapshot GetHealthSnapshot() =>
        new(IsJournalCircuitOpen, IsBloomPersistCircuitOpen, File.Exists(_journalDirtyMarkerPath));

    /// <summary>
    /// Checks if a message ID exists and has not been expired.
    /// </summary>
    public bool HistoryLookup(string messageId) =>
        HistoryLookupAsync(messageId, default).AsTask().ConfigureAwait(false).GetAwaiter().GetResult();

    /// <summary>Async lookup; avoids extra thread-pool hop when used from async hosts.</summary>
    public async ValueTask<bool> HistoryLookupAsync(string messageId, CancellationToken cancellationToken = default)
    {
        Hash128 messageHash = ComputeMd5(messageId);
        lock (_stateLock)
        {
            if (_expired.Contains(messageHash))
            {
                return false;
            }
        }

        return await _writer.ExistsAndRecordAccessAsync(messageHash.Hi, messageHash.Lo, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Adds a message ID with server UUID. Returns true when inserted, false when duplicate/full/probe-limit.
    /// </summary>
    public bool HistoryAdd(string messageId, string serverId) =>
        HistoryAddAsync(messageId, serverId, default).AsTask().ConfigureAwait(false).GetAwaiter().GetResult();

    /// <summary>Async insert with journal/walk/expired persistence ordering.</summary>
    public async ValueTask<bool> HistoryAddAsync(string messageId, string serverId, CancellationToken cancellationToken = default)
    {
        if (!Guid.TryParse(serverId, out _))
        {
            throw new ArgumentException("serverId must be a valid UUID string.", nameof(serverId));
        }

        Hash128 messageHash = ComputeMd5(messageId);
        Hash128 serverHash = ComputeMd5(serverId);
        ShardInsertResult result = await _writer.EnqueueAsync(messageHash.Hi, messageHash.Lo, serverHash.Hi, serverHash.Lo, cancellationToken).ConfigureAwait(false);
        if (result == ShardInsertResult.Inserted)
        {
            if (_enableWalkJournal)
            {
                try
                {
                    AppendJournalLineBuffered(messageId, serverId, messageHash);
                }
                catch (Exception ex)
                {
                    LogJournalAppendAfterInsertFailed(ex);
                    WriteJournalDirtyMarker();
                    throw;
                }

                lock (_stateLock)
                {
                    _expired.Remove(messageHash);
                    AppendWalkEntry_NoLock(new HistoryWalkEntry(messageId, serverId, DateTimeOffset.UtcNow, messageHash));
                    PersistExpiredSetCore();
                }
            }
            else
            {
                lock (_stateLock)
                {
                    _expired.Remove(messageHash);
                    PersistExpiredSetCore();
                }
            }

            return true;
        }

        return false;
    }

    /// <summary>
    /// Benchmark-only async insert that measures end-to-end enqueue completion without blocking a thread pool thread.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is internal to the assembly and intended for <c>HisBench</c> via <c>InternalsVisibleTo</c>. Public callers should use <see cref="HistoryAdd"/>.
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

        Hash128 messageHash = ComputeMd5(messageId);
        Hash128 serverHash = ComputeMd5(serverId);
        ShardInsertBenchmarkResult bench =
            await _writer.EnqueueForBenchmarkAsync(messageHash.Hi, messageHash.Lo, serverHash.Hi, serverHash.Lo, cancellationToken).ConfigureAwait(false);
        return bench;
    }

    /// <summary>
    /// Expires one message ID. Expired entries are skipped by lookup/walk and a best-effort tombstone broadcast is sent to every retained generation's shard channel.
    /// </summary>
    public bool HistoryExpire(string messageId)
    {
        Hash128 messageHash = ComputeMd5(messageId);
        bool added;
        lock (_stateLock)
        {
            added = _expired.Add(messageHash);
            if (added)
            {
                PersistExpiredSetCore();
            }
        }

        if (added)
        {
            BroadcastTombstoneFireAndForget(messageHash);
        }

        return added;
    }

    /// <summary>
    /// Expires many message IDs. Returns number of new expirations applied.
    /// </summary>
    public int HistoryExpire(string[] messageIds)
    {
        if (messageIds is null)
        {
            throw new ArgumentNullException(nameof(messageIds));
        }

        int changed = 0;
        List<Hash128>? newlyExpired = null;
        lock (_stateLock)
        {
            for (int i = 0; i < messageIds.Length; i++)
            {
                Hash128 hash = ComputeMd5(messageIds[i]);
                if (_expired.Add(hash))
                {
                    changed++;
                    (newlyExpired ??= new List<Hash128>(messageIds.Length)).Add(hash);
                }
            }

            if (changed > 0)
            {
                PersistExpiredSetCore();
            }
        }

        if (newlyExpired is not null)
        {
            for (int i = 0; i < newlyExpired.Count; i++)
            {
                BroadcastTombstoneFireAndForget(newlyExpired[i]);
            }
        }

        return changed;
    }

    /// <summary>
    /// Returns whether the MD5 message hash is present in the in-memory expired filter (same semantics as lookup suppression).
    /// </summary>
    public bool IsMessageHashExpired(Hash128 messageHash)
    {
        lock (_stateLock)
        {
            return _expired.Contains(messageHash);
        }
    }

    /// <summary>
    /// Expires many content hashes (for example from a slot harvest). Adds each hash to the expired set, persists
    /// <c>expired-md5.bin</c>, and tombstones across retained generations in bounded parallel batches.
    /// </summary>
    public async Task<int> HistoryExpireByHashesAsync(
        IReadOnlyList<Hash128> hashes,
        int parallelTombstones = 64,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(hashes);
        if (hashes.Count == 0)
        {
            return 0;
        }

        if (parallelTombstones < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(parallelTombstones), "parallelTombstones must be >= 1.");
        }

        List<Hash128> newlyExpired = [];
        lock (_stateLock)
        {
            for (int i = 0; i < hashes.Count; i++)
            {
                Hash128 h = hashes[i];
                if (_expired.Add(h))
                {
                    newlyExpired.Add(h);
                }
            }
        }

        if (newlyExpired.Count == 0)
        {
            return 0;
        }

        for (int offset = 0; offset < newlyExpired.Count; offset += parallelTombstones)
        {
            int batch = Math.Min(parallelTombstones, newlyExpired.Count - offset);
            Task[] tasks = new Task[batch];
            for (int j = 0; j < batch; j++)
            {
                Hash128 h = newlyExpired[offset + j];
                tasks[j] = _writer.EnqueueTombstoneAcrossGenerationsAsync(h.Hi, h.Lo, cancellationToken).AsTask();
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        PersistExpiredSet();
        return newlyExpired.Count;
    }

    /// <summary>
    /// Enumerates occupied slot metadata across all generations. Optionally skips hashes already in the expired set.
    /// </summary>
    public async IAsyncEnumerable<StoredArticleMetadata> EnumerateStoredArticleMetadataAsync(
        bool skipIfExpired = true,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        HashSet<Hash128>? expiredSnap = null;
        if (skipIfExpired)
        {
            lock (_stateLock)
            {
                expiredSnap = new HashSet<Hash128>(_expired);
            }
        }

        await foreach (StoredArticleMetadata row in _writer.EnumerateStoredArticleMetadataAsync(cancellationToken).ConfigureAwait(false))
        {
            if (expiredSnap is not null && expiredSnap.Contains(row.MessageHash))
            {
                continue;
            }

            yield return row;
        }
    }

    private void BroadcastTombstoneFireAndForget(Hash128 messageHash)
    {
        try
        {
            ValueTask<int> pending = _writer.EnqueueTombstoneAcrossGenerationsAsync(messageHash.Hi, messageHash.Lo, CancellationToken.None);
            if (!pending.IsCompletedSuccessfully)
            {
                pending.AsTask().ContinueWith(
                    static (t, state) =>
                    {
                        HistoryDatabase db = (HistoryDatabase)state!;
                        if (t.IsFaulted && t.Exception is not null)
                        {
                            db.LogTombstoneBroadcastTaskFaulted(t.Exception.GetBaseException());
                        }
                    },
                    this,
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted,
                    TaskScheduler.Default);
            }
        }
        catch (Exception ex)
        {
            LogTombstoneBroadcastSyncFailed(ex);
        }
    }

    /// <summary>
    /// Synchronizes internal history metadata, shard headers, and bloom files to disk.
    /// </summary>
    public void HistorySync()
    {
        lock (_journalWriteLock)
        {
            JournalSyncMaintenance_NoLock();
        }

        _writer.Sync();
        PersistExpiredSet();
    }

    /// <summary>
    /// Returns the number of walk entries evicted from the in-memory window since process start (FIFO retention).
    /// </summary>
    public long GetEvictedWalkEntryCount()
    {
        lock (_stateLock)
        {
            return _walkEntriesEvicted;
        }
    }

    public void SetBloomCheckpointInsertInterval(ulong inserts) => _writer.SetBloomCheckpointInsertInterval(inserts);

    public void SetRolloverThresholds(ulong usedSlotsThreshold, int queueDepthThreshold, long aggregateProbeFailuresThreshold) =>
        _writer.SetRolloverThresholds(usedSlotsThreshold, queueDepthThreshold, aggregateProbeFailuresThreshold);

    public void SetSlotScrubberTuning(int samplesPerTick, int intervalMilliseconds) =>
        _writer.SetSlotScrubberTuning(samplesPerTick, intervalMilliseconds);

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

    /// <summary>Flushes metadata and releases mmap-backed shard writers.</summary>
    public async ValueTask DisposeAsync()
    {
        HistorySync();
        lock (_journalWriteLock)
        {
            try
            {
                _journalWriter?.Flush();
            }
            catch (Exception ex)
            {
                LogJournalWriterFlushDisposeFailed(ex);
            }

            _journalWriter?.Dispose();
            _journalWriter = null;
            try
            {
                _journalStream?.Flush(flushToDisk: true);
            }
            catch (Exception ex)
            {
                LogJournalStreamFlushDisposeFailed(ex);
            }

            _journalStream?.Dispose();
            _journalStream = null;
        }

        await _writer.DisposeAsync().ConfigureAwait(false);
    }

    public static Task<HistoryRepairReport> ScanAsync(
        string rootPath,
        HistoryScanOptions? options = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        HistoryRepairEngine.ScanAsync(rootPath, options, logger, cancellationToken);

    public static Task<HistoryRepairReport> RepairAsync(
        string rootPath,
        HistoryRepairOptions options,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        HistoryRepairEngine.RepairAsync(rootPath, options, logger, cancellationToken);

    public Task<HistoryDefragReport> HistoryDefragAsync(
        HistoryDefragOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        HistoryDefragOptions effective = options ?? new HistoryDefragOptions();
        IReadOnlySet<Hash128> snapshot = SnapshotExpiredHashesForDefrag();
        return HistoryDefragEngine.RunAsync(this, _writer, snapshot, effective, _logger, cancellationToken);
    }

    private HashSet<Hash128> SnapshotExpiredHashesForDefrag()
    {
        lock (_stateLock)
        {
            return new HashSet<Hash128>(_expired);
        }
    }

    internal async Task<long> PruneExpiredSetAfterDefragAsync(CancellationToken cancellationToken)
    {
        Hash128[] snapshot;
        lock (_stateLock)
        {
            snapshot = new Hash128[_expired.Count];
            int idx = 0;
            foreach (Hash128 h in _expired)
            {
                snapshot[idx++] = h;
            }
        }

        if (snapshot.Length == 0)
        {
            return 0;
        }

        List<Hash128> toRemove = new();
        int yieldCounter = 0;
        for (int i = 0; i < snapshot.Length; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if ((++yieldCounter & 0x3FF) == 0)
            {
                await Task.Yield();
            }

            Hash128 h = snapshot[i];
            if (!_writer.Exists(h.Hi, h.Lo))
            {
                toRemove.Add(h);
            }
        }

        if (toRemove.Count == 0)
        {
            return 0;
        }

        long pruned = 0;
        lock (_stateLock)
        {
            for (int i = 0; i < toRemove.Count; i++)
            {
                if (_expired.Remove(toRemove[i]))
                {
                    pruned++;
                }
            }
        }

        if (pruned > 0)
        {
            try
            {
                PersistExpiredSet();
            }
            catch (Exception ex)
            {
                LogPersistExpiredAfterPruneFailed(ex);
            }
        }

        return pruned;
    }

    private void LoadExpiredSet()
    {
        if (!File.Exists(_expiredPath))
        {
            return;
        }

        if (ExpiredSetFile.TryLoadFormatted(_expiredPath, _expired, out string? formattedError))
        {
            if (formattedError is not null)
            {
                throw new InvalidDataException(formattedError);
            }

            return;
        }

        byte[] bytes = File.ReadAllBytes(_expiredPath);
        if (bytes.Length % 16 != 0)
        {
            throw new InvalidDataException(
                $"Legacy expired-md5.bin length {bytes.Length} is not a multiple of 16 bytes (partial write or corruption).");
        }

        for (int offset = 0; offset + 16 <= bytes.Length; offset += 16)
        {
            ulong hi = BitConverter.ToUInt64(bytes, offset);
            ulong lo = BitConverter.ToUInt64(bytes, offset + 8);
            _expired.Add(new Hash128(hi, lo));
        }
    }

    private void PersistExpiredSet()
    {
        lock (_stateLock)
        {
            PersistExpiredSetCore();
        }
    }

    private void PersistExpiredSetCore()
    {
        ExpiredSetFile.WriteAtomic(_expiredPath, _expired);
    }
}

/// <summary>
/// A walked history record returned by <see cref="HistoryDatabase.HistoryWalk(ref long, out HistoryWalkEntry, int)"/>.
/// </summary>
public readonly record struct HistoryWalkEntry(string MessageId, string ServerId, DateTimeOffset CreatedAtUtc, Hash128 MessageHash);

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

/// <summary>Host-facing degraded-mode indicators.</summary>
public readonly record struct HistoryHealthSnapshot(
    bool JournalCircuitOpen,
    bool BloomPersistCircuitOpen,
    bool JournalDirtyMarkerPresent);
