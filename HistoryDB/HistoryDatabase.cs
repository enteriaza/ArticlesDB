// HistoryDatabase.cs -- public HistoryDB API using string message IDs and server UUIDs, internally hashed to MD5 for storage.

using System.Security.Cryptography;
using System.Text;
using System.Threading;

using Microsoft.Extensions.Logging;

namespace HistoryDB;

/// <summary>
/// Public HistoryDB entry point. Accepts string message IDs and server IDs, hashes them to MD5 internally, and delegates to the internal shard engine.
/// </summary>
public sealed class HistoryDatabase
{
    private readonly ShardedHistoryWriter _writer;
    private readonly string _expiredPath;
    private readonly string _journalPath;
    private readonly object _stateLock = new();
    private readonly object _journalWriteLock = new();
    private readonly HashSet<Hash128> _expired = [];
    private readonly List<HistoryWalkEntry> _walkEntries = [];

    /// <summary>
    /// Initializes a HistoryDB instance and activates internal generation processing.
    /// </summary>
    public HistoryDatabase(
        string rootPath,
        int shardCount = 128,
        ulong slotsPerShard = 1UL << 22,
        ulong maxLoadFactorPercent = 75,
        int queueCapacityPerShard = 1_000_000,
        int maxRetainedGenerations = 8,
        ILogger? logger = null)
    {
        _writer = new ShardedHistoryWriter(
            rootPath: rootPath,
            shardCount: shardCount,
            slotsPerShard: slotsPerShard,
            maxLoadFactorPercent: maxLoadFactorPercent,
            queueCapacityPerShard: queueCapacityPerShard,
            maxRetainedGenerations: maxRetainedGenerations,
            logger: logger);

        string normalizedRoot = Path.GetFullPath(rootPath);
        Directory.CreateDirectory(normalizedRoot);
        _expiredPath = Path.Combine(normalizedRoot, "expired-md5.bin");
        _journalPath = Path.Combine(normalizedRoot, "history-journal.log");
        LoadExpiredSet();
        LoadJournal();
    }

    /// <summary>
    /// Checks if a message ID exists and has not been expired.
    /// </summary>
    public bool HistoryLookup(string messageId)
    {
        Hash128 messageHash = ComputeMd5(messageId);
        lock (_stateLock)
        {
            if (_expired.Contains(messageHash))
            {
                return false;
            }
        }

        return _writer.Exists(messageHash.Hi, messageHash.Lo);
    }

    /// <summary>
    /// Adds a message ID with server UUID. Returns true when inserted, false when duplicate/full/probe-limit.
    /// </summary>
    public bool HistoryAdd(string messageId, string serverId)
    {
        if (!Guid.TryParse(serverId, out _))
        {
            throw new ArgumentException("serverId must be a valid UUID string.", nameof(serverId));
        }

        Hash128 messageHash = ComputeMd5(messageId);
        Hash128 serverHash = ComputeMd5(serverId);
        ShardInsertResult result = _writer.EnqueueAsync(messageHash.Hi, messageHash.Lo, serverHash.Hi, serverHash.Lo).AsTask().GetAwaiter().GetResult();
        if (result == ShardInsertResult.Inserted)
        {
            lock (_stateLock)
            {
                _expired.Remove(messageHash);
                _walkEntries.Add(new HistoryWalkEntry(messageId, serverId, DateTimeOffset.UtcNow));
            }

            AppendJournalLine(messageId, serverId);
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
    /// Expires one message ID. Expired entries are skipped by lookup/walk.
    /// </summary>
    public bool HistoryExpire(string messageId)
    {
        Hash128 messageHash = ComputeMd5(messageId);
        lock (_stateLock)
        {
            return _expired.Add(messageHash);
        }
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
        lock (_stateLock)
        {
            for (int i = 0; i < messageIds.Length; i++)
            {
                if (_expired.Add(ComputeMd5(messageIds[i])))
                {
                    changed++;
                }
            }
        }

        return changed;
    }

    /// <summary>
    /// Synchronizes internal history metadata, shard headers, and bloom files to disk.
    /// </summary>
    public void HistorySync()
    {
        _writer.Sync();
        PersistExpiredSet();
    }

    /// <summary>
    /// Procedural walk API. Returns 1 when an entry is produced; returns 0 when end is reached.
    /// </summary>
    public int HistoryWalk(ref long position, out HistoryWalkEntry entry, int flags = 0)
    {
        lock (_stateLock)
        {
            while (position >= 0 && position < _walkEntries.Count)
            {
                HistoryWalkEntry current = _walkEntries[(int)position++];
                if (_expired.Contains(ComputeMd5(current.MessageId)))
                {
                    continue;
                }

                entry = current;
                return 1;
            }
        }

        entry = default;
        return 0;
    }

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

    internal async ValueTask DisposeAsync()
    {
        HistorySync();
        await _writer.DisposeAsync().ConfigureAwait(false);
    }

    private void LoadExpiredSet()
    {
        if (!File.Exists(_expiredPath))
        {
            return;
        }

        byte[] bytes = File.ReadAllBytes(_expiredPath);
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
            byte[] payload = new byte[_expired.Count * 16];
            int offset = 0;
            foreach (Hash128 hash in _expired)
            {
                BitConverter.TryWriteBytes(payload.AsSpan(offset, 8), hash.Hi);
                BitConverter.TryWriteBytes(payload.AsSpan(offset + 8, 8), hash.Lo);
                offset += 16;
            }

            File.WriteAllBytes(_expiredPath, payload);
        }
    }

    private void LoadJournal()
    {
        if (!File.Exists(_journalPath))
        {
            return;
        }

        foreach (string line in File.ReadLines(_journalPath))
        {
            string[] parts = line.Split('|');
            if (parts.Length != 3)
            {
                continue;
            }

            string messageId = Encoding.UTF8.GetString(Convert.FromBase64String(parts[0]));
            string serverId = Encoding.UTF8.GetString(Convert.FromBase64String(parts[1]));
            if (!long.TryParse(parts[2], out long ticks))
            {
                continue;
            }

            _walkEntries.Add(new HistoryWalkEntry(messageId, serverId, new DateTimeOffset(ticks, TimeSpan.Zero)));
        }
    }

    private void AppendJournalLine(string messageId, string serverId)
    {
        string line = $"{Convert.ToBase64String(Encoding.UTF8.GetBytes(messageId))}|{Convert.ToBase64String(Encoding.UTF8.GetBytes(serverId))}|{DateTimeOffset.UtcNow.Ticks}";
        lock (_journalWriteLock)
        {
            File.AppendAllText(_journalPath, line + Environment.NewLine);
        }
    }

    private static Hash128 ComputeMd5(string text)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(text);
        byte[] data = Encoding.UTF8.GetBytes(text);
        Span<byte> hash = stackalloc byte[16];
        MD5.HashData(data, hash);
        ulong hi = BitConverter.ToUInt64(hash[..8]);
        ulong lo = BitConverter.ToUInt64(hash[8..]);
        return new Hash128(hi, lo);
    }

    private readonly record struct Hash128(ulong Hi, ulong Lo);
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
