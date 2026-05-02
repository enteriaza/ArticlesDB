// HistoryDatabase.cs -- public HistoryDB API using string message IDs and server UUIDs, internally hashed to MD5 for storage.

using System.Buffers;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

using HistoryDB.Utilities;

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
    private readonly int _maxWalkEntries;
    private readonly WalkRow[] _walkRing;
    private int _walkHead;
    private int _walkCount;

    /// <summary>
    /// Initializes a HistoryDB instance and activates internal generation processing.
    /// </summary>
    /// <param name="maxWalkEntries">Maximum in-memory walk rows (FIFO ring). Oldest rows are overwritten after an insert when full.</param>
    public HistoryDatabase(
        string rootPath,
        int shardCount = 128,
        ulong slotsPerShard = 1UL << 22,
        ulong maxLoadFactorPercent = 75,
        int queueCapacityPerShard = 1_000_000,
        int maxRetainedGenerations = 8,
        int maxWalkEntries = 1_000_000,
        ILogger? logger = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxWalkEntries, 1);

        _maxWalkEntries = maxWalkEntries;
        _walkRing = new WalkRow[maxWalkEntries];
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
    /// <exception cref="InvalidDataException">Thrown when on-disk structures fail validation.</exception>
    /// <exception cref="IOException">Thrown when storage access fails.</exception>
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
    public bool HistoryAdd(string messageId, string serverId) =>
        HistoryAddAsync(messageId, serverId, CancellationToken.None).GetAwaiter().GetResult();

    /// <summary>
    /// Adds a message ID with server UUID. On successful insert, appends the journal line before updating the in-memory walk ring.
    /// </summary>
    public async Task<bool> HistoryAddAsync(string messageId, string serverId, CancellationToken cancellationToken = default)
    {
        if (!Guid.TryParse(serverId, out _))
        {
            throw new ArgumentException("serverId must be a valid UUID string.", nameof(serverId));
        }

        Hash128 messageHash = ComputeMd5(messageId);
        Hash128 serverHash = ComputeMd5(serverId);
        ShardInsertResult result =
            await _writer.EnqueueAsync(messageHash.Hi, messageHash.Lo, serverHash.Hi, serverHash.Lo, cancellationToken)
                .ConfigureAwait(false);
        if (result != ShardInsertResult.Inserted)
        {
            return false;
        }

        AppendJournalLine(messageId, serverId);
        lock (_stateLock)
        {
            _expired.Remove(messageHash);
            AppendWalkRow(new WalkRow(messageHash, new HistoryWalkEntry(messageId, serverId, DateTimeOffset.UtcNow)));
        }

        return true;
    }

    /// <summary>
    /// Benchmark-only async insert that measures end-to-end enqueue completion without blocking a thread pool thread.
    /// </summary>
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
        return await _writer.EnqueueForBenchmarkAsync(messageHash.Hi, messageHash.Lo, serverHash.Hi, serverHash.Lo, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Expires one message ID. Expired entries are skipped by lookup/walk.
    /// </summary>
    public bool HistoryExpire(string messageId)
    {
        Hash128 messageHash = ComputeMd5(messageId);
        lock (_stateLock)
        {
            bool added = _expired.Add(messageHash);
            if (added)
            {
                PersistExpiredSetWhileLocked();
            }

            return added;
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

            if (changed != 0)
            {
                PersistExpiredSetWhileLocked();
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
        lock (_stateLock)
        {
            PersistExpiredSetWhileLocked();
        }
    }

    /// <summary>
    /// Procedural walk API. Returns 1 when an entry is produced; returns 0 when end is reached.
    /// </summary>
    public int HistoryWalk(ref long position, out HistoryWalkEntry entry, int flags = 0)
    {
        lock (_stateLock)
        {
            while (position >= 0 && position < _walkCount)
            {
                int idx = (int)(((ulong)_walkHead + (ulong)position) % (ulong)_maxWalkEntries);
                position++;
                WalkRow current = _walkRing[idx];
                if (_expired.Contains(current.MessageHash))
                {
                    continue;
                }

                entry = current.Entry;
                return 1;
            }
        }

        entry = default;
        return 0;
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

    internal async ValueTask DisposeAsync()
    {
        HistorySync();
        await _writer.DisposeAsync().ConfigureAwait(false);
    }

    private void AppendWalkRow(WalkRow row)
    {
        if (_walkCount < _maxWalkEntries)
        {
            int idx = (int)(((ulong)_walkHead + (ulong)_walkCount) % (ulong)_maxWalkEntries);
            _walkRing[idx] = row;
            _walkCount++;
            return;
        }

        _walkRing[_walkHead] = row;
        _walkHead = (_walkHead + 1) % _maxWalkEntries;
    }

    private void LoadExpiredSet()
    {
        if (!File.Exists(_expiredPath))
        {
            return;
        }

        byte[] bytes = File.ReadAllBytes(_expiredPath);
        ReadOnlyMemory<byte> hashRegion = ExpiredSetFile.ParseHashesOrThrow(bytes);
        ReadOnlySpan<byte> span = hashRegion.Span;
        for (int offset = 0; offset + 16 <= span.Length; offset += 16)
        {
            ulong hi = BitConverter.ToUInt64(span[offset..]);
            ulong lo = BitConverter.ToUInt64(span[(offset + 8)..]);
            _expired.Add(new Hash128(hi, lo));
        }
    }

    private void PersistExpiredSetWhileLocked()
    {
        byte[] entries = new byte[_expired.Count * 16];
        int o = 0;
        foreach (Hash128 hash in _expired)
        {
            BitConverter.TryWriteBytes(entries.AsSpan(o, 8), hash.Hi);
            BitConverter.TryWriteBytes(entries.AsSpan(o + 8, 8), hash.Lo);
            o += 16;
        }

        byte[] payload = ExpiredSetFile.BuildPayload(entries);
        File.WriteAllBytes(_expiredPath, payload);
    }

    private void LoadJournal()
    {
        if (!File.Exists(_journalPath))
        {
            return;
        }

        long lineNumber = 0;
        foreach (string line in File.ReadLines(_journalPath))
        {
            lineNumber++;
            try
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

                Hash128 mh = ComputeMd5(messageId);
                AppendWalkRow(new WalkRow(mh, new HistoryWalkEntry(messageId, serverId, new DateTimeOffset(ticks, TimeSpan.Zero))));
            }
            catch (Exception ex)
            {
                Trace.TraceWarning(
                    "[HistoryDB] Skipping malformed journal line {0} in {1}: {2}",
                    lineNumber,
                    _journalPath,
                    ex.Message);
            }
        }
    }

    private void AppendJournalLine(string messageId, string serverId)
    {
        string line =
            $"{Convert.ToBase64String(Encoding.UTF8.GetBytes(messageId))}|{Convert.ToBase64String(Encoding.UTF8.GetBytes(serverId))}|{DateTimeOffset.UtcNow.Ticks}";
        lock (_journalWriteLock)
        {
            using var stream = new FileStream(
                _journalPath,
                FileMode.Append,
                FileAccess.Write,
                FileShare.Read,
                bufferSize: 4096,
                options: FileOptions.None);
            using var writer = new StreamWriter(stream, Encoding.UTF8);
            writer.WriteLine(line);
            writer.Flush();
            stream.Flush(flushToDisk: true);
        }
    }

    private static Hash128 ComputeMd5(string text)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(text);
        int byteCount = Encoding.UTF8.GetByteCount(text);
        const int StackThreshold = 512;
        byte[]? rented = null;
        Span<byte> utf8 = byteCount <= StackThreshold ? stackalloc byte[byteCount] : (rented = ArrayPool<byte>.Shared.Rent(byteCount)).AsSpan(0, byteCount);
        try
        {
            Encoding.UTF8.GetBytes(text, utf8);
            Span<byte> hash = stackalloc byte[16];
            MD5.HashData(utf8, hash);
            ulong hi = BitConverter.ToUInt64(hash[..8]);
            ulong lo = BitConverter.ToUInt64(hash[8..]);
            return new Hash128(hi, lo);
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    private readonly record struct Hash128(ulong Hi, ulong Lo);

    private readonly record struct WalkRow(Hash128 MessageHash, HistoryWalkEntry Entry);
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
