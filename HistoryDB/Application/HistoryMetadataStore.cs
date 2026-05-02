// HistoryMetadataStore.cs -- durable walk/expiry metadata store for the public HistoryDatabase API.

using System.Buffers.Binary;
using System.Globalization;
using System.IO;
using System.IO.Hashing;
using System.Text;
using System.Threading;

using HistoryDB.Contracts;
using Microsoft.Extensions.Logging;

namespace HistoryDB.Application;

internal sealed partial class HistoryMetadataStore : IHistoryMetadataStore
{
    /// <summary>Magic for <c>expired-md5.bin</c> with checksum trailer (ASCII 'EXPD').</summary>
    private const uint ExpiredFileMagic = 0x44505845;

    private const uint ExpiredFileVersion = 1;

    private const int JournalFlushLineInterval = 64;

    private readonly string _expiredPath;
    private readonly string _journalPath;
    private readonly object _stateLock = new();
    private readonly object _journalWriteLock = new();
    private readonly HashSet<Hash128> _expired = [];
    private readonly LinkedList<JournalEntry> _walkEntries = new();
    private readonly IMessageHashProvider _hashProvider;
    private readonly ILogger _logger;
    private readonly int _maxWalkEntriesInMemory;
    private long _walkEntriesEvicted;
    private int _invalidJournalLines;
    private int _journalLinesSinceFlush;
    private FileStream? _journalStream;
    private StreamWriter? _journalWriter;

    public HistoryMetadataStore(
        string rootPath,
        IMessageHashProvider hashProvider,
        ILogger logger,
        int maxWalkEntriesInMemory = 1_048_576,
        bool loadJournal = true)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootPath);
        ArgumentNullException.ThrowIfNull(hashProvider);
        ArgumentNullException.ThrowIfNull(logger);
        if (maxWalkEntriesInMemory < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxWalkEntriesInMemory), "maxWalkEntriesInMemory must be >= 0 (0 = unbounded).");
        }

        string normalizedRoot = Path.GetFullPath(rootPath);
        Directory.CreateDirectory(normalizedRoot);
        _expiredPath = Path.Combine(normalizedRoot, "expired-md5.bin");
        _journalPath = Path.Combine(normalizedRoot, "history-journal.log");
        _hashProvider = hashProvider;
        _logger = logger;
        _maxWalkEntriesInMemory = maxWalkEntriesInMemory;

        LoadExpiredSet();
        if (loadJournal)
        {
            LoadJournal();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await FlushJournalAsync().ConfigureAwait(false);
        lock (_journalWriteLock)
        {
            _journalWriter?.Dispose();
            _journalWriter = null;
            _journalStream?.Dispose();
            _journalStream = null;
        }
    }

    public ValueTask FlushJournalAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_journalWriteLock)
        {
            if (_journalWriter is not null)
            {
                _journalWriter.Flush();
            }

            if (_journalStream is not null)
            {
                _journalStream.Flush(flushToDisk: true);
            }

            _journalLinesSinceFlush = 0;
        }

        return ValueTask.CompletedTask;
    }

    public bool IsExpired(Hash128 messageHash)
    {
        lock (_stateLock)
        {
            return _expired.Contains(messageHash);
        }
    }

    public void RecordInserted(string messageId, string serverId, Hash128 messageHash, DateTimeOffset createdAtUtc, bool persistJournalAndWalk = true)
    {
        lock (_stateLock)
        {
            _expired.Remove(messageHash);
            if (persistJournalAndWalk)
            {
                AppendWalkEntry_NoLock(new JournalEntry(new HistoryWalkEntry(messageId, serverId, createdAtUtc), messageHash));
            }
        }

        if (persistJournalAndWalk)
        {
            AppendJournalLine(messageId, serverId, messageHash, createdAtUtc);
        }
    }

    public bool Expire(Hash128 messageHash)
    {
        lock (_stateLock)
        {
            return _expired.Add(messageHash);
        }
    }

    public int Expire(ReadOnlySpan<Hash128> messageHashes)
    {
        int changed = 0;
        lock (_stateLock)
        {
            for (int i = 0; i < messageHashes.Length; i++)
            {
                if (_expired.Add(messageHashes[i]))
                {
                    changed++;
                }
            }
        }

        return changed;
    }

    public void PersistExpiredSet()
    {
        FlushJournalWriterSync();
        lock (_stateLock)
        {
            int payloadByteLength = _expired.Count * 16;
            byte[] fileBytes = new byte[8 + payloadByteLength + 4];
            BinaryPrimitives.WriteUInt32LittleEndian(fileBytes.AsSpan(0), ExpiredFileMagic);
            BinaryPrimitives.WriteUInt32LittleEndian(fileBytes.AsSpan(4), ExpiredFileVersion);
            int payloadOffset = 0;
            foreach (Hash128 hash in _expired)
            {
                BitConverter.TryWriteBytes(fileBytes.AsSpan(8 + payloadOffset, 8), hash.Hi);
                BitConverter.TryWriteBytes(fileBytes.AsSpan(8 + payloadOffset + 8, 8), hash.Lo);
                payloadOffset += 16;
            }

            ReadOnlySpan<byte> payloadSpan = fileBytes.AsSpan(8, payloadByteLength);
            uint crc = Crc32.HashToUInt32(payloadSpan);
            BinaryPrimitives.WriteUInt32LittleEndian(fileBytes.AsSpan(8 + payloadByteLength), crc);
            File.WriteAllBytes(_expiredPath, fileBytes);
        }
    }

    public int Walk(ref long position, out HistoryWalkEntry entry)
    {
        lock (_stateLock)
        {
            if (position < _walkEntriesEvicted)
            {
                position = _walkEntriesEvicted;
            }

            long localIndex = position - _walkEntriesEvicted;
            if (localIndex < 0 || localIndex >= _walkEntries.Count)
            {
                entry = default;
                return 0;
            }

            LinkedListNode<JournalEntry>? node = _walkEntries.First;
            for (long i = 0; i < localIndex && node is not null; i++)
            {
                node = node.Next;
            }

            while (node is not null)
            {
                JournalEntry current = node.Value;
                position++;
                node = node.Next;
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

    public long GetEvictedWalkEntryCount()
    {
        lock (_stateLock)
        {
            return _walkEntriesEvicted;
        }
    }

    private void LoadExpiredSet()
    {
        if (!File.Exists(_expiredPath))
        {
            return;
        }

        byte[] bytes = File.ReadAllBytes(_expiredPath);

        const int newFormatOverhead = 12;
        if (bytes.Length >= newFormatOverhead &&
            BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(0)) == ExpiredFileMagic &&
            BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(4)) == ExpiredFileVersion)
        {
            int payloadLength = bytes.Length - newFormatOverhead;
            if ((payloadLength % 16) != 0)
            {
                throw new InvalidDataException($"Expired hash store '{_expiredPath}' has inconsistent payload length ({payloadLength} bytes).");
            }

            ReadOnlySpan<byte> payload = bytes.AsSpan(8, payloadLength);
            uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(bytes.Length - sizeof(uint)));
            if (Crc32.HashToUInt32(payload) != storedCrc)
            {
                throw new InvalidDataException($"Expired hash store '{_expiredPath}' failed CRC32 validation.");
            }

            for (int offset = 0; offset < payloadLength; offset += 16)
            {
                ulong hi = BitConverter.ToUInt64(payload.Slice(offset, 8));
                ulong lo = BitConverter.ToUInt64(payload.Slice(offset + 8, 8));
                _expired.Add(new Hash128(hi, lo));
            }

            return;
        }

        for (int offset = 0; offset + 16 <= bytes.Length; offset += 16)
        {
            ulong hi = BitConverter.ToUInt64(bytes, offset);
            ulong lo = BitConverter.ToUInt64(bytes, offset + 8);
            _expired.Add(new Hash128(hi, lo));
        }
    }

    private void LoadJournal()
    {
        if (!File.Exists(_journalPath))
        {
            return;
        }

        int lineNumber = 0;
        using StreamReader reader = new(_journalPath, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
        while (reader.ReadLine() is { } line)
        {
            lineNumber++;
            if (!TryParseJournalLine(line, out JournalEntry entry))
            {
                Interlocked.Increment(ref _invalidJournalLines);
                LogInvalidJournalLine(_logger, _journalPath, lineNumber);
                continue;
            }

            AppendWalkEntry_NoLock(entry);
        }

        int invalid = Volatile.Read(ref _invalidJournalLines);
        if (invalid > 0)
        {
            LogJournalReplayInvalidSummary(_journalPath, invalid);
        }
    }

    private void AppendWalkEntry_NoLock(JournalEntry entry)
    {
        _walkEntries.AddLast(entry);
        if (_maxWalkEntriesInMemory > 0)
        {
            while (_walkEntries.Count > _maxWalkEntriesInMemory)
            {
                _walkEntries.RemoveFirst();
                _walkEntriesEvicted++;
            }
        }
    }

    private bool TryParseJournalLine(string line, out JournalEntry entry)
    {
        entry = default;
        string[] parts = line.Split('|');
        if (parts.Length is not 3 and not 5)
        {
            return false;
        }

        try
        {
            string messageId = Encoding.UTF8.GetString(Convert.FromBase64String(parts[0]));
            string serverId = Encoding.UTF8.GetString(Convert.FromBase64String(parts[1]));
            if (!long.TryParse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out long ticks))
            {
                return false;
            }

            Hash128 messageHash;
            if (parts.Length == 5)
            {
                if (!ulong.TryParse(parts[3], NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong hashHi) ||
                    !ulong.TryParse(parts[4], NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong hashLo))
                {
                    return false;
                }

                messageHash = new Hash128(hashHi, hashLo);
            }
            else
            {
                messageHash = _hashProvider.ComputeHash(messageId);
            }

            entry = new JournalEntry(
                new HistoryWalkEntry(messageId, serverId, new DateTimeOffset(ticks, TimeSpan.Zero)),
                messageHash);
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    private void AppendJournalLine(string messageId, string serverId, Hash128 messageHash, DateTimeOffset createdAtUtc)
    {
        string line = string.Create(
            CultureInfo.InvariantCulture,
            $"{Convert.ToBase64String(Encoding.UTF8.GetBytes(messageId))}|{Convert.ToBase64String(Encoding.UTF8.GetBytes(serverId))}|{createdAtUtc.Ticks}|{messageHash.Hi}|{messageHash.Lo}\n");

        lock (_journalWriteLock)
        {
            EnsureJournalWriter();
            _journalWriter!.Write(line);
            if (++_journalLinesSinceFlush >= JournalFlushLineInterval)
            {
                _journalWriter.Flush();
                _journalStream!.Flush(flushToDisk: true);
                _journalLinesSinceFlush = 0;
            }
        }
    }

    private void EnsureJournalWriter()
    {
        if (_journalWriter is not null)
        {
            return;
        }

        _journalStream = new FileStream(
            _journalPath,
            FileMode.Append,
            FileAccess.Write,
            FileShare.Read,
            bufferSize: 65536,
            FileOptions.SequentialScan);
        _journalWriter = new StreamWriter(_journalStream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), bufferSize: 65536, leaveOpen: true)
        {
            AutoFlush = false
        };
    }

    private void FlushJournalWriterSync()
    {
        lock (_journalWriteLock)
        {
            if (_journalWriter is not null)
            {
                _journalWriter.Flush();
            }

            if (_journalStream is not null)
            {
                _journalStream.Flush(flushToDisk: true);
            }

            _journalLinesSinceFlush = 0;
        }
    }

    [LoggerMessage(EventId = 2000, Level = LogLevel.Warning, Message = "Ignoring invalid HistoryDB journal line {LineNumber} in {JournalPath}.")]
    private static partial void LogInvalidJournalLine(ILogger logger, string journalPath, int lineNumber);

    private readonly record struct JournalEntry(HistoryWalkEntry Entry, Hash128 MessageHash);
}
