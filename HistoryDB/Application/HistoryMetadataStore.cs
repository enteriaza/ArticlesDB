// HistoryMetadataStore.cs -- durable walk/expiry metadata store for the public HistoryDatabase API.

using System.Globalization;
using System.Text;

using HistoryDB.Contracts;
using Microsoft.Extensions.Logging;

namespace HistoryDB.Application;

internal sealed partial class HistoryMetadataStore : IHistoryMetadataStore
{
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

    public HistoryMetadataStore(string rootPath, IMessageHashProvider hashProvider, ILogger logger, int maxWalkEntriesInMemory = 1_048_576)
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
        LoadJournal();
    }

    public bool IsExpired(Hash128 messageHash)
    {
        lock (_stateLock)
        {
            return _expired.Contains(messageHash);
        }
    }

    public void RecordInserted(string messageId, string serverId, Hash128 messageHash, DateTimeOffset createdAtUtc)
    {
        lock (_stateLock)
        {
            _expired.Remove(messageHash);
            AppendWalkEntry_NoLock(new JournalEntry(new HistoryWalkEntry(messageId, serverId, createdAtUtc), messageHash));
        }

        AppendJournalLine(messageId, serverId, messageHash, createdAtUtc);
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

    private void LoadJournal()
    {
        if (!File.Exists(_journalPath))
        {
            return;
        }

        int lineNumber = 0;
        foreach (string line in File.ReadLines(_journalPath))
        {
            lineNumber++;
            if (!TryParseJournalLine(line, out JournalEntry entry))
            {
                LogInvalidJournalLine(_logger, _journalPath, lineNumber);
                continue;
            }

            AppendWalkEntry_NoLock(entry);
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
        // Always emit '\n' (not Environment.NewLine) so journal files migrate cleanly between Windows and Linux hosts.
        string line = string.Create(
            CultureInfo.InvariantCulture,
            $"{Convert.ToBase64String(Encoding.UTF8.GetBytes(messageId))}|{Convert.ToBase64String(Encoding.UTF8.GetBytes(serverId))}|{createdAtUtc.Ticks}|{messageHash.Hi}|{messageHash.Lo}\n");

        lock (_journalWriteLock)
        {
            File.AppendAllText(_journalPath, line);
        }
    }

    [LoggerMessage(EventId = 2000, Level = LogLevel.Warning, Message = "Ignoring invalid HistoryDB journal line {LineNumber} in {JournalPath}.")]
    private static partial void LogInvalidJournalLine(ILogger logger, string journalPath, int lineNumber);

    private readonly record struct JournalEntry(HistoryWalkEntry Entry, Hash128 MessageHash);
}
