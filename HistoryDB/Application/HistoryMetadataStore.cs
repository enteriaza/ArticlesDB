// HistoryMetadataStore.cs -- durable walk/expiry metadata store for the public HistoryDatabase API.

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
    private readonly List<JournalEntry> _walkEntries = [];
    private readonly IMessageHashProvider _hashProvider;
    private readonly ILogger _logger;

    public HistoryMetadataStore(string rootPath, IMessageHashProvider hashProvider, ILogger logger)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootPath);
        ArgumentNullException.ThrowIfNull(hashProvider);
        ArgumentNullException.ThrowIfNull(logger);

        string normalizedRoot = Path.GetFullPath(rootPath);
        Directory.CreateDirectory(normalizedRoot);
        _expiredPath = Path.Combine(normalizedRoot, "expired-md5.bin");
        _journalPath = Path.Combine(normalizedRoot, "history-journal.log");
        _hashProvider = hashProvider;
        _logger = logger;

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
            _walkEntries.Add(new JournalEntry(new HistoryWalkEntry(messageId, serverId, createdAtUtc), messageHash));
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
            while (position >= 0 && position < _walkEntries.Count)
            {
                JournalEntry current = _walkEntries[(int)position++];
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

            _walkEntries.Add(entry);
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
            if (!long.TryParse(parts[2], out long ticks))
            {
                return false;
            }

            Hash128 messageHash;
            if (parts.Length == 5)
            {
                if (!ulong.TryParse(parts[3], out ulong hashHi) ||
                    !ulong.TryParse(parts[4], out ulong hashLo))
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
            System.Globalization.CultureInfo.InvariantCulture,
            $"{Convert.ToBase64String(Encoding.UTF8.GetBytes(messageId))}|{Convert.ToBase64String(Encoding.UTF8.GetBytes(serverId))}|{createdAtUtc.Ticks}|{messageHash.Hi}|{messageHash.Lo}");

        lock (_journalWriteLock)
        {
            File.AppendAllText(_journalPath, line + Environment.NewLine);
        }
    }

    [LoggerMessage(EventId = 2000, Level = LogLevel.Warning, Message = "Ignoring invalid HistoryDB journal line {LineNumber} in {JournalPath}.")]
    private static partial void LogInvalidJournalLine(ILogger logger, string journalPath, int lineNumber);

    private readonly record struct JournalEntry(HistoryWalkEntry Entry, Hash128 MessageHash);
}
