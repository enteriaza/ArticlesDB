// HistoryDatabase.WalkJournal.cs -- walk window, journal replay/checkpoint, batching, and trimming (partial class).

using System.Buffers;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;

using HistoryDB.Contracts;
using HistoryDB.Utilities;

using Microsoft.Extensions.Logging;

namespace HistoryDB;

public sealed partial class HistoryDatabase
{
    public const long DefaultJournalTailRetainBytes = 32L * 1024 * 1024;
    public const int DefaultJournalBufferFlushThresholdBytes = 16 * 1024;
    public const int DefaultJournalCheckpointLineInterval = 128;

    private readonly string _journalCheckpointPath;
    private readonly string _journalDirtyMarkerPath;
    private readonly long _journalMaxFileBytesBeforeTrim;
    private readonly long _journalTailRetainBytes;
    private readonly int _journalBufferFlushThreshold;
    private readonly int _journalCheckpointEveryNLines;
    private readonly ConsecutiveFailureCircuit _journalCircuit;

    private readonly List<byte> _journalUtf8Buffer = [];
    private int _journalLinesSinceCheckpoint;

    private LinkedList<HistoryWalkEntry>? _walkList;
    private HistoryWalkEntry[]? _walkRing;
    private int _walkRingHead;
    private int _walkRingCount;
    private int _walkRingCapacity;

    private void InitWalkStorage()
    {
        if (_maxWalkEntriesInMemory > 0)
        {
            _walkRingCapacity = _maxWalkEntriesInMemory;
            _walkRing = new HistoryWalkEntry[_walkRingCapacity];
            _walkRingHead = 0;
            _walkRingCount = 0;
        }
        else
        {
            _walkList = new LinkedList<HistoryWalkEntry>();
        }
    }

    private void AppendWalkEntry_NoLock(HistoryWalkEntry entry)
    {
        if (_walkRing is not null)
        {
            if (_walkRingCount < _walkRingCapacity)
            {
                int idx = (_walkRingHead + _walkRingCount) % _walkRingCapacity;
                _walkRing[idx] = entry;
                _walkRingCount++;
            }
            else
            {
                _walkRingHead = (_walkRingHead + 1) % _walkRingCapacity;
                _walkEntriesEvicted++;
                int tailIdx = (_walkRingHead + _walkRingCount - 1) % _walkRingCapacity;
                _walkRing[tailIdx] = entry;
            }
        }
        else
        {
            _walkList!.AddLast(entry);
        }
    }

    private int WalkRingCount => _walkRing is not null ? _walkRingCount : _walkList!.Count;

    public int HistoryWalk(ref long position, out HistoryWalkEntry entry, int flags = 0)
    {
        lock (_stateLock)
        {
            if (position < _walkEntriesEvicted)
            {
                position = _walkEntriesEvicted;
            }

            long localIndex = position - _walkEntriesEvicted;
            int windowCount = WalkRingCount;
            if (localIndex < 0 || localIndex >= windowCount)
            {
                entry = default;
                return 0;
            }

            if (_walkRing is not null)
            {
                for (long i = localIndex; i < _walkRingCount; i++)
                {
                    int phys = (int)((_walkRingHead + i) % _walkRingCapacity);
                    HistoryWalkEntry current = _walkRing[phys];
                    position++;
                    if (_expired.Contains(current.MessageHash))
                    {
                        continue;
                    }

                    entry = current;
                    return 1;
                }
            }
            else
            {
                LinkedListNode<HistoryWalkEntry>? node = _walkList!.First;
                for (long j = 0; j < localIndex && node is not null; j++)
                {
                    node = node.Next;
                }

                while (node is not null)
                {
                    HistoryWalkEntry current = node.Value;
                    position++;
                    node = node.Next;
                    if (_expired.Contains(current.MessageHash))
                    {
                        continue;
                    }

                    entry = current;
                    return 1;
                }
            }
        }

        entry = default;
        return 0;
    }

    private void LoadJournal()
    {
        if (!File.Exists(_journalPath))
        {
            return;
        }

        long startOffset = JournalReplayCheckpoint.TryReadOffset(_journalCheckpointPath);
        using FileStream fs = new(_journalPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 65536);
        if (startOffset > fs.Length)
        {
            startOffset = 0;
        }

        fs.Position = startOffset;
        int lineNumber = 0;
        long nextCheckpointOffset = startOffset;
        while (fs.Position < fs.Length)
        {
            long lineStartByte = fs.Position;
            string? line = ReadJournalPhysicalLine(fs);
            if (line is null)
            {
                break;
            }

            lineNumber++;
            if (!TryParseJournalLine(line, lineNumber, lineStartByte, out HistoryWalkEntry walkEntry))
            {
                continue;
            }

            AppendWalkEntry_NoLock(walkEntry);
            nextCheckpointOffset = fs.Position;

            if (_journalCheckpointEveryNLines > 0 && (++_journalLinesSinceCheckpoint % _journalCheckpointEveryNLines) == 0)
            {
                JournalReplayCheckpoint.Write(_journalCheckpointPath, nextCheckpointOffset);
            }
        }

        JournalReplayCheckpoint.Write(_journalCheckpointPath, nextCheckpointOffset);
    }

    private bool TryParseJournalLine(string line, int lineNumber, long lineStartByte, out HistoryWalkEntry walkEntry)
    {
        walkEntry = default;
        string[] parts = line.Split('|');
        if (parts.Length != 3 && parts.Length != 5)
        {
            LogJournalLineInvalid(lineNumber, lineStartByte, "expected 3 or 5 pipe-separated fields.");
            return false;
        }

        string messageId;
        string serverId;
        try
        {
            messageId = Encoding.UTF8.GetString(Convert.FromBase64String(parts[0]));
            serverId = Encoding.UTF8.GetString(Convert.FromBase64String(parts[1]));
        }
        catch (FormatException ex)
        {
            LogJournalLineInvalid(lineNumber, lineStartByte, ex.Message);
            return false;
        }

        if (!long.TryParse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out long ticks))
        {
            LogJournalLineInvalid(lineNumber, lineStartByte, "invalid ticks.");
            return false;
        }

        Hash128 messageHash;
        if (parts.Length == 5)
        {
            if (!ulong.TryParse(parts[3], NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong hi) ||
                !ulong.TryParse(parts[4], NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong lo))
            {
                LogJournalLineInvalid(lineNumber, lineStartByte, "invalid message hash quadwords.");
                return false;
            }

            messageHash = new Hash128(hi, lo);
        }
        else
        {
            messageHash = ComputeMd5(messageId);
        }

        walkEntry = new HistoryWalkEntry(messageId, serverId, new DateTimeOffset(ticks, TimeSpan.Zero), messageHash);
        return true;
    }

    private static string? ReadJournalPhysicalLine(FileStream fs)
    {
        using MemoryStream line = new();
        int b;
        while ((b = fs.ReadByte()) >= 0)
        {
            if (b == '\n')
            {
                break;
            }

            if (b != '\r')
            {
                line.WriteByte((byte)b);
            }
        }

        if (b < 0 && line.Length == 0)
        {
            return null;
        }

        return Encoding.UTF8.GetString(line.ToArray());
    }

    private void AppendJournalLineBuffered(string messageId, string serverId, Hash128 messageHash)
    {
        byte[] lineUtf8 = BuildJournalLineUtf8(messageId, serverId, messageHash);
        lock (_journalWriteLock)
        {
            _journalUtf8Buffer.AddRange(lineUtf8);
            if (_journalUtf8Buffer.Count >= _journalBufferFlushThreshold)
            {
                FlushJournalBufferCore();
            }
        }
    }

    private static byte[] BuildJournalLineUtf8(string messageId, string serverId, Hash128 messageHash)
    {
        string text = string.Create(
            CultureInfo.InvariantCulture,
            $"{Convert.ToBase64String(Encoding.UTF8.GetBytes(messageId))}|{Convert.ToBase64String(Encoding.UTF8.GetBytes(serverId))}|{DateTimeOffset.UtcNow.Ticks}|{messageHash.Hi}|{messageHash.Lo}\n");
        return Encoding.UTF8.GetBytes(text);
    }

    private void FlushJournalBufferCore(bool bypassCircuit = false)
    {
        if (_journalUtf8Buffer.Count == 0)
        {
            return;
        }

        if (!bypassCircuit && _journalCircuit.IsOpen && !_journalCircuit.TryEnterRetry())
        {
            LogJournalCircuitOpenSkipFlush();
            return;
        }

        try
        {
            EnsureJournalWriter();
            FileStream fs = (FileStream)_journalWriter!.BaseStream;
            fs.Write(CollectionsMarshal.AsSpan(_journalUtf8Buffer));
            _journalWriter.Flush();
            _journalStream!.Flush(flushToDisk: true);
            _journalUtf8Buffer.Clear();
            _journalCircuit.RecordSuccess();
        }
        catch (Exception ex)
        {
            _journalCircuit.RecordFailure();
            LogJournalFlushFailed(ex);
            throw;
        }
    }

    private void FlushJournalBuffer()
    {
        lock (_journalWriteLock)
        {
            FlushJournalBufferCore();
        }
    }

    /// <summary>Flush pending journal bytes, release file handles, optionally trim file. Caller must not hold <see cref="_stateLock"/>.</summary>
    private void JournalSyncMaintenance_NoLock()
    {
        FlushJournalBufferCore(bypassCircuit: true);
        try
        {
            _journalWriter?.Flush();
        }
        catch (Exception ex)
        {
            LogJournalFlushFailed(ex);
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
        MaybeTrimJournalFile_NoLock();
    }

    private void EnsureJournalWriter()
    {
        if (_journalStream is not null)
        {
            return;
        }

        _journalStream = new FileStream(
            _journalPath,
            FileMode.Append,
            FileAccess.Write,
            FileShare.Read,
            bufferSize: 4096,
            FileOptions.WriteThrough);
        _journalWriter = new StreamWriter(_journalStream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false))
        {
            AutoFlush = false,
        };
    }

    private void MaybeTrimJournalFile_NoLock()
    {
        if (_journalMaxFileBytesBeforeTrim <= 0 || _journalTailRetainBytes <= 0)
        {
            return;
        }

        if (!File.Exists(_journalPath))
        {
            return;
        }

        long len = new FileInfo(_journalPath).Length;
        if (len <= _journalMaxFileBytesBeforeTrim)
        {
            return;
        }

        long keep = Math.Min(_journalTailRetainBytes, len);
        long start = len - keep;
        string tempPath = _journalPath + ".trim.tmp";
        using (FileStream src = new(_journalPath, FileMode.Open, FileAccess.Read, FileShare.Read))
        using (FileStream dst = File.Open(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            src.Seek(start, SeekOrigin.Begin);
            src.CopyTo(dst);
            dst.Flush(true);
        }

        File.Move(tempPath, _journalPath, overwrite: true);
        JournalReplayCheckpoint.Write(_journalCheckpointPath, 0);
        LogJournalTrimmed(len, keep);
    }

    private void TryDeleteJournalDirtyMarker()
    {
        try
        {
            if (File.Exists(_journalDirtyMarkerPath))
            {
                File.Delete(_journalDirtyMarkerPath);
            }
        }
        catch
        {
            // best effort
        }
    }

    private void WriteJournalDirtyMarker()
    {
        try
        {
            File.WriteAllText(_journalDirtyMarkerPath, $"journal_append_failed_utc={DateTimeOffset.UtcNow:O}\n");
        }
        catch (Exception ex)
        {
            LogJournalDirtyMarkerFailed(ex);
        }
    }

    private static Hash128 ComputeMd5(string text)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(text);
        int byteCount = Encoding.UTF8.GetByteCount(text);
        byte[]? rented = byteCount <= 512 ? null : ArrayPool<byte>.Shared.Rent(byteCount);
        Span<byte> utf8 = byteCount <= 512 ? stackalloc byte[byteCount] : rented.AsSpan(0, byteCount);
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
}
