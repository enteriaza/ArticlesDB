using System.Buffers.Binary;
using System.Diagnostics;
using System.IO.Hashing;

using HistoryDB.Contracts;

namespace HistoryDB.Utilities;

/// <summary>
/// Atomic on-disk format for <c>expired-md5.bin</c>: header + CRC32 over payload + 16-byte MD5 hashes per entry.
/// </summary>
internal static class ExpiredSetFile
{
    internal const uint FileMagic = 0x44505845; // "EXPD" little-endian

    internal const uint FileVersion = 1;

    /// <summary>magic(4) + version(4) + count(8) + payloadCrc(4) + reserved(4).</summary>
    internal const int HeaderByteLength = 24;

    internal static void WriteAtomic(string destinationPath, IReadOnlyCollection<Hash128> hashes)
    {
        ArgumentNullException.ThrowIfNull(hashes);
        string? directory = Path.GetDirectoryName(destinationPath);
        if (string.IsNullOrEmpty(directory))
        {
            throw new ArgumentException("path must include a directory component.", nameof(destinationPath));
        }

        Directory.CreateDirectory(directory);
        string tempPath = destinationPath + ".tmp";

        byte[] payload = new byte[checked(hashes.Count * 16)];
        int offset = 0;
        foreach (Hash128 hash in hashes)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(offset, 8), hash.Hi);
            BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(offset + 8, 8), hash.Lo);
            offset += 16;
        }

        uint payloadCrc = Crc32.HashToUInt32(payload);

        using (FileStream stream = File.Open(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
        using (BinaryWriter writer = new(stream))
        {
            writer.Write(FileMagic);
            writer.Write(FileVersion);
            writer.Write((ulong)hashes.Count);
            writer.Write(payloadCrc);
            writer.Write(0u);
            writer.Write(payload);
            writer.Flush();
            stream.Flush(flushToDisk: true);
        }

        File.Move(tempPath, destinationPath, overwrite: true);
    }

    /// <summary>
    /// Returns <see langword="true"/> when the file uses the formatted header (magic matches), even if validation fails (<paramref name="error"/> set).
    /// Returns <see langword="false"/> for legacy raw files (no magic).
    /// </summary>
    internal static bool TryLoadFormatted(string path, HashSet<Hash128> into, out string? error)
    {
        error = null;
        byte[] bytes = File.ReadAllBytes(path);
        if (bytes.Length < HeaderByteLength)
        {
            return false;
        }

        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(0, 4));
        if (magic != FileMagic)
        {
            return false;
        }

        uint version = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(4, 4));
        if (version != FileVersion)
        {
            error = $"Unsupported expired-set file version {version}.";
            Trace.TraceWarning("[HistoryDB] {0}", error);
            return true;
        }

        ulong count = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(8, 8));
        uint declaredCrc = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(16, 4));

        long expectedLen = HeaderByteLength + checked((long)count * 16L);
        if (bytes.Length != expectedLen)
        {
            error = $"Expired-set file length {bytes.Length} != expected {expectedLen} for count={count}.";
            Trace.TraceWarning("[HistoryDB] {0}", error);
            return true;
        }

        ReadOnlySpan<byte> payload = bytes.AsSpan(HeaderByteLength);
        uint computed = Crc32.HashToUInt32(payload);
        if (computed != declaredCrc)
        {
            error = "Expired-set payload CRC mismatch.";
            Trace.TraceWarning("[HistoryDB] {0}", error);
            return true;
        }

        for (int i = 0; i < payload.Length; i += 16)
        {
            ulong hi = BinaryPrimitives.ReadUInt64LittleEndian(payload.Slice(i, 8));
            ulong lo = BinaryPrimitives.ReadUInt64LittleEndian(payload.Slice(i + 8, 8));
            into.Add(new Hash128(hi, lo));
        }

        return true;
    }
}
