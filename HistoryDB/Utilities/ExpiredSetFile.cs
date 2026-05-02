// ExpiredSetFile.cs -- versioned expired-hash blob with trailing CRC-32; migrates legacy raw 16-byte records.

using System.Buffers.Binary;
using System.IO;

namespace HistoryDB.Utilities;

/// <summary>
/// On-disk layout for <c>expired-md5.bin</c>.
/// </summary>
internal static class ExpiredSetFile
{
    /// <summary>Magic "EXPD" little-endian.</summary>
    internal const uint FileMagic = 0x44505845;

    internal const uint FileVersion = 1;

    /// <summary>
    /// Header (12 bytes) + <c>entryCount * 16</c> + CRC-32 (4 bytes). CRC covers bytes [0 .. 12+count*16).
    /// </summary>
    internal static byte[] BuildPayload(ReadOnlySpan<byte> entries16)
    {
        if ((entries16.Length % 16) != 0)
        {
            throw new ArgumentException("Expired hash payload length must be a multiple of 16.", nameof(entries16));
        }

        int entryCount = entries16.Length / 16;
        byte[] buffer = new byte[12 + entries16.Length + 4];
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(0, 4), FileMagic);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(4, 4), FileVersion);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(8, 4), (uint)entryCount);
        entries16.CopyTo(buffer.AsSpan(12));
        uint crc = Crc32Ieee.Compute(buffer.AsSpan(0, 12 + entries16.Length));
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(12 + entries16.Length, 4), crc);
        return buffer;
    }

    /// <summary>
    /// Parses legacy (raw hashes) or versioned file. Returns 16-byte-aligned hash entries only.
    /// </summary>
    internal static ReadOnlyMemory<byte> ParseHashesOrThrow(ReadOnlySpan<byte> fileBytes)
    {
        if (fileBytes.Length == 0)
        {
            return ReadOnlyMemory<byte>.Empty;
        }

        if (fileBytes.Length >= 12
            && BinaryPrimitives.ReadUInt32LittleEndian(fileBytes) == FileMagic)
        {
            uint version = BinaryPrimitives.ReadUInt32LittleEndian(fileBytes[4..]);
            if (version != FileVersion)
            {
                throw new InvalidDataException($"Unsupported expired set version {version}.");
            }

            int entryCount = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(fileBytes[8..]));
            long payloadBytes = 12L + (entryCount * 16L) + 4L;
            if (fileBytes.Length < payloadBytes)
            {
                throw new InvalidDataException("Expired set file truncated (headers/entries/CRC).");
            }

            if (fileBytes.Length != payloadBytes)
            {
                throw new InvalidDataException("Expired set file has trailing garbage after CRC.");
            }

            ReadOnlySpan<byte> bodyForCrc = fileBytes[..(12 + (entryCount * 16))];
            uint expected = BinaryPrimitives.ReadUInt32LittleEndian(fileBytes[(12 + (entryCount * 16))..]);
            uint actual = Crc32Ieee.Compute(bodyForCrc);
            if (expected != actual)
            {
                throw new InvalidDataException($"Expired set CRC mismatch (expected {expected:X8}, actual {actual:X8}).");
            }

            return fileBytes[12..(12 + (entryCount * 16))].ToArray();
        }

        // Legacy: raw multiple of 16 bytes, no CRC
        if ((fileBytes.Length % 16) != 0)
        {
            throw new InvalidDataException("Legacy expired set file length is not a multiple of 16.");
        }

        return fileBytes.ToArray();
    }
}
