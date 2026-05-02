// BloomSidecarFile.cs -- binary layout and atomic write for per-shard Bloom filter sidecars (.idx) with trailing CRC-32 (v2).

using System.Buffers.Binary;
using System.IO;

namespace HistoryDB.Utilities;

/// <summary>
/// File format helpers for Bloom sidecar persistence (magic, versioned header, ulong[] payload, CRC-32 in v2).
/// </summary>
internal static class BloomSidecarFile
{
    internal const uint FileMagic = 0x4D4F4F4C;

    internal const uint FileVersionV1 = 1;

    internal const uint FileVersionV2 = 2;

    /// <summary>
    /// Writes v2 sidecar atomically: metadata + words + IEEE CRC-32 over all preceding bytes.
    /// </summary>
    internal static void WriteSidecar(string path, ulong[] words, int bitCount, int hashCount)
    {
        int wordCount = bitCount >> 6;
        if (words.Length != wordCount)
        {
            throw new ArgumentException("Bloom word buffer length mismatch.", nameof(words));
        }

        string? directory = Path.GetDirectoryName(path);
        if (string.IsNullOrEmpty(directory))
        {
            throw new ArgumentException("path must include a directory component.", nameof(path));
        }

        Directory.CreateDirectory(directory);
        string tempPath = path + ".tmp";

        byte[] body;
        using (var ms = new MemoryStream(capacity: 20 + (wordCount * 8)))
        using (var bw = new BinaryWriter(ms))
        {
            bw.Write(FileMagic);
            bw.Write(FileVersionV2);
            bw.Write(bitCount);
            bw.Write(hashCount);
            bw.Write(wordCount);
            for (int i = 0; i < wordCount; i++)
            {
                bw.Write(words[i]);
            }

            bw.Flush();
            body = ms.ToArray();
        }

        uint crc = Crc32Ieee.Compute(body);
        using (FileStream stream = File.Open(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            stream.Write(body);
            Span<byte> crcB = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32LittleEndian(crcB, crc);
            stream.Write(crcB);
            stream.Flush(flushToDisk: true);
        }

        File.Move(tempPath, path, overwrite: true);
    }

    /// <summary>
    /// Reads and validates Bloom sidecar (v1 legacy or v2 with CRC).
    /// </summary>
    internal static ulong[] ReadWordsOrThrow(string path, int expectedBitCount, int expectedHashCount)
    {
        byte[] fileBytes = File.ReadAllBytes(path);
        if (fileBytes.Length < 20)
        {
            throw new InvalidDataException("Bloom sidecar too small for header.");
        }

        ReadOnlySpan<byte> s = fileBytes;
        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(s);
        uint version = BinaryPrimitives.ReadUInt32LittleEndian(s[4..]);
        int bitCount = BinaryPrimitives.ReadInt32LittleEndian(s[8..]);
        int hashCount = BinaryPrimitives.ReadInt32LittleEndian(s[12..]);
        int wordCount = BinaryPrimitives.ReadInt32LittleEndian(s[16..]);

        if (magic != FileMagic
            || bitCount != expectedBitCount
            || hashCount != expectedHashCount
            || wordCount != (expectedBitCount >> 6))
        {
            throw new InvalidDataException("Bloom sidecar metadata mismatch.");
        }

        int v1Payload = 20 + (wordCount * 8);
        if (version == FileVersionV1)
        {
            if (fileBytes.Length != v1Payload)
            {
                throw new InvalidDataException("Bloom v1 sidecar size mismatch.");
            }
        }
        else if (version == FileVersionV2)
        {
            if (fileBytes.Length != v1Payload + 4)
            {
                throw new InvalidDataException("Bloom v2 sidecar size mismatch.");
            }

            uint expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(s[v1Payload..]);
            uint actual = Crc32Ieee.Compute(s[..v1Payload]);
            if (expectedCrc != actual)
            {
                throw new InvalidDataException($"Bloom sidecar CRC mismatch (expected {expectedCrc:X8}, actual {actual:X8}).");
            }
        }
        else
        {
            throw new InvalidDataException($"Unsupported Bloom sidecar version {version}.");
        }

        ulong[] words = new ulong[wordCount];
        int offset = 20;
        for (int i = 0; i < wordCount; i++)
        {
            words[i] = BinaryPrimitives.ReadUInt64LittleEndian(s[offset..]);
            offset += 8;
        }

        return words;
    }
}
