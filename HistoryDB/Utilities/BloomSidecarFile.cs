// BloomSidecarFile.cs -- binary layout and atomic write for per-shard Bloom filter sidecars (.bloom) shared by load and save paths.
// Uses temp file plus replace to avoid torn reads; callers supply word buffers matching bitCount / hashCount metadata.

using System.IO;

namespace HistoryDB.Utilities;

/// <summary>
/// File format helpers for Bloom sidecar persistence (magic, versioned header, ulong[] payload).
/// </summary>
/// <remarks>
/// <para>
/// <b>On-disk layout:</b> <c>uint magic</c>, <c>uint version</c>, <c>int bitCount</c>, <c>int hashCount</c>, <c>int wordCount</c>, then <c>wordCount</c> little-endian <see cref="ulong"/> values.
/// </para>
/// <para>
/// <b>Thread safety:</b> <see cref="WriteSidecar(string, ulong[], int, int)"/> must not be invoked concurrently for the same <paramref name="path"/>; concurrent writes to different paths are supported by the OS.
/// </para>
/// </remarks>
internal static class BloomSidecarFile
{
    /// <summary>
    /// File magic identifying HistoryDB Bloom sidecars (truncated ASCII tag).
    /// </summary>
    internal const uint FileMagic = 0x4D4F4F4C;

    /// <summary>
    /// Current on-disk format version for <see cref="FileMagic"/>.
    /// </summary>
    internal const uint FileVersion = 1;

    /// <summary>
    /// Writes a Bloom sidecar atomically (temp file, flush, move) so readers never observe a partial file.
    /// </summary>
    /// <param name="path">Destination path, typically <c>shard-xxxxx.bloom</c> under a generation directory.</param>
    /// <param name="words">Little-endian bit array in 64-bit words; length must equal <c>bitCount / 64</c>.</param>
    /// <param name="bitCount">Power-of-two bit slot count.</param>
    /// <param name="hashCount">Number of hash functions used when inserting.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="words"/> length does not match <paramref name="bitCount"/>.</exception>
    /// <exception cref="IOException">Thrown when the underlying file system rejects create, flush, or rename.</exception>
    /// <exception cref="UnauthorizedAccessException">Thrown when the process lacks permission to write <paramref name="path"/>.</exception>
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

        using (FileStream stream = File.Open(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
        using (BinaryWriter writer = new(stream))
        {
            writer.Write(FileMagic);
            writer.Write(FileVersion);
            writer.Write(bitCount);
            writer.Write(hashCount);
            writer.Write(wordCount);
            for (int i = 0; i < wordCount; i++)
            {
                writer.Write(words[i]);
            }

            writer.Flush();
            stream.Flush(flushToDisk: true);
        }

        File.Move(tempPath, path, overwrite: true);
    }
}
