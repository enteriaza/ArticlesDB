using System.Buffers.Binary;
using System.IO.Hashing;
using HistoryDB.Tests.Infrastructure.Contracts;
using HistoryDB.Tests.Infrastructure.Contracts.Dtos;
using HistoryDB.Utilities;
using Microsoft.Extensions.Logging;

namespace HistoryDB.Tests.Infrastructure.Bloom;

internal sealed class BloomSidecarFormatReader : IBloomSidecarFormatReader
{
    private readonly ILogger<BloomSidecarFormatReader> _logger;

    public BloomSidecarFormatReader(ILogger<BloomSidecarFormatReader> logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<BloomSidecarPayload> ReadAsync(string path, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        byte[] bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        try
        {
            return Decode(bytes);
        }
        catch (InvalidDataException ex)
        {
            _logger.LogWarning(ex, "Bloom sidecar decode failed for {Path}", path);
            throw;
        }
    }

    private static BloomSidecarPayload Decode(ReadOnlySpan<byte> span)
    {
        const int HeaderSize = sizeof(uint) + sizeof(uint) + sizeof(int) + sizeof(int) + sizeof(int);
        if (span.Length < HeaderSize)
        {
            throw new InvalidDataException("Bloom sidecar is shorter than the fixed header.");
        }

        int offset = 0;
        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(offset, sizeof(uint)));
        offset += sizeof(uint);
        uint version = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(offset, sizeof(uint)));
        offset += sizeof(uint);
        int bitCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(offset, sizeof(int)));
        offset += sizeof(int);
        int hashCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(offset, sizeof(int)));
        offset += sizeof(int);
        int wordCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(offset, sizeof(int)));
        offset += sizeof(int);

        if (magic != BloomSidecarFile.FileMagic)
        {
            throw new InvalidDataException("Bloom sidecar magic mismatch.");
        }

        if (version != BloomSidecarFile.LegacyFileVersion && version != BloomSidecarFile.FileVersion)
        {
            throw new InvalidDataException($"Bloom sidecar version {version} is not supported.");
        }

        if (wordCount < 0)
        {
            throw new InvalidDataException("Bloom sidecar reported a negative word count.");
        }

        int wordsByteLength = wordCount * sizeof(ulong);
        int offsetAfterWords = offset + wordsByteLength;
        if (offsetAfterWords > span.Length)
        {
            throw new InvalidDataException("Bloom sidecar word payload length is inconsistent with the buffer.");
        }

        if (version == BloomSidecarFile.LegacyFileVersion && span.Length != offsetAfterWords)
        {
            throw new InvalidDataException("Bloom legacy sidecar has trailing bytes.");
        }

        if (version == BloomSidecarFile.FileVersion && span.Length != offsetAfterWords + sizeof(uint))
        {
            throw new InvalidDataException("Bloom sidecar with checksum has unexpected length.");
        }

        ReadOnlySpan<byte> wordsSpan = span.Slice(offset, wordsByteLength);
        ulong[] words = wordCount == 0 ? Array.Empty<ulong>() : new ulong[wordCount];
        for (int i = 0; i < wordCount; i++)
        {
            words[i] = BinaryPrimitives.ReadUInt64LittleEndian(wordsSpan.Slice(i * sizeof(ulong), sizeof(ulong)));
        }

        if (version == BloomSidecarFile.FileVersion)
        {
            uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(offsetAfterWords, sizeof(uint)));
            uint computed = Crc32.HashToUInt32(span.Slice(0, offsetAfterWords));
            if (storedCrc != computed)
            {
                throw new InvalidDataException("Bloom sidecar CRC32 mismatch.");
            }
        }

        return new BloomSidecarPayload(magic, version, bitCount, hashCount, words);
    }
}
