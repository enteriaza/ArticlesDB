using System.Buffers.Binary;
using HistoryDB.Tests.Infrastructure.Contracts;
using HistoryDB.Tests.Infrastructure.Contracts.Dtos;
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

        if (wordCount < 0)
        {
            throw new InvalidDataException("Bloom sidecar reported a negative word count.");
        }

        long payloadBytes = (long)wordCount * sizeof(ulong);
        if (payloadBytes > span.Length - offset)
        {
            throw new InvalidDataException("Bloom sidecar word payload length is inconsistent with the buffer.");
        }

        ulong[] words = wordCount == 0 ? Array.Empty<ulong>() : new ulong[wordCount];
        ReadOnlySpan<byte> wordsSpan = span.Slice(offset, wordCount * sizeof(ulong));
        for (int i = 0; i < wordCount; i++)
        {
            words[i] = BinaryPrimitives.ReadUInt64LittleEndian(wordsSpan.Slice(i * sizeof(ulong), sizeof(ulong)));
        }

        return new BloomSidecarPayload(magic, version, bitCount, hashCount, words);
    }
}
