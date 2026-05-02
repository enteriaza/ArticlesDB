using HistoryDB.Tests.Infrastructure.Contracts;
using HistoryDB.Tests.Infrastructure.Contracts.Dtos;
using HistoryDB.Tests.Infrastructure.Fixtures;
using HistoryDB.Utilities;

namespace HistoryDB.Tests.Bloom;

public sealed class BloomSidecarFileTests : IClassFixture<TestHostFixture>
{
    private readonly TestHostFixture _host;

    public BloomSidecarFileTests(TestHostFixture host)
    {
        ArgumentNullException.ThrowIfNull(host);
        _host = host;
    }

    [Fact]
    public async Task WriteSidecar_RoundTripsHeaderAndWordsAsync()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.BloomSidecarPurposeFolderName);
        string path = workspace.Combine("shard-00000.bloom");

        const int bitCount = 64;
        const int hashCount = 7;
        ulong[] words = [0x0123456789ABCDEFUL];

        BloomSidecarFile.WriteSidecar(path, words, bitCount, hashCount);

        BloomSidecarPayload payload = await _host.BloomSidecarReader.ReadAsync(path, CancellationToken.None);

        Assert.Equal(BloomSidecarFile.FileMagic, payload.Magic);
        Assert.Equal(BloomSidecarFile.FileVersion, payload.Version);
        Assert.Equal(bitCount, payload.BitCount);
        Assert.Equal(hashCount, payload.HashCount);
        Assert.Equal(words, payload.Words);
    }

    [Fact]
    public async Task WriteSidecar_RejectsTamperedCrcAsync()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.BloomSidecarPurposeFolderName);
        string path = workspace.Combine("shard-corrupt.bloom");

        const int bitCount = 64;
        const int hashCount = 7;
        ulong[] words = [0x0123456789ABCDEFUL];

        BloomSidecarFile.WriteSidecar(path, words, bitCount, hashCount);

        byte[] raw = await File.ReadAllBytesAsync(path);
        raw[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(path, raw);

        InvalidDataException exception = await Assert.ThrowsAsync<InvalidDataException>(() =>
            _host.BloomSidecarReader.ReadAsync(path, CancellationToken.None));

        Assert.True(
            exception.Message.Contains("CRC32", StringComparison.Ordinal) ||
            exception.Message.Contains("checksum", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void WriteSidecar_RejectsWordBufferLengthMismatch()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.BloomSidecarPurposeFolderName);
        string path = workspace.Combine("shard-00000.bloom");

        ArgumentException exception = Assert.Throws<ArgumentException>(() =>
            BloomSidecarFile.WriteSidecar(path, words: [], bitCount: 64, hashCount: 7));

        Assert.Equal("words", exception.ParamName);
    }

    [Fact]
    public void WriteSidecar_RejectsPathWithoutDirectoryComponent()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(() =>
            BloomSidecarFile.WriteSidecar(
                path: "sidecar-only.bloom",
                words: [0UL],
                bitCount: 64,
                hashCount: 7));

        Assert.Equal("path", exception.ParamName);
    }
}
