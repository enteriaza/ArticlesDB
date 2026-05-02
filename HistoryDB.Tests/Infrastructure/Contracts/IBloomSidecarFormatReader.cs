using HistoryDB.Tests.Infrastructure.Contracts.Dtos;

namespace HistoryDB.Tests.Infrastructure.Contracts;

/// <summary>
/// Reads Bloom sidecar files using the HistoryDB binary contract (test-side codec mirroring production load logic).
/// </summary>
public interface IBloomSidecarFormatReader
{
    /// <summary>
    /// Loads and decodes the sidecar at <paramref name="path"/>.
    /// </summary>
    Task<BloomSidecarPayload> ReadAsync(string path, CancellationToken cancellationToken = default);
}
