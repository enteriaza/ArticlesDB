namespace HistoryDB.Tests.Infrastructure.Contracts.Dtos;

/// <summary>
/// Strongly typed decoded Bloom sidecar binary payload (header + word buffer).
/// Matches the on-disk layout produced by <see cref="HistoryDB.Utilities.BloomSidecarFile.WriteSidecar"/>.
/// </summary>
public sealed record BloomSidecarPayload(
    uint Magic,
    uint Version,
    int BitCount,
    int HashCount,
    IReadOnlyList<ulong> Words);
