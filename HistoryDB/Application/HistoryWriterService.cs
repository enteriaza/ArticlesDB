// HistoryWriterService.cs -- application-layer facade that composes writer orchestration behind the IHistoryWriter contract.

using HistoryDB.Contracts;
using Microsoft.Extensions.Logging;

namespace HistoryDB.Application;

/// <summary>
/// Default application service implementation for <see cref="IHistoryWriter"/>.
/// </summary>
internal sealed class HistoryWriterService : IHistoryWriter
{
    private readonly ShardedHistoryWriter _inner;

    public HistoryWriterService(HistoryWriterOptions options, ILogger? logger = null)
    {
        _inner = new ShardedHistoryWriter(options, logger);
    }

    public ValueTask<ShardInsertResult> EnqueueAsync(ShardInsertRequest request, CancellationToken cancellationToken = default) =>
        _inner.EnqueueAsync(request, cancellationToken);

    public ValueTask<ShardInsertResult> EnqueueAsync(ulong hashHi, ulong hashLo, ulong serverHi, ulong serverLo, CancellationToken cancellationToken = default) =>
        _inner.EnqueueAsync(hashHi, hashLo, serverHi, serverLo, cancellationToken);

    public bool Exists(ulong hashHi, ulong hashLo) => _inner.Exists(hashHi, hashLo);

    public Task StopAsync() => _inner.StopAsync();

    public IReadOnlyList<ShardPressureSnapshot> GetActiveGenerationShardPressure() => _inner.GetActiveGenerationShardPressure();

    public SlotScrubberCounters GetSlotScrubberCounters() => _inner.GetSlotScrubberCounters();

    public ValueTask DisposeAsync() => _inner.DisposeAsync();

    public void Dispose() => _inner.Dispose();
}

/// <summary>
/// Default factory for <see cref="IHistoryWriter"/> instances.
/// </summary>
internal sealed class HistoryWriterFactory : IHistoryWriterFactory
{
    public IHistoryWriter Create(HistoryWriterOptions options, ILogger? logger = null) => new HistoryWriterService(options, logger);
}
