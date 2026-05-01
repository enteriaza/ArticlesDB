// MmapShardStore.cs -- infrastructure adapter exposing HistoryShard through the IShardStore abstraction.

using HistoryDB.Contracts;

namespace HistoryDB.Infrastructure.Storage;

internal sealed class MmapShardStore : IShardStore
{
    private readonly HistoryShard _inner;

    internal MmapShardStore(string path, ulong tableSize, ulong maxLoadFactor)
    {
        _inner = new HistoryShard(path, tableSize, maxLoadFactor);
    }

    public ulong TableSize => _inner.TableSize;
    public ulong UsedSlots => _inner.UsedSlots;
    public ulong LoadLimitSlots => _inner.LoadLimitSlots;
    public ulong MaxOccupiedSlotIndex => _inner.MaxOccupiedSlotIndex;

    public bool Exists(ulong hashHi, ulong hashLo) => _inner.Exists(hashHi, hashLo);

    public ShardInsertResult TryExistsOrInsertUnchecked(ulong hashHi, ulong hashLo, ulong serverHi, ulong serverLo) =>
        _inner.TryExistsOrInsertUnchecked(hashHi, hashLo, serverHi, serverLo);

    public void FlushUsedSlots(ulong usedSlots) => _inner.FlushUsedSlots(usedSlots);

    public bool TryReadHashAt(ulong index, out ulong hashHi, out ulong hashLo) => _inner.TryReadHashAt(index, out hashHi, out hashLo);

    public bool TryScrubSlotLayout(ulong index, out SlotScrubOutcome outcome) => _inner.TryScrubSlotLayout(index, out outcome);

    public void Dispose() => _inner.Dispose();
}

internal sealed class MmapShardStoreFactory : IShardStoreFactory
{
    public IShardStore Create(string path, ulong tableSize, ulong maxLoadFactor) => new MmapShardStore(path, tableSize, maxLoadFactor);
}
