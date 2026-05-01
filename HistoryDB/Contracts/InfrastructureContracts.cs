// InfrastructureContracts.cs -- infrastructure abstractions used by HistoryDB application orchestration.

namespace HistoryDB.Contracts;

internal interface IShardStore : IDisposable
{
    ulong TableSize { get; }
    ulong UsedSlots { get; }
    ulong LoadLimitSlots { get; }
    ulong MaxOccupiedSlotIndex { get; }

    bool Exists(ulong hashHi, ulong hashLo);
    ShardInsertResult TryExistsOrInsertUnchecked(ulong hashHi, ulong hashLo, ulong serverHi, ulong serverLo);
    void FlushUsedSlots(ulong usedSlots);
    bool TryReadHashAt(ulong index, out ulong hashHi, out ulong hashLo);
    bool TryScrubSlotLayout(ulong index, out SlotScrubOutcome outcome);
}

internal interface IShardStoreFactory
{
    IShardStore Create(string path, ulong tableSize, ulong maxLoadFactor);
}

internal interface ISystemMemoryInfo
{
    ulong GetInstalledMemoryBytes();
}

internal interface IPathValidator
{
    string ValidateRootDirectory(string rootPath);
    string ValidateDataFilePath(string path);
}

internal interface IBloomSidecarStore
{
    void WriteSidecar(string path, ulong[] words, int bitCount, int hashCount);
}

internal interface IThreadAffinityService
{
    bool TryPinCurrentThreadToLogicalProcessor(int logicalProcessorIndex);
}

internal interface INumaTopologyService
{
    bool TryGetProcessorsNumaNodeOrderGroup0(out int[]? processors);
}
