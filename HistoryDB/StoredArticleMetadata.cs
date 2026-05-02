using HistoryDB.Contracts;

namespace HistoryDB;

/// <summary>
/// One occupied slot observed during a metadata harvest (generation, shard, slot index, hash, and access timestamps).
/// </summary>
public readonly record struct StoredArticleMetadata(
    int GenerationId,
    int ShardId,
    ulong SlotIndex,
    Hash128 MessageHash,
    long DateObtainedTicks,
    long DateAccessedTicks,
    ulong AccessedCounter);
