// BloomSizingPolicy.cs -- pure bloom sizing policy for per-shard bit allocation under memory budgets.

namespace HistoryDB.Core.Policies;

internal static class BloomSizingPolicy
{
    internal static ulong ComputeTargetBitsByEntries(ulong slotsPerShard, ulong maxLoadFactorPercent, double targetBitsPerEntry)
    {
        double expectedEntriesPerShard = slotsPerShard * (maxLoadFactorPercent / 100.0);
        return (ulong)Math.Ceiling(expectedEntriesPerShard * targetBitsPerEntry);
    }

    internal static ulong ComputeBudgetBitsPerShard(ulong budgetBytes, int shardCount, int nextGenerationCount)
    {
        ulong budgetBits = checked(budgetBytes * 8UL);
        ulong shardGenerationCount = (ulong)shardCount * (ulong)Math.Max(1, nextGenerationCount);
        return shardGenerationCount == 0 ? budgetBits : budgetBits / shardGenerationCount;
    }

    internal static ulong SelectDesiredBits(ulong targetBitsByEntries, ulong budgetBitsPerShard, int minBloomBitsPerShard)
    {
        ulong desiredBits = Math.Min(targetBitsByEntries, budgetBitsPerShard);
        return desiredBits < (ulong)minBloomBitsPerShard ? (ulong)minBloomBitsPerShard : desiredBits;
    }
}
