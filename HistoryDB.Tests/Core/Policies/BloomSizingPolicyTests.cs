using HistoryDB.Core.Policies;

namespace HistoryDB.Tests.Core.Policies;

public sealed class BloomSizingPolicyTests
{
    [Fact]
    public void ComputeTargetBitsByEntries_ScalesExpectedEntriesAndCeils()
    {
        ulong slotsPerShard = 100;
        ulong maximumLoadFactorPercent = 50UL;
        double targetBitsPerEntry = 10.0;
        ulong expectedBits = 500UL;
        ulong actualBits = BloomSizingPolicy.ComputeTargetBitsByEntries(
            slotsPerShard,
            maximumLoadFactorPercent,
            targetBitsPerEntry);
        Assert.Equal(expectedBits, actualBits);
    }

    [Fact]
    public void ComputeBudgetBitsPerShard_DividesAcrossShardsAndGenerations()
    {
        ulong budgetBytes = 1024;
        int shardCount = 4;
        int nextGenerationCount = 2;
        ulong budgetBits = budgetBytes * 8UL;
        ulong expectedBitsPerShardGenerationCell = budgetBits / (ulong)(shardCount * nextGenerationCount);
        ulong actual = BloomSizingPolicy.ComputeBudgetBitsPerShard(budgetBytes, shardCount, nextGenerationCount);
        Assert.Equal(expectedBitsPerShardGenerationCell, actual);
    }

    [Fact]
    public void ComputeBudgetBitsPerShard_WhenNextGenerationCountIsZero_TreatsAsSingleGeneration()
    {
        ulong budgetBits = 800UL;
        ulong expected = budgetBits / 8UL;
        ulong actual = BloomSizingPolicy.ComputeBudgetBitsPerShard(
            budgetBytes: 100,
            shardCount: 8,
            nextGenerationCount: 0);
        Assert.Equal(expected, actual);
    }

    [Theory]
    [MemberData(nameof(BloomSizingPolicyTheoryData.SelectDesiredBitsCases), MemberType = typeof(BloomSizingPolicyTheoryData))]
    public void SelectDesiredBits_ReturnsBudgetCapThenMinimumFloor(ulong targetBits, ulong budgetBitsPerShard, int minimumBloomBitsPerShard, ulong expectedDesiredBits)
    {
        ulong actual = BloomSizingPolicy.SelectDesiredBits(targetBits, budgetBitsPerShard, minimumBloomBitsPerShard);
        Assert.Equal(expectedDesiredBits, actual);
    }
}
