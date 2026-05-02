using HistoryDB.Core.Policies;

namespace HistoryDB.Tests.Core.Policies;

public sealed class RolloverPolicyTests
{
    [Fact]
    public void ComputeUsedSlotsThreshold_WhenPercentIsZero_ReturnsZero()
    {
        ulong slotsPerShard = 1000;
        ulong proactiveRolloverLoadFactorPercent = 0;
        ulong actual = RolloverPolicy.ComputeUsedSlotsThreshold(slotsPerShard, proactiveRolloverLoadFactorPercent);
        Assert.Equal(0UL, actual);
    }

    [Theory]
    [MemberData(nameof(RolloverPolicyTheoryData.UsedSlotsThresholdScalingCases), MemberType = typeof(RolloverPolicyTheoryData))]
    public void ComputeUsedSlotsThreshold_ScalesByPercent(ulong slotsPerShard, ulong proactivePercent, ulong expectedThreshold)
    {
        ulong actual = RolloverPolicy.ComputeUsedSlotsThreshold(slotsPerShard, proactivePercent);
        Assert.Equal(expectedThreshold, actual);
    }

    [Fact]
    public void ComputeQueueDepthThreshold_WhenPercentIsZero_ReturnsZero()
    {
        int queueCapacityPerShard = 1000;
        int proactiveRolloverQueueWatermarkPercent = 0;
        int actual = RolloverPolicy.ComputeQueueDepthThreshold(queueCapacityPerShard, proactiveRolloverQueueWatermarkPercent);
        Assert.Equal(0, actual);
    }

    [Theory]
    [MemberData(nameof(RolloverPolicyTheoryData.QueueDepthThresholdCases), MemberType = typeof(RolloverPolicyTheoryData))]
    public void ComputeQueueDepthThreshold_IsAtLeastOneWhenPercentNonZero(int queueCapacityPerShard, int watermarkPercent, int expectedThreshold)
    {
        int actual = RolloverPolicy.ComputeQueueDepthThreshold(queueCapacityPerShard, watermarkPercent);
        Assert.Equal(expectedThreshold, actual);
    }

    [Theory]
    [MemberData(nameof(RolloverPolicyTheoryData.ShouldRolloverFromUsedSlotsCases), MemberType = typeof(RolloverPolicyTheoryData))]
    public void ShouldRolloverFromUsedSlots_RequiresNonZeroThreshold(ulong localUsedSlots, ulong thresholdSlots, bool expected)
    {
        bool actual = RolloverPolicy.ShouldRolloverFromUsedSlots(localUsedSlots, thresholdSlots);
        Assert.Equal(expected, actual);
    }

    [Theory]
    [MemberData(nameof(RolloverPolicyTheoryData.ShouldRolloverFromQueueDepthCases), MemberType = typeof(RolloverPolicyTheoryData))]
    public void ShouldRolloverFromQueueDepth_RequiresPositiveThreshold(long pendingWrites, int depthThreshold, bool expected)
    {
        bool actual = RolloverPolicy.ShouldRolloverFromQueueDepth(pendingWrites, depthThreshold);
        Assert.Equal(expected, actual);
    }
}
