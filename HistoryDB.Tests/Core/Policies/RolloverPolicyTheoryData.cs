namespace HistoryDB.Tests.Core.Policies;

public static class RolloverPolicyTheoryData
{
    public static TheoryData<ulong, ulong, ulong> UsedSlotsThresholdScalingCases => new TheoryData<ulong, ulong, ulong>
    {
        { 100UL, 50UL, 50UL },
        { 10UL, 33UL, 3UL },
    };

    public static TheoryData<int, int, int> QueueDepthThresholdCases => new TheoryData<int, int, int>
    {
        { 100, 10, 10 },
        { 100, 1, 1 },
        { 10, 5, 1 },
    };

    public static TheoryData<ulong, ulong, bool> ShouldRolloverFromUsedSlotsCases => new TheoryData<ulong, ulong, bool>
    {
        { 5UL, 10UL, false },
        { 10UL, 10UL, true },
        { 11UL, 10UL, true },
    };

    public static TheoryData<long, int, bool> ShouldRolloverFromQueueDepthCases => new TheoryData<long, int, bool>
    {
        { 4L, 5, false },
        { 5L, 5, true },
        { 0L, 0, false },
    };
}
