// RolloverPolicy.cs -- pure policy helpers for proactive rollover thresholds and decisions.

namespace HistoryDB.Core.Policies;

internal static class RolloverPolicy
{
    internal static ulong ComputeUsedSlotsThreshold(ulong slotsPerShard, ulong proactiveRolloverLoadFactorPercent) =>
        proactiveRolloverLoadFactorPercent == 0 ? 0UL : checked((slotsPerShard * proactiveRolloverLoadFactorPercent) / 100);

    internal static int ComputeQueueDepthThreshold(int queueCapacityPerShard, int proactiveRolloverQueueWatermarkPercent) =>
        proactiveRolloverQueueWatermarkPercent == 0 ? 0 : Math.Max(1, (queueCapacityPerShard * proactiveRolloverQueueWatermarkPercent) / 100);

    internal static bool ShouldRolloverFromUsedSlots(ulong localUsedSlots, ulong threshold) =>
        threshold != 0 && localUsedSlots >= threshold;

    internal static bool ShouldRolloverFromQueueDepth(long pendingWrites, int depthThreshold) =>
        depthThreshold > 0 && pendingWrites >= depthThreshold;
}
