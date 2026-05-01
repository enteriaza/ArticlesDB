// HistoryWriterOptions.cs -- strongly typed configuration model for HistoryDB writer construction.

namespace HistoryDB.Contracts;

/// <summary>
/// Top-level options for configuring <see cref="IHistoryWriter"/>.
/// </summary>
internal sealed record HistoryWriterOptions
{
    public required string RootPath { get; init; }

    public int ShardCount { get; init; } = 128;

    public ulong SlotsPerShard { get; init; } = 1UL << 22;

    public ulong MaxLoadFactorPercent { get; init; } = 75;

    public int QueueCapacityPerShard { get; init; } = 1_000_000;

    public int MaxRetainedGenerations { get; init; } = 8;

    public BloomOptions Bloom { get; init; } = new();

    public RolloverOptions Rollover { get; init; } = new();

    public SlotScrubberOptions SlotScrubber { get; init; } = new();

    public AffinityOptions Affinity { get; init; } = new();
}

internal sealed record BloomOptions
{
    public double MemoryFraction { get; init; } = 0.80;

    public double TargetBitsPerEntry { get; init; } = 10.0;

    public ulong MemoryBudgetBytes { get; init; }

    public ulong CheckpointInsertInterval { get; init; } = 1_000_000;

    public ulong CheckpointMaxSnapshotBytes { get; init; } = 32UL * 1024 * 1024;
}

internal sealed record RolloverOptions
{
    public ulong ProactiveLoadFactorPercent { get; init; } = 72;

    public int ProactiveQueueWatermarkPercent { get; init; }

    public long ProactiveAggregateProbeFailuresThreshold { get; init; }
}

internal sealed record SlotScrubberOptions
{
    public bool Enabled { get; init; }

    public int SamplesPerTick { get; init; } = 256;

    public int IntervalMilliseconds { get; init; } = 30_000;

    public bool InvalidateBloomOnOccupiedMismatch { get; init; } = true;
}

internal sealed record AffinityOptions
{
    public bool PinShardWriterThreads { get; init; }

    public bool PreferNumaNodeOrderingForShardPinsOnWindows { get; init; } = true;

    public int ShardWriterLogicalProcessorOffset { get; init; }

    public bool PinBloomPersistThread { get; init; }

    public int BloomPersistLogicalProcessor { get; init; } = -1;

    public bool PinBloomWarmupThread { get; init; }

    public int BloomWarmupLogicalProcessor { get; init; } = -1;

    public bool PinSlotScrubberThread { get; init; }

    public int SlotScrubberLogicalProcessor { get; init; } = -1;
}
