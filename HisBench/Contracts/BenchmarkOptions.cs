namespace HisBench.Contracts;

internal sealed record BenchmarkOptions(
    long AddCount,
    int AddForks,
    string HistoryDirectory,
    long LookupCount,
    int LookupForks,
    bool RandomMessageIds,
    bool ShowHelp,
    int DurationSeconds,
    int TargetGenerations,
    double FillFactor,
    long DatasetSize,
    bool ZipfianSkew,
    bool MixedMode,
    int MixInsert,
    int MixLookup,
    int MixExpire,
    bool RestartTest,
    bool ColdRestart,
    bool BurstMode,
    BurstProfile BurstProfile,
    int BurstDutyPercent,
    int BurstSleepMs,
    int CorrelationSampleMs,
    long SaturationQueueDepthThreshold,
    double DegradedP99Microseconds,
    int ShardCount,
    ulong SlotsPerShard,
    ulong MaxLoadFactorPercent,
    int QueueCapacityPerShard,
    int MaxRetainedGenerations,
    ulong BloomCheckpointInsertInterval,
    int ScrubberSamplesPerTick,
    int ScrubberIntervalMs,
    int QueueDepthThreshold,
    long ProbeFailureThreshold,
    bool AutoTune);

internal enum BurstProfile
{
    Spike = 0,
    Ramp = 1,
    Wave = 2
}
