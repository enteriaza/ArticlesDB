using HistoryDB.Defrag;
using HistoryDB.Repair;

namespace HisCTL.Contracts;

internal sealed class HisCtlOptions
{
    public HisCtlVerb Verb { get; init; }

    public string HistoryDirectory { get; init; } = "./hisctl-data";

    public bool ShowHelp { get; init; }

    public bool SharedScan { get; init; }

    public int ShardCount { get; init; } = 128;

    public ulong SlotsPerShard { get; init; } = 1UL << 22;

    public ulong MaxLoadFactorPercent { get; init; } = 75;

    public int QueueCapacityPerShard { get; init; } = 1_000_000;

    public int MaxRetainedGenerations { get; init; } = 8;

    public RepairPolicy RepairPolicy { get; init; } = RepairPolicy.FailFast;

    public HistoryDefragOptions DefragOptions { get; init; } = new();

    /// <summary>Expire sub-mode: <c>time</c> or <c>recycle</c>.</summary>
    public string ExpireMode { get; init; } = "time";

    /// <summary>MySQL-style interval, e.g. <c>INTERVAL 1 DAY</c>.</summary>
    public string? OlderThanInterval { get; init; }

    /// <summary><c>obtained</c> (default) or <c>accessed</c>.</summary>
    public string ExpireByField { get; init; } = "obtained";

    public int RecycleLimit { get; init; }

    public bool ApplyExpire { get; init; }

    public int ExpireParallelTombstones { get; init; } = 64;
}
