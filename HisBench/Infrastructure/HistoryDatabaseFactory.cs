using HisBench.Contracts;
using HistoryDB;

namespace HisBench.Infrastructure;

internal sealed class HistoryDatabaseFactory : IHistoryDatabaseFactory
{
    public HistoryDatabase Create(BenchmarkOptions options) =>
        new(
            rootPath: options.HistoryDirectory,
            shardCount: options.ShardCount,
            slotsPerShard: options.SlotsPerShard,
            maxLoadFactorPercent: options.MaxLoadFactorPercent,
            queueCapacityPerShard: options.QueueCapacityPerShard,
            maxRetainedGenerations: options.MaxRetainedGenerations);
}
