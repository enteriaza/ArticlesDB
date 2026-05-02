using System.Linq;

using HistoryDB;
using HistoryDB.Repair;
using HistoryDB.Tests.Infrastructure.Contracts;
using HistoryDB.Tests.Infrastructure.Fixtures;

namespace HistoryDB.Tests.Repair;

public sealed class HistorySharedScanTests : IClassFixture<TestHostFixture>
{
    private const int ShardCount = 1;
    private const ulong SlotsPerShard = 1024UL;
    private const ulong MaxLoadFactorPercent = 75UL;

    private readonly TestHostFixture _host;

    public HistorySharedScanTests(TestHostFixture host)
    {
        ArgumentNullException.ThrowIfNull(host);
        _host = host;
    }

    [Fact]
    public async Task ScanAsync_UseSharedRead_SucceedsWhileDatabaseOpen()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create("shared-scan-open-db");
        await SeedAsync(workspace.RootPath, messageCount: 8);

        HistoryDatabase db = new(
            rootPath: workspace.RootPath,
            shardCount: ShardCount,
            slotsPerShard: SlotsPerShard,
            maxLoadFactorPercent: MaxLoadFactorPercent,
            queueCapacityPerShard: 256,
            maxRetainedGenerations: 2,
            enableWalkJournal: false,
            maxWalkEntriesInMemory: 0,
            logger: null);

        try
        {
            HistoryRepairReport report = await HistoryDatabase.ScanAsync(
                workspace.RootPath,
                new HistoryScanOptions
                {
                    UseSharedRead = true,
                    ExpectedShardCount = ShardCount,
                    ExpectedSlotsPerShard = SlotsPerShard,
                    ExpectedMaxLoadFactorPercent = MaxLoadFactorPercent,
                });

            Assert.False(report.AbortedDueToLockContention);
            Assert.True(report.IsHealthy, string.Join("; ", report.Findings.Select(f => f.ToString())));
        }
        finally
        {
            await db.DisposeAsync();
        }
    }

    private static async Task SeedAsync(string rootPath, int messageCount)
    {
        HistoryDatabase db = new(
            rootPath: rootPath,
            shardCount: ShardCount,
            slotsPerShard: SlotsPerShard,
            maxLoadFactorPercent: MaxLoadFactorPercent,
            queueCapacityPerShard: 256,
            maxRetainedGenerations: 2,
            enableWalkJournal: true,
            maxWalkEntriesInMemory: messageCount * 2,
            logger: null);

        try
        {
            string serverId = Guid.NewGuid().ToString();
            for (int i = 0; i < messageCount; i++)
            {
                db.HistoryAdd($"<shared-scan-{i}@test>", serverId);
            }

            db.HistorySync();
        }
        finally
        {
            await db.DisposeAsync();
        }
    }
}
