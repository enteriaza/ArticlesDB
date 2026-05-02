using HistoryDB.Tests.Infrastructure.Contracts;
using HistoryDB.Tests.Infrastructure.Fixtures;

namespace HistoryDB.Tests.Application;

public sealed class WriterCoalesceAdaptiveTests : IClassFixture<TestHostFixture>
{
    private const string ServerA = "11111111-1111-1111-1111-111111111111";

    private readonly TestHostFixture _host;

    public WriterCoalesceAdaptiveTests(TestHostFixture host) => _host = host;

    [Fact]
    public void Sync_inserts_with_default_adaptive_coalesce_then_lookups()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        using HistoryDatabase db = new(
            workspace.RootPath,
            shardCount: 8,
            slotsPerShard: 1UL << 18,
            enableWalkJournal: false);

        db.SetWriterCoalesceBatchSize(64);
        const int count = 256;
        for (int i = 0; i < count; i++)
        {
            Assert.True(db.HistoryAdd($"coalesce-adapt-{i}", ServerA));
        }

        db.HistorySync();
        for (int i = 0; i < count; i++)
        {
            Assert.True(db.HistoryLookup($"coalesce-adapt-{i}"));
        }
    }

    [Fact]
    public void Sync_inserts_with_fixed_coalesce_then_lookups()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        using HistoryDatabase db = new(
            workspace.RootPath,
            shardCount: 8,
            slotsPerShard: 1UL << 18,
            enableWalkJournal: false);

        db.SetWriterCoalesceBatchSize(64);
        db.SetWriterCoalesceAdaptive(false);
        const int count = 256;
        for (int i = 0; i < count; i++)
        {
            Assert.True(db.HistoryAdd($"coalesce-fixed-{i}", ServerA));
        }

        db.HistorySync();
        for (int i = 0; i < count; i++)
        {
            Assert.True(db.HistoryLookup($"coalesce-fixed-{i}"));
        }
    }
}
