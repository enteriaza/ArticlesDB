using HistoryDB.Tests.Infrastructure.Contracts;
using HistoryDB.Tests.Infrastructure.Fixtures;

namespace HistoryDB.Tests.Application;

public sealed class HistoryDatabaseBatchTests : IClassFixture<TestHostFixture>
{
    private const string ServerA = "11111111-1111-1111-1111-111111111111";

    private readonly TestHostFixture _host;

    public HistoryDatabaseBatchTests(TestHostFixture host) => _host = host;

    [Fact]
    public async Task HistoryAddBatchAsync_Inserts_And_Lookup_Sees_All()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        using HistoryDatabase db = new(
            workspace.RootPath,
            shardCount: 4,
            slotsPerShard: 1UL << 16,
            enableWalkJournal: false);

        (string messageId, string serverId)[] items =
        [
            ("batch-a-0", ServerA),
            ("batch-a-1", ServerA),
            ("batch-a-2", ServerA),
            ("batch-a-3", ServerA),
        ];

        var outcomes = new ShardInsertResult[items.Length];
        await db.HistoryAddBatchAsync(items, outcomes);

        Assert.All(outcomes, r => Assert.Equal(ShardInsertResult.Inserted, r));

        foreach ((string messageId, _) in items)
        {
            Assert.True(db.HistoryLookup(messageId));
        }
    }

    [Fact]
    public async Task HistoryAddBatchAsync_Second_Batch_Returns_Duplicate()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        using HistoryDatabase db = new(
            workspace.RootPath,
            shardCount: 2,
            slotsPerShard: 1UL << 16,
            enableWalkJournal: false);

        (string messageId, string serverId)[] items = [("dup-batch-x", ServerA)];
        var first = new ShardInsertResult[1];
        var second = new ShardInsertResult[1];

        await db.HistoryAddBatchAsync(items, first);
        Assert.Equal(ShardInsertResult.Inserted, first[0]);

        await db.HistoryAddBatchAsync(items, second);
        Assert.Equal(ShardInsertResult.Duplicate, second[0]);
    }

    [Fact]
    public async Task HistoryAddBatchAsync_Rejects_Over_Max()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        using HistoryDatabase db = new(workspace.RootPath, enableWalkJournal: false);
        var items = new (string, string)[HistoryDatabase.MaxHistoryAddBatch + 1];
        for (int i = 0; i < items.Length; i++)
        {
            items[i] = ($"x{i}", ServerA);
        }

        var outcomes = new ShardInsertResult[items.Length];
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => db.HistoryAddBatchAsync(items, outcomes).AsTask());
    }
}
