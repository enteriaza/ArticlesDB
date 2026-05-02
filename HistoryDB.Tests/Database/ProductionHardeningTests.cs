using System.Security.Cryptography;
using System.Text;

using HistoryDB;
using HistoryDB.Application;
using HistoryDB.Contracts;
using HistoryDB.Tests.Infrastructure.Contracts;
using HistoryDB.Tests.Infrastructure.Fixtures;
using HistoryDB.Utilities;

namespace HistoryDB.Tests.Database;

public sealed class ProductionHardeningTests : IClassFixture<TestHostFixture>
{
    private readonly TestHostFixture _host;

    public ProductionHardeningTests(TestHostFixture host)
    {
        _host = host;
    }

    [Fact]
    public async Task HistoryExpire_PersistsFormattedExpiredSet()
    {
        using ITemporaryDirectoryWorkspace ws = _host.WorkspaceFactory.Create("expired-persist");
        string root = ws.RootPath;
        string expiredPath = Path.Combine(root, "expired-md5.bin");
        string msgId = "persist-expire-test@example.com";

        await using (HistoryDatabase db = new(root, enableWalkJournal: false, logger: null))
        {
            db.HistoryExpire(msgId);
        }

        Assert.True(File.Exists(expiredPath));
        HashSet<Hash128> loaded = [];
        Assert.True(ExpiredSetFile.TryLoadFormatted(expiredPath, loaded, out string? err));
        Assert.Null(err);
        Hash128 expected = HashUtf8MessageId(msgId);
        Assert.Contains(expected, loaded);
    }

    private static Hash128 HashUtf8MessageId(string messageId)
    {
        byte[] hash = MD5.HashData(Encoding.UTF8.GetBytes(messageId));
        return new Hash128(BitConverter.ToUInt64(hash, 0), BitConverter.ToUInt64(hash, 8));
    }

    [Fact]
    public async Task LoadJournal_ResumesFromCheckpointOffset()
    {
        using ITemporaryDirectoryWorkspace ws = _host.WorkspaceFactory.Create("journal-chk");
        string root = ws.RootPath;
        string journalPath = Path.Combine(root, "history-journal.log");
        string checkpointPath = Path.Combine(root, "history-journal.replay.chk");
        Guid s1 = Guid.NewGuid();
        Guid s2 = Guid.NewGuid();

        await using (HistoryDatabase db1 = new(
                         root,
                         enableWalkJournal: true,
                         maxWalkEntriesInMemory: 10_000,
                         logger: null))
        {
            Assert.True(db1.HistoryAdd("chk-a@x", s1.ToString()));
            Assert.True(db1.HistoryAdd("chk-b@x", s2.ToString()));
            db1.HistorySync();
        }

        byte[] journalBytes = await File.ReadAllBytesAsync(journalPath);
        int firstLf = Array.IndexOf(journalBytes, (byte)'\n');
        Assert.True(firstLf >= 0);
        long resumeOffset = firstLf + 1L;
        JournalReplayCheckpoint.Write(checkpointPath, resumeOffset);

        await using HistoryDatabase db2 = new(
            root,
            enableWalkJournal: true,
            maxWalkEntriesInMemory: 10_000,
            logger: null);
        long pos = 0;
        int n = db2.HistoryWalk(ref pos, out HistoryWalkEntry e);
        Assert.Equal(1, n);
        Assert.Equal("chk-b@x", e.MessageId);
    }

    [Fact]
    public async Task HistoryDatabaseHostedService_StopAsync_DisposesDatabase()
    {
        using ITemporaryDirectoryWorkspace ws = _host.WorkspaceFactory.Create("hosted-stop");
        HistoryDatabase db = new(ws.RootPath, enableWalkJournal: false, logger: null);
        HistoryDatabaseHostedService hosted = new(db);
        await hosted.StopAsync(default);
    }
}
