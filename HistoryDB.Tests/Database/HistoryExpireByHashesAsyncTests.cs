using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

using HistoryDB;
using HistoryDB.Contracts;
using HistoryDB.Tests.Infrastructure.Contracts;
using HistoryDB.Tests.Infrastructure.Fixtures;

namespace HistoryDB.Tests.Database;

public sealed class HistoryExpireByHashesAsyncTests : IClassFixture<TestHostFixture>
{
    private const ulong SlotsPerShard = 1024UL;
    private const ulong MaxLoadFactorPercent = 75UL;

    private readonly TestHostFixture _host;

    public HistoryExpireByHashesAsyncTests(TestHostFixture host)
    {
        ArgumentNullException.ThrowIfNull(host);
        _host = host;
    }

    [Fact]
    public async Task HistoryExpireByHashesAsync_IsIdempotent_AndPersistsExpiredFile()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create("expire-by-hashes");
        string root = workspace.RootPath;
        const string serverId = "11111111-1111-1111-1111-111111111111";

        HistoryDatabase db = new(
            rootPath: root,
            shardCount: 1,
            slotsPerShard: SlotsPerShard,
            maxLoadFactorPercent: MaxLoadFactorPercent,
            queueCapacityPerShard: 256,
            maxRetainedGenerations: 2,
            enableWalkJournal: false,
            maxWalkEntriesInMemory: 0,
            logger: null);

        try
        {
            Assert.True(db.HistoryAdd("<one@x>", serverId));
            Assert.True(db.HistoryAdd("<two@x>", serverId));

            Hash128 one = Md5Hash("<one@x>");
            Hash128 two = Md5Hash("<two@x>");

            int first = await db.HistoryExpireByHashesAsync([one, one], parallelTombstones: 2);
            Assert.Equal(1, first);
            Assert.True(db.IsMessageHashExpired(one));
            Assert.False(db.IsMessageHashExpired(two));

            string expiredPath = Path.Combine(root, "expired-md5.bin");
            Assert.True(File.Exists(expiredPath));
            long sizeAfterFirst = new FileInfo(expiredPath).Length;
            Assert.True(sizeAfterFirst > 0);

            int second = await db.HistoryExpireByHashesAsync([one], parallelTombstones: 2);
            Assert.Equal(0, second);
            Assert.Equal(sizeAfterFirst, new FileInfo(expiredPath).Length);
        }
        finally
        {
            await db.DisposeAsync();
        }
    }

    private static Hash128 Md5Hash(string text)
    {
        byte[] d = MD5.HashData(Encoding.UTF8.GetBytes(text));
        ulong hi = BinaryPrimitives.ReadUInt64LittleEndian(d.AsSpan(0, 8));
        ulong lo = BinaryPrimitives.ReadUInt64LittleEndian(d.AsSpan(8, 8));
        return new Hash128(hi, lo);
    }
}
