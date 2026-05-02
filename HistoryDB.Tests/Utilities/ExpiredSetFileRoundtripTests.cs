using HistoryDB.Contracts;
using HistoryDB.Tests.Infrastructure.Contracts;
using HistoryDB.Tests.Infrastructure.Fixtures;
using HistoryDB.Utilities;

namespace HistoryDB.Tests.Utilities;

public sealed class ExpiredSetFileRoundtripTests : IClassFixture<TestHostFixture>
{
    private readonly TestHostFixture _host;

    public ExpiredSetFileRoundtripTests(TestHostFixture host)
    {
        ArgumentNullException.ThrowIfNull(host);
        _host = host;
    }

    [Fact]
    public void WriteAtomic_ThenTryLoadFormatted_Roundtrips()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create("expired-set-roundtrip");
        string path = Path.Combine(workspace.RootPath, "expired-md5.bin");
        Hash128 a = new(0x1111UL, 0x2222UL);
        Hash128 b = new(0x3333UL, 0x4444UL);
        List<Hash128> list = [a, b];

        ExpiredSetFile.WriteAtomic(path, list);

        HashSet<Hash128> loaded = [];
        Assert.True(ExpiredSetFile.TryLoadFormatted(path, loaded, out string? err));
        Assert.Null(err);
        Assert.Equal(2, loaded.Count);
        Assert.Contains(a, loaded);
        Assert.Contains(b, loaded);
    }
}
