using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using HistoryDB.Repair;
using HistoryDB.Tests.Infrastructure.Contracts;
using HistoryDB.Tests.Infrastructure.Fixtures;

namespace HistoryDB.Tests.Repair;

public sealed class HistoryRepairEngineTests : IClassFixture<TestHostFixture>
{
    private const int ShardCount = 1;
    private const ulong SlotsPerShard = 1024UL;
    private const ulong MaxLoadFactorPercent = 75UL;
    private const int UsedSlotsHeaderOffset = 24;
    private const int MaxOccupiedSlotIndexHeaderOffset = 40;

    private readonly TestHostFixture _host;

    public HistoryRepairEngineTests(TestHostFixture host)
    {
        ArgumentNullException.ThrowIfNull(host);
        _host = host;
    }

    [Fact]
    public async Task ScanAsync_HealthyDirectory_ProducesNoErrors()
    {
        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        await SeedDatabaseAsync(workspace.RootPath, messageCount: 32);

        HistoryRepairReport report = await HistoryDatabase.ScanAsync(workspace.RootPath);

        Assert.True(report.IsHealthy);
        Assert.DoesNotContain(report.Findings, f => f.Severity == RepairSeverity.Error);
        Assert.Empty(report.Actions);
        Assert.True(report.ScanOnly);
    }

    [Fact]
    public async Task ScanAsync_DoesNotMutateFilesystem()
    {
        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        await SeedDatabaseAsync(workspace.RootPath, messageCount: 16);

        // Inject a corruption that ScanAsync will surface but must not repair.
        WriteShardHeaderUsedSlots(workspace.RootPath, generation: 1, shard: 0, value: 9999);

        string before = HashDirectory(workspace.RootPath);
        HistoryRepairReport report = await HistoryDatabase.ScanAsync(workspace.RootPath);
        string after = HashDirectory(workspace.RootPath);

        Assert.Equal(before, after);
        Assert.DoesNotContain(report.Actions, a => a.Applied);
        Assert.Contains(report.Findings, f => f.Code == RepairFindingCode.UsedSlotsHeaderStale);
    }

    [Fact]
    public async Task RepairAsync_StaleUsedSlotsHeader_RepairedUnderFailFast()
    {
        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        await SeedDatabaseAsync(workspace.RootPath, messageCount: 24);

        WriteShardHeaderUsedSlots(workspace.RootPath, generation: 1, shard: 0, value: 9999);

        HistoryRepairReport repair = await HistoryDatabase.RepairAsync(
            workspace.RootPath,
            new HistoryRepairOptions { Policy = RepairPolicy.FailFast });

        Assert.True(repair.IsHealthy);
        Assert.Contains(repair.Findings, f => f.Code == RepairFindingCode.UsedSlotsHeaderStale);
        Assert.Contains(repair.Actions, a => a.Code == RepairActionCode.UsedSlotsHeaderRewritten && a.Applied);

        HistoryRepairReport rescan = await HistoryDatabase.ScanAsync(workspace.RootPath);
        Assert.DoesNotContain(rescan.Findings, f => f.Code == RepairFindingCode.UsedSlotsHeaderStale);
    }

    [Fact]
    public async Task RepairAsync_StaleMaxOccupiedSlotIndex_RepairedUnderFailFast()
    {
        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        await SeedDatabaseAsync(workspace.RootPath, messageCount: 24);

        WriteShardHeaderMaxOccupiedSlotIndex(workspace.RootPath, generation: 1, shard: 0, value: 0);

        HistoryRepairReport repair = await HistoryDatabase.RepairAsync(
            workspace.RootPath,
            new HistoryRepairOptions { Policy = RepairPolicy.FailFast });

        Assert.True(repair.IsHealthy);
        Assert.Contains(repair.Findings, f => f.Code == RepairFindingCode.MaxOccupiedSlotIndexHeaderStale);
        Assert.Contains(repair.Actions, a => a.Code == RepairActionCode.MaxOccupiedSlotIndexHeaderRewritten && a.Applied);

        HistoryRepairReport rescan = await HistoryDatabase.ScanAsync(workspace.RootPath);
        Assert.DoesNotContain(rescan.Findings, f => f.Code == RepairFindingCode.MaxOccupiedSlotIndexHeaderStale);
    }

    [Fact]
    public async Task RepairAsync_TruncatedSidecar_Rebuilt()
    {
        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        await SeedDatabaseAsync(workspace.RootPath, messageCount: 24);

        string idxPath = ShardSidecarPath(workspace.RootPath, generation: 1, shard: 0);
        Assert.True(File.Exists(idxPath));

        File.WriteAllBytes(idxPath, new byte[3]);

        HistoryRepairReport repair = await HistoryDatabase.RepairAsync(
            workspace.RootPath,
            new HistoryRepairOptions
            {
                Policy = RepairPolicy.FailFast,
                OverrideBloomBitsPerShard = 1024,
            });

        Assert.True(repair.IsHealthy);
        Assert.Contains(repair.Actions, a => a.Code == RepairActionCode.BloomSidecarRewritten && a.Applied);
        Assert.True(new FileInfo(idxPath).Length > 20, "Sidecar must be longer than its header after rebuild.");
    }

    [Fact]
    public async Task RepairAsync_DivergentSidecarBits_Rebuilt()
    {
        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        await SeedDatabaseAsync(workspace.RootPath, messageCount: 24);

        string idxPath = ShardSidecarPath(workspace.RootPath, generation: 1, shard: 0);
        FlipFirstSidecarWord(idxPath);

        HistoryRepairReport repair = await HistoryDatabase.RepairAsync(
            workspace.RootPath,
            new HistoryRepairOptions { Policy = RepairPolicy.FailFast });

        Assert.True(repair.IsHealthy);
        Assert.Contains(repair.Findings, f => f.Code == RepairFindingCode.BloomSidecarBitsDivergent);
        Assert.Contains(repair.Actions, a => a.Code == RepairActionCode.BloomSidecarRewritten && a.Applied);

        HistoryRepairReport rescan = await HistoryDatabase.ScanAsync(workspace.RootPath);
        Assert.DoesNotContain(rescan.Findings, f => f.Code == RepairFindingCode.BloomSidecarBitsDivergent);
    }

    [Fact]
    public async Task RepairAsync_OrphanIdx_Quarantined_UnderQuarantinePolicy()
    {
        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        await SeedDatabaseAsync(workspace.RootPath, messageCount: 4);

        string genDir = GenerationDir(workspace.RootPath, generation: 1);
        string orphanIdx = Path.Combine(genDir, "00ff.idx");
        File.WriteAllBytes(orphanIdx, new byte[] { 0x4C, 0x4F, 0x4F, 0x4D, 1, 0, 0, 0 });

        HistoryRepairReport scanReport = await HistoryDatabase.ScanAsync(workspace.RootPath);
        Assert.Contains(scanReport.Findings, f => f.Code == RepairFindingCode.OrphanBloomSidecar);
        Assert.True(File.Exists(orphanIdx), "ScanAsync must not move files.");

        HistoryRepairReport repair = await HistoryDatabase.RepairAsync(
            workspace.RootPath,
            new HistoryRepairOptions { Policy = RepairPolicy.QuarantineAndRecreate });

        Assert.False(File.Exists(orphanIdx));
        Assert.Contains(repair.Actions, a => a.Code == RepairActionCode.QuarantinedFile && a.Applied);
        string[] quarantined = Directory.GetFiles(genDir, "00ff.idx.corrupt-*");
        Assert.Single(quarantined);
    }

    [Fact]
    public async Task RepairAsync_ExpiredSetTrailingPartial_Truncated()
    {
        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        await SeedDatabaseAsync(workspace.RootPath, messageCount: 4);

        string expiredPath = Path.Combine(workspace.RootPath, "expired-md5.bin");
        byte[] payload = new byte[16 + 7];
        new Random(1).NextBytes(payload);
        File.WriteAllBytes(expiredPath, payload);

        HistoryRepairReport repair = await HistoryDatabase.RepairAsync(
            workspace.RootPath,
            new HistoryRepairOptions { Policy = RepairPolicy.FailFast });

        Assert.Contains(repair.Findings, f => f.Code == RepairFindingCode.ExpiredSetTrailingPartial);
        Assert.Contains(repair.Actions, a => a.Code == RepairActionCode.ExpiredSetTruncated && a.Applied);
        Assert.Equal(16, new FileInfo(expiredPath).Length);
    }

    [Fact]
    public async Task RepairAsync_JournalInvalidLine_KeptByDefaultRewrittenWhenEnabled()
    {
        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        await SeedDatabaseAsync(workspace.RootPath, messageCount: 4);

        string journalPath = Path.Combine(workspace.RootPath, "history-journal.log");
        Assert.True(File.Exists(journalPath));

        string original = File.ReadAllText(journalPath);
        File.WriteAllText(journalPath, original + "this-is-not-a-valid-journal-line\n", Encoding.UTF8);

        HistoryRepairReport keep = await HistoryDatabase.RepairAsync(
            workspace.RootPath,
            new HistoryRepairOptions { Policy = RepairPolicy.FailFast });

        Assert.Contains(keep.Findings, f => f.Code == RepairFindingCode.JournalInvalidLine);
        Assert.DoesNotContain(keep.Actions, a => a.Code == RepairActionCode.JournalRewritten && a.Applied);
        Assert.Equal(original + "this-is-not-a-valid-journal-line\n", File.ReadAllText(journalPath));

        HistoryRepairReport rewrite = await HistoryDatabase.RepairAsync(
            workspace.RootPath,
            new HistoryRepairOptions
            {
                Policy = RepairPolicy.FailFast,
                RewriteJournalDroppingInvalidLines = true,
            });

        Assert.Contains(rewrite.Actions, a => a.Code == RepairActionCode.JournalRewritten && a.Applied);
        Assert.DoesNotContain("this-is-not-a-valid-journal-line", File.ReadAllText(journalPath));
    }

    [Fact]
    public async Task RepairAsync_DatMagicZeroed_FailFastLeavesFileUntouched()
    {
        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        await SeedDatabaseAsync(workspace.RootPath, messageCount: 4);

        string datPath = ShardDataPath(workspace.RootPath, generation: 1, shard: 0);
        ZeroBytes(datPath, offset: 0, length: 8);

        long lengthBefore = new FileInfo(datPath).Length;
        HistoryRepairReport repair = await HistoryDatabase.RepairAsync(
            workspace.RootPath,
            new HistoryRepairOptions { Policy = RepairPolicy.FailFast });

        Assert.Contains(repair.Findings, f => f.Severity == RepairSeverity.Error && f.Code == RepairFindingCode.HeaderMagicMismatch);
        Assert.False(repair.IsHealthy);
        Assert.True(File.Exists(datPath));
        Assert.Equal(lengthBefore, new FileInfo(datPath).Length);
        Assert.DoesNotContain(repair.Actions, a => a.Code == RepairActionCode.QuarantinedFile && a.Applied);
    }

    [Fact]
    public async Task RepairAsync_DatMagicZeroed_QuarantineRenamesArtifacts()
    {
        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        await SeedDatabaseAsync(workspace.RootPath, messageCount: 4);

        string datPath = ShardDataPath(workspace.RootPath, generation: 1, shard: 0);
        string idxPath = ShardSidecarPath(workspace.RootPath, generation: 1, shard: 0);
        ZeroBytes(datPath, offset: 0, length: 8);

        HistoryRepairReport repair = await HistoryDatabase.RepairAsync(
            workspace.RootPath,
            new HistoryRepairOptions { Policy = RepairPolicy.QuarantineAndRecreate });

        Assert.False(File.Exists(datPath));
        Assert.False(File.Exists(idxPath));
        Assert.Single(Directory.GetFiles(GenerationDir(workspace.RootPath, generation: 1), "0000.dat.corrupt-*"));
        Assert.Single(Directory.GetFiles(GenerationDir(workspace.RootPath, generation: 1), "0000.idx.corrupt-*"));
        Assert.Equal(2, repair.Actions.Count(a => a.Code == RepairActionCode.QuarantinedFile && a.Applied));
    }

    [Fact]
    public async Task RepairAsync_LockedShardFile_AbortsWithLockedFinding()
    {
        if (!OperatingSystem.IsWindows())
        {
            // Linux file locks via FileShare.None do not block other openers in the same process the way Windows does.
            return;
        }

        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        await SeedDatabaseAsync(workspace.RootPath, messageCount: 4);

        string datPath = ShardDataPath(workspace.RootPath, generation: 1, shard: 0);
        string before = HashDirectory(workspace.RootPath);

        using (FileStream _ = File.Open(datPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        {
            HistoryRepairReport repair = await HistoryDatabase.RepairAsync(
                workspace.RootPath,
                new HistoryRepairOptions { Policy = RepairPolicy.FailFast });

            Assert.True(repair.AbortedDueToLockContention);
            Assert.Contains(repair.Findings, f => f.Code == RepairFindingCode.LockedByOtherProcess);
            Assert.DoesNotContain(repair.Actions, a => a.Applied);
        }

        Assert.Equal(before, HashDirectory(workspace.RootPath));
    }

    private ITemporaryDirectoryWorkspace NewWorkspace() =>
        _host.WorkspaceFactory.Create("repair-engine");

    private static async Task SeedDatabaseAsync(string rootPath, int messageCount)
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
                db.HistoryAdd($"<msg-{i}@test>", serverId);
            }

            db.HistorySync();
        }
        finally
        {
            await db.DisposeAsync();
        }
    }

    private static string HashDirectory(string root)
    {
        StringBuilder sb = new();
        foreach (string path in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories).OrderBy(static p => p, StringComparer.Ordinal))
        {
            byte[] hash = SHA256.HashData(File.ReadAllBytes(path));
            sb.Append(path);
            sb.Append(':');
            sb.Append(Convert.ToHexString(hash));
            sb.Append('\n');
        }

        return sb.ToString();
    }

    private static string GenerationDir(string root, int generation) =>
        Path.Combine(root, generation.ToString("x5", System.Globalization.CultureInfo.InvariantCulture));

    private static string ShardDataPath(string root, int generation, int shard) =>
        Path.Combine(GenerationDir(root, generation), shard.ToString("x4", System.Globalization.CultureInfo.InvariantCulture) + ".dat");

    private static string ShardSidecarPath(string root, int generation, int shard) =>
        Path.Combine(GenerationDir(root, generation), shard.ToString("x4", System.Globalization.CultureInfo.InvariantCulture) + ".idx");

    private static void WriteShardHeaderUsedSlots(string root, int generation, int shard, ulong value)
    {
        string datPath = ShardDataPath(root, generation, shard);
        using FileStream stream = File.Open(datPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(buffer, value);
        stream.Seek(UsedSlotsHeaderOffset, SeekOrigin.Begin);
        stream.Write(buffer);
    }

    private static void WriteShardHeaderMaxOccupiedSlotIndex(string root, int generation, int shard, ulong value)
    {
        string datPath = ShardDataPath(root, generation, shard);
        using FileStream stream = File.Open(datPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(buffer, value);
        stream.Seek(MaxOccupiedSlotIndexHeaderOffset, SeekOrigin.Begin);
        stream.Write(buffer);
    }

    private static void ZeroBytes(string path, long offset, int length)
    {
        using FileStream stream = File.Open(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        stream.Seek(offset, SeekOrigin.Begin);
        stream.Write(new byte[length], 0, length);
    }

    private static void FlipFirstSidecarWord(string idxPath)
    {
        using FileStream stream = File.Open(idxPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        const int HeaderInts = 5;
        long wordOffset = HeaderInts * sizeof(int);
        Assert.True(stream.Length >= wordOffset + sizeof(ulong));
        stream.Seek(wordOffset, SeekOrigin.Begin);
        Span<byte> buffer = stackalloc byte[sizeof(ulong)];
        int read = stream.Read(buffer);
        Assert.Equal(buffer.Length, read);
        ulong word = BinaryPrimitives.ReadUInt64LittleEndian(buffer);
        BinaryPrimitives.WriteUInt64LittleEndian(buffer, word ^ 0xFFFFFFFFFFFFFFFFUL);
        stream.Seek(wordOffset, SeekOrigin.Begin);
        stream.Write(buffer);
    }

}
