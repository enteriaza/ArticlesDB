using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using HistoryDB;
using HistoryDB.Tests.Infrastructure.Contracts;
using HistoryDB.Tests.Infrastructure.Fixtures;

namespace HistoryDB.Tests.Writer;

public sealed class TombstoneTests : IClassFixture<TestHostFixture>
{
    private const ulong SlotsPerShard = 1024UL;
    private const ulong Mask = SlotsPerShard - 1UL;
    private const ulong MaxLoadFactorPercent = 75UL;
    private const int VersionHeaderOffset = 8;

    private readonly TestHostFixture _host;

    public TombstoneTests(TestHostFixture host)
    {
        ArgumentNullException.ThrowIfNull(host);
        _host = host;
    }

    [Fact]
    public void TryTombstone_OverwritesMatchingSlotAndDecrementsExists()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create("tombstone-direct");
        string datPath = Path.Combine(workspace.RootPath, "shard.dat");

        ulong hashHi = 0xCAFEBABEDEADBEEFUL;
        ulong hashLo = 0x1234567890ABCDEFUL;

        using (HistoryShard shard = new(datPath, SlotsPerShard, MaxLoadFactorPercent))
        {
            ShardInsertResult result = shard.TryExistsOrInsert(hashHi, hashLo, 0xAAAA, 0xBBBB);
            Assert.Equal(ShardInsertResult.Inserted, result);
            Assert.True(shard.Exists(hashHi, hashLo));
            ulong usedBefore = shard.UsedSlots;
            Assert.Equal(1UL, usedBefore);

            ShardTombstoneResult tombstone = shard.TryTombstone(hashHi, hashLo);
            Assert.Equal(ShardTombstoneResult.Tombstoned, tombstone);

            Assert.False(shard.Exists(hashHi, hashLo));
        }

        // Re-open and verify the slot bytes are exactly the tombstone marker (1, 0, 0, 0).
        ReadRawSlot(datPath, slotIndex: hashLo & Mask, out ulong hi, out ulong lo, out ulong sHi, out ulong sLo);
        Assert.Equal(1UL, hi);
        Assert.Equal(0UL, lo);
        Assert.Equal(0UL, sHi);
        Assert.Equal(0UL, sLo);
    }

    [Fact]
    public void TryTombstone_NotFound_ReturnsNotFoundAndLeavesSlotUntouched()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create("tombstone-direct");
        string datPath = Path.Combine(workspace.RootPath, "shard.dat");

        using HistoryShard shard = new(datPath, SlotsPerShard, MaxLoadFactorPercent);

        ShardTombstoneResult result = shard.TryTombstone(0x1111UL, 0x2222UL);
        Assert.Equal(ShardTombstoneResult.NotFound, result);
        Assert.Equal(0UL, shard.UsedSlots);
    }

    [Fact]
    public void ProbeChainIntegrity_TombstoneInMiddle_DoesNotShortCircuitLookup()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create("tombstone-direct");
        string datPath = Path.Combine(workspace.RootPath, "shard.dat");

        using HistoryShard shard = new(datPath, SlotsPerShard, MaxLoadFactorPercent);

        // Three hashes that collide on primary slot 0x055 (hashLo & 0x3FF == 0x055).
        ulong primarySlot = 0x055UL;
        ulong aHi = 0xA000UL, aLo = primarySlot;
        ulong bHi = 0xB000UL, bLo = primarySlot;
        ulong cHi = 0xC000UL, cLo = primarySlot;

        Assert.Equal(ShardInsertResult.Inserted, shard.TryExistsOrInsert(aHi, aLo, 1, 1));
        Assert.Equal(ShardInsertResult.Inserted, shard.TryExistsOrInsert(bHi, bLo, 2, 2));
        Assert.Equal(ShardInsertResult.Inserted, shard.TryExistsOrInsert(cHi, cLo, 3, 3));

        // Tombstone the middle one (B).
        Assert.Equal(ShardTombstoneResult.Tombstoned, shard.TryTombstone(bHi, bLo));

        // Probe chain must continue past the tombstone for both A and C.
        Assert.True(shard.Exists(aHi, aLo));
        Assert.False(shard.Exists(bHi, bLo));
        Assert.True(shard.Exists(cHi, cLo));
    }

    [Fact]
    public void InsertReclaimsTombstoneSlot_AndDoesNotIncrementUsedSlots()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create("tombstone-direct");
        string datPath = Path.Combine(workspace.RootPath, "shard.dat");

        using HistoryShard shard = new(datPath, SlotsPerShard, MaxLoadFactorPercent);

        ulong primarySlot = 0x10AUL;
        ulong aHi = 0xA000UL, aLo = primarySlot;
        ulong bHi = 0xB000UL, bLo = primarySlot;

        Assert.Equal(ShardInsertResult.Inserted, shard.TryExistsOrInsert(aHi, aLo, 1, 1));
        Assert.Equal(ShardInsertResult.Inserted, shard.TryExistsOrInsert(bHi, bLo, 2, 2));
        Assert.Equal(2UL, shard.UsedSlots);

        // Tombstone B (lands at primarySlot+1 because of linear probing). TryTombstone does not touch the UsedSlots header
        // (the per-shard writer is responsible for FlushUsedSlots; in this direct test we observe only the slot transition).
        Assert.Equal(ShardTombstoneResult.Tombstoned, shard.TryTombstone(bHi, bLo));
        Assert.True(shard.IsTombstoneAt(primarySlot + 1UL));

        // Insert D with the same primary slot. Reclamation should put D into the tombstone at primarySlot+1, and -- crucially --
        // UsedSlots must not grow (it would have grown to 3 without reclamation).
        ulong dHi = 0xD000UL, dLo = primarySlot;
        Assert.Equal(ShardInsertResult.Inserted, shard.TryExistsOrInsert(dHi, dLo, 4, 4));

        Assert.Equal(2UL, shard.UsedSlots);
        Assert.True(shard.Exists(dHi, dLo));
        Assert.False(shard.IsTombstoneAt(primarySlot + 1UL));

        Assert.True(shard.TryReadOccupiedSlotAt(primarySlot + 1UL, out ulong reclaimedHi, out ulong reclaimedLo, out _, out _));
        Assert.Equal(dHi, reclaimedHi);
        Assert.Equal(dLo, reclaimedLo);
    }

    [Fact]
    public void InsertReclaimsFirstTombstoneAlongProbeChain()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create("tombstone-direct");
        string datPath = Path.Combine(workspace.RootPath, "shard.dat");

        using HistoryShard shard = new(datPath, SlotsPerShard, MaxLoadFactorPercent);

        ulong primarySlot = 0x200UL;
        ulong aHi = 0xA000UL, aLo = primarySlot;
        ulong bHi = 0xB000UL, bLo = primarySlot;
        ulong cHi = 0xC000UL, cLo = primarySlot;

        Assert.Equal(ShardInsertResult.Inserted, shard.TryExistsOrInsert(aHi, aLo, 0, 0));
        Assert.Equal(ShardInsertResult.Inserted, shard.TryExistsOrInsert(bHi, bLo, 0, 0));
        Assert.Equal(ShardInsertResult.Inserted, shard.TryExistsOrInsert(cHi, cLo, 0, 0));

        // Tombstone B (slot+1) and C (slot+2). Both slots are now tombstones in the linear chain.
        Assert.Equal(ShardTombstoneResult.Tombstoned, shard.TryTombstone(bHi, bLo));
        Assert.Equal(ShardTombstoneResult.Tombstoned, shard.TryTombstone(cHi, cLo));
        Assert.True(shard.IsTombstoneAt(primarySlot + 1UL));
        Assert.True(shard.IsTombstoneAt(primarySlot + 2UL));

        // Insert E sharing the probe chain. E must land at the FIRST tombstone (primarySlot + 1), not the second.
        ulong eHi = 0xE000UL, eLo = primarySlot;
        Assert.Equal(ShardInsertResult.Inserted, shard.TryExistsOrInsert(eHi, eLo, 0, 0));

        Assert.True(shard.TryReadOccupiedSlotAt(primarySlot + 1UL, out ulong reclaimedHi, out _, out _, out _));
        Assert.Equal(eHi, reclaimedHi);
        Assert.True(shard.IsTombstoneAt(primarySlot + 2UL)); // second tombstone untouched
    }

    [Fact]
    public void NormalizeReservedHashes_PreventsRealHashFromMatchingTombstoneMarker()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create("tombstone-direct");
        string datPath = Path.Combine(workspace.RootPath, "shard.dat");

        using HistoryShard shard = new(datPath, SlotsPerShard, MaxLoadFactorPercent);

        // Insert with the exact reserved tombstone bit pattern (1, 0). NormalizeReservedHashes nudges it to (1, 1).
        Assert.Equal(ShardInsertResult.Inserted, shard.TryExistsOrInsert(hashHi: 1UL, hashLo: 0UL, serverHi: 7, serverLo: 7));

        // Lookup with the same input value still finds it (Exists applies the same normalization).
        Assert.True(shard.Exists(hashHi: 1UL, hashLo: 0UL));

        // The slot at primary index 0 (the nudged hashLo=1, slot index = 1) should NOT read as a tombstone.
        Assert.False(shard.IsTombstoneAt(1UL));
    }

    [Fact]
    public void UnsupportedHeaderVersionRejectedOnOpen()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create("shard-bad-version");
        string datPath = Path.Combine(workspace.RootPath, "shard.dat");

        using (HistoryShard shard = new(datPath, SlotsPerShard, MaxLoadFactorPercent))
        {
            Assert.Equal(ShardInsertResult.Inserted, shard.TryExistsOrInsert(0xAAAA1111UL, 0x55UL, 1, 1));
        }

        Assert.Equal(HistoryShard.DiskFormatVersion1, ReadVersionHeader(datPath));
        WriteVersionHeader(datPath, value: 99UL);

        InvalidDataException ex = Assert.Throws<InvalidDataException>(() => _ = new HistoryShard(datPath, SlotsPerShard, MaxLoadFactorPercent));
        Assert.Contains("version", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task HistoryExpire_BroadcastsTombstoneToShardSlot()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create("tombstone-broadcast");
        string rootPath = workspace.RootPath;

        const string serverId = "11111111-1111-1111-1111-111111111111";
        const string messageId = "<broadcast-test@example>";

        HistoryDatabase db = new(
            rootPath: rootPath,
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
            Assert.True(db.HistoryAdd(messageId, serverId));
            Assert.True(db.HistoryLookup(messageId));

            Assert.True(db.HistoryExpire(messageId));

            // Wait for the fire-and-forget tombstone broadcast to land on the per-shard channel.
            byte[] hash = ComputeMd5Bytes(messageId);
            ulong hashHi = BinaryPrimitives.ReadUInt64LittleEndian(hash.AsSpan(0, 8));
            ulong hashLo = BinaryPrimitives.ReadUInt64LittleEndian(hash.AsSpan(8, 8));

            ulong slotIdx = hashLo & Mask;
            string datPath = Path.Combine(rootPath, "00001", "0000.dat");

            // Poll briefly for the tombstone to be observed in the shard slot.
            bool observed = await PollForAsync(() =>
            {
                ReadRawSlot(datPath, slotIdx, out ulong hi, out ulong lo, out _, out _);
                return hi == 1UL && lo == 0UL;
            }, timeoutMs: 5000);

            Assert.True(observed, "Tombstone marker (1, 0) was not observed in the shard slot within 5s after HistoryExpire.");
            Assert.False(db.HistoryLookup(messageId));
        }
        finally
        {
            await db.DisposeAsync();
        }
    }

    private static void ReadRawSlot(string datPath, ulong slotIndex, out ulong hashHi, out ulong hashLo, out ulong serverHi, out ulong serverLo)
    {
        using FileStream stream = File.Open(datPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        long offset = 64L + (long)slotIndex * HistoryShard.SlotStrideWithCrc;
        stream.Seek(offset, SeekOrigin.Begin);
        Span<byte> buffer = stackalloc byte[HistoryShard.SlotPayloadByteLength];
        int read = stream.Read(buffer);
        Assert.Equal(buffer.Length, read);
        hashHi = BinaryPrimitives.ReadUInt64LittleEndian(buffer[..8]);
        hashLo = BinaryPrimitives.ReadUInt64LittleEndian(buffer.Slice(8, 8));
        serverHi = BinaryPrimitives.ReadUInt64LittleEndian(buffer.Slice(16, 8));
        serverLo = BinaryPrimitives.ReadUInt64LittleEndian(buffer.Slice(24, 8));
    }

    private static ulong ReadVersionHeader(string datPath)
    {
        using FileStream stream = File.Open(datPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        Span<byte> buffer = stackalloc byte[8];
        stream.Seek(VersionHeaderOffset, SeekOrigin.Begin);
        Assert.Equal(buffer.Length, stream.Read(buffer));
        return BinaryPrimitives.ReadUInt64LittleEndian(buffer);
    }

    private static void WriteVersionHeader(string datPath, ulong value)
    {
        using FileStream stream = File.Open(datPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(buffer, value);
        stream.Seek(VersionHeaderOffset, SeekOrigin.Begin);
        stream.Write(buffer);
    }

    private static byte[] ComputeMd5Bytes(string text)
    {
        byte[] data = Encoding.UTF8.GetBytes(text);
        return System.Security.Cryptography.MD5.HashData(data);
    }

    private static async Task<bool> PollForAsync(Func<bool> predicate, int timeoutMs)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate())
            {
                return true;
            }

            await Task.Delay(20);
        }

        return predicate();
    }
}
