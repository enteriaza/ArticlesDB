using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using HistoryDB.Defrag;
using HistoryDB.Repair;
using HistoryDB.Tests.Infrastructure.Contracts;
using HistoryDB.Tests.Infrastructure.Fixtures;

namespace HistoryDB.Tests.Defrag;

public sealed class HistoryDefragEngineTests : IClassFixture<TestHostFixture>
{
    private const int ShardCount = 1;
    private const ulong SlotsPerShard = 1024UL;
    private const ulong MaxLoadFactorPercent = 75UL;
    private const int MaxRetainedGenerations = 4;

    private readonly TestHostFixture _host;

    public HistoryDefragEngineTests(TestHostFixture host)
    {
        ArgumentNullException.ThrowIfNull(host);
        _host = host;
    }

    [Fact]
    public async Task HistoryDefragAsync_ExpiredHalf_ShrinksUsedSlotsAndKeepsLiveLookups()
    {
        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        const string serverId = "11111111-1111-1111-1111-111111111111";

        HistoryDatabase db = NewDatabase(workspace.RootPath);
        try
        {
            const int total = 64;
            string[] ids = Enumerable.Range(0, total).Select(i => $"<msg-{i}@example>").ToArray();
            for (int i = 0; i < total; i++)
            {
                Assert.True(db.HistoryAdd(ids[i], serverId));
            }

            db.HistorySync();

            // Expire half the messages.
            int expired = 0;
            for (int i = 0; i < total; i += 2)
            {
                if (db.HistoryExpire(ids[i]))
                {
                    expired++;
                }
            }

            Assert.Equal(total / 2, expired);

            HistoryDefragReport report = await db.HistoryDefragAsync(new HistoryDefragOptions
            {
                ForceRolloverActiveGeneration = true,
                MinExpiredFractionToCompact = 0.0,
                MaxParallelGenerations = 1,
                PruneExpiredSetAfterCompaction = true,
            });

            Assert.True(report.GenerationsCompacted >= 1);
            // HistoryExpire broadcasts tombstones eagerly; by the time defrag scans the source generation the matching
            // slots may be tombstones rather than live-with-expired-hash. The compaction must drop them either way.
            Assert.True(report.SurvivorsCopied + report.ExpiredEntriesDropped + report.TombstonesDropped >= total);
            Assert.Equal(total - expired, report.SurvivorsCopied);
            Assert.Equal(expired, report.ExpiredEntriesDropped + report.TombstonesDropped);

            // Live messages still resolve.
            for (int i = 1; i < total; i += 2)
            {
                Assert.True(db.HistoryLookup(ids[i]));
            }

            // Expired messages do not resolve (the in-memory expired set may have been pruned because the slots
            // were physically removed; either way Lookup must return false).
            for (int i = 0; i < total; i += 2)
            {
                Assert.False(db.HistoryLookup(ids[i]));
            }

            // The renamed-source directory should be cleaned up by the background task; allow generous slack.
            await PollUntilAsync(() => !Directory.EnumerateDirectories(workspace.RootPath, "*compact-old-*").Any(), timeoutMs: 5000);
        }
        finally
        {
            await db.DisposeAsync();
        }
    }

    [Fact]
    public async Task HistoryDefragAsync_NoCandidates_ReturnsClean()
    {
        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        HistoryDatabase db = NewDatabase(workspace.RootPath);
        try
        {
            HistoryDefragReport report = await db.HistoryDefragAsync(new HistoryDefragOptions
            {
                ForceRolloverActiveGeneration = false,
                MinExpiredFractionToCompact = 0.0,
            });

            Assert.Equal(0, report.GenerationsCompacted);
        }
        finally
        {
            await db.DisposeAsync();
        }
    }

    [Fact]
    public async Task HistoryDefragAsync_MinFractionThreshold_SkipsCleanGenerations()
    {
        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        const string serverId = "11111111-1111-1111-1111-111111111111";
        HistoryDatabase db = NewDatabase(workspace.RootPath);
        try
        {
            for (int i = 0; i < 32; i++)
            {
                Assert.True(db.HistoryAdd($"<msg-{i}@clean>", serverId));
            }

            db.HistorySync();

            // Threshold is high; nothing has been expired so no candidate should be compacted.
            HistoryDefragReport report = await db.HistoryDefragAsync(new HistoryDefragOptions
            {
                ForceRolloverActiveGeneration = true,
                MinExpiredFractionToCompact = 0.5,
            });

            Assert.Equal(0, report.GenerationsCompacted);
            Assert.True(report.GenerationsConsidered >= 1);
            Assert.All(report.Generations, g => Assert.False(g.Compacted));
        }
        finally
        {
            await db.DisposeAsync();
        }
    }

    [Fact]
    public async Task HistoryDefragAsync_PrunesExpiredSetAfterCompaction()
    {
        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        const string serverId = "11111111-1111-1111-1111-111111111111";
        HistoryDatabase db = NewDatabase(workspace.RootPath);
        try
        {
            const int total = 32;
            string[] ids = Enumerable.Range(0, total).Select(i => $"<prune-{i}@example>").ToArray();
            for (int i = 0; i < total; i++)
            {
                Assert.True(db.HistoryAdd(ids[i], serverId));
            }

            db.HistorySync();

            for (int i = 0; i < total; i++)
            {
                Assert.True(db.HistoryExpire(ids[i]));
            }

            string expiredFilePath = Path.Combine(workspace.RootPath, "expired-md5.bin");
            db.HistorySync();
            Assert.True(File.Exists(expiredFilePath));

            HistoryDefragReport report = await db.HistoryDefragAsync(new HistoryDefragOptions
            {
                ForceRolloverActiveGeneration = true,
                MinExpiredFractionToCompact = 0.0,
                PruneExpiredSetAfterCompaction = true,
            });

            Assert.True(report.GenerationsCompacted >= 1);
            // HistoryExpire broadcasts tombstones; by the time defrag runs the matching slots may be tombstones
            // rather than live-with-expired-hash. Either way they must be dropped.
            Assert.Equal(total, report.ExpiredEntriesDropped + report.TombstonesDropped);
            Assert.Equal(total, report.ExpiredEntriesPruned);
        }
        finally
        {
            await db.DisposeAsync();
        }
    }

    [Fact]
    public async Task HistoryDefragAsync_ConcurrentAdds_AllSucceedAgainstNewActiveGeneration()
    {
        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        const string serverId = "11111111-1111-1111-1111-111111111111";
        HistoryDatabase db = NewDatabase(workspace.RootPath);
        try
        {
            const int seedCount = 64;
            for (int i = 0; i < seedCount; i++)
            {
                Assert.True(db.HistoryAdd($"<seed-{i}@example>", serverId));
            }

            db.HistorySync();

            for (int i = 0; i < seedCount; i += 2)
            {
                db.HistoryExpire($"<seed-{i}@example>");
            }

            // Launch the defrag in parallel with new inserts targeting the (rolled-over) active generation.
            using CancellationTokenSource cts = new();
            Task<HistoryDefragReport> defragTask = db.HistoryDefragAsync(new HistoryDefragOptions
            {
                ForceRolloverActiveGeneration = true,
                MinExpiredFractionToCompact = 0.0,
                MaxParallelGenerations = 1,
            }, cts.Token);

            const int concurrentAdds = 64;
            Task<bool>[] addTasks = new Task<bool>[concurrentAdds];
            for (int i = 0; i < concurrentAdds; i++)
            {
                int captured = i;
                addTasks[i] = Task.Run(() => db.HistoryAdd($"<live-{captured}@example>", serverId));
            }

            HistoryDefragReport report = await defragTask;
            bool[] addResults = await Task.WhenAll(addTasks);

            // All concurrent adds must have succeeded against some retained generation.
            Assert.All(addResults, ok => Assert.True(ok));

            for (int i = 0; i < concurrentAdds; i++)
            {
                Assert.True(db.HistoryLookup($"<live-{i}@example>"));
            }

            Assert.True(report.GenerationsCompacted >= 1);
        }
        finally
        {
            await db.DisposeAsync();
        }
    }

    [Fact]
    public async Task HistoryDefragAsync_ConcurrentLookups_NoExceptions()
    {
        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        const string serverId = "11111111-1111-1111-1111-111111111111";
        HistoryDatabase db = NewDatabase(workspace.RootPath);
        try
        {
            const int seedCount = 48;
            for (int i = 0; i < seedCount; i++)
            {
                Assert.True(db.HistoryAdd($"<lk-{i}@example>", serverId));
            }

            db.HistorySync();

            for (int i = 0; i < seedCount; i += 2)
            {
                db.HistoryExpire($"<lk-{i}@example>");
            }

            using CancellationTokenSource cts = new(TimeSpan.FromSeconds(30));
            Task<HistoryDefragReport> defragTask = db.HistoryDefragAsync(new HistoryDefragOptions
            {
                ForceRolloverActiveGeneration = true,
                MinExpiredFractionToCompact = 0.0,
                MaxParallelGenerations = 1,
            }, cts.Token);

            const int lookupWorkers = 8;
            const int lookupsPerWorker = 200;
            Task lookupTask = Task.Run(() =>
            {
                Parallel.For(0, lookupWorkers, workerIndex =>
                {
                    for (int j = 0; j < lookupsPerWorker; j++)
                    {
                        for (int i = 0; i < seedCount; i++)
                        {
                            db.HistoryLookup($"<lk-{i}@example>");
                        }
                    }
                });
            });

            HistoryDefragReport report = await defragTask;
            await lookupTask;

            Assert.True(report.GenerationsCompacted >= 1);
        }
        finally
        {
            await db.DisposeAsync();
        }
    }

    [Fact]
    public async Task HistoryDefragAsync_FollowedByScanAsync_ReportsHealthy()
    {
        using ITemporaryDirectoryWorkspace workspace = NewWorkspace();
        const string serverId = "11111111-1111-1111-1111-111111111111";
        HistoryDatabase db = NewDatabase(workspace.RootPath);
        try
        {
            const int total = 48;
            for (int i = 0; i < total; i++)
            {
                Assert.True(db.HistoryAdd($"<scan-after-defrag-{i}@example>", serverId));
            }

            db.HistorySync();

            for (int i = 0; i < total; i += 3)
            {
                db.HistoryExpire($"<scan-after-defrag-{i}@example>");
            }

            HistoryDefragReport defrag = await db.HistoryDefragAsync(new HistoryDefragOptions
            {
                ForceRolloverActiveGeneration = true,
                MinExpiredFractionToCompact = 0.0,
            });

            Assert.True(defrag.GenerationsCompacted >= 1);

            db.HistorySync();
            await db.DisposeAsync();

            // Workspace ownership has been released; ScanAsync can now exclusively open the artifacts.
            HistoryRepairReport report = await HistoryDatabase.ScanAsync(workspace.RootPath);
            Assert.True(report.IsHealthy, FormatRepairFindings(report));
            Assert.DoesNotContain(report.Findings, f => f.Severity == RepairSeverity.Error);
        }
        finally
        {
            // db is already disposed above; calling DisposeAsync again is a no-op.
        }
    }

    private ITemporaryDirectoryWorkspace NewWorkspace() =>
        _host.WorkspaceFactory.Create("defrag-engine");

    private static HistoryDatabase NewDatabase(string rootPath) =>
        new(
            rootPath: rootPath,
            shardCount: ShardCount,
            slotsPerShard: SlotsPerShard,
            maxLoadFactorPercent: MaxLoadFactorPercent,
            queueCapacityPerShard: 256,
            maxRetainedGenerations: MaxRetainedGenerations,
            enableWalkJournal: false,
            maxWalkEntriesInMemory: 0,
            logger: null);

    private static async Task PollUntilAsync(Func<bool> predicate, int timeoutMs)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate())
            {
                return;
            }

            await Task.Delay(25);
        }
    }

    private static string FormatRepairFindings(HistoryRepairReport report)
    {
        if (report.Findings.Count == 0)
        {
            return "report has zero findings";
        }

        return string.Join("\n", report.Findings.Select(f => $"[{f.Severity}] {f.Code}: {f.Detail} ({f.Path})"));
    }
}
