using System.Globalization;
using System.Linq;
using HisCTL.Contracts;
using HistoryDB;
using HistoryDB.Contracts;
using HistoryDB.Defrag;
using HistoryDB.Repair;
using HistoryDB.Utilities;

using Microsoft.Extensions.Logging;

namespace HisCTL.Application;

internal static class HisCtlRunner
{
    public static async Task<int> RunAsync(HisCtlOptions o, ILogger logger, CancellationToken cancellationToken)
    {
        return o.Verb switch
        {
            HisCtlVerb.Scan => await RunScanAsync(o, logger, cancellationToken).ConfigureAwait(false),
            HisCtlVerb.Repair => await RunRepairAsync(o, logger, cancellationToken).ConfigureAwait(false),
            HisCtlVerb.Defrag => await RunDefragAsync(o, logger, cancellationToken).ConfigureAwait(false),
            HisCtlVerb.Expire => await RunExpireAsync(o, logger, cancellationToken).ConfigureAwait(false),
            _ => 1,
        };
    }

    private static async Task<int> RunScanAsync(HisCtlOptions o, ILogger logger, CancellationToken cancellationToken)
    {
        HistoryScanOptions scan = new()
        {
            ExpectedShardCount = o.ShardCount,
            ExpectedSlotsPerShard = o.SlotsPerShard,
            ExpectedMaxLoadFactorPercent = o.MaxLoadFactorPercent,
            UseSharedRead = o.SharedScan,
        };

        logger.LogInformation("Scanning {Root} (sharedRead={Shared})", Path.GetFullPath(o.HistoryDirectory), o.SharedScan);
        HistoryRepairReport report =
            await HistoryDatabase.ScanAsync(Path.GetFullPath(o.HistoryDirectory), scan, logger, cancellationToken).ConfigureAwait(false);

        LogReportSummary(logger, report);
        return report.IsHealthy ? 0 : 2;
    }

    private static async Task<int> RunRepairAsync(HisCtlOptions o, ILogger logger, CancellationToken cancellationToken)
    {
        HistoryRepairOptions repair = new()
        {
            Policy = o.RepairPolicy,
            ExpectedShardCount = o.ShardCount,
            ExpectedSlotsPerShard = o.SlotsPerShard,
            ExpectedMaxLoadFactorPercent = o.MaxLoadFactorPercent,
            UseSharedRead = false,
        };

        logger.LogInformation("Repairing {Root}", Path.GetFullPath(o.HistoryDirectory));
        HistoryRepairReport report =
            await HistoryDatabase.RepairAsync(Path.GetFullPath(o.HistoryDirectory), repair, logger, cancellationToken).ConfigureAwait(false);

        LogReportSummary(logger, report);
        return report.IsHealthy ? 0 : 2;
    }

    private static async Task<int> RunDefragAsync(HisCtlOptions o, ILogger logger, CancellationToken cancellationToken)
    {
        string root = Path.GetFullPath(o.HistoryDirectory);
        logger.LogInformation("Defrag {Root}", root);
        await using HistoryDatabase db = new(
            rootPath: root,
            shardCount: o.ShardCount,
            slotsPerShard: o.SlotsPerShard,
            maxLoadFactorPercent: o.MaxLoadFactorPercent,
            queueCapacityPerShard: o.QueueCapacityPerShard,
            maxRetainedGenerations: o.MaxRetainedGenerations,
            enableWalkJournal: false,
            maxWalkEntriesInMemory: 0,
            logger: logger);

        HistoryDefragReport defrag = await db.HistoryDefragAsync(o.DefragOptions, cancellationToken).ConfigureAwait(false);
        logger.LogInformation(
            "Defrag complete: considered={Considered}, compacted={Compacted}, slotsScanned={Slots}, survivors={Surv}, expiredDropped={Drop}, tombstonesDropped={Tomb}, bytesReclaimed={Bytes}, expiredPruned={Prune}",
            defrag.GenerationsConsidered,
            defrag.GenerationsCompacted,
            defrag.SlotsScanned,
            defrag.SurvivorsCopied,
            defrag.ExpiredEntriesDropped,
            defrag.TombstonesDropped,
            defrag.BytesReclaimed,
            defrag.ExpiredEntriesPruned);

        return 0;
    }

    private static async Task<int> RunExpireAsync(HisCtlOptions o, ILogger logger, CancellationToken cancellationToken)
    {
        string root = Path.GetFullPath(o.HistoryDirectory);
        logger.LogInformation(
            "Expire {Mode} on {Root} (apply={Apply})",
            o.ExpireMode,
            root,
            o.ApplyExpire);

        await using HistoryDatabase db = new(
            rootPath: root,
            shardCount: o.ShardCount,
            slotsPerShard: o.SlotsPerShard,
            maxLoadFactorPercent: o.MaxLoadFactorPercent,
            queueCapacityPerShard: o.QueueCapacityPerShard,
            maxRetainedGenerations: o.MaxRetainedGenerations,
            enableWalkJournal: false,
            maxWalkEntriesInMemory: 0,
            logger: logger);

        List<Hash128> targets = [];
        DateTime anchor = DateTime.UtcNow;

        if (string.Equals(o.ExpireMode, "time", StringComparison.OrdinalIgnoreCase))
        {
            if (!MySqlIntervalExpression.TryParseCutoffUtc(o.OlderThanInterval!, anchor, out DateTime cutoffUtc, out string? err))
            {
                logger.LogError("Invalid --older-than: {Error}", err);
                return 1;
            }

            long cutoffTicks = cutoffUtc.Ticks;
            bool useAccessed = string.Equals(o.ExpireByField, "accessed", StringComparison.OrdinalIgnoreCase);
            await foreach (StoredArticleMetadata row in db.EnumerateStoredArticleMetadataAsync(skipIfExpired: true, cancellationToken)
                               .ConfigureAwait(false))
            {
                long fieldTicks = useAccessed ? row.DateAccessedTicks : row.DateObtainedTicks;
                if (fieldTicks < cutoffTicks)
                {
                    targets.Add(row.MessageHash);
                }
            }

            logger.LogInformation("Matched {Count} hashes older than cutoff {Cutoff:o}", targets.Count, cutoffUtc);
        }
        else
        {
            List<StoredArticleMetadata> rows = [];
            await foreach (StoredArticleMetadata row in db.EnumerateStoredArticleMetadataAsync(skipIfExpired: true, cancellationToken)
                               .ConfigureAwait(false))
            {
                rows.Add(row);
            }

            rows.Sort(static (a, b) => CompareRecyclePriority(a, b));
            int take = Math.Min(o.RecycleLimit, rows.Count);
            for (int i = 0; i < take; i++)
            {
                targets.Add(rows[i].MessageHash);
            }

            logger.LogInformation("Recycle selection: candidates={Total}, selected={Sel}", rows.Count, take);
        }

        if (targets.Count == 0)
        {
            logger.LogInformation("Nothing to expire.");
            return 0;
        }

        if (!o.ApplyExpire)
        {
            logger.LogWarning("Dry-run only (omit --apply). First few hashes:");
            for (int i = 0; i < Math.Min(8, targets.Count); i++)
            {
                Hash128 h = targets[i];
                logger.LogInformation("  hi={Hi:x16} lo={Lo:x16}", h.Hi, h.Lo);
            }

            return 0;
        }

        int expired = await db.HistoryExpireByHashesAsync(targets, o.ExpireParallelTombstones, cancellationToken).ConfigureAwait(false);
        db.HistorySync();
        logger.LogInformation("Expired {Count} new hashes (parallel={Parallel}).", expired, o.ExpireParallelTombstones);
        return 0;
    }

    private static int CompareRecyclePriority(StoredArticleMetadata a, StoredArticleMetadata b)
    {
        int c = a.DateAccessedTicks.CompareTo(b.DateAccessedTicks);
        if (c != 0)
        {
            return c;
        }

        c = a.AccessedCounter.CompareTo(b.AccessedCounter);
        if (c != 0)
        {
            return c;
        }

        c = a.DateObtainedTicks.CompareTo(b.DateObtainedTicks);
        if (c != 0)
        {
            return c;
        }

        c = a.MessageHash.Hi.CompareTo(b.MessageHash.Hi);
        return c != 0 ? c : a.MessageHash.Lo.CompareTo(b.MessageHash.Lo);
    }

    private static void LogReportSummary(ILogger logger, HistoryRepairReport report)
    {
        logger.LogInformation(
            "Scan/repair finished: healthy={Healthy}, abortedLock={Lock}, generations={Gens}, shards={Shards}, slots={Slots}, findings={Findings}, actions={Actions}",
            report.IsHealthy,
            report.AbortedDueToLockContention,
            report.GenerationsScanned,
            report.ShardsScanned,
            report.SlotsScanned,
            report.Findings.Count,
            report.Actions.Count);

        foreach (RepairFinding f in report.Findings.Take(20))
        {
            logger.LogWarning("{Severity} {Code} {Path}: {Detail}", f.Severity, f.Code, f.Path, f.Detail);
        }

        if (report.Findings.Count > 20)
        {
            logger.LogWarning("... {More} more findings not shown.", report.Findings.Count - 20);
        }
    }
}
