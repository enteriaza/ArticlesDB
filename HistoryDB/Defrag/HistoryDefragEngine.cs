// HistoryDefragEngine.cs -- orchestrates the online compaction flow for HistoryDatabase.HistoryDefragAsync.
// Steps: (1) optionally roll over the active generation so the previously-active generation becomes a candidate;
//        (2) pre-scan candidates to compute reclaimable fraction, skipping any below MinExpiredFractionToCompact;
//        (3) compact remaining candidates with bounded parallelism via ShardedHistoryWriter.CompactGenerationAsync;
//        (4) optionally prune hashes from the in-memory expired set whose only retained occurrences were just compacted out.

using HistoryDB.Contracts;

using Microsoft.Extensions.Logging;

namespace HistoryDB.Defrag;

internal static partial class HistoryDefragEngine
{
    public static async Task<HistoryDefragReport> RunAsync(
        HistoryDatabase database,
        ShardedHistoryWriter writer,
        IReadOnlySet<Hash128> expiredSnapshot,
        HistoryDefragOptions options,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(expiredSnapshot);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        if (options.MinExpiredFractionToCompact < 0.0 || options.MinExpiredFractionToCompact > 1.0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "MinExpiredFractionToCompact must be in [0.0, 1.0].");
        }

        if (options.MaxParallelGenerations < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "MaxParallelGenerations must be >= 1.");
        }

        HistoryDefragReport report = new();
        LogDefragStarted(logger, expiredSnapshot.Count, options.ForceRolloverActiveGeneration, options.MinExpiredFractionToCompact, options.MaxParallelGenerations);

        if (options.ForceRolloverActiveGeneration)
        {
            try
            {
                writer.ForceGenerationRollover();
            }
            catch (Exception ex)
            {
                LogDefragRolloverFailed(logger, ex);
            }
        }

        int activeId = writer.ActiveGenerationId;
        int[] retainedIds = writer.GetRetainedGenerationIdsNewestFirst();
        List<int> candidates = new(retainedIds.Length);
        for (int i = 0; i < retainedIds.Length; i++)
        {
            if (retainedIds[i] != activeId)
            {
                candidates.Add(retainedIds[i]);
            }
        }

        report.GenerationsConsidered = candidates.Count;

        if (candidates.Count == 0)
        {
            LogDefragNoCandidates(logger);
            if (options.PruneExpiredSetAfterCompaction)
            {
                report.ExpiredEntriesPruned = await database.PruneExpiredSetAfterDefragAsync(cancellationToken).ConfigureAwait(false);
            }

            return report;
        }

        List<int> toCompact = new(candidates.Count);
        foreach (int gid in candidates)
        {
            cancellationToken.ThrowIfCancellationRequested();

            (double fraction, long slotsScanned, long reclaimable) = await writer
                .EstimateExpiredFractionAsync(gid, expiredSnapshot, cancellationToken)
                .ConfigureAwait(false);
            report.SlotsScanned += slotsScanned;

            if (fraction < options.MinExpiredFractionToCompact)
            {
                LogDefragGenerationSkipped(logger, gid, fraction, options.MinExpiredFractionToCompact, reclaimable);
                report.AddGeneration(new GenerationDefragOutcome(
                    gid,
                    Compacted: false,
                    SlotsScanned: slotsScanned,
                    SurvivorsCopied: 0,
                    ExpiredEntriesDropped: 0,
                    TombstonesDropped: 0,
                    BytesReclaimed: 0,
                    SkipReason: $"reclaimable fraction {fraction:F4} < threshold {options.MinExpiredFractionToCompact:F4}"));
                continue;
            }

            toCompact.Add(gid);
        }

        if (toCompact.Count == 0)
        {
            LogDefragAllSkipped(logger, candidates.Count);
            if (options.PruneExpiredSetAfterCompaction)
            {
                report.ExpiredEntriesPruned = await database.PruneExpiredSetAfterDefragAsync(cancellationToken).ConfigureAwait(false);
            }

            return report;
        }

        using SemaphoreSlim semaphore = new(options.MaxParallelGenerations, options.MaxParallelGenerations);
        List<Task<GenerationDefragOutcome>> compactTasks = new(toCompact.Count);
        foreach (int gid in toCompact)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            int capturedGid = gid;
            Task<GenerationDefragOutcome> task = Task.Run(async () =>
            {
                try
                {
                    return await writer
                        .CompactGenerationAsync(capturedGid, expiredSnapshot, cancellationToken)
                        .ConfigureAwait(false);
                }
                finally
                {
                    semaphore.Release();
                }
            }, cancellationToken);
            compactTasks.Add(task);
        }

        GenerationDefragOutcome[] outcomes;
        try
        {
            outcomes = await Task.WhenAll(compactTasks).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            LogDefragCompactionBatchFailed(logger, ex);
            outcomes = new GenerationDefragOutcome[compactTasks.Count];
            for (int i = 0; i < compactTasks.Count; i++)
            {
                if (compactTasks[i].IsCompletedSuccessfully)
                {
                    outcomes[i] = compactTasks[i].Result;
                }
                else if (compactTasks[i].IsFaulted)
                {
                    Exception? aggEx = compactTasks[i].Exception;
                    string detail = aggEx?.GetBaseException().Message ?? "unknown";
                    outcomes[i] = new GenerationDefragOutcome(
                        toCompact[i],
                        false,
                        0,
                        0,
                        0,
                        0,
                        0,
                        $"Task faulted: {detail}");
                }
                else
                {
                    outcomes[i] = new GenerationDefragOutcome(toCompact[i], false, 0, 0, 0, 0, 0, "Task faulted");
                }
            }
        }

        foreach (GenerationDefragOutcome outcome in outcomes)
        {
            report.AddGeneration(outcome);
            report.SlotsScanned += outcome.SlotsScanned;
            if (outcome.Compacted)
            {
                report.GenerationsCompacted++;
                report.SurvivorsCopied += outcome.SurvivorsCopied;
                report.ExpiredEntriesDropped += outcome.ExpiredEntriesDropped;
                report.TombstonesDropped += outcome.TombstonesDropped;
                report.BytesReclaimed += outcome.BytesReclaimed;
            }
            else
            {
                LogDefragGenerationSkippedDuringCompaction(logger, outcome.GenerationId, outcome.SkipReason ?? "<unknown>");
            }
        }

        if (options.PruneExpiredSetAfterCompaction)
        {
            report.ExpiredEntriesPruned = await database.PruneExpiredSetAfterDefragAsync(cancellationToken).ConfigureAwait(false);
        }

        LogDefragCompleted(
            logger,
            report.GenerationsConsidered,
            report.GenerationsCompacted,
            report.SurvivorsCopied,
            report.ExpiredEntriesDropped,
            report.TombstonesDropped,
            report.BytesReclaimed,
            report.ExpiredEntriesPruned);
        return report;
    }
}
