using HdrHistogram;
using HisBench.Contracts;
using HisBench.Core;
using HistoryDB;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Globalization;

namespace HisBench.Application;

internal sealed partial class BenchmarkOrchestrator : IBenchmarkOrchestrator
{
    [LoggerMessage(EventId = 4000, Level = LogLevel.Information, Message = "Benchmark canceled.")]
    private static partial void LogBenchmarkCanceled(ILogger logger);

    private readonly IHistoryDatabaseFactory _databaseFactory;
    private readonly IColdRestartCoordinator _coldRestartCoordinator;
    private readonly IProcessTelemetryReader _telemetryReader;
    private readonly IBenchmarkReporter _reporter;
    private readonly ILogger<BenchmarkOrchestrator> _logger;

    public BenchmarkOrchestrator(
        IHistoryDatabaseFactory databaseFactory,
        IColdRestartCoordinator coldRestartCoordinator,
        IProcessTelemetryReader telemetryReader,
        IBenchmarkReporter reporter,
        ILogger<BenchmarkOrchestrator> logger)
    {
        _databaseFactory = databaseFactory;
        _coldRestartCoordinator = coldRestartCoordinator;
        _telemetryReader = telemetryReader;
        _reporter = reporter;
        _logger = logger;
    }

    public async Task<int> RunAsync(BenchmarkOptions options, CancellationToken cancellationToken)
    {
        if (options.AutoTune)
        {
            return await RunAutoTuneAsync(options, cancellationToken).ConfigureAwait(false);
        }

        BenchmarkRunReport report = await RunSingleAsync(options, cancellationToken, emitReport: true).ConfigureAwait(false);
        _ = report;
        return 0;
    }

    private async Task<int> RunAutoTuneAsync(BenchmarkOptions options, CancellationToken cancellationToken)
    {
        BenchmarkOptions normalized = options with
        {
            AddCount = 4_000_000,
            LookupCount = 4_000_000,
            DurationSeconds = 0,
            MixedMode = false,
            TargetGenerations = 0,
            RestartTest = false,
            ColdRestart = false,
            BurstMode = false
        };

        Directory.CreateDirectory(normalized.HistoryDirectory);
        string autoRoot = Path.Combine(normalized.HistoryDirectory, "auto-tune");
        Directory.CreateDirectory(autoRoot);

        List<TrialConfig> trials = BuildAutoTuneTrials(normalized, autoRoot);
        Console.WriteLine($"[AutoTune] Starting {trials.Count} trials using add-count=4000000 and lookup-count=4000000.");

        TrialResult? best = null;
        for (int i = 0; i < trials.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            TrialConfig trial = trials[i];
            Console.WriteLine();
            Console.WriteLine($"[AutoTune] Trial {i + 1}/{trials.Count}: {trial.Label}");
            Console.WriteLine($"[AutoTune] Args: --shards {trial.Options.ShardCount} --slots-per-shard {trial.Options.SlotsPerShard} --add-forks {trial.Options.AddForks} --lookup-forks {trial.Options.LookupForks} --queue-capacity {trial.Options.QueueCapacityPerShard}");

            BenchmarkRunReport report = await RunSingleAsync(trial.Options, cancellationToken, emitReport: false).ConfigureAwait(false);
            double score = ScoreReport(report);
            TrialResult result = new(trial, report, score);

            Console.WriteLine($"[AutoTune] Result: score={score.ToString("F2", CultureInfo.InvariantCulture)}, insert/s={RatePerSecond(report.Metrics.InsertedOk, report.RunElapsedMilliseconds).ToString("F0", CultureInfo.InvariantCulture)}, lookup/s={RatePerSecond(report.Metrics.LookupAttempted, report.RunElapsedMilliseconds).ToString("F0", CultureInfo.InvariantCulture)}");
            Console.WriteLine($"[AutoTune] Latency p99(us): insert={TimeUtil.P99Microseconds(report.Metrics.InsertWall).ToString("F2", CultureInfo.InvariantCulture)}, queueWait={TimeUtil.P99Microseconds(report.Metrics.InsertQueueWait).ToString("F2", CultureInfo.InvariantCulture)}, writerExec={TimeUtil.P99Microseconds(report.Metrics.InsertWriterExec).ToString("F2", CultureInfo.InvariantCulture)}");

            if (best is null || result.Score > best.Value.Score)
            {
                best = result;
            }
        }

        if (best is null)
        {
            Console.WriteLine("[AutoTune] No trial was executed.");
            return 1;
        }

        TrialResult winning = best.Value;
        Console.WriteLine();
        Console.WriteLine("[AutoTune] Best configuration");
        Console.WriteLine($"[AutoTune] Label={winning.Config.Label}, score={winning.Score.ToString("F2", CultureInfo.InvariantCulture)}");
        Console.WriteLine($"[AutoTune] Recommended flags: --add-forks {winning.Config.Options.AddForks} --lookup-forks {winning.Config.Options.LookupForks} --shards {winning.Config.Options.ShardCount} --slots-per-shard {winning.Config.Options.SlotsPerShard} --queue-capacity {winning.Config.Options.QueueCapacityPerShard}");
        Console.WriteLine($"[AutoTune] Trial data directory: {winning.Config.Options.HistoryDirectory}");

        _reporter.PrintReport(winning.Report);
        return 0;
    }

    private async Task<BenchmarkRunReport> RunSingleAsync(BenchmarkOptions options, CancellationToken cancellationToken, bool emitReport)
    {
        Directory.CreateDirectory(options.HistoryDirectory);
        if (emitReport)
        {
            _reporter.PrintBanner(options);
        }
        long runStartTicks = Stopwatch.GetTimestamp();
        long storageStartBytes = DirectorySizeUtil.GetTotalBytes(options.HistoryDirectory);

        BenchmarkMetrics metrics = new();
        using CancellationTokenSource runtimeCts = options.DurationSeconds > 0
            ? new CancellationTokenSource(TimeSpan.FromSeconds(options.DurationSeconds))
            : new CancellationTokenSource();
        using CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, runtimeCts.Token);
        CancellationTokenSource samplerCts = new();

        ProcessResourceSnapshot startResources = _telemetryReader.ReadCurrent();
        HistoryDatabase database = _databaseFactory.Create(options);
        ConfigureDatabase(database, options);
        metrics.DatabaseOpenConfigureElapsedMs = TimeUtil.ElapsedMilliseconds(runStartTicks);

        Task samplerTask = options.CorrelationSampleMs > 0
            ? RunCorrelationSamplerAsync(database, metrics, TimeSpan.FromMilliseconds(options.CorrelationSampleMs), samplerCts.Token)
            : Task.CompletedTask;

        try
        {
            if (options.MixedMode)
            {
                await RunMixedAsync(database, options, metrics, linkedCts.Token).ConfigureAwait(false);
            }
            else
            {
                await RunPhasedAsync(database, options, metrics, linkedCts.Token).ConfigureAwait(false);
            }

            if (options.RestartTest)
            {
                samplerCts.Cancel();
                try
                {
                    await samplerTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
                samplerCts.Dispose();

                await database.DisposeAsync().ConfigureAwait(false);
                if (options.ColdRestart)
                {
                    await _coldRestartCoordinator.TryColdRestartAsync(options.HistoryDirectory, linkedCts.Token).ConfigureAwait(false);
                }

                database = _databaseFactory.Create(options);
                ConfigureDatabase(database, options);
                samplerCts = new CancellationTokenSource();
                samplerTask = options.CorrelationSampleMs > 0
                    ? RunCorrelationSamplerAsync(database, metrics, TimeSpan.FromMilliseconds(options.CorrelationSampleMs), samplerCts.Token)
                    : Task.CompletedTask;
                await RunRestartLookupAsync(database, options, metrics, linkedCts.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (linkedCts.IsCancellationRequested)
        {
            LogBenchmarkCanceled(_logger);
        }
        finally
        {
            long historySyncStart = Stopwatch.GetTimestamp();
            database.HistorySync();
            metrics.HistorySyncElapsedMs = TimeUtil.ElapsedMilliseconds(historySyncStart);

            long samplerShutdownStart = Stopwatch.GetTimestamp();
            samplerCts.Cancel();
            try
            {
                await samplerTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            samplerCts.Dispose();
            metrics.SamplerShutdownElapsedMs = TimeUtil.ElapsedMilliseconds(samplerShutdownStart);
        }

        BenchmarkRunReport report = new(
            options,
            metrics,
            database.GetPerformanceSnapshot(),
            startResources,
            _telemetryReader.ReadCurrent(),
            storageStartBytes,
            DirectorySizeUtil.GetTotalBytes(options.HistoryDirectory),
            TimeUtil.ElapsedMilliseconds(runStartTicks));
        if (emitReport)
        {
            _reporter.PrintReport(report);
        }

        return report;
    }

    private static List<TrialConfig> BuildAutoTuneTrials(BenchmarkOptions options, string autoRoot)
    {
        int baseForks = Math.Max(2, Math.Max(options.AddForks, options.LookupForks));
        int downForks = Math.Max(2, baseForks / 2);
        int upForks = Math.Min(64, baseForks * 2);
        int downShards = Math.Max(32, options.ShardCount / 2);
        int upShards = Math.Min(255, options.ShardCount * 2);
        ulong downSlots = Math.Max(1UL << 20, options.SlotsPerShard / 2);
        ulong upSlots = Math.Min(1UL << 24, options.SlotsPerShard * 2);
        int downQueueCapacity = Math.Max(100_000, options.QueueCapacityPerShard / 2);
        int upQueueCapacity = Math.Min(4_000_000, options.QueueCapacityPerShard * 2);

        (int Shards, ulong Slots, int AddForks, int LookupForks, int QueueCapacity)[] tuples =
        [
            (options.ShardCount, options.SlotsPerShard, baseForks, baseForks, options.QueueCapacityPerShard),
            (options.ShardCount, options.SlotsPerShard, upForks, upForks, options.QueueCapacityPerShard),
            (options.ShardCount, options.SlotsPerShard, downForks, downForks, options.QueueCapacityPerShard),
            (downShards, upSlots, baseForks, baseForks, options.QueueCapacityPerShard),
            (upShards, downSlots, baseForks, baseForks, options.QueueCapacityPerShard),
            (options.ShardCount, options.SlotsPerShard, baseForks, baseForks, downQueueCapacity),
            (options.ShardCount, options.SlotsPerShard, baseForks, baseForks, upQueueCapacity)
        ];

        List<TrialConfig> trials = [];
        HashSet<string> dedupe = new(StringComparer.Ordinal);
        int trialOrdinal = 0;
        foreach ((int shards, ulong slots, int addForks, int lookupForks, int queueCapacity) in tuples)
        {
            ulong normalizedSlots = RoundDownToPowerOfTwo(slots);
            string key = $"{shards}:{normalizedSlots}:{addForks}:{lookupForks}:{queueCapacity}";
            if (!dedupe.Add(key))
            {
                continue;
            }

            string label = $"s{shards}-sl{normalizedSlots}-af{addForks}-lf{lookupForks}-qc{queueCapacity}";
            string trialDir = Path.Combine(autoRoot, $"trial-{trialOrdinal:D2}-{label}");
            BenchmarkOptions trialOptions = options with
            {
                HistoryDirectory = trialDir,
                ShardCount = shards,
                SlotsPerShard = normalizedSlots,
                AddForks = addForks,
                LookupForks = lookupForks,
                QueueCapacityPerShard = queueCapacity
            };
            trials.Add(new TrialConfig(label, trialOptions));
            trialOrdinal++;
        }

        return trials;
    }

    private static ulong RoundDownToPowerOfTwo(ulong value)
    {
        if (value == 0)
        {
            return 1UL << 20;
        }

        ulong x = value;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        x |= x >> 32;
        return x - (x >> 1);
    }

    private static double ScoreReport(BenchmarkRunReport report)
    {
        double elapsedSeconds = Math.Max(1e-9, report.RunElapsedMilliseconds / 1000.0);
        double insertRate = report.Metrics.InsertedOk / elapsedSeconds;
        double lookupRate = report.Metrics.LookupAttempted / elapsedSeconds;
        double insertP99Us = TimeUtil.P99Microseconds(report.Metrics.InsertWall);
        double queueP99Us = TimeUtil.P99Microseconds(report.Metrics.InsertQueueWait);
        double failRate = report.Metrics.InsertAttempted == 0
            ? 0
            : (double)report.Metrics.InsertFail / report.Metrics.InsertAttempted;

        return
            (insertRate * 1.0) +
            (lookupRate * 0.35) -
            (insertP99Us * 20.0) -
            (queueP99Us * 12.0) -
            (failRate * 1_000_000.0);
    }

    private static double RatePerSecond(long count, long elapsedMs) =>
        elapsedMs <= 0 ? 0.0 : count / (elapsedMs / 1000.0);

    private readonly record struct TrialConfig(string Label, BenchmarkOptions Options);
    private readonly record struct TrialResult(TrialConfig Config, BenchmarkRunReport Report, double Score);

    private static void ConfigureDatabase(HistoryDatabase db, BenchmarkOptions options)
    {
        db.SetBloomCheckpointPersistMode(options.BloomCheckpointPersistMode);
        db.SetCrossGenerationDuplicateCheck(options.CrossGenerationDuplicateCheck);
        db.SetBloomCheckpointInsertInterval(options.BloomCheckpointInsertInterval);
        db.SetSlotScrubberTuning(options.ScrubberSamplesPerTick, options.ScrubberIntervalMs);
        if (options.InsertBatchSize > 0)
        {
            db.SetWriterCoalesceBatchSize(Math.Clamp(options.InsertBatchSize, 1, HistoryDatabase.MaxHistoryAddBatch));
        }
        else
        {
            db.SetWriterCoalesceBatchSize(1);
        }
        if (options.TargetGenerations > 0)
        {
            ulong perGen = (ulong)(options.ShardCount * (double)options.SlotsPerShard * options.MaxLoadFactorPercent / 100.0 * options.FillFactor);
            db.SetRolloverThresholds(Math.Max(1, perGen / (ulong)Math.Max(1, options.ShardCount)), options.QueueDepthThreshold, options.ProbeFailureThreshold);
        }
    }

    private static async Task RunCorrelationSamplerAsync(HistoryDatabase db, BenchmarkMetrics metrics, TimeSpan interval, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(interval, ct).ConfigureAwait(false);
                HistoryPerformanceSnapshot snapshot = db.GetPerformanceSnapshot();
                long probeDelta = Math.Max(0, snapshot.ProbeLimitFailures - Interlocked.Read(ref metrics.LastProbeFails));
                long fullDelta = Math.Max(0, snapshot.FullInsertFailures - Interlocked.Read(ref metrics.LastFullFails));
                metrics.RecordQueueDepth(Math.Max(0, snapshot.PendingQueueItemsApprox));
                metrics.RecordProbeFailuresDelta(probeDelta);
                metrics.RecordFullFailsDelta(fullDelta);
                metrics.AddCorrelationSample(new CorrelationSample(
                    TimeUtil.ElapsedMilliseconds(metrics.BenchmarkStartTicks),
                    snapshot.PendingQueueItemsApprox,
                    probeDelta,
                    fullDelta,
                    TimeUtil.P99Microseconds(metrics.InsertWall)));
                Interlocked.Exchange(ref metrics.LastProbeFails, snapshot.ProbeLimitFailures);
                Interlocked.Exchange(ref metrics.LastFullFails, snapshot.FullInsertFailures);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private static async Task RunPhasedAsync(HistoryDatabase db, BenchmarkOptions options, BenchmarkMetrics metrics, CancellationToken ct)
    {
        long addTarget = ResolveAddTarget(options);
        long insertPhaseStart = Stopwatch.GetTimestamp();
        await ExecuteWorkersAsync(options.AddForks, async _ =>
        {
            while (!ct.IsCancellationRequested)
            {
                long index = Interlocked.Increment(ref metrics.InsertAttempted) - 1;
                if (index >= addTarget)
                {
                    break;
                }

                await MaybeBurstDelayAsync(options, ct).ConfigureAwait(false);
                await ExecuteInsertAsync(db, options, metrics, index, ct).ConfigureAwait(false);
            }
        }).ConfigureAwait(false);

        metrics.InsertPhaseElapsedMs = TimeUtil.ElapsedMilliseconds(insertPhaseStart);

        long duplicatePhaseStart = Stopwatch.GetTimestamp();
        await ExecuteWorkersAsync(options.AddForks, async _ =>
        {
            while (!ct.IsCancellationRequested)
            {
                long index = Interlocked.Increment(ref metrics.DuplicateAttempted) - 1;
                if (index >= addTarget)
                {
                    break;
                }

                await MaybeBurstDelayAsync(options, ct).ConfigureAwait(false);
                long idx = ClampIndex(index, options);
                long t0 = Stopwatch.GetTimestamp();
                ShardInsertBenchmarkResult result = await db.HistoryAddForBenchmarkAsync(
                    MessageIdentityBuilder.MessageId(idx, options.RandomMessageIds),
                    MessageIdentityBuilder.ServerId(idx),
                    ct).ConfigureAwait(false);
                metrics.RecordDuplicateWall(TimeUtil.ToNanoseconds(Stopwatch.GetTimestamp() - t0));
                if (result.Result == ShardInsertResult.Duplicate)
                {
                    Interlocked.Increment(ref metrics.DuplicateDetected);
                }
            }
        }).ConfigureAwait(false);
        metrics.DuplicatePhaseElapsedMs = TimeUtil.ElapsedMilliseconds(duplicatePhaseStart);

        long lookupPhaseStart = Stopwatch.GetTimestamp();
        await ExecuteLookupsAsync(db, options, metrics, options.LookupCount, ct).ConfigureAwait(false);
        metrics.LookupPhaseElapsedMs = TimeUtil.ElapsedMilliseconds(lookupPhaseStart);
    }

    private static async Task RunMixedAsync(HistoryDatabase db, BenchmarkOptions options, BenchmarkMetrics metrics, CancellationToken ct)
    {
        int totalWeight = options.MixInsert + options.MixLookup + options.MixExpire;
        await ExecuteWorkersAsync(Math.Max(options.AddForks, options.LookupForks), async _ =>
        {
            while (!ct.IsCancellationRequested)
            {
                await MaybeBurstDelayAsync(options, ct).ConfigureAwait(false);
                int roll = Random.Shared.Next(totalWeight);
                if (roll < options.MixInsert)
                {
                    long idx = Interlocked.Increment(ref metrics.InsertAttempted) - 1;
                    await ExecuteInsertAsync(db, options, metrics, idx, ct).ConfigureAwait(false);
                    continue;
                }

                if (roll < options.MixInsert + options.MixLookup)
                {
                    long existingMax = Math.Max(1, Interlocked.Read(ref metrics.InsertedOk));
                    long lookupIdx = PickLookupIndex(existingMax, options);
                    long t0 = Stopwatch.GetTimestamp();
                    bool found = db.HistoryLookup(MessageIdentityBuilder.MessageId(lookupIdx, options.RandomMessageIds));
                    metrics.RecordLookupWall(TimeUtil.ToNanoseconds(Stopwatch.GetTimestamp() - t0));
                    Interlocked.Increment(ref metrics.LookupAttempted);
                    if (found)
                    {
                        Interlocked.Increment(ref metrics.LookupFound);
                    }
                    else
                    {
                        Interlocked.Increment(ref metrics.LookupMiss);
                    }

                    continue;
                }

                long expMax = Math.Max(1, Interlocked.Read(ref metrics.InsertedOk));
                long expIdx = ClampIndex(Random.Shared.NextInt64(expMax), options);
                long te = Stopwatch.GetTimestamp();
                bool expired = db.HistoryExpire(MessageIdentityBuilder.MessageId(expIdx, options.RandomMessageIds));
                metrics.RecordExpireWall(TimeUtil.ToNanoseconds(Stopwatch.GetTimestamp() - te));
                Interlocked.Increment(ref metrics.ExpireAttempted);
                if (expired)
                {
                    Interlocked.Increment(ref metrics.ExpiredOk);
                }
            }
        }).ConfigureAwait(false);
    }

    private static async Task RunRestartLookupAsync(HistoryDatabase db, BenchmarkOptions options, BenchmarkMetrics metrics, CancellationToken ct)
    {
        const int batchSize = 10_000;
        long restartOps = Math.Max(100_000, options.LookupCount / 2);
        LongHistogram batchHistogram = BenchHistogramFactory.CreateLatencyHistogram();
        long start = Stopwatch.GetTimestamp();
        for (long i = 0; i < restartOps && !ct.IsCancellationRequested; i++)
        {
            long maxInserted = Math.Max(1, Interlocked.Read(ref metrics.InsertedOk));
            long idx = PickLookupIndex(maxInserted, options);
            long t0 = Stopwatch.GetTimestamp();
            bool found = db.HistoryLookup(MessageIdentityBuilder.MessageId(idx, options.RandomMessageIds));
            long ns = TimeUtil.ToNanoseconds(Stopwatch.GetTimestamp() - t0);
            metrics.RecordLookupWall(ns);
            batchHistogram.RecordValue(ns);
            Interlocked.Increment(ref metrics.LookupAttempted);
            if (found)
            {
                Interlocked.Increment(ref metrics.LookupFound);
            }
            else
            {
                Interlocked.Increment(ref metrics.LookupMiss);
            }

            if ((i + 1) % batchSize == 0 || i + 1 == restartOps)
            {
                double p99Us = TimeUtil.P99Microseconds(batchHistogram);
                metrics.RestartWarmupTimeline.Add(new RestartWarmupSample(
                    TimeUtil.ElapsedMilliseconds(start),
                    TimeUtil.P50Microseconds(batchHistogram),
                    p99Us,
                    batchHistogram.TotalCount));
                if (options.DegradedP99Microseconds > 0 &&
                    metrics.RestartSteadyStateMs < 0 &&
                    p99Us <= options.DegradedP99Microseconds)
                {
                    metrics.RestartSteadyStateMs = TimeUtil.ElapsedMilliseconds(start);
                }

                batchHistogram = BenchHistogramFactory.CreateLatencyHistogram();
            }
        }
    }

    private static async Task ExecuteLookupsAsync(HistoryDatabase db, BenchmarkOptions options, BenchmarkMetrics metrics, long count, CancellationToken ct)
    {
        await ExecuteWorkersAsync(options.LookupForks, async _ =>
        {
            while (!ct.IsCancellationRequested)
            {
                long i = Interlocked.Increment(ref metrics.LookupAttempted) - 1;
                if (i >= count)
                {
                    break;
                }

                await MaybeBurstDelayAsync(options, ct).ConfigureAwait(false);
                long existingMax = Math.Max(1, Interlocked.Read(ref metrics.InsertedOk));
                long idx = PickLookupIndex(existingMax, options);
                long t0 = Stopwatch.GetTimestamp();
                bool found = db.HistoryLookup(MessageIdentityBuilder.MessageId(idx, options.RandomMessageIds));
                metrics.RecordLookupWall(TimeUtil.ToNanoseconds(Stopwatch.GetTimestamp() - t0));
                if (found)
                {
                    Interlocked.Increment(ref metrics.LookupFound);
                }
                else
                {
                    Interlocked.Increment(ref metrics.LookupMiss);
                }
            }
        }).ConfigureAwait(false);
    }

    private static async Task ExecuteInsertAsync(HistoryDatabase db, BenchmarkOptions options, BenchmarkMetrics metrics, long index, CancellationToken ct)
    {
        long idx = ClampIndex(index, options);
        long wallStart = Stopwatch.GetTimestamp();
        ShardInsertBenchmarkResult result = await db.HistoryAddForBenchmarkAsync(
            MessageIdentityBuilder.MessageId(idx, options.RandomMessageIds),
            MessageIdentityBuilder.ServerId(idx),
            ct).ConfigureAwait(false);
        metrics.RecordInsertWall(TimeUtil.ToNanoseconds(Stopwatch.GetTimestamp() - wallStart));
        metrics.RecordInsertQueueWait(TimeUtil.ToNanoseconds(result.QueueWaitTicks));
        metrics.RecordInsertWriterExec(TimeUtil.ToNanoseconds(result.WriterExecutionTicks));
        if (result.Result == ShardInsertResult.Inserted)
        {
            Interlocked.Increment(ref metrics.InsertedOk);
        }
        else
        {
            Interlocked.Increment(ref metrics.InsertFail);
        }
    }

    private static async Task ExecuteWorkersAsync(int workers, Func<int, Task> action)
    {
        Task[] tasks = new Task[workers];
        for (int i = 0; i < workers; i++)
        {
            int localWorker = i;
            tasks[i] = action(localWorker);
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    private static long ResolveAddTarget(BenchmarkOptions options)
    {
        if (options.DurationSeconds > 0)
        {
            return options.DatasetSize > 0 ? options.DatasetSize : long.MaxValue;
        }

        if (options.TargetGenerations <= 0)
        {
            return options.AddCount;
        }

        double perGen = options.ShardCount * (double)options.SlotsPerShard * (options.MaxLoadFactorPercent / 100.0) * options.FillFactor;
        return Math.Max(options.AddCount, (long)(perGen * options.TargetGenerations));
    }

    private static long PickLookupIndex(long maxInserted, BenchmarkOptions options)
    {
        long idx = options.ZipfianSkew && maxInserted > 10
            ? (Random.Shared.NextDouble() < 0.80
                ? Random.Shared.NextInt64(Math.Max(1, maxInserted / 5))
                : Random.Shared.NextInt64(maxInserted))
            : Random.Shared.NextInt64(maxInserted);
        return ClampIndex(idx, options);
    }

    private static long ClampIndex(long index, BenchmarkOptions options)
    {
        if (options.DatasetSize <= 0)
        {
            return index;
        }

        long mod = index % options.DatasetSize;
        return mod < 0 ? mod + options.DatasetSize : mod;
    }

    private static async Task MaybeBurstDelayAsync(BenchmarkOptions options, CancellationToken ct)
    {
        if (!options.BurstMode)
        {
            return;
        }

        double duty = options.BurstProfile switch
        {
            BurstProfile.Spike => options.BurstDutyPercent,
            BurstProfile.Ramp => BurstProfileEvaluator.GetRampDuty(options.BurstDutyPercent),
            BurstProfile.Wave => BurstProfileEvaluator.GetWaveDuty(options.BurstDutyPercent),
            _ => options.BurstDutyPercent
        };
        if (Random.Shared.NextDouble() * 100.0 >= duty)
        {
            await Task.Delay(options.BurstSleepMs, ct).ConfigureAwait(false);
        }
    }
}
