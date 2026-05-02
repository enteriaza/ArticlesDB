using HisBench.Contracts;
using HisBench.Core;
using System.Globalization;
using System.Linq;

namespace HisBench.Infrastructure;

internal sealed class ConsoleBenchmarkReporter : IBenchmarkReporter
{
    public void PrintBanner(BenchmarkOptions options)
    {
        Console.Error.WriteLine("A simple history performance tester");
        Console.Error.WriteLine();
        Console.Error.WriteLine($"HistoryDir={Path.GetFullPath(options.HistoryDirectory)}");
        Console.Error.WriteLine($"AddCount={I(options.AddCount)}, AddForks={I(options.AddForks)}, LookupCount={I(options.LookupCount)}, LookupForks={I(options.LookupForks)}");
        Console.Error.WriteLine($"Duration={I(options.DurationSeconds)}s, TargetGenerations={I(options.TargetGenerations)}, FillFactor={F2(options.FillFactor)}, DatasetSize={I(options.DatasetSize)}");
        Console.Error.WriteLine($"Mixed={options.MixedMode} ({I(options.MixInsert)}:{I(options.MixLookup)}:{I(options.MixExpire)}), Zipfian={options.ZipfianSkew}, RandomMsgIds={options.RandomMessageIds}");
        Console.Error.WriteLine($"Burst={options.BurstMode} profile={options.BurstProfile} duty={I(options.BurstDutyPercent)}% sleep={I(options.BurstSleepMs)}ms, CorrelationSampleMs={I(options.CorrelationSampleMs)}");
        Console.Error.WriteLine($"RestartTest={options.RestartTest}, ColdRestart={options.ColdRestart}");
        Console.Error.WriteLine($"Thresholds: saturationQueue={I(options.SaturationQueueDepthThreshold)}, degradedP99Us={F2(options.DegradedP99Microseconds)}");
        Console.Error.WriteLine($"BloomPersist={options.BloomCheckpointPersistMode}, EnqueueDupCheck={options.CrossGenerationDuplicateCheck}");
        Console.Error.WriteLine(
            options.InsertBatchSize > 0
                ? $"InsertBatch={I(options.InsertBatchSize)} (writer coalesce ceiling N; adaptive burst up to N; same HistoryAddForBenchmarkAsync enqueue path)"
                : "InsertBatch=0 (writer coalesce max=1, one single per shard drain)");
        Console.Error.WriteLine();
        Console.Error.WriteLine("WARNING: This program writes garbage entries to the history file");
        Console.Error.WriteLine();
    }

    public void PrintReport(BenchmarkRunReport report)
    {
        BenchmarkMetrics m = report.Metrics;
        var snap = report.FinalPerformanceSnapshot;
        long processReadDelta = report.ResourceEnd.IoReadBytes - report.ResourceStart.IoReadBytes;
        long processWriteDelta = report.ResourceEnd.IoWriteBytes - report.ResourceStart.IoWriteBytes;
        long storageDelta = report.StorageBytesEnd - report.StorageBytesStart;
        double elapsedSeconds = report.RunElapsedMilliseconds / 1000.0;

        Console.WriteLine("==== HISBENCH REPORT ====");
        Console.WriteLine($"VERDICT: {BuildVerdict(report)}");
        Console.WriteLine();

        Console.WriteLine("[Run Summary]");
        Console.WriteLine($"Elapsed: {I(report.RunElapsedMilliseconds)} ms ({F2(elapsedSeconds)} s)");
        Console.WriteLine($"Inserts: attempted={I(m.InsertAttempted)}, inserted={I(m.InsertedOk)}, failed={I(m.InsertFail)}");
        Console.WriteLine($"Duplicates: attempted={I(m.DuplicateAttempted)}, detected={I(m.DuplicateDetected)}");
        Console.WriteLine($"Lookups: attempted={I(m.LookupAttempted)}, found={I(m.LookupFound)}, miss={I(m.LookupMiss)}");
        Console.WriteLine($"Expires: attempted={I(m.ExpireAttempted)}, applied={I(m.ExpiredOk)}");
        long fullRunInsertPerSec = (long)SafeRate(m.InsertedOk, report.RunElapsedMilliseconds);
        Console.WriteLine($"Insert throughput (full run clock): {I(fullRunInsertPerSec)} inserts/sec");
        if (m.InsertPhaseElapsedMs > 0)
        {
            Console.WriteLine(
                $"Insert throughput (insert phase only): {I((long)SafeRate(m.InsertedOk, m.InsertPhaseElapsedMs))} inserts/sec over {I(m.InsertPhaseElapsedMs)} ms");
            Console.WriteLine($"Phased wall: duplicate pass {I(m.DuplicatePhaseElapsedMs)} ms, lookup pass {I(m.LookupPhaseElapsedMs)} ms");
            long phasedSum = m.InsertPhaseElapsedMs + m.DuplicatePhaseElapsedMs + m.LookupPhaseElapsedMs;
            long accounted =
                m.DatabaseOpenConfigureElapsedMs +
                phasedSum +
                m.HistorySyncElapsedMs +
                m.SamplerShutdownElapsedMs;
            long residual = report.RunElapsedMilliseconds - accounted;
            Console.WriteLine(
                $"Clock: phased work {I(phasedSum)} ms + engine cold open {I(m.DatabaseOpenConfigureElapsedMs)} ms (telemetry + HistoryDatabase ctor + ConfigureDatabase) + HistorySync {I(m.HistorySyncElapsedMs)} ms + sampler shutdown {I(m.SamplerShutdownElapsedMs)} ms ≈ {I(accounted)} ms (residual {I(residual)} ms)");
        }
        else if (m.DatabaseOpenConfigureElapsedMs > 0 || m.HistorySyncElapsedMs > 0)
        {
            Console.WriteLine(
                $"Clock: engine cold open {I(m.DatabaseOpenConfigureElapsedMs)} ms + HistorySync {I(m.HistorySyncElapsedMs)} ms + sampler shutdown {I(m.SamplerShutdownElapsedMs)} ms");
        }

        long fullRunLookupPerSec = (long)SafeRate(m.LookupAttempted, report.RunElapsedMilliseconds);
        Console.WriteLine($"Lookup throughput (full run clock): {I(fullRunLookupPerSec)} lookups/sec");
        if (m.LookupPhaseElapsedMs > 0 && m.LookupAttempted > 0)
        {
            Console.WriteLine(
                $"Lookup throughput (lookup phase only): {I((long)SafeRate(m.LookupAttempted, m.LookupPhaseElapsedMs))} lookups/sec over {I(m.LookupPhaseElapsedMs)} ms");
        }

        Console.WriteLine();

        Console.WriteLine("[Engine Health]");
        Console.WriteLine($"Generation={I(snap.ActiveGenerationId)}, Retained={I(snap.RetainedGenerations)}, QueueApprox={I(snap.PendingQueueItemsApprox)}, FullFails={I(snap.FullInsertFailures)}, ProbeFails={I(snap.ProbeLimitFailures)}, BloomCkptDropped={I(snap.BloomCheckpointsDropped)}");
        Console.WriteLine(snap.FullInsertFailures == 0 && snap.ProbeLimitFailures == 0
            ? "Interpretation: No shard pressure failures observed."
            : "Interpretation: Pressure/failure signals observed; inspect thresholds and capacity.");
        Console.WriteLine();

        Console.WriteLine("[Resource Usage]");
        Console.WriteLine($"WorkingSet={I(report.ResourceEnd.WorkingSetBytes / (1024 * 1024))} MiB, ManagedHeap={I(report.ResourceEnd.ManagedHeapBytes / (1024 * 1024))} MiB, GC0={I(report.ResourceEnd.Gc0Collections)}, GC1={I(report.ResourceEnd.Gc1Collections)}, GC2={I(report.ResourceEnd.Gc2Collections)}");
        Console.WriteLine($"CPU time delta={F2((report.ResourceEnd.TotalProcessorTime - report.ResourceStart.TotalProcessorTime).TotalMilliseconds)} ms");
        if (report.ResourceEnd.HasIoCounters && report.ResourceStart.HasIoCounters)
        {
            Console.WriteLine($"ProcessIO API: read={F2(ToMiB(processReadDelta))} MiB, write={F2(ToMiB(processWriteDelta))} MiB");
            Console.WriteLine($"ProcessIO API rates: read={F2(ToMiB(SafeRate(processReadDelta, report.RunElapsedMilliseconds)))} MiB/s, write={F2(ToMiB(SafeRate(processWriteDelta, report.RunElapsedMilliseconds)))} MiB/s");
        }
        else
        {
            Console.WriteLine("ProcessIO API: unavailable");
        }

        Console.WriteLine($"HistoryDir footprint: start={F2(ToMiB(report.StorageBytesStart))} MiB, end={F2(ToMiB(report.StorageBytesEnd))} MiB, delta={F2(ToMiB(storageDelta))} MiB");
        Console.WriteLine($"HistoryDir effective write rate: {F2(ToMiB(SafeRate(storageDelta, report.RunElapsedMilliseconds)))} MiB/s");
        Console.WriteLine("Note: mmap-heavy workloads can show low process write bytes while directory footprint grows due to OS-managed page flush.");
        Console.WriteLine();

        Console.WriteLine("[Latency Breakdown]");
        PrintHdr("Insert wall (caller observed)", m.InsertWall);
        PrintHdr("Insert queue wait", m.InsertQueueWait);
        PrintHdr("Insert writer execution", m.InsertWriterExec);
        PrintHdr("Lookup wall", m.LookupWall);
        PrintHdr("Expire wall", m.ExpireWall);
        PrintHdr("Queue depth samples", m.QueueDepth);
        PrintHdr("Probe failure delta per sample", m.ProbeFailuresDelta);
        PrintHdr("Full failure delta per sample", m.FullFailsDelta);
        Console.WriteLine();

        Console.WriteLine("[Readability Summary]");
        PrintWhatItMeans(report);
        Console.WriteLine();

        Console.WriteLine("[Status Flags]");
        PrintStatus(report);
        Console.WriteLine();

        PrintRestartWarmup(report);
        PrintCorrelationTail(report);
    }

    private static string BuildVerdict(BenchmarkRunReport report)
    {
        double insertP99Us = TimeUtil.P99Microseconds(report.Metrics.InsertWall);
        double queueP99Us = TimeUtil.P99Microseconds(report.Metrics.InsertQueueWait);
        double writerP99Us = TimeUtil.P99Microseconds(report.Metrics.InsertWriterExec);
        double queueDepthP99 = TimeUtil.P99Microseconds(report.Metrics.QueueDepth);
        bool saturated = report.Options.SaturationQueueDepthThreshold > 0 &&
                         report.FinalPerformanceSnapshot.PendingQueueItemsApprox >= report.Options.SaturationQueueDepthThreshold;
        bool degraded = report.Options.DegradedP99Microseconds > 0 &&
                        insertP99Us >= report.Options.DegradedP99Microseconds;

        if (saturated) return "Saturated";
        if (degraded) return "Degraded";
        if (queueP99Us > writerP99Us * 2.0 && queueDepthP99 < 1.0) return "Scheduler-bound";
        if (queueP99Us > writerP99Us * 2.0) return "Queue-bound";
        if (writerP99Us > queueP99Us * 2.0) return "Engine-bound";
        return "Healthy";
    }

    private static void PrintWhatItMeans(BenchmarkRunReport report)
    {
        double wallP99 = TimeUtil.P99Microseconds(report.Metrics.InsertWall);
        double queueP99 = TimeUtil.P99Microseconds(report.Metrics.InsertQueueWait);
        double writerP99 = TimeUtil.P99Microseconds(report.Metrics.InsertWriterExec);

        string dominant = queueP99 > writerP99 * 2.0
            ? (TimeUtil.P99Microseconds(report.Metrics.QueueDepth) < 1.0
                ? "Queue wait dominates, but queue depth stays near zero: likely scheduler/async handoff overhead, not queue saturation."
                : "Queue wait dominates insert latency (likely scheduling/backpressure overhead).")
            : writerP99 > queueP99 * 2.0
                ? "Shard writer execution dominates insert latency (engine path is the main cost)."
                : "Insert latency is balanced between queue wait and writer execution.";

        Console.WriteLine($"- Insert p99 is {F2(wallP99)} us total ({F2(queueP99)} us queue, {F2(writerP99)} us writer).");
        Console.WriteLine($"- {dominant}");
        Console.WriteLine($"- Lookup p99 is {F2(TimeUtil.P99Microseconds(report.Metrics.LookupWall))} us.");
    }

    private static void PrintStatus(BenchmarkRunReport report)
    {
        double insertP99Us = TimeUtil.P99Microseconds(report.Metrics.InsertWall);
        bool saturated = report.Options.SaturationQueueDepthThreshold > 0 &&
                         report.FinalPerformanceSnapshot.PendingQueueItemsApprox >= report.Options.SaturationQueueDepthThreshold;
        bool degraded = report.Options.DegradedP99Microseconds > 0 &&
                        insertP99Us >= report.Options.DegradedP99Microseconds;
        Console.WriteLine($"Saturated={saturated}, Degraded={degraded}, InsertP99Us={F2(insertP99Us)}, QueueApprox={I(report.FinalPerformanceSnapshot.PendingQueueItemsApprox)}");
    }

    private static void PrintRestartWarmup(BenchmarkRunReport report)
    {
        if (report.Metrics.RestartWarmupTimeline.Count == 0)
        {
            return;
        }

        const int headCount = 12;
        const int tailCount = 20;
        int total = report.Metrics.RestartWarmupTimeline.Count;
        Console.WriteLine("Restart warmup timeline:");
        if (total <= headCount + tailCount)
        {
            foreach (RestartWarmupSample sample in report.Metrics.RestartWarmupTimeline)
            {
                Console.WriteLine($"  +{I(sample.ElapsedMs)} ms : p50={F2(sample.P50Microseconds)} us, p99={F2(sample.P99Microseconds)} us, n={I(sample.SampleCount)}");
            }
        }
        else
        {
            foreach (RestartWarmupSample sample in report.Metrics.RestartWarmupTimeline.Take(headCount))
            {
                Console.WriteLine($"  +{I(sample.ElapsedMs)} ms : p50={F2(sample.P50Microseconds)} us, p99={F2(sample.P99Microseconds)} us, n={I(sample.SampleCount)}");
            }

            int omitted = total - headCount - tailCount;
            Console.WriteLine($"  ... {I(omitted)} samples omitted ...");

            foreach (RestartWarmupSample sample in report.Metrics.RestartWarmupTimeline.Skip(total - tailCount))
            {
                Console.WriteLine($"  +{I(sample.ElapsedMs)} ms : p50={F2(sample.P50Microseconds)} us, p99={F2(sample.P99Microseconds)} us, n={I(sample.SampleCount)}");
            }
        }

        if (report.Metrics.RestartSteadyStateMs >= 0)
        {
            Console.WriteLine($"Restart steady-state time: {I(report.Metrics.RestartSteadyStateMs)} ms");
        }
    }

    private static void PrintCorrelationTail(BenchmarkRunReport report)
    {
        IReadOnlyCollection<CorrelationSample> samples = report.Metrics.GetCorrelationTail();
        if (samples.Count == 0)
        {
            return;
        }

        Console.WriteLine("Correlation samples (tail):");
        foreach (CorrelationSample sample in samples.Skip(Math.Max(0, samples.Count - 8)))
        {
            Console.WriteLine($"  +{I(sample.ElapsedMs)} ms : queue={I(sample.QueueDepth)}, probeDelta={I(sample.ProbeFailureDelta)}, fullDelta={I(sample.FullFailureDelta)}, insertP99={F2(sample.InsertP99Microseconds)} us");
        }
    }

    private static void PrintHdr(string label, HdrHistogram.LongHistogram histogram)
    {
        if (histogram.TotalCount == 0)
        {
            Console.WriteLine($"{label}: no samples");
            return;
        }

        double p50 = histogram.GetValueAtPercentile(50) / 1000.0;
        double p95 = histogram.GetValueAtPercentile(95) / 1000.0;
        double p99 = histogram.GetValueAtPercentile(99) / 1000.0;
        double max = histogram.GetValueAtPercentile(100) / 1000.0;
        Console.WriteLine($"{label} (us): p50={F2(p50)}, p95={F2(p95)}, p99={F2(p99)}, max={F2(max)}, count={I(histogram.TotalCount)}");
    }

    private static double SafeRate(long numerator, long elapsedMs)
    {
        if (elapsedMs <= 0)
        {
            return 0;
        }

        return numerator * 1000.0 / elapsedMs;
    }

    private static double ToMiB(double bytes) => bytes / (1024.0 * 1024.0);

    private static string I(long value) => value.ToString("0", CultureInfo.InvariantCulture);

    private static string F2(double value) => value.ToString("0.00", CultureInfo.InvariantCulture);
}
