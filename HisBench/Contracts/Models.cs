using HdrHistogram;
using HistoryDB;
using System.Diagnostics;

namespace HisBench.Contracts;

internal readonly record struct ProcessResourceSnapshot(
    long WorkingSetBytes,
    TimeSpan TotalProcessorTime,
    long IoReadBytes,
    long IoWriteBytes,
    bool HasIoCounters,
    int Gc0Collections,
    int Gc1Collections,
    int Gc2Collections,
    long ManagedHeapBytes);

internal readonly record struct CorrelationSample(
    long ElapsedMs,
    long QueueDepth,
    long ProbeFailureDelta,
    long FullFailureDelta,
    double InsertP99Microseconds);

internal readonly record struct RestartWarmupSample(
    long ElapsedMs,
    double P50Microseconds,
    double P99Microseconds,
    long SampleCount);

internal sealed class BenchmarkMetrics
{
    internal long BenchmarkStartTicks { get; } = Stopwatch.GetTimestamp();

    internal long InsertAttempted;
    internal long InsertedOk;
    internal long InsertFail;
    internal long DuplicateAttempted;
    internal long DuplicateDetected;
    internal long LookupAttempted;
    internal long LookupFound;
    internal long LookupMiss;
    internal long ExpireAttempted;
    internal long ExpiredOk;

    internal long LastProbeFails;
    internal long LastFullFails;

    internal readonly LongHistogram InsertWall = BenchHistogramFactory.CreateLatencyHistogram();
    internal readonly LongHistogram InsertQueueWait = BenchHistogramFactory.CreateLatencyHistogram();
    internal readonly LongHistogram InsertWriterExec = BenchHistogramFactory.CreateLatencyHistogram();
    internal readonly LongHistogram DuplicateWall = BenchHistogramFactory.CreateLatencyHistogram();
    internal readonly LongHistogram LookupWall = BenchHistogramFactory.CreateLatencyHistogram();
    internal readonly LongHistogram ExpireWall = BenchHistogramFactory.CreateLatencyHistogram();
    internal readonly LongHistogram QueueDepth = BenchHistogramFactory.CreateLatencyHistogram();
    internal readonly LongHistogram ProbeFailuresDelta = BenchHistogramFactory.CreateLatencyHistogram();
    internal readonly LongHistogram FullFailsDelta = BenchHistogramFactory.CreateLatencyHistogram();

    internal readonly List<RestartWarmupSample> RestartWarmupTimeline = [];
    internal long RestartSteadyStateMs = -1;

    /// <summary>Wall time of the insert-only phase in <see cref="BenchmarkOrchestrator.RunPhasedAsync"/>; zero when not measured (e.g. mixed mode).</summary>
    internal long InsertPhaseElapsedMs;

    /// <summary>Wall time of the duplicate pass in phased mode.</summary>
    internal long DuplicatePhaseElapsedMs;

    /// <summary>Wall time of the lookup pass in phased mode.</summary>
    internal long LookupPhaseElapsedMs;

    /// <summary>Wall time from run clock start through storage sizing, telemetry snapshot, <c>HistoryDatabase</c> construction, and <c>ConfigureDatabase</c>.</summary>
    internal long DatabaseOpenConfigureElapsedMs;

    /// <summary>Wall time of <c>HistorySync()</c> in <c>finally</c>.</summary>
    internal long HistorySyncElapsedMs;

    /// <summary>Wall time to cancel and await the correlation sampler task in <c>finally</c>.</summary>
    internal long SamplerShutdownElapsedMs;

    private readonly object _sampleLock = new();
    private readonly Queue<CorrelationSample> _correlationTail = new();
    private const int MaxCorrelationSamples = 2048;

    internal IReadOnlyCollection<CorrelationSample> GetCorrelationTail()
    {
        lock (_sampleLock)
        {
            return _correlationTail.ToArray();
        }
    }

    internal void AddCorrelationSample(CorrelationSample sample)
    {
        lock (_sampleLock)
        {
            if (_correlationTail.Count == MaxCorrelationSamples)
            {
                _correlationTail.Dequeue();
            }

            _correlationTail.Enqueue(sample);
        }
    }

    internal void RecordInsertWall(long value) => RecordThreadSafe(InsertWall, value);
    internal void RecordInsertQueueWait(long value) => RecordThreadSafe(InsertQueueWait, value);
    internal void RecordInsertWriterExec(long value) => RecordThreadSafe(InsertWriterExec, value);
    internal void RecordDuplicateWall(long value) => RecordThreadSafe(DuplicateWall, value);
    internal void RecordLookupWall(long value) => RecordThreadSafe(LookupWall, value);
    internal void RecordExpireWall(long value) => RecordThreadSafe(ExpireWall, value);
    internal void RecordQueueDepth(long value) => RecordThreadSafe(QueueDepth, value);
    internal void RecordProbeFailuresDelta(long value) => RecordThreadSafe(ProbeFailuresDelta, value);
    internal void RecordFullFailsDelta(long value) => RecordThreadSafe(FullFailsDelta, value);

    private static void RecordThreadSafe(LongHistogram histogram, long value)
    {
        long safe = value <= 0 ? 1 : value;
        if (safe > BenchHistogramFactory.MaxTrackableValueNs)
        {
            safe = BenchHistogramFactory.MaxTrackableValueNs;
        }

        lock (histogram)
        {
            histogram.RecordValue(safe);
        }
    }
}

internal sealed record BenchmarkRunReport(
    BenchmarkOptions Options,
    BenchmarkMetrics Metrics,
    HistoryPerformanceSnapshot FinalPerformanceSnapshot,
    ProcessResourceSnapshot ResourceStart,
    ProcessResourceSnapshot ResourceEnd,
    long StorageBytesStart,
    long StorageBytesEnd,
    long RunElapsedMilliseconds);

internal static class BenchHistogramFactory
{
    internal const long MaxTrackableValueNs = 1_000_000_000;

    internal static LongHistogram CreateLatencyHistogram() => new(1, MaxTrackableValueNs, 3);
}
