// ShardedHistoryWriter.cs -- coordinates generation-scoped mmap shards, per-shard writer queues, Bloom sidecars, warm-up, retention, and shutdown.
// Background tasks drain bounded channels into HistoryShard; Bloom checkpoints use a single-writer channel. Optional slot scrubber samples mmap slots, detects volatile layout mismatch, and can invalidate Bloom trust then re-warm one shard from disk.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

using HistoryDB.Contracts;
using HistoryDB.Core.Policies;
using HistoryDB.Utilities;
using Microsoft.Extensions.Logging;

namespace HistoryDB;

/// <summary>
/// Multi-generation, multi-shard history writer with Bloom pre-filters and background persistence.
/// </summary>
/// <remarks>
/// <para>
/// <b>Lifecycle:</b> Construction initializes the writer and activates the first generation. Call <see cref="StopAsync"/>
/// or <see cref="DisposeAsync"/> for graceful drain; synchronous <see cref="Dispose"/> blocks until asynchronous disposal completes.
/// </para>
/// <para>
/// <b>Thread safety:</b> Public members may be invoked concurrently except disposal, which must not overlap with new enqueue operations.
/// </para>
/// <para>
/// <b>Bloom trust model:</b> When a sidecar is missing or invalid, the filter is rebuilt (warm-up) before being trusted for negative answers; false negatives are not
/// considered a correctness bug for this component (see product documentation).
/// </para>
/// <para>
/// <b>Shard lifecycle / rollover policy:</b> Inserts still hard-stop at <see cref="HistoryShard.LoadLimitSlots"/> from <c>maxLoadFactorPercent</c>. Proactive rollover can open the next generation earlier using
/// configured used-slot, queue-depth, and aggregate probe-failure thresholds so traffic moves before soft limits become painful. Hash-to-shard routing is fixed for a writer instance; changing shard counts requires a new writer.
/// </para>
/// <para>
/// <b>Data hygiene:</b> An optional background scrubber calls <see cref="HistoryShard.TryScrubSlotLayout"/> on random slots across retained generations, increments corruption-style counters, and may invalidate Bloom trust then re-warm a single shard from mmap when a trusted filter disagrees with an occupied slot.
/// </para>
/// <para>
/// <b>CPU / NUMA:</b> Optional pinning uses <see cref="CpuAffinity"/> (Windows: single-group masks up to 64 logical processors; Linux: pthread affinity). When <c>preferNumaNodeOrderingForShardPinsOnWindows</c> is enabled on Windows, shard writers are spread in NUMA-node order so per-shard mmap and Bloom work tends to stay on one package before spilling to the next.
/// </para>
/// </remarks>
internal sealed partial class ShardedHistoryWriter
{
    #region Constants
    private const ulong UsedSlotsFlushMask = 0xFFF;
    private const int MaxRolloverRetries = 4;
    private const int BloomHashFunctions = 7;
    private const int MinBloomBitsPerShard = 1 << 24;
    private const double DefaultBloomMemoryFraction = 0.80;
    private const double DefaultBloomBitsPerEntry = 10.0;
    private const ulong WarmupYieldMask = 0xFFFF;
    private const ulong DefaultBloomCheckpointInsertInterval = 1_000_000;
    private const int CompletionPoolMax = 8192;
    private const ulong DefaultProactiveRolloverLoadFactorPercent = 72;
    private const int DefaultSlotScrubberSamplesPerTick = 256;
    private const int DefaultSlotScrubberIntervalMilliseconds = 30_000;
    private const int BloomPersistQueueCapacity = 4096;

    #endregion

    #region Fields

    private readonly string _rootPath;
    private readonly int _shardCount;
    private readonly ulong _slotsPerShard;
    private readonly ulong _maxLoadFactorPercent;
    private readonly int _queueCapacityPerShard;
    private readonly int _maxRetainedGenerations;
    private readonly double _bloomMemoryFraction;
    private readonly double _bloomTargetBitsPerEntry;
    private readonly ulong _configuredBloomMemoryBudgetBytes;
    private ulong _bloomCheckpointInsertInterval;
    private readonly ulong _bloomCheckpointMaxSnapshotBytes;
    /// <summary>Per-shard slot count at or above which writers request the next generation (0 disables load-based proactive rollover).</summary>
    private ulong _proactiveRolloverUsedSlotsThreshold;
    /// <summary>Bounded channel items at or above which enqueue requests the next generation (0 disables queue-based proactive rollover).</summary>
    private int _proactiveRolloverQueueDepthThreshold;
    /// <summary>Sum of per-shard probe-limit failures at or above which writers request the next generation (0 disables).</summary>
    private long _proactiveRolloverAggregateProbeFailuresThreshold;
    private readonly bool _slotScrubberEnabled;
    private int _slotScrubberSamplesPerTick;
    private int _slotScrubberIntervalMilliseconds;
    private readonly bool _slotScrubberBloomRepairOnMismatch;
    private readonly int[]? _shardWriterLogicalProcessors;
    private readonly int? _bloomPersistLogicalProcessor;
    private readonly int? _bloomWarmupLogicalProcessor;
    private readonly int? _slotScrubberLogicalProcessor;
    private readonly ILogger _logger;
    private int _shardWriterPinFailureLogged;

    private readonly List<GenerationState> _generations = [];
    private readonly List<Task> _retiredGenerationCleanupTasks = [];
    private readonly CancellationTokenSource _stopCts = new();
    private readonly object _lifecycleLock = new();
    private readonly ConcurrentStack<ShardCompletionSource> _completionPool = new();
    private int _completionPooledApprox;
    private readonly object _bloomPersistStartLock = new();
    private Channel<BloomPersistWork>? _bloomPersistChannel;
    private Task? _bloomPersistTask;
    private readonly object _stopTaskLock = new();
    private Task? _stopTask;
    private Task? _slotScrubTask;
    private readonly ConcurrentDictionary<(int GenerationId, int ShardId), byte> _bloomRewarmInFlight = new();

    private long _scrubSamplesTotal;
    private long _scrubVolatileMismatches;
    private long _scrubBloomMismatchRepairs;
    private long _scrubOccupiedBeyondWatermarkHints;

    private bool _started;
    private bool _disposed;
    private int _activeGenerationId;
    private int _disposeOnce;

    #endregion

    #region Constructors

    /// <summary>
    /// Initializes writer options, validates configuration, and activates the first generation.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <paramref name="rootPath"/> is normalized via <see cref="WritableDirectory.ValidateRootDirectory(string)"/>. <paramref name="shardCount"/>
    /// must be greater than zero and <paramref name="slotsPerShard"/> must be a power of two. Bloom sizing uses <paramref name="bloomMemoryBudgetBytes"/> when non-zero; otherwise a fraction of
    /// installed RAM from <see cref="HistoryDB.Utilities.SystemMemoryReader.GetInstalledMemoryBytesForBloomBudget"/>.
    /// </para>
    /// </remarks>
    /// <param name="rootPath">Directory that will contain <c>00001</c>-style hexadecimal generation folders.</param>
    /// <param name="shardCount">Shard count (&gt; 0); each generation opens this many mmap files.</param>
    /// <param name="slotsPerShard">Open-addressing table slots per shard (power of two).</param>
    /// <param name="maxLoadFactorPercent">Maximum occupancy percentage before inserts return <see cref="ShardInsertResult.Full"/>.</param>
    /// <param name="queueCapacityPerShard">Bounded channel capacity per shard writer.</param>
    /// <param name="maxRetainedGenerations">Number of generations kept open before retiring oldest to background cleanup.</param>
    /// <param name="bloomMemoryFraction">Fraction of detected physical RAM used as Bloom bit budget cap when <paramref name="bloomMemoryBudgetBytes"/> is zero.</param>
    /// <param name="bloomTargetBitsPerEntry">Desired bits per expected entry before RAM cap.</param>
    /// <param name="bloomMemoryBudgetBytes">When non-zero, overrides RAM-derived Bloom budget.</param>
    /// <param name="bloomCheckpointInsertInterval">Insert count between best-effort Bloom checkpoints; zero disables periodic checkpoints.</param>
    /// <param name="bloomCheckpointMaxSnapshotBytes">Maximum serialized Bloom size for a background checkpoint snapshot.</param>
    /// <param name="proactiveRolloverLoadFactorPercent">
    /// When non-zero, writers call <see cref="EnsureNextGeneration"/> once per shard when local used slots reach <c>slotsPerShard * percent / 100</c>, strictly below <paramref name="maxLoadFactorPercent"/> hard reject.
    /// Use <c>0</c> to disable load-based proactive rollover (defaults to <c>72</c> when <paramref name="maxLoadFactorPercent"/> is 75).
    /// </param>
    /// <param name="proactiveRolloverQueueWatermarkPercent">
    /// When non-zero, enqueue and shard writers may request the next generation when bounded queue depth reaches <c>queueCapacityPerShard * percent / 100</c>. Use <c>0</c> to disable (default).
    /// </param>
    /// <param name="proactiveRolloverAggregateProbeFailuresThreshold">
    /// When positive, writers request the next generation when the sum of per-shard <see cref="ShardInsertResult.ProbeLimitExceeded"/> counts in the active generation reaches this value. Use <c>0</c> to disable (default).
    /// </param>
    /// <param name="enableBackgroundSlotScrubber">When <see langword="true"/>, starts a low-rate background task that samples mmap slots and updates <see cref="GetSlotScrubberCounters"/>.</param>
    /// <param name="slotScrubberSamplesPerTick">Random slots to scrub after each interval (1..100000).</param>
    /// <param name="slotScrubberIntervalMilliseconds">Delay between scrub ticks (250 ms .. 24 h).</param>
    /// <param name="slotScrubberInvalidateBloomOnOccupiedMismatch">When <see langword="true"/>, an occupied slot that a trusted Bloom filter claims cannot exist schedules a single-shard Bloom re-warm from the shard.</param>
    /// <param name="pinShardWriterThreads">Pins each shard writer thread to a logical processor for locality with mmap and Bloom updates.</param>
    /// <param name="preferNumaNodeOrderingForShardPinsOnWindows">When <see langword="true"/> on Windows, uses NUMA-node order (group 0) for the shard-to-processor map when available.</param>
    /// <param name="shardWriterLogicalProcessorOffset">Added to the shard index before modulo when assigning processors.</param>
    /// <param name="pinBloomPersistThread">Pins the Bloom checkpoint writer thread.</param>
    /// <param name="bloomPersistLogicalProcessor">Logical processor for Bloom persist; negative values mean last processor (<c>ProcessorCount - 1</c>).</param>
    /// <param name="pinBloomWarmupThread">Pins the Bloom warm-up scan thread.</param>
    /// <param name="bloomWarmupLogicalProcessor">Logical processor index for warm-up (negative means last processor).</param>
    /// <param name="pinSlotScrubberThread">Pins the slot scrubber thread.</param>
    /// <param name="slotScrubberLogicalProcessor">Logical processor index for scrubber (negative means last processor).</param>
    public ShardedHistoryWriter(
        string rootPath,
        int shardCount = 128,
        ulong slotsPerShard = 1UL << 22,
        ulong maxLoadFactorPercent = 75,
        int queueCapacityPerShard = 1_000_000,
        int maxRetainedGenerations = 8,
        double bloomMemoryFraction = DefaultBloomMemoryFraction,
        double bloomTargetBitsPerEntry = DefaultBloomBitsPerEntry,
        ulong bloomMemoryBudgetBytes = 0,
        ulong bloomCheckpointInsertInterval = DefaultBloomCheckpointInsertInterval,
        ulong bloomCheckpointMaxSnapshotBytes = 32UL * 1024 * 1024,
        ulong proactiveRolloverLoadFactorPercent = DefaultProactiveRolloverLoadFactorPercent,
        int proactiveRolloverQueueWatermarkPercent = 0,
        long proactiveRolloverAggregateProbeFailuresThreshold = 0,
        bool enableBackgroundSlotScrubber = false,
        int slotScrubberSamplesPerTick = DefaultSlotScrubberSamplesPerTick,
        int slotScrubberIntervalMilliseconds = DefaultSlotScrubberIntervalMilliseconds,
        bool slotScrubberInvalidateBloomOnOccupiedMismatch = true,
        bool pinShardWriterThreads = false,
        bool preferNumaNodeOrderingForShardPinsOnWindows = true,
        int shardWriterLogicalProcessorOffset = 0,
        bool pinBloomPersistThread = false,
        int bloomPersistLogicalProcessor = -1,
        bool pinBloomWarmupThread = false,
        int bloomWarmupLogicalProcessor = -1,
        bool pinSlotScrubberThread = false,
        int slotScrubberLogicalProcessor = -1,
        ILogger? logger = null)
    {
        _logger = logger ?? TraceFallbackLogger.Instance;
        rootPath = WritableDirectory.ValidateRootDirectory(rootPath);

        if (shardCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(shardCount), "shardCount must be > 0.");
        }

        if (!BitOperationsUtil.IsPowerOfTwo(slotsPerShard))
        {
            throw new ArgumentOutOfRangeException(nameof(slotsPerShard), "slotsPerShard must be a power of 2 and > 0.");
        }

        if (queueCapacityPerShard <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(queueCapacityPerShard), "queueCapacityPerShard must be > 0.");
        }

        if (maxRetainedGenerations <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxRetainedGenerations), "maxRetainedGenerations must be > 0.");
        }

        if (bloomMemoryFraction <= 0 || bloomMemoryFraction > 0.95)
        {
            throw new ArgumentOutOfRangeException(nameof(bloomMemoryFraction), "bloomMemoryFraction must be in range (0, 0.95].");
        }

        if (bloomTargetBitsPerEntry < 4.0 || bloomTargetBitsPerEntry > 32.0)
        {
            throw new ArgumentOutOfRangeException(nameof(bloomTargetBitsPerEntry), "bloomTargetBitsPerEntry must be in range [4, 32].");
        }

        if (proactiveRolloverLoadFactorPercent > 0)
        {
            if (proactiveRolloverLoadFactorPercent >= maxLoadFactorPercent)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(proactiveRolloverLoadFactorPercent),
                    "proactiveRolloverLoadFactorPercent must be zero (disabled) or strictly less than maxLoadFactorPercent.");
            }

            ulong rolloverSlots = (slotsPerShard * proactiveRolloverLoadFactorPercent) / 100;
            ulong hardLimitSlots = (slotsPerShard * maxLoadFactorPercent) / 100;
            if (rolloverSlots == 0 || rolloverSlots >= hardLimitSlots)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(proactiveRolloverLoadFactorPercent),
                    "proactive rollover load threshold must yield a slot count below the hard load limit.");
            }
        }

        if (proactiveRolloverQueueWatermarkPercent < 0 || proactiveRolloverQueueWatermarkPercent > 99)
        {
            throw new ArgumentOutOfRangeException(nameof(proactiveRolloverQueueWatermarkPercent), "proactiveRolloverQueueWatermarkPercent must be in range [0, 99].");
        }

        if (proactiveRolloverAggregateProbeFailuresThreshold < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(proactiveRolloverAggregateProbeFailuresThreshold), "Threshold must be non-negative; use 0 to disable.");
        }

        if (slotScrubberSamplesPerTick < 1 || slotScrubberSamplesPerTick > 100_000)
        {
            throw new ArgumentOutOfRangeException(nameof(slotScrubberSamplesPerTick), "slotScrubberSamplesPerTick must be in range [1, 100000].");
        }

        if (slotScrubberIntervalMilliseconds < 250 || slotScrubberIntervalMilliseconds > 86_400_000)
        {
            throw new ArgumentOutOfRangeException(nameof(slotScrubberIntervalMilliseconds), "slotScrubberIntervalMilliseconds must be in range [250, 86400000].");
        }

        _shardWriterLogicalProcessors = BuildShardWriterProcessorAssignment(
            shardCount,
            pinShardWriterThreads,
            preferNumaNodeOrderingForShardPinsOnWindows,
            shardWriterLogicalProcessorOffset);
        _bloomPersistLogicalProcessor = pinBloomPersistThread ? NormalizeLogicalProcessor(bloomPersistLogicalProcessor) : null;
        _bloomWarmupLogicalProcessor = pinBloomWarmupThread ? NormalizeLogicalProcessor(bloomWarmupLogicalProcessor) : null;
        _slotScrubberLogicalProcessor = pinSlotScrubberThread ? NormalizeLogicalProcessor(slotScrubberLogicalProcessor) : null;

        _rootPath = rootPath;
        _shardCount = shardCount;
        _slotsPerShard = slotsPerShard;
        _maxLoadFactorPercent = maxLoadFactorPercent;
        _queueCapacityPerShard = queueCapacityPerShard;
        _maxRetainedGenerations = maxRetainedGenerations;
        _bloomMemoryFraction = bloomMemoryFraction;
        _bloomTargetBitsPerEntry = bloomTargetBitsPerEntry;
        _configuredBloomMemoryBudgetBytes = bloomMemoryBudgetBytes;
        _bloomCheckpointInsertInterval = bloomCheckpointInsertInterval;
        _bloomCheckpointMaxSnapshotBytes = bloomCheckpointMaxSnapshotBytes;
        _proactiveRolloverUsedSlotsThreshold = RolloverPolicy.ComputeUsedSlotsThreshold(slotsPerShard, proactiveRolloverLoadFactorPercent);
        _proactiveRolloverQueueDepthThreshold = RolloverPolicy.ComputeQueueDepthThreshold(queueCapacityPerShard, proactiveRolloverQueueWatermarkPercent);
        _proactiveRolloverAggregateProbeFailuresThreshold = proactiveRolloverAggregateProbeFailuresThreshold;
        _slotScrubberEnabled = enableBackgroundSlotScrubber;
        _slotScrubberSamplesPerTick = slotScrubberSamplesPerTick;
        _slotScrubberIntervalMilliseconds = slotScrubberIntervalMilliseconds;
        _slotScrubberBloomRepairOnMismatch = slotScrubberInvalidateBloomOnOccupiedMismatch;

        EnsureStarted();
        LogInitialized(
            _rootPath,
            _shardCount,
            _slotsPerShard,
            _maxLoadFactorPercent);
    }

    /// <summary>
    /// Initializes writer using strongly typed options.
    /// </summary>
    /// <param name="options">Writer configuration options.</param>
    /// <param name="logger">Optional structured logger.</param>
    public ShardedHistoryWriter(HistoryWriterOptions options, ILogger? logger = null)
        : this(
            rootPath: options.RootPath,
            shardCount: options.ShardCount,
            slotsPerShard: options.SlotsPerShard,
            maxLoadFactorPercent: options.MaxLoadFactorPercent,
            queueCapacityPerShard: options.QueueCapacityPerShard,
            maxRetainedGenerations: options.MaxRetainedGenerations,
            bloomMemoryFraction: options.Bloom.MemoryFraction,
            bloomTargetBitsPerEntry: options.Bloom.TargetBitsPerEntry,
            bloomMemoryBudgetBytes: options.Bloom.MemoryBudgetBytes,
            bloomCheckpointInsertInterval: options.Bloom.CheckpointInsertInterval,
            bloomCheckpointMaxSnapshotBytes: options.Bloom.CheckpointMaxSnapshotBytes,
            proactiveRolloverLoadFactorPercent: options.Rollover.ProactiveLoadFactorPercent,
            proactiveRolloverQueueWatermarkPercent: options.Rollover.ProactiveQueueWatermarkPercent,
            proactiveRolloverAggregateProbeFailuresThreshold: options.Rollover.ProactiveAggregateProbeFailuresThreshold,
            enableBackgroundSlotScrubber: options.SlotScrubber.Enabled,
            slotScrubberSamplesPerTick: options.SlotScrubber.SamplesPerTick,
            slotScrubberIntervalMilliseconds: options.SlotScrubber.IntervalMilliseconds,
            slotScrubberInvalidateBloomOnOccupiedMismatch: options.SlotScrubber.InvalidateBloomOnOccupiedMismatch,
            pinShardWriterThreads: options.Affinity.PinShardWriterThreads,
            preferNumaNodeOrderingForShardPinsOnWindows: options.Affinity.PreferNumaNodeOrderingForShardPinsOnWindows,
            shardWriterLogicalProcessorOffset: options.Affinity.ShardWriterLogicalProcessorOffset,
            pinBloomPersistThread: options.Affinity.PinBloomPersistThread,
            bloomPersistLogicalProcessor: options.Affinity.BloomPersistLogicalProcessor,
            pinBloomWarmupThread: options.Affinity.PinBloomWarmupThread,
            bloomWarmupLogicalProcessor: options.Affinity.BloomWarmupLogicalProcessor,
            pinSlotScrubberThread: options.Affinity.PinSlotScrubberThread,
            slotScrubberLogicalProcessor: options.Affinity.SlotScrubberLogicalProcessor,
            logger: logger)
    {
    }

    #endregion

    internal ValueTask<ShardInsertResult> EnqueueAsync(ShardInsertRequest request, CancellationToken cancellationToken = default) =>
        EnqueueAsync(request.HashHi, request.HashLo, request.ServerHi, request.ServerLo, cancellationToken);

    private static int[]? BuildShardWriterProcessorAssignment(int shardCount, bool pin, bool preferNumaWindows, int offset)
    {
        if (!pin)
        {
            return null;
        }

        int cpuCount = Environment.ProcessorCount;
        if (cpuCount <= 0)
        {
            return null;
        }

        int[] assignment = new int[shardCount];
        int[]? numaOrder = null;
        if (preferNumaWindows && OperatingSystem.IsWindows() &&
            WindowsNumaProcessorOrder.TryGetProcessorsNumaNodeOrderGroup0(out int[]? order) &&
            order is not null &&
            order.Length > 0)
        {
            numaOrder = order;
        }

        for (int shardId = 0; shardId < shardCount; shardId++)
        {
            int idx = shardId + offset;
            if (numaOrder is not null)
            {
                assignment[shardId] = numaOrder[idx % numaOrder.Length];
            }
            else
            {
                assignment[shardId] = ((idx % cpuCount) + cpuCount) % cpuCount;
            }
        }

        return assignment;
    }

    private static int NormalizeLogicalProcessor(int indexOrNegativeForLast)
    {
        int n = Environment.ProcessorCount;
        if (n <= 0)
        {
            return 0;
        }

        if (indexOrNegativeForLast < 0)
        {
            return n - 1;
        }

        int m = indexOrNegativeForLast % n;
        return m < 0 ? m + n : m;
    }

    private void TryApplyShardWriterAffinity(int shardId)
    {
        if (_shardWriterLogicalProcessors is null)
        {
            return;
        }

        int p = _shardWriterLogicalProcessors[shardId];
        if (CpuAffinity.TryPinCurrentThreadToLogicalProcessor(p))
        {
            return;
        }

        if (Interlocked.CompareExchange(ref _shardWriterPinFailureLogged, 1, 0) == 0)
        {
            LogShardPinFailed(
                shardId,
                p);
        }
    }

    private void TryApplyBloomPersistAffinity()
    {
        if (_bloomPersistLogicalProcessor is not int p)
        {
            return;
        }

        _ = CpuAffinity.TryPinCurrentThreadToLogicalProcessor(p);
    }

    private void TryApplyBloomWarmupAffinity()
    {
        if (_bloomWarmupLogicalProcessor is not int p)
        {
            return;
        }

        _ = CpuAffinity.TryPinCurrentThreadToLogicalProcessor(p);
    }

    private void TryApplySlotScrubberAffinity()
    {
        if (_slotScrubberLogicalProcessor is not int p)
        {
            return;
        }

        _ = CpuAffinity.TryPinCurrentThreadToLogicalProcessor(p);
    }

    private void EnsureStarted()
    {
        ThrowIfDisposed();

        lock (_lifecycleLock)
        {
            if (_started)
            {
                return;
            }

            Directory.CreateDirectory(_rootPath);
            CreateAndActivateGeneration_NoLock(1);
            _started = true;
            LogStarted(_activeGenerationId);
            if (_slotScrubberEnabled)
            {
                _slotScrubTask ??= Task.Run(() => RunSlotScrubberLoopAsync(_stopCts.Token), _stopCts.Token);
                LogSlotScrubberStarted();
            }
        }
    }

    /// <summary>
    /// Enqueues an insert-or-dedupe operation on the shard selected by <paramref name="hashLo"/>, with automatic generation rollover on capacity signals.
    /// </summary>
    /// <param name="hashHi">High 64 bits of the content hash.</param>
    /// <param name="hashLo">Low 64 bits of the content hash.</param>
    /// <param name="serverHi">Opaque payload high 64 bits stored when inserted.</param>
    /// <param name="serverLo">Opaque payload low 64 bits stored when inserted.</param>
    /// <param name="cancellationToken">Cancellation while waiting on channel back-pressure.</param>
    /// <returns>Completion with the shard outcome.</returns>
    /// <remarks>
    /// <para>
    /// <b>Failure handling:</b> Exceptions from the writer pipeline propagate to the caller; <see cref="OperationCanceledException"/> is used for cooperative cancellation.
    /// </para>
    /// </remarks>
    internal ValueTask<ShardInsertResult> EnqueueAsync(ulong hashHi, ulong hashLo, ulong serverHi, ulong serverLo, CancellationToken cancellationToken = default)
    {
        try
        {
            ThrowIfDisposed();
            EnsureStarted();
            return AwaitBenchResult(EnqueueWithRolloverAsync(hashHi, hashLo, serverHi, serverLo, captureTiming: false, cancellationToken));
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            LogEnqueueFailedCritical(ex);
            return ValueTask.FromResult(ShardInsertResult.Full);
        }
    }

    /// <summary>
    /// Benchmark-only enqueue that returns queue-wait vs shard-writer execution ticks for requests that reach the per-shard channel.
    /// </summary>
    internal async ValueTask<ShardInsertBenchmarkResult> EnqueueForBenchmarkAsync(
        ulong hashHi,
        ulong hashLo,
        ulong serverHi,
        ulong serverLo,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        EnsureStarted();
        return await EnqueueWithRolloverAsync(hashHi, hashLo, serverHi, serverLo, captureTiming: true, cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask<ShardInsertResult> AwaitBenchResult(ValueTask<ShardInsertBenchmarkResult> pending)
    {
        ShardInsertBenchmarkResult bench = await pending.ConfigureAwait(false);
        return bench.Result;
    }

    /// <summary>
    /// Tests whether a hash is present in any retained generation, consulting Bloom filters when trusted.
    /// </summary>
    /// <param name="hashHi">High 64 bits of the content hash.</param>
    /// <param name="hashLo">Low 64 bits of the content hash.</param>
    /// <returns><see langword="true"/> if a matching occupied slot exists in any generation/shard scan path.</returns>
    /// <remarks>
    /// <para>
    /// <b>Thread safety:</b> Safe concurrent with writers; reads do not take the lifecycle lock across shard probes.
    /// </para>
    /// </remarks>
    internal bool Exists(ulong hashHi, ulong hashLo)
    {
        try
        {
            ThrowIfDisposed();
            EnsureStarted();

            int shardId = GetShardId(hashLo);
            foreach (GenerationState generation in GetGenerationsSnapshotNewestFirst())
            {
                if (generation.BloomShardTrusted[shardId] && !generation.BloomFilters[shardId].MayContain(hashHi, hashLo))
                {
                    continue;
                }

                if (generation.Shards[shardId].Exists(hashHi, hashLo))
                {
                    return true;
                }
            }

            return false;
        }
        catch (Exception ex)
        {
            LogExistsFailed(ex);
            return false;
        }
    }

    private async ValueTask<ShardInsertBenchmarkResult> EnqueueWithRolloverAsync(
        ulong hashHi,
        ulong hashLo,
        ulong serverHi,
        ulong serverLo,
        bool captureTiming,
        CancellationToken cancellationToken)
    {
        int shardId = GetShardId(hashLo);

        for (int attempt = 0; attempt < MaxRolloverRetries; attempt++)
        {
            GenerationState generation = GetActiveGeneration();

            if (ExistsInGenerationsBeforeOrEqual(generation.GenerationId, shardId, hashHi, hashLo))
            {
                return new ShardInsertBenchmarkResult(ShardInsertResult.Duplicate, 0, 0);
            }

            ShardCompletionSource completion = RentCompletion();
            try
            {
                completion.Reset();
                ValueTask<ShardInsertResult> pending = completion.AsValueTask();
                ShardWriteRequest request = new(hashHi, hashLo, serverHi, serverLo, completion, null);

                try
                {
                    Interlocked.Increment(ref generation.PendingWriteApprox[shardId]);
                    try
                    {
                        MaybeProactiveRolloverFromQueueDepth(generation, shardId);
                        if (captureTiming)
                        {
                            request = request with { ChannelWriteStartTicks = Stopwatch.GetTimestamp() };
                        }

                        await generation.Queues[shardId].Writer.WriteAsync(request, cancellationToken).ConfigureAwait(false);
                    }
                    catch
                    {
                        Interlocked.Decrement(ref generation.PendingWriteApprox[shardId]);
                        throw;
                    }

                    ShardInsertResult result = await pending.ConfigureAwait(false);
                    long queueTicks = captureTiming ? completion.BenchQueueWaitTicks : 0;
                    long execTicks = captureTiming ? completion.BenchWriterExecutionTicks : 0;
                    if (result is ShardInsertResult.Inserted or ShardInsertResult.Duplicate)
                    {
                        return new ShardInsertBenchmarkResult(result, queueTicks, execTicks);
                    }

                    EnsureNextGeneration(generation.GenerationId);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    LogEnqueueAttemptFailed(ex, generation.GenerationId, shardId, attempt);
                    throw;
                }
            }
            finally
            {
                ReturnCompletion(completion, reset: false);
            }
        }

        return new ShardInsertBenchmarkResult(ShardInsertResult.Full, 0, 0);
    }

    /// <summary>
    /// Requests cooperative shutdown: completes shard channels, awaits writers and warm-up, persists Bloom filters, and disposes mmap views.
    /// </summary>
    /// <returns>A task that completes when shutdown work has finished.</returns>
    /// <remarks>
    /// <para>
    /// <b>Idempotent:</b> Repeated calls return the same logical completion; cancellation is driven by an internal token source.
    /// </para>
    /// </remarks>
    internal Task StopAsync()
    {
        if (!_started)
        {
            return Task.CompletedTask;
        }

        lock (_stopTaskLock)
        {
            _stopTask ??= StopInternalAsync();
        }

        LogStopRequested();
        return _stopTask;
    }

    private async Task StopInternalAsync()
    {
        _stopCts.Cancel();

        GenerationState[] generations;
        Task[] retiredCleanupTasks;
        lock (_lifecycleLock)
        {
            generations = [.. _generations];
            retiredCleanupTasks = [.. _retiredGenerationCleanupTasks];
        }

        foreach (GenerationState generation in generations)
        {
            for (int shardId = 0; shardId < _shardCount; shardId++)
            {
                generation.Queues[shardId].Writer.TryComplete();
            }
        }

        try
        {
            List<Task> allTasks = [];
            foreach (GenerationState generation in generations)
            {
                if (generation.WarmupTask is not null)
                {
                    allTasks.Add(generation.WarmupTask);
                }

                allTasks.AddRange(generation.WriterTasks);
            }

            allTasks.AddRange(retiredCleanupTasks);

            if (_slotScrubTask is not null)
            {
                allTasks.Add(_slotScrubTask);
            }

            await Task.WhenAll(allTasks).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown.
        }
        catch (Exception ex)
        {
            LogShutdownWaitFailed(ex);
        }

        await CompleteBloomPersistAsync().ConfigureAwait(false);

        foreach (GenerationState generation in generations)
        {
            PersistGenerationBlooms(generation);
            for (int shardId = 0; shardId < _shardCount; shardId++)
            {
                try
                {
                    generation.Shards[shardId].Dispose();
                }
                catch (Exception ex)
                {
                    LogShardDisposeFailedDuringShutdown(
                        ex,
                        generation.GenerationId,
                        shardId);
                }
            }
        }
        LogStopped();
    }

    private async Task RunShardWriterAsync(GenerationState generation, int shardId, CancellationToken stoppingToken)
    {
        TryApplyShardWriterAffinity(shardId);
        ChannelReader<ShardWriteRequest> reader = generation.Queues[shardId].Reader;
        HistoryShard shard = generation.Shards[shardId];
        ulong localUsedSlots = shard.UsedSlots;
        ulong insertsSinceBloomCheckpoint = 0;

        try
        {
            while (await reader.WaitToReadAsync(stoppingToken).ConfigureAwait(false))
            {
                while (reader.TryRead(out ShardWriteRequest request))
                {
                    Interlocked.Decrement(ref generation.PendingWriteApprox[shardId]);
                    long dequeuedTicks = Stopwatch.GetTimestamp();
                    long queueWaitTicks = 0;
                    if (request.ChannelWriteStartTicks is long writeStartTicks)
                    {
                        queueWaitTicks = dequeuedTicks - writeStartTicks;
                    }

                    long execStartTicks = Stopwatch.GetTimestamp();
                    ShardInsertResult result = localUsedSlots >= shard.LoadLimitSlots
                        ? ShardInsertResult.Full
                        : shard.TryExistsOrInsertUnchecked(request.HashHi, request.HashLo, request.ServerHi, request.ServerLo);
                    long execEndTicks = Stopwatch.GetTimestamp();
                    if (request.ChannelWriteStartTicks is not null)
                    {
                        request.Completion.SetBenchTiming(queueWaitTicks, execEndTicks - execStartTicks);
                    }

                    if (result == ShardInsertResult.Inserted)
                    {
                        generation.BloomFilters[shardId].Add(request.HashHi, request.HashLo);
                        generation.BloomShardTrusted[shardId] = true;
                        localUsedSlots++;
                        MaybeProactiveRolloverFromUsedSlots(generation, localUsedSlots);
                        if ((localUsedSlots & UsedSlotsFlushMask) == 0)
                        {
                            shard.FlushUsedSlots(localUsedSlots);
                        }

                        if (_bloomCheckpointInsertInterval != 0)
                        {
                            insertsSinceBloomCheckpoint++;
                            if (insertsSinceBloomCheckpoint >= _bloomCheckpointInsertInterval)
                            {
                                TryEnqueueBloomPersist(generation.BloomPaths[shardId], generation.BloomFilters[shardId]);
                                insertsSinceBloomCheckpoint = 0;
                            }
                        }

                        request.Completion.SetResult(ShardInsertResult.Inserted);
                        continue;
                    }

                    if (result == ShardInsertResult.Full)
                    {
                        Interlocked.Increment(ref generation.FullInsertFailures[shardId]);
                        request.Completion.SetResult(ShardInsertResult.Full);
                        continue;
                    }

                    if (result == ShardInsertResult.ProbeLimitExceeded)
                    {
                        Interlocked.Increment(ref generation.ProbeLimitFailures[shardId]);
                        MaybeProactiveRolloverFromAggregateProbeFailures(generation);
                        request.Completion.SetResult(ShardInsertResult.ProbeLimitExceeded);
                        continue;
                    }

                    request.Completion.SetResult(result);
                }

                MaybeProactiveRolloverFromQueueDepth(generation, shardId);
            }
        }
        catch (OperationCanceledException)
        {
            DrainPendingRequestsAsCanceled(reader, generation, shardId, stoppingToken);
        }
        catch (Exception ex)
        {
            LogShardWriterCrashed(ex, generation.GenerationId, shardId);
            DrainPendingRequestsAsFault(reader, generation, shardId, ex);
        }
        finally
        {
            shard.FlushUsedSlots(localUsedSlots);
        }
    }

    /// <summary>
    /// Returns monotonic failure counters for the active generation and given shard index.
    /// </summary>
    /// <param name="shardId">Zero-based shard index.</param>
    /// <returns>Counts of full-table and probe-limit outcomes since generation activation.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="shardId"/> is outside the configured shard count.</exception>
    internal (long fullInsertFailures, long probeLimitFailures) GetShardFailureCounts(int shardId)
    {
        GenerationState generation = GetActiveGeneration();
        if ((uint)shardId >= (uint)generation.FullInsertFailures.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(shardId));
        }

        return (
            Interlocked.Read(ref generation.FullInsertFailures[shardId]),
            Interlocked.Read(ref generation.ProbeLimitFailures[shardId]));
    }

    /// <summary>
    /// Returns per-shard pressure metrics for the active generation (approximate queue backlog, header used slots, failure counters).
    /// </summary>
    /// <returns>A new array with one row per configured shard.</returns>
    /// <remarks>
    /// <para>
    /// <b>Thread safety:</b> Values are a best-effort snapshot under concurrent writers; use for telemetry and coarse backpressure, not strict linearizability proofs.
    /// </para>
    /// </remarks>
    internal IReadOnlyList<ShardPressureSnapshot> GetActiveGenerationShardPressure()
    {
        GenerationState generation = GetActiveGeneration();
        ShardPressureSnapshot[] rows = new ShardPressureSnapshot[_shardCount];
        for (int shardId = 0; shardId < _shardCount; shardId++)
        {
            long pendingLong = Interlocked.Read(ref generation.PendingWriteApprox[shardId]);
            int pending = pendingLong > int.MaxValue ? int.MaxValue : (int)Math.Max(0L, pendingLong);
            rows[shardId] = new ShardPressureSnapshot(
                shardId,
                pending,
                generation.Shards[shardId].UsedSlots,
                Interlocked.Read(ref generation.FullInsertFailures[shardId]),
                Interlocked.Read(ref generation.ProbeLimitFailures[shardId]));
        }

        return rows;
    }

    /// <summary>
    /// Returns the shard index with the largest combined full + probe-limit failure counts on the active generation (hot-shard hint).
    /// </summary>
    /// <returns>Shard id in <c>[0, shardCount)</c>; when all counters are zero, returns <c>0</c>.</returns>
    internal int GetHottestShardIdByFailurePressure()
    {
        GenerationState generation = GetActiveGeneration();
        int bestShard = 0;
        long bestScore = long.MinValue;
        for (int shardId = 0; shardId < _shardCount; shardId++)
        {
            long score = Interlocked.Read(ref generation.FullInsertFailures[shardId]) +
                         Interlocked.Read(ref generation.ProbeLimitFailures[shardId]);
            if (score > bestScore)
            {
                bestScore = score;
                bestShard = shardId;
            }
        }

        return bestShard;
    }

    internal WriterInternalMetrics GetInternalMetrics()
    {
        GenerationState generation = GetActiveGeneration();
        long full = 0;
        long probe = 0;
        long pending = 0;
        ulong used = 0;
        for (int shardId = 0; shardId < _shardCount; shardId++)
        {
            full += Interlocked.Read(ref generation.FullInsertFailures[shardId]);
            probe += Interlocked.Read(ref generation.ProbeLimitFailures[shardId]);
            pending += Interlocked.Read(ref generation.PendingWriteApprox[shardId]);
            used += generation.Shards[shardId].UsedSlots;
        }

        return new WriterInternalMetrics(
            _activeGenerationId,
            _generations.Count,
            full,
            probe,
            pending,
            used);
    }

    /// <summary>
    /// Returns monotonic counters maintained by the optional background slot scrubber.
    /// </summary>
    /// <returns>Snapshot of scrub activity; all zeros when the scrubber was not enabled at construction.</returns>
    internal SlotScrubberCounters GetSlotScrubberCounters() =>
        new(
            Interlocked.Read(ref _scrubSamplesTotal),
            Interlocked.Read(ref _scrubVolatileMismatches),
            Interlocked.Read(ref _scrubBloomMismatchRepairs),
            Interlocked.Read(ref _scrubOccupiedBeyondWatermarkHints));

    private GenerationState GetActiveGeneration()
    {
        lock (_lifecycleLock)
        {
            return _generations[^1];
        }
    }

    private void EnsureNextGeneration(int seenGenerationId)
    {
        lock (_lifecycleLock)
        {
            if (_activeGenerationId != seenGenerationId)
            {
                return;
            }

            CreateAndActivateGeneration_NoLock(seenGenerationId + 1);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void MaybeProactiveRolloverFromUsedSlots(GenerationState generation, ulong localUsedSlots)
    {
        ulong th = _proactiveRolloverUsedSlotsThreshold;
        if (!RolloverPolicy.ShouldRolloverFromUsedSlots(localUsedSlots, th))
        {
            return;
        }

        EnsureNextGeneration(generation.GenerationId);
    }

    private void MaybeProactiveRolloverFromQueueDepth(GenerationState generation, int shardId)
    {
        int depthThreshold = _proactiveRolloverQueueDepthThreshold;
        long pending = Interlocked.Read(ref generation.PendingWriteApprox[shardId]);
        if (RolloverPolicy.ShouldRolloverFromQueueDepth(pending, depthThreshold))
        {
            EnsureNextGeneration(generation.GenerationId);
        }
    }

    private void MaybeProactiveRolloverFromAggregateProbeFailures(GenerationState generation)
    {
        long threshold = _proactiveRolloverAggregateProbeFailuresThreshold;
        if (threshold <= 0)
        {
            return;
        }

        long sum = 0;
        for (int i = 0; i < generation.ProbeLimitFailures.Length; i++)
        {
            sum += Interlocked.Read(ref generation.ProbeLimitFailures[i]);
            if (sum >= threshold)
            {
                EnsureNextGeneration(generation.GenerationId);
                return;
            }
        }
    }

    private static ulong PickRandomSlotIndexForScrub(ulong tableSize)
    {
        if (tableSize == 0)
        {
            return 0;
        }

        return (ulong)(Random.Shared.NextDouble() * tableSize);
    }

    private async Task RunSlotScrubberLoopAsync(CancellationToken cancellationToken)
    {
        TryApplySlotScrubberAffinity();
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_slotScrubberIntervalMilliseconds, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            GenerationState[] snapshot = GetGenerationSnapshotForScrub();
            if (snapshot.Length == 0)
            {
                continue;
            }

            for (int i = 0; i < _slotScrubberSamplesPerTick; i++)
            {
                RunOneScrubSample(snapshot);
            }
        }
    }

    private GenerationState[] GetGenerationSnapshotForScrub()
    {
        lock (_lifecycleLock)
        {
            if (!_started || _generations.Count == 0)
            {
                return [];
            }

            return [.. _generations];
        }
    }

    private void RunOneScrubSample(GenerationState[] snapshot)
    {
        GenerationState generation = snapshot[Random.Shared.Next(snapshot.Length)];
        if (generation.IsRetired)
        {
            return;
        }

        int shardId = Random.Shared.Next(_shardCount);
        HistoryShard shard;
        try
        {
            shard = generation.Shards[shardId];
        }
        catch (IndexOutOfRangeException)
        {
            return;
        }

        ulong index = PickRandomSlotIndexForScrub(shard.TableSize);
        Interlocked.Increment(ref _scrubSamplesTotal);
        SlotScrubOutcome outcome;
        try
        {
            _ = shard.TryScrubSlotLayout(index, out outcome);
        }
        catch (ObjectDisposedException)
        {
            return;
        }

        if (outcome == SlotScrubOutcome.VolatileLayoutMismatch)
        {
            Interlocked.Increment(ref _scrubVolatileMismatches);
            LogSlotScrubMismatch(
                generation.GenerationId,
                shardId,
                index);
            return;
        }

        if (outcome != SlotScrubOutcome.OkOccupiedStable)
        {
            return;
        }

        ulong watermark = shard.MaxOccupiedSlotIndex;
        if (index > watermark)
        {
            Interlocked.Increment(ref _scrubOccupiedBeyondWatermarkHints);
        }

        if (!shard.TryReadHashAt(index, out ulong hashHi, out ulong hashLo))
        {
            return;
        }

        ScheduleBloomRepairFromOccupiedMismatch(generation, shardId, hashHi, hashLo);
    }

    private void ScheduleBloomRepairFromOccupiedMismatch(GenerationState generation, int shardId, ulong hashHi, ulong hashLo)
    {
        if (!_slotScrubberBloomRepairOnMismatch)
        {
            return;
        }

        if (!generation.BloomShardTrusted[shardId])
        {
            return;
        }

        BloomFilter64 filter = generation.BloomFilters[shardId];
        if (filter.MayContain(hashHi, hashLo))
        {
            return;
        }

        generation.BloomShardTrusted[shardId] = false;
        Interlocked.Increment(ref _scrubBloomMismatchRepairs);
        LogTrustedBloomMissed(
            generation.GenerationId,
            shardId);

        (int GenerationId, int ShardId) key = (generation.GenerationId, shardId);
        if (!_bloomRewarmInFlight.TryAdd(key, 0))
        {
            return;
        }

        GenerationState capturedGeneration = generation;
        _ = Task.Run(async () =>
        {
            try
            {
                await WarmupOneShardBloomAsync(capturedGeneration, shardId, _stopCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown.
            }
            catch (Exception ex)
            {
                LogBloomRewarmFailed(
                    ex,
                    capturedGeneration.GenerationId,
                    shardId);
            }
            finally
            {
                _bloomRewarmInFlight.TryRemove(key, out _);
            }
        });
    }

    private void CreateAndActivateGeneration_NoLock(int generationId)
    {
        GenerationState generation = CreateGeneration(generationId);
        _generations.Add(generation);
        _activeGenerationId = generationId;
        LogGenerationActivated(generationId);

        // Sweep finished retire tasks so the list does not grow unbounded over the lifetime of a long-running process.
        // Each retired task captures a GenerationState (shard arrays, queues, blooms) via closure; pruning lets the GC reclaim it.
        PruneCompletedRetiredCleanupTasks_NoLock();

        while (_generations.Count > _maxRetainedGenerations)
        {
            GenerationState retired = _generations[0];
            _generations.RemoveAt(0);
            _retiredGenerationCleanupTasks.Add(Task.Run(() => RetireGenerationAsync(retired)));
            LogRetireScheduled(retired.GenerationId);
        }
    }

    private void PruneCompletedRetiredCleanupTasks_NoLock()
    {
        if (_retiredGenerationCleanupTasks.Count == 0)
        {
            return;
        }

        int writeIndex = 0;
        for (int readIndex = 0; readIndex < _retiredGenerationCleanupTasks.Count; readIndex++)
        {
            Task task = _retiredGenerationCleanupTasks[readIndex];
            if (task.IsCompleted)
            {
                continue;
            }

            if (writeIndex != readIndex)
            {
                _retiredGenerationCleanupTasks[writeIndex] = task;
            }

            writeIndex++;
        }

        if (writeIndex < _retiredGenerationCleanupTasks.Count)
        {
            _retiredGenerationCleanupTasks.RemoveRange(writeIndex, _retiredGenerationCleanupTasks.Count - writeIndex);
        }
    }

    private GenerationState CreateGeneration(int generationId)
    {
        string generationToken = FormatGenerationToken(generationId);
        string generationRoot = Path.Combine(_rootPath, generationToken);
        Directory.CreateDirectory(generationRoot);

        StartBloomPersistLoopIfNeeded_NoLock();

        int bloomBitsPerShard = ComputeBloomBitsPerShard(nextGenerationCount: _generations.Count + 1);
        HistoryShard[] shards = new HistoryShard[_shardCount];
        Channel<ShardWriteRequest>[] queues = new Channel<ShardWriteRequest>[_shardCount];
        Task[] writerTasks = new Task[_shardCount];
        long[] fullInsertFailures = new long[_shardCount];
        long[] probeLimitFailures = new long[_shardCount];
        long[] pendingWriteApprox = new long[_shardCount];
        BloomFilter64[] bloomFilters = new BloomFilter64[_shardCount];
        string[] bloomPaths = new string[_shardCount];
        bool[] bloomShardTrusted = new bool[_shardCount];

        for (int shardId = 0; shardId < _shardCount; shardId++)
        {
            string shardToken = FormatShardSequenceToken(shardId);
            string shardPath = Path.Combine(generationRoot, $"{shardToken}.dat");
            string bloomPath = Path.Combine(generationRoot, $"{shardToken}.idx");

            shards[shardId] = new HistoryShard(shardPath, _slotsPerShard, _maxLoadFactorPercent);
            queues[shardId] = Channel.CreateBounded<ShardWriteRequest>(
                new BoundedChannelOptions(_queueCapacityPerShard)
                {
                    SingleReader = true,
                    SingleWriter = false,
                    AllowSynchronousContinuations = false,
                    FullMode = BoundedChannelFullMode.Wait
                });
            bloomPaths[shardId] = bloomPath;
            string bloomDiagTag = $"generation {generationToken}, shard-{shardToken}";
            bloomFilters[shardId] = BloomFilter64.TryLoadFromFile(
                    bloomPath,
                    bloomBitsPerShard,
                    BloomHashFunctions,
                    bloomDiagTag,
                    _logger,
                    out BloomFilter64? loadedFilter)
                ? loadedFilter!
                : new BloomFilter64(bloomBitsPerShard, BloomHashFunctions);
            bloomShardTrusted[shardId] = loadedFilter is not null;
        }

        GenerationState generation = new(
            generationId,
            shards,
            queues,
            writerTasks,
            fullInsertFailures,
            probeLimitFailures,
            pendingWriteApprox,
            bloomFilters,
            bloomPaths,
            bloomShardTrusted);
        for (int shardId = 0; shardId < _shardCount; shardId++)
        {
            int capturedShardId = shardId;
            writerTasks[shardId] = Task.Run(() => RunShardWriterAsync(generation, capturedShardId, _stopCts.Token), _stopCts.Token);
        }

        StartBloomWarmupIfNeeded(generation);

        return generation;
    }

    private static string FormatShardSequenceToken(int shardId)
    {
        if ((uint)shardId > 0xFFFF)
        {
            throw new ArgumentOutOfRangeException(nameof(shardId), "Shard sequence must be in range 0000..ffff.");
        }

        return shardId.ToString("x4");
    }

    private static string FormatGenerationToken(int generationId)
    {
        if (generationId < 0 || generationId > 0xFFFFF)
        {
            throw new ArgumentOutOfRangeException(nameof(generationId), "Generation id must be in range 00000..fffff.");
        }

        return generationId.ToString("x5");
    }

    private bool ExistsInGenerationsBeforeOrEqual(int generationId, int shardId, ulong hashHi, ulong hashLo)
    {
        foreach (GenerationState generation in GetGenerationsSnapshotNewestFirst())
        {
            if (generation.GenerationId > generationId)
            {
                continue;
            }

            if (generation.BloomShardTrusted[shardId] && !generation.BloomFilters[shardId].MayContain(hashHi, hashLo))
            {
                continue;
            }

            if (generation.Shards[shardId].Exists(hashHi, hashLo))
            {
                return true;
            }
        }

        return false;
    }

    private GenerationState[] GetGenerationsSnapshotNewestFirst()
    {
        lock (_lifecycleLock)
        {
            GenerationState[] snapshot = [.. _generations];
            Array.Reverse(snapshot);
            return snapshot;
        }
    }

    private int GetShardId(ulong hashLo) => (int)(hashLo % (ulong)_shardCount);

    private static void DrainPendingRequestsAsCanceled(ChannelReader<ShardWriteRequest> reader, GenerationState generation, int shardId, CancellationToken cancellationToken)
    {
        while (reader.TryRead(out ShardWriteRequest pending))
        {
            Interlocked.Decrement(ref generation.PendingWriteApprox[shardId]);
            pending.Completion.SetCanceled(cancellationToken);
        }
    }

    private static void DrainPendingRequestsAsFault(ChannelReader<ShardWriteRequest> reader, GenerationState generation, int shardId, Exception exception)
    {
        while (reader.TryRead(out ShardWriteRequest pending))
        {
            Interlocked.Decrement(ref generation.PendingWriteApprox[shardId]);
            pending.Completion.SetException(exception);
        }
    }

    private int ComputeBloomBitsPerShard(int nextGenerationCount)
    {
        ulong targetBitsByEntries = BloomSizingPolicy.ComputeTargetBitsByEntries(_slotsPerShard, _maxLoadFactorPercent, _bloomTargetBitsPerEntry);

        ulong budgetBytes = _configuredBloomMemoryBudgetBytes != 0
            ? _configuredBloomMemoryBudgetBytes
            : (ulong)(SystemMemoryReader.GetInstalledMemoryBytesForBloomBudget() * _bloomMemoryFraction);
        ulong budgetBitsPerShard = BloomSizingPolicy.ComputeBudgetBitsPerShard(budgetBytes, _shardCount, nextGenerationCount);
        ulong desiredBits = BloomSizingPolicy.SelectDesiredBits(targetBitsByEntries, budgetBitsPerShard, MinBloomBitsPerShard);

        return (int)BitOperationsUtil.RoundDownToPowerOfTwo(desiredBits);
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(ShardedHistoryWriter));
        }
    }

    internal void Sync()
    {
        ThrowIfDisposed();
        lock (_lifecycleLock)
        {
            foreach (GenerationState generation in _generations)
            {
                for (int shardId = 0; shardId < _shardCount; shardId++)
                {
                    generation.Shards[shardId].FlushUsedSlots(generation.Shards[shardId].UsedSlots);
                }

                PersistGenerationBlooms(generation);
            }
        }
    }

    internal void SetBloomCheckpointInsertInterval(ulong inserts)
    {
        _bloomCheckpointInsertInterval = inserts;
    }

    internal void SetRolloverThresholds(ulong usedSlotsThreshold, int queueDepthThreshold, long aggregateProbeFailuresThreshold)
    {
        _proactiveRolloverUsedSlotsThreshold = usedSlotsThreshold;
        _proactiveRolloverQueueDepthThreshold = queueDepthThreshold;
        _proactiveRolloverAggregateProbeFailuresThreshold = aggregateProbeFailuresThreshold;
    }

    internal void SetSlotScrubberTuning(int samplesPerTick, int intervalMilliseconds)
    {
        _slotScrubberSamplesPerTick = samplesPerTick;
        _slotScrubberIntervalMilliseconds = intervalMilliseconds;
    }

    /// <summary>
    /// Performs asynchronous disposal by awaiting <see cref="StopAsync"/> and releasing the internal cancellation source.
    /// </summary>
    /// <returns>A value task that completes when shutdown has finished.</returns>
    /// <remarks>
    /// <para>
    /// <b>Thread safety:</b> First successful caller runs shutdown; concurrent callers await the same stop work then return.
    /// </para>
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref _disposeOnce, 1, 0) != 0)
        {
            await StopAsync().ConfigureAwait(false);
            return;
        }

        try
        {
            await StopAsync().ConfigureAwait(false);
        }
        finally
        {
            _stopCts.Dispose();
            _disposed = true;
            GC.SuppressFinalize(this);
        }
    }

    /// <summary>
    /// Synchronously disposes the writer by blocking on <see cref="DisposeAsync"/> when necessary.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Thread safety:</b> Avoid calling from a synchronization context that must not block; prefer <see cref="DisposeAsync"/>.
    /// </para>
    /// </remarks>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        ValueTask vt = DisposeAsync();
        if (vt.IsCompletedSuccessfully)
        {
            return;
        }

        Task task = vt.AsTask();
        if (SynchronizationContext.Current is not null)
        {
            Task.Run(() => task.ConfigureAwait(false).GetAwaiter().GetResult()).GetAwaiter().GetResult();
        }
        else
        {
            task.ConfigureAwait(false).GetAwaiter().GetResult();
        }
    }

    private ShardCompletionSource RentCompletion()
    {
        if (_completionPool.TryPop(out ShardCompletionSource? rented))
        {
            Interlocked.Decrement(ref _completionPooledApprox);
            return rented;
        }

        return new ShardCompletionSource();
    }

    private void ReturnCompletion(ShardCompletionSource completion, bool reset = true)
    {
        int reserved = Interlocked.Increment(ref _completionPooledApprox);
        if (reserved > CompletionPoolMax)
        {
            Interlocked.Decrement(ref _completionPooledApprox);
            return;
        }

        if (reset)
        {
            completion.Reset();
        }

        _completionPool.Push(completion);
    }

    private void StartBloomPersistLoopIfNeeded_NoLock()
    {
        lock (_bloomPersistStartLock)
        {
            if (_bloomPersistTask is not null)
            {
                return;
            }

            _bloomPersistChannel = Channel.CreateBounded<BloomPersistWork>(new BoundedChannelOptions(BloomPersistQueueCapacity)
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
                FullMode = BoundedChannelFullMode.DropOldest
            });
            _bloomPersistTask = Task.Run(() => RunBloomPersistLoopAsync(_bloomPersistChannel.Reader, _stopCts.Token));
        }
    }

    private async Task RunBloomPersistLoopAsync(ChannelReader<BloomPersistWork> reader, CancellationToken cancellationToken)
    {
        TryApplyBloomPersistAffinity();
        try
        {
            while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (reader.TryRead(out BloomPersistWork work))
                {
                    try
                    {
                        BloomFilter64.SaveWordsToFile(work.Path, work.Words, work.BitCount, work.HashCount);
                    }
                    catch (Exception ex)
                    {
                        LogBloomCheckpointWriteFailed(ex, work.Path);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            while (reader.TryRead(out BloomPersistWork work))
            {
                try
                {
                    BloomFilter64.SaveWordsToFile(work.Path, work.Words, work.BitCount, work.HashCount);
                }
                catch (Exception ex)
                {
                    LogBloomCheckpointWriteFailed(ex, work.Path);
                }
            }
        }
    }

    private void TryEnqueueBloomPersist(string path, BloomFilter64 filter)
    {
        ChannelWriter<BloomPersistWork>? writer = _bloomPersistChannel?.Writer;
        if (writer is null)
        {
            return;
        }

        ulong[]? words = filter.TrySnapshotWordsForPersist(_bloomCheckpointMaxSnapshotBytes);
        if (words is null)
        {
            LogBloomCheckpointSkipped(
                _bloomCheckpointMaxSnapshotBytes,
                path);
            return;
        }

        if (!writer.TryWrite(new BloomPersistWork(path, words, filter.BitCount, filter.HashFunctionCount)))
        {
            LogBloomCheckpointDropped(path);
        }
    }

    private async Task CompleteBloomPersistAsync()
    {
        Channel<BloomPersistWork>? channel = _bloomPersistChannel;
        Task? persistTask = _bloomPersistTask;
        if (channel is null || persistTask is null)
        {
            return;
        }

        channel.Writer.TryComplete();
        try
        {
            await persistTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown.
        }
        catch (Exception ex)
        {
            LogBloomPersistLoopFaulted(ex);
        }
    }

    private readonly record struct ShardWriteRequest(
        ulong HashHi,
        ulong HashLo,
        ulong ServerHi,
        ulong ServerLo,
        ShardCompletionSource Completion,
        long? ChannelWriteStartTicks);

    private readonly record struct BloomPersistWork(string Path, ulong[] Words, int BitCount, int HashCount);

    private sealed class ShardCompletionSource : IValueTaskSource<ShardInsertResult>
    {
        private ManualResetValueTaskSourceCore<ShardInsertResult> _core;
        public long BenchQueueWaitTicks { get; private set; }
        public long BenchWriterExecutionTicks { get; private set; }

        public ValueTask<ShardInsertResult> AsValueTask() => new(this, _core.Version);

        public void Reset()
        {
            BenchQueueWaitTicks = 0;
            BenchWriterExecutionTicks = 0;
            _core.Reset();
        }

        public void SetBenchTiming(long queueWaitTicks, long writerExecutionTicks)
        {
            BenchQueueWaitTicks = queueWaitTicks;
            BenchWriterExecutionTicks = writerExecutionTicks;
        }

        public void SetResult(ShardInsertResult value) => _core.SetResult(value);

        public void SetCanceled(CancellationToken cancellationToken) =>
            _core.SetException(new OperationCanceledException(cancellationToken));

        public void SetException(Exception exception) => _core.SetException(exception);

        ShardInsertResult IValueTaskSource<ShardInsertResult>.GetResult(short token) => _core.GetResult(token);

        ValueTaskSourceStatus IValueTaskSource<ShardInsertResult>.GetStatus(short token) => _core.GetStatus(token);

        void IValueTaskSource<ShardInsertResult>.OnCompleted(
            Action<object?> continuation,
            object? state,
            short token,
            ValueTaskSourceOnCompletedFlags flags) =>
            _core.OnCompleted(continuation, state, token, flags);
    }

    private sealed record GenerationState(
        int GenerationId,
        HistoryShard[] Shards,
        Channel<ShardWriteRequest>[] Queues,
        Task[] WriterTasks,
        long[] FullInsertFailures,
        long[] ProbeLimitFailures,
        long[] PendingWriteApprox,
        BloomFilter64[] BloomFilters,
        string[] BloomPaths,
        bool[] BloomShardTrusted)
    {
        public Task? WarmupTask { get; set; }
        public bool IsRetired { get; set; }
    }

    private void StartBloomWarmupIfNeeded(GenerationState generation)
    {
        bool anyUntrusted = false;
        for (int shardId = 0; shardId < generation.BloomShardTrusted.Length; shardId++)
        {
            if (!generation.BloomShardTrusted[shardId])
            {
                anyUntrusted = true;
                break;
            }
        }

        if (!anyUntrusted)
        {
            return;
        }

        generation.WarmupTask = Task.Run(() => WarmupGenerationBloomAsync(generation, _stopCts.Token), _stopCts.Token);
    }

    private async Task WarmupGenerationBloomAsync(GenerationState generation, CancellationToken cancellationToken)
    {
        TryApplyBloomWarmupAffinity();
        for (int shardId = 0; shardId < _shardCount; shardId++)
        {
            if (generation.BloomShardTrusted[shardId])
            {
                continue;
            }

            cancellationToken.ThrowIfCancellationRequested();
            await WarmupOneShardBloomAsync(generation, shardId, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task WarmupOneShardBloomAsync(GenerationState generation, int shardId, CancellationToken cancellationToken)
    {
        if (generation.IsRetired)
        {
            return;
        }

        HistoryShard shard = generation.Shards[shardId];
        BloomFilter64 bloom = generation.BloomFilters[shardId];
        ulong expectedUsedSlots = shard.UsedSlots;
        ulong discoveredSlots = 0;
        ulong scanUpperExclusive = shard.TableSize;
        ulong watermarkExclusive = shard.MaxOccupiedSlotIndex + 1;
        if (watermarkExclusive != 0 && watermarkExclusive < scanUpperExclusive)
        {
            scanUpperExclusive = watermarkExclusive;
        }

        for (ulong phase = 0; phase < 2; phase++)
        {
            ulong start = phase == 0 ? 0 : scanUpperExclusive;
            ulong endExclusive = phase == 0 ? scanUpperExclusive : shard.TableSize;

            for (ulong index = start; index < endExclusive; index++)
            {
                if ((index & WarmupYieldMask) == 0)
                {
                    await Task.Yield();
                    cancellationToken.ThrowIfCancellationRequested();
                }

                if (shard.TryReadHashAt(index, out ulong hashHi, out ulong hashLo))
                {
                    bloom.Add(hashHi, hashLo);
                    discoveredSlots++;
                    if (expectedUsedSlots != 0 && discoveredSlots >= expectedUsedSlots)
                    {
                        goto WarmupShardDone;
                    }
                }
            }

            if (expectedUsedSlots == 0 || discoveredSlots >= expectedUsedSlots || scanUpperExclusive >= shard.TableSize || phase == 1)
            {
                break;
            }
        }

    WarmupShardDone:

        try
        {
            BloomFilter64.SaveToFile(generation.BloomPaths[shardId], bloom);
            generation.BloomShardTrusted[shardId] = true;
        }
        catch (Exception ex)
        {
            LogBloomWarmupSaveFailed(
                ex,
                generation.GenerationId,
                shardId);
        }
    }

    private async Task RetireGenerationAsync(GenerationState generation)
    {
        if (generation.IsRetired)
        {
            return;
        }

        generation.IsRetired = true;

        for (int shardId = 0; shardId < _shardCount; shardId++)
        {
            generation.Queues[shardId].Writer.TryComplete();
        }

        List<Task> tasks = [];
        if (generation.WarmupTask is not null)
        {
            tasks.Add(generation.WarmupTask);
        }

        tasks.AddRange(generation.WriterTasks);

        try
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Normal during global shutdown.
        }
        catch (Exception ex)
        {
            LogRetireGenerationWaitFailed(ex, generation.GenerationId);
        }

        PersistGenerationBlooms(generation);

        for (int shardId = 0; shardId < _shardCount; shardId++)
        {
            try
            {
                generation.Shards[shardId].Dispose();
            }
            catch (Exception ex)
            {
                LogShardDisposeFailedDuringRetire(
                    ex,
                    generation.GenerationId,
                    shardId);
            }
        }
    }

    private void PersistGenerationBlooms(GenerationState generation)
    {
        for (int shardId = 0; shardId < generation.BloomFilters.Length; shardId++)
        {
            try
            {
                BloomFilter64.SaveToFile(generation.BloomPaths[shardId], generation.BloomFilters[shardId]);
            }
            catch (Exception ex)
            {
                LogBloomSaveFailed(
                    ex,
                    generation.GenerationId,
                    shardId);
            }
        }
    }

    private sealed class BloomFilter64
    {
        private readonly ulong[] _words;
        private readonly int _hashCount;
        private readonly int _bitMask;

        public int BitCount => _bitMask + 1;

        public int HashFunctionCount => _hashCount;

        public BloomFilter64(int bitCount, int hashCount)
        {
            if (bitCount <= 0 || (bitCount & (bitCount - 1)) != 0)
            {
                throw new ArgumentOutOfRangeException(nameof(bitCount), "bitCount must be a power of 2 and > 0.");
            }

            if (hashCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(hashCount), "hashCount must be > 0.");
            }

            _words = new ulong[bitCount >> 6];
            _hashCount = hashCount;
            _bitMask = bitCount - 1;
        }

        public static bool TryLoadFromFile(
            string path,
            int expectedBitCount,
            int expectedHashCount,
            string diagnosticsTag,
            ILogger logger,
            out BloomFilter64? filter)
        {
            filter = null;
            if (!File.Exists(path))
            {
                return false;
            }

            try
            {
                filter = LoadFromFile(path, expectedBitCount, expectedHashCount);
                return true;
            }
            catch (Exception ex)
            {
                LogBloomSidecarRejected(
                    logger,
                    ex,
                    diagnosticsTag,
                    path);
                return false;
            }
        }

        public static BloomFilter64 LoadFromFile(string path, int expectedBitCount, int expectedHashCount)
        {
            using FileStream stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            using BinaryReader reader = new(stream);

            uint magic = reader.ReadUInt32();
            uint version = reader.ReadUInt32();
            int bitCount = reader.ReadInt32();
            int hashCount = reader.ReadInt32();
            int wordCount = reader.ReadInt32();

            if (magic != BloomSidecarFile.FileMagic || version != BloomSidecarFile.FileVersion ||
                bitCount != expectedBitCount || hashCount != expectedHashCount ||
                wordCount != (expectedBitCount >> 6))
            {
                throw new InvalidDataException("Bloom sidecar metadata mismatch.");
            }

            BloomFilter64 filter = new(bitCount, hashCount);
            for (int i = 0; i < wordCount; i++)
            {
                filter._words[i] = reader.ReadUInt64();
            }

            return filter;
        }

        public static void SaveToFile(string path, BloomFilter64 filter)
        {
            ulong[] words = filter.CloneWordsLocked();
            SaveWordsToFile(path, words, filter.BitCount, filter.HashFunctionCount);
        }

        public static void SaveWordsToFile(string path, ulong[] words, int bitCount, int hashCount) =>
            BloomSidecarFile.WriteSidecar(path, words, bitCount, hashCount);

        public ulong[]? TrySnapshotWordsForPersist(ulong maxBytes)
        {
            ulong byteLength = (ulong)_words.LongLength * sizeof(ulong);
            if (byteLength > maxBytes)
            {
                return null;
            }

            return CloneWordsLocked();
        }

        private ulong[] CloneWordsLocked()
        {
            lock (this)
            {
                ulong[] copy = new ulong[_words.Length];
                Array.Copy(_words, copy, _words.Length);
                return copy;
            }
        }

        public void Add(ulong hashHi, ulong hashLo)
        {
            ulong h1 = Mix(hashLo);
            ulong h2 = Mix(hashHi ^ 0x9E3779B97F4A7C15UL) | 1UL;

            lock (this)
            {
                for (int i = 0; i < _hashCount; i++)
                {
                    ulong combined = h1 + ((ulong)i * h2);
                    int bitIndex = (int)(combined & (uint)_bitMask);
                    int wordIndex = bitIndex >> 6;
                    ulong bit = 1UL << (bitIndex & 63);
                    _words[wordIndex] |= bit;
                }
            }
        }

        public bool MayContain(ulong hashHi, ulong hashLo)
        {
            ulong h1 = Mix(hashLo);
            ulong h2 = Mix(hashHi ^ 0x9E3779B97F4A7C15UL) | 1UL;

            for (int i = 0; i < _hashCount; i++)
            {
                ulong combined = h1 + ((ulong)i * h2);
                int bitIndex = (int)(combined & (uint)_bitMask);
                int wordIndex = bitIndex >> 6;
                ulong bit = 1UL << (bitIndex & 63);
                if ((Volatile.Read(ref _words[wordIndex]) & bit) == 0)
                {
                    return false;
                }
            }

            return true;
        }

        private static ulong Mix(ulong x)
        {
            x ^= x >> 30;
            x *= 0xBF58476D1CE4E5B9UL;
            x ^= x >> 27;
            x *= 0x94D049BB133111EBUL;
            x ^= x >> 31;
            return x;
        }
    }

    private sealed class TraceFallbackLogger : ILogger
    {
        internal static readonly TraceFallbackLogger Instance = new();

        private TraceFallbackLogger()
        {
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NoopScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            string message = formatter(state, exception);
            if (exception is not null)
            {
                message = $"{message}{Environment.NewLine}{exception}";
            }

            if (logLevel >= LogLevel.Error)
            {
                Trace.TraceError("[HistoryDB] {0}", message);
                return;
            }

            if (logLevel == LogLevel.Warning)
            {
                Trace.TraceWarning("[HistoryDB] {0}", message);
                return;
            }

            Trace.TraceInformation("[HistoryDB] {0}", message);
        }
    }

    private sealed class NoopScope : IDisposable
    {
        internal static readonly NoopScope Instance = new();

        private NoopScope()
        {
        }

        public void Dispose()
        {
        }
    }

    internal readonly record struct WriterInternalMetrics(
        int ActiveGenerationId,
        int RetainedGenerations,
        long FullInsertFailures,
        long ProbeLimitFailures,
        long PendingQueueItemsApprox,
        ulong UsedSlotsApprox);

}
