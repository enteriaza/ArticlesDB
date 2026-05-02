// HistoryShard.cs -- memory-mapped 128-bit hash table shard with linear-then-secondary probing, optional SSE2 slot compares, and header v2 with MaxOccupiedSlotIndex watermark.
// Hash visibility uses hash_hi as the published flag (written last). Consumed by ShardedHistoryWriter per generation shard file. SSE2 is used when supported (typical Windows/Linux x64); otherwise scalar compares apply.

using System;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Threading;

using HistoryDB.Utilities;

namespace HistoryDB;

/// <summary>
/// Result of a best-effort volatile consistency check on one 32-byte mmap slot (hygiene / scrubber).
/// </summary>
internal enum SlotScrubOutcome
{
    /// <summary>Slot reads as empty (<c>hash_hi == 0</c>) with stable double-sampled quadwords.</summary>
    OkEmpty = 0,

    /// <summary>Slot reads as published occupied with stable layout across samples.</summary>
    OkOccupiedStable = 1,

    /// <summary>Two sampled views of the slot disagreed after bounded retries (possible tear, corruption, or extreme races).</summary>
    VolatileLayoutMismatch = 2
}

/// <summary>
/// Result of an insert-or-probe operation against a single mmap-backed shard.
/// </summary>
internal enum ShardInsertResult
{
    /// <summary>An empty slot was filled with the new hash and server payload.</summary>
    Inserted = 0,

    /// <summary>The hash was already present; no slot write occurred.</summary>
    Duplicate = 1,

    /// <summary>The shard is at its load limit before insert (see <see cref="HistoryShard.TryExistsOrInsert"/>).</summary>
    Full = 2,

    /// <summary>Linear/secondary probing exceeded the soft probe budget without resolving empty or match.</summary>
    ProbeLimitExceeded = 3
}

/// <summary>
/// Benchmark-only latency breakdown for inserts that traverse the shard writer queue.
/// Tick values use <see cref="System.Diagnostics.Stopwatch"/> frequency; convert with <c>ticks * (1e9 / Stopwatch.Frequency)</c> for nanoseconds.
/// </summary>
/// <param name="Result">Shard outcome reported to callers.</param>
/// <param name="QueueWaitTicks">Time from the start of <c>WriteAsync</c> into the shard channel until the writer dequeued the request.</param>
/// <param name="WriterExecutionTicks">Time spent in the shard writer thread executing the insert path for the dequeued request.</param>
internal readonly record struct ShardInsertBenchmarkResult(
    ShardInsertResult Result,
    long QueueWaitTicks,
    long WriterExecutionTicks);

/// <summary>
/// One generation shard: fixed-size open addressing table stored in a memory-mapped file with a small binary header.
/// </summary>
/// <remarks>
/// <para>
/// <b>Lifecycle:</b> Construct with a path and capacity; the file is created or opened and the header is validated or initialized. Call <see cref="Dispose"/> to release the view.
/// </para>
/// <para>
/// <b>Thread safety:</b> Concurrent readers with a single writer match the intended use (per-shard writer task). Multiple writers without external synchronization can corrupt slots or header counts.
/// </para>
/// <para>
/// <b>SIMD:</b> When <see cref="Sse2.IsSupported"/> is true, slot equality uses 128-bit compares; otherwise two scalar compares are used. Prefetch uses <see cref="Sse"/> when available.
/// </para>
/// <para>
/// <b>Hygiene:</b> <see cref="TryScrubSlotLayout"/> double-samples the 32-byte slot for volatile consistency (used by optional background scrubbers in <see cref="ShardedHistoryWriter"/>).
/// </para>
/// </remarks>
internal unsafe sealed class HistoryShard : IDisposable
{
    #region Constants
    private const ulong Magic = 0x3130424454534948UL; // "HISTDB01"
    private const ulong CurrentVersion = 3;

    private const int HeaderSize = 64;
    private const int EntrySize = 32;

    private const int HeaderMagicOffset = 0;
    private const int HeaderVersionOffset = 8;
    private const int HeaderTableSizeOffset = 16;
    private const int HeaderUsedSlotsOffset = 24;
    private const int HeaderMaxLoadFactorOffset = 32;
    private const int HeaderMaxOccupiedSlotIndexOffset = 40;
    private const int HeaderSlotRegionCrc32Offset = 48;
    private const ulong ProbeSoftLimit = 64;
    private const ulong SecondaryProbeStart = 16;
    private const ulong LinearProbeWindow = 4;
    private const ulong LinearProbeBatchSize = 2;
    private const int VectorAllBytesMatchMask = 0xFFFF;
    private const int VectorFirstLaneMatchMask = 0xFFFF;
    private const int VectorSecondLaneMatchMask = unchecked((int)0xFFFF0000);

    #endregion

    #region Fields

    private readonly MemoryMappedFile _mmf;
    private readonly MemoryMappedViewAccessor _accessor;
    private readonly byte* _basePtr;
    private readonly ulong _tableSize;
    private readonly ulong _mask;
    private readonly ulong _maxLoadFactor;
    private readonly ulong _loadLimitSlots;
    private readonly long _viewByteLength;
    private bool _disposed;

    #endregion

    #region Properties

    /// <summary>Number of 32-byte slots in the table (power of two).</summary>
    /// <remarks><para><b>Validation:</b> Fixed at construction; must match on-disk header when opening an existing file.</para></remarks>
    public ulong TableSize => _tableSize;

    /// <summary>Occupancy counter persisted in the header (writer-maintained).</summary>
    /// <remarks><para>May lag slightly between flushes; writers call <see cref="FlushUsedSlots"/> periodically.</para></remarks>
    public ulong UsedSlots => ReadUInt64(HeaderUsedSlotsOffset);

    /// <summary>Configured maximum load factor percentage used to compute <see cref="LoadLimitSlots"/>.</summary>
    public ulong MaxLoadFactor => _maxLoadFactor;

    /// <summary>Slot count at which inserts are rejected as <see cref="ShardInsertResult.Full"/> in checked paths.</summary>
    public ulong LoadLimitSlots => _loadLimitSlots;

    /// <summary>Highest slot index known to contain a published entry; used to bound warm-up scans.</summary>
    /// <remarks><para>Header v2 field; upgraded from v1 files on open.</para></remarks>
    public ulong MaxOccupiedSlotIndex => ReadUInt64(HeaderMaxOccupiedSlotIndexOffset);

    #endregion

    #region Constructors

    /// <summary>
    /// Opens or creates the mmap shard file at <paramref name="path"/> with the given logical table size.
    /// </summary>
    /// <param name="path">Absolute or relative path to the shard data file (normalized to a rooted path).</param>
    /// <param name="tableSize">Slot count; must be a power of two greater than zero.</param>
    /// <param name="maxLoadFactor">Maximum fill percentage before inserts return <see cref="ShardInsertResult.Full"/> (1..99).</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="tableSize"/> or <paramref name="maxLoadFactor"/> is invalid.</exception>
    /// <exception cref="InvalidDataException">Thrown when an existing file header does not match this instance's parameters.</exception>
    public HistoryShard(string path, ulong tableSize, ulong maxLoadFactor = 75)
    {
        path = WritableDirectory.ValidateMmapDataFilePath(path);

        if (!BitOperationsUtil.IsPowerOfTwo(tableSize))
        {
            throw new ArgumentOutOfRangeException(nameof(tableSize), "tableSize must be a power of 2 and > 0.");
        }

        if (maxLoadFactor == 0 || maxLoadFactor >= 100)
        {
            throw new ArgumentOutOfRangeException(nameof(maxLoadFactor), "maxLoadFactor must be in range [1, 99].");
        }

        Directory.CreateDirectory(Path.GetDirectoryName(path)!);

        _tableSize = tableSize;
        _mask = tableSize - 1;
        _maxLoadFactor = maxLoadFactor;
        _loadLimitSlots = (tableSize * maxLoadFactor) / 100;

        checked
        {
            long fileSize = HeaderSize + (long)tableSize * EntrySize;
            _viewByteLength = fileSize;
            _mmf = MemoryMappedFile.CreateFromFile(
                path,
                FileMode.OpenOrCreate,
                null,
                fileSize,
                MemoryMappedFileAccess.ReadWrite);

            _accessor = _mmf.CreateViewAccessor(0, fileSize, MemoryMappedFileAccess.ReadWrite);
        }

        byte* ptr = null;
        _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
        _basePtr = ptr + _accessor.PointerOffset;

        InitializeOrValidateHeader();
    }

    #endregion

    #region Public Methods

    /// <summary>
    /// Returns whether an occupied slot matches the given 128-bit hash (read-only probe).
    /// </summary>
    /// <param name="hashHi">High 64 bits of the content hash.</param>
    /// <param name="hashLo">Low 64 bits of the content hash.</param>
    /// <returns><see langword="true"/> if a matching entry exists within probe limits.</returns>
    /// <remarks>
    /// <para>
    /// <b>Thread safety:</b> Safe concurrent with a single writer if the writer follows the publish protocol (hash_hi written last).
    /// </para>
    /// </remarks>
    public bool Exists(ulong hashHi, ulong hashLo)
    {
        ThrowIfDisposed();

        if (hashHi == 0 && hashLo == 0)
        {
            return false;
        }

        ulong idx = hashLo & _mask;
        ulong secondaryStep = (hashHi | 1UL) & _mask;
        if (secondaryStep == 0)
        {
            secondaryStep = 1;
        }

        ulong probes = 0;

        // Fast path: probe first segment in small contiguous windows for better cache behavior.
        while (probes < _tableSize && probes < SecondaryProbeStart)
        {
            ulong remainingLinear = SecondaryProbeStart - probes;
            ulong window = remainingLinear < LinearProbeWindow ? remainingLinear : LinearProbeWindow;

            ulong i = 0;
            while ((i + LinearProbeBatchSize) <= window)
            {
                ulong firstIdx = (idx + i) & _mask;
                ulong secondIdx = (idx + i + 1) & _mask;
                SlotProbeResult batchProbe = ProbeSlotPair(firstIdx, secondIdx, hashHi, hashLo, out bool matchedFirstSlot);
                if (batchProbe == SlotProbeResult.Empty)
                {
                    return false;
                }

                if (batchProbe == SlotProbeResult.Match)
                {
                    return true;
                }

                i += LinearProbeBatchSize;
            }

            for (; i < window; i++)
            {
                ulong candidateIdx = (idx + i) & _mask;
                byte* slot = GetSlotPtr(candidateIdx);
                SlotProbeResult probe = ProbeSlot(slot, hashHi, hashLo);
                if (probe == SlotProbeResult.Empty)
                {
                    return false;
                }

                if (probe == SlotProbeResult.Match)
                {
                    return true;
                }
            }

            probes += window;
            idx = (idx + window) & _mask;
            PrefetchProbeSlot(idx, probes, secondaryStep);

            if (probes > ProbeSoftLimit)
            {
                return false;
            }
        }

        while (probes < _tableSize)
        {
            byte* slot = GetSlotPtr(idx);
            SlotProbeResult probe = ProbeSlot(slot, hashHi, hashLo);
            if (probe == SlotProbeResult.Empty)
            {
                return false;
            }

            if (probe == SlotProbeResult.Match)
            {
                return true;
            }

            PrefetchProbeSlot(idx, probes, secondaryStep);
            idx = probes >= SecondaryProbeStart
                ? (idx + secondaryStep) & _mask
                : (idx + 1) & _mask;
            probes++;

            if (probes > ProbeSoftLimit)
            {
                return false;
            }
        }

        return false;
    }

    /// <summary>
    /// Inserts if absent after checking the load limit, otherwise returns duplicate/full/probe outcomes.
    /// </summary>
    /// <param name="hashHi">High 64 bits of the content hash.</param>
    /// <param name="hashLo">Low 64 bits of the content hash.</param>
    /// <param name="serverHi">Opaque high 64 bits stored when inserted.</param>
    /// <param name="serverLo">Opaque low 64 bits stored when inserted.</param>
    /// <returns>The shard outcome; increments header used count on <see cref="ShardInsertResult.Inserted"/>.</returns>
    public ShardInsertResult TryExistsOrInsert(ulong hashHi, ulong hashLo, ulong serverHi, ulong serverLo)
    {
        ThrowIfDisposed();
        NormalizeZeroHash(hashHi, ref hashLo);

        ulong usedSlots = UsedSlots;
        if (usedSlots >= _loadLimitSlots)
        {
            return ShardInsertResult.Full;
        }

        ShardInsertResult result = TryExistsOrInsertUnchecked(hashHi, hashLo, serverHi, serverLo);
        if (result == ShardInsertResult.Inserted)
        {
            WriteUInt64(HeaderUsedSlotsOffset, usedSlots + 1);
        }

        return result;
    }

    /// <summary>
    /// Like <see cref="TryExistsOrInsert"/> but skips the load-limit pre-check; intended for callers that already track occupancy.
    /// </summary>
    /// <param name="hashHi">High 64 bits of the content hash.</param>
    /// <param name="hashLo">Low 64 bits of the content hash.</param>
    /// <param name="serverHi">Opaque high 64 bits stored when inserted.</param>
    /// <param name="serverLo">Opaque low 64 bits stored when inserted.</param>
    /// <returns>The shard outcome without mutating header used count here.</returns>
    public ShardInsertResult TryExistsOrInsertUnchecked(ulong hashHi, ulong hashLo, ulong serverHi, ulong serverLo)
    {
        ThrowIfDisposed();
        NormalizeZeroHash(hashHi, ref hashLo);

        ulong idx = hashLo & _mask;
        ulong secondaryStep = (hashHi | 1UL) & _mask;
        if (secondaryStep == 0)
        {
            secondaryStep = 1;
        }

        ulong probes = 0;

        // Fast path: probe first segment in contiguous windows before secondary-step mode.
        while (probes < _tableSize && probes < SecondaryProbeStart)
        {
            ulong remainingLinear = SecondaryProbeStart - probes;
            ulong window = remainingLinear < LinearProbeWindow ? remainingLinear : LinearProbeWindow;

            ulong i = 0;
            while ((i + LinearProbeBatchSize) <= window)
            {
                ulong firstIdx = (idx + i) & _mask;
                ulong secondIdx = (idx + i + 1) & _mask;
                SlotProbeResult batchProbe = ProbeSlotPair(firstIdx, secondIdx, hashHi, hashLo, out bool matchedFirstSlot);
                if (batchProbe == SlotProbeResult.Empty)
                {
                    ulong insertIdx = matchedFirstSlot ? secondIdx : firstIdx;
                    byte* slot = GetSlotPtr(insertIdx);
                    // Publish protocol: payload first, hash_hi last. Readers can safely treat hash_hi == 0 as EMPTY.
                    *(ulong*)(slot + 16) = serverHi;
                    *(ulong*)(slot + 24) = serverLo;
                    *(ulong*)(slot + 8) = hashLo;
                    Volatile.Write(ref *(ulong*)slot, hashHi);
                    TryAdvanceMaxOccupiedSlotIndex(insertIdx);
                    return ShardInsertResult.Inserted;
                }

                if (batchProbe == SlotProbeResult.Match)
                {
                    return ShardInsertResult.Duplicate;
                }

                i += LinearProbeBatchSize;
            }

            for (; i < window; i++)
            {
                ulong candidateIdx = (idx + i) & _mask;
                byte* slot = GetSlotPtr(candidateIdx);
                SlotProbeResult probe = ProbeSlot(slot, hashHi, hashLo);
                if (probe == SlotProbeResult.Empty)
                {
                    // Publish protocol: payload first, hash_hi last. Readers can safely treat hash_hi == 0 as EMPTY.
                    *(ulong*)(slot + 16) = serverHi;
                    *(ulong*)(slot + 24) = serverLo;
                    *(ulong*)(slot + 8) = hashLo;
                    Volatile.Write(ref *(ulong*)slot, hashHi);
                    TryAdvanceMaxOccupiedSlotIndex(candidateIdx);
                    return ShardInsertResult.Inserted;
                }

                if (probe == SlotProbeResult.Match)
                {
                    return ShardInsertResult.Duplicate;
                }
            }

            probes += window;
            idx = (idx + window) & _mask;
            PrefetchProbeSlot(idx, probes, secondaryStep);

            if (probes > ProbeSoftLimit)
            {
                return ShardInsertResult.ProbeLimitExceeded;
            }
        }

        while (probes < _tableSize)
        {
            byte* slot = GetSlotPtr(idx);
            SlotProbeResult probe = ProbeSlot(slot, hashHi, hashLo);
            if (probe == SlotProbeResult.Empty)
            {
                // Publish protocol: payload first, hash_hi last. Readers can safely treat hash_hi == 0 as EMPTY.
                *(ulong*)(slot + 16) = serverHi;
                *(ulong*)(slot + 24) = serverLo;
                *(ulong*)(slot + 8) = hashLo;
                Volatile.Write(ref *(ulong*)slot, hashHi);
                TryAdvanceMaxOccupiedSlotIndex(idx);
                return ShardInsertResult.Inserted;
            }

            if (probe == SlotProbeResult.Match)
            {
                return ShardInsertResult.Duplicate;
            }

            PrefetchProbeSlot(idx, probes, secondaryStep);
            idx = probes >= SecondaryProbeStart
                ? (idx + secondaryStep) & _mask
                : (idx + 1) & _mask;
            probes++;

            if (probes > ProbeSoftLimit)
            {
                return ShardInsertResult.ProbeLimitExceeded;
            }
        }

        return ShardInsertResult.Full;
    }

    /// <summary>
    /// Persists the header used-slot counter (best-effort flush point for writers).
    /// </summary>
    /// <param name="usedSlots">Monotonic writer-side occupancy count.</param>
    public void FlushUsedSlots(ulong usedSlots)
    {
        ThrowIfDisposed();
        WriteUInt64(HeaderUsedSlotsOffset, usedSlots);
    }

    /// <summary>
    /// Recomputes IEEE CRC-32 over the slot region, stores it in the header, and attempts to flush the mapped view.
    /// </summary>
    /// <remarks>Call from <see cref="ShardedHistoryWriter.Sync"/> and shutdown paths; not on per-insert hot path.</remarks>
    public void WriteSlotRegionCrc32AndFlushView()
    {
        ThrowIfDisposed();
        uint crc = ComputeSlotRegionCrc32();
        WriteUInt32(HeaderSlotRegionCrc32Offset, crc);
        FlushViewBestEffort();
    }

    /// <summary>
    /// Reads the hash pair at a slot index if the slot is published (hash_hi non-zero).
    /// </summary>
    /// <param name="index">Slot index in <c>[0, TableSize)</c>.</param>
    /// <param name="hashHi">Receives high 64 bits when the method returns <see langword="true"/>.</param>
    /// <param name="hashLo">Receives low 64 bits when the method returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> if the slot contains a published entry.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="index"/> is out of range.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadHashAt(ulong index, out ulong hashHi, out ulong hashLo)
    {
        ThrowIfDisposed();
        if (index >= _tableSize)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        byte* slot = GetSlotPtr(index);
        hashHi = Volatile.Read(ref *(ulong*)slot);
        hashLo = *(ulong*)(slot + 8);
        return hashHi != 0;
    }

    /// <summary>
    /// Double-samples the 32-byte slot with volatile reads and bounded retries to detect torn or inconsistent mmap layout.
    /// </summary>
    /// <param name="index">Slot index in <c>[0, TableSize)</c>.</param>
    /// <param name="outcome">Stable classification or <see cref="SlotScrubOutcome.VolatileLayoutMismatch"/>.</param>
    /// <returns><see langword="true"/> when <paramref name="index"/> is in range.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="index"/> is out of range.</exception>
    /// <remarks>
    /// <para>
    /// <b>Thread safety:</b> Safe concurrent with the shard writer protocol; transient mismatches are retried with spin-waits before declaring mismatch.
    /// </para>
    /// </remarks>
    public bool TryScrubSlotLayout(ulong index, out SlotScrubOutcome outcome)
    {
        ThrowIfDisposed();
        if (index >= _tableSize)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        byte* slot = GetSlotPtr(index);
        const int maxAttempts = 6;
        for (int attempt = 0; attempt < maxAttempts; attempt++)
        {
            ulong hiA = Volatile.Read(ref *(ulong*)slot);
            ulong loA = Volatile.Read(ref *(ulong*)(slot + 8));
            ulong sHiA = Volatile.Read(ref *(ulong*)(slot + 16));
            ulong sLoA = Volatile.Read(ref *(ulong*)(slot + 24));
            Thread.MemoryBarrier();
            ulong hiB = Volatile.Read(ref *(ulong*)slot);
            ulong loB = Volatile.Read(ref *(ulong*)(slot + 8));
            ulong sHiB = Volatile.Read(ref *(ulong*)(slot + 16));
            ulong sLoB = Volatile.Read(ref *(ulong*)(slot + 24));
            if (hiA == hiB && loA == loB && sHiA == sHiB && sLoA == sLoB)
            {
                outcome = hiA == 0 ? SlotScrubOutcome.OkEmpty : SlotScrubOutcome.OkOccupiedStable;
                return true;
            }

            Thread.SpinWait(16 * (attempt + 1));
        }

        outcome = SlotScrubOutcome.VolatileLayoutMismatch;
        return true;
    }

    /// <summary>
    /// Convenience wrapper returning <see langword="true"/> only for <see cref="ShardInsertResult.Duplicate"/>.
    /// </summary>
    public bool ExistsOrInsert(ulong hashHi, ulong hashLo, ulong serverHi, ulong serverLo)
    {
        ShardInsertResult result = TryExistsOrInsert(hashHi, hashLo, serverHi, serverLo);
        return result == ShardInsertResult.Duplicate;
    }

    /// <summary>
    /// Releases the mmap view and file mapping.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Failure handling:</b> Each native dispose step is isolated so one failure does not skip subsequent releases.
    /// </para>
    /// </remarks>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        try
        {
            _accessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }
        catch (Exception ex)
        {
            Trace.TraceWarning("[HistoryDB] HistoryShard ReleasePointer failed: {0}", ex.Message);
        }

        try
        {
            _accessor.Dispose();
        }
        catch (Exception ex)
        {
            Trace.TraceWarning("[HistoryDB] HistoryShard accessor dispose failed: {0}", ex.Message);
        }

        try
        {
            _mmf.Dispose();
        }
        catch (Exception ex)
        {
            Trace.TraceWarning("[HistoryDB] HistoryShard mmap dispose failed: {0}", ex.Message);
        }

        _disposed = true;
    }

    #endregion

    #region Private Methods

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private byte* GetSlotPtr(ulong index) => _basePtr + HeaderSize + ((long)index * EntrySize);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ulong ReadUInt64(int offset)
    {
        ThrowIfDisposed();
        return *(ulong*)(_basePtr + offset);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void WriteUInt64(int offset, ulong value) => *(ulong*)(_basePtr + offset) = value;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void NormalizeZeroHash(ulong hashHi, ref ulong hashLo)
    {
        if (hashHi == 0 && hashLo == 0)
        {
            hashLo = 1;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SlotProbeResult ProbeSlot(byte* slot, ulong hashHi, ulong hashLo)
    {
        // Preserve publish ordering semantics: hash_hi is the visibility flag.
        ulong existingHi = Volatile.Read(ref *(ulong*)slot);
        if (existingHi == 0)
        {
            return SlotProbeResult.Empty;
        }

        if (Sse2.IsSupported)
        {
            return ProbeSlotSse2(slot, hashHi, hashLo);
        }

        ulong existingLo = *(ulong*)(slot + 8);
        return (existingHi == hashHi && existingLo == hashLo)
            ? SlotProbeResult.Match
            : SlotProbeResult.Occupied;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SlotProbeResult ProbeSlotSse2(byte* slot, ulong hashHi, ulong hashLo)
    {
        Vector128<long> slotVector = Sse2.LoadVector128((long*)slot);
        Vector128<long> target = Vector128.Create((long)hashHi, (long)hashLo);
        Vector128<byte> matchCompare = Sse2.CompareEqual(slotVector.AsByte(), target.AsByte());
        return Sse2.MoveMask(matchCompare) == VectorAllBytesMatchMask
            ? SlotProbeResult.Match
            : SlotProbeResult.Occupied;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private SlotProbeResult ProbeSlotPair(ulong firstIdx, ulong secondIdx, ulong hashHi, ulong hashLo, out bool matchedFirstSlot)
    {
        matchedFirstSlot = false;
        byte* firstSlot = GetSlotPtr(firstIdx);
        ulong firstHi = Volatile.Read(ref *(ulong*)firstSlot);
        if (firstHi == 0)
        {
            return SlotProbeResult.Empty;
        }

        byte* secondSlot = GetSlotPtr(secondIdx);
        ulong secondHi = Volatile.Read(ref *(ulong*)secondSlot);
        if (secondHi == 0)
        {
            matchedFirstSlot = true;
            return ProbeSlot(firstSlot, hashHi, hashLo) == SlotProbeResult.Match
                ? SlotProbeResult.Match
                : SlotProbeResult.Empty;
        }

        // Use a single 256-bit compare only when slots are physically contiguous (no ring wrap).
        if (Avx2.IsSupported && firstIdx + 1 == secondIdx)
        {
            Vector256<long> slots = Avx.LoadVector256((long*)firstSlot);
            Vector256<long> target = Vector256.Create((long)hashHi, (long)hashLo, (long)hashHi, (long)hashLo);
            Vector256<byte> matchCompare = Avx2.CompareEqual(slots.AsByte(), target.AsByte());
            int mask = Avx2.MoveMask(matchCompare);

            if ((mask & VectorFirstLaneMatchMask) == VectorFirstLaneMatchMask)
            {
                return SlotProbeResult.Match;
            }

            if ((mask & VectorSecondLaneMatchMask) == VectorSecondLaneMatchMask)
            {
                matchedFirstSlot = true;
                return SlotProbeResult.Match;
            }

            return SlotProbeResult.Occupied;
        }

        if (ProbeSlot(firstSlot, hashHi, hashLo) == SlotProbeResult.Match)
        {
            return SlotProbeResult.Match;
        }

        matchedFirstSlot = true;
        return ProbeSlot(secondSlot, hashHi, hashLo) == SlotProbeResult.Match
            ? SlotProbeResult.Match
            : SlotProbeResult.Occupied;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void PrefetchProbeSlot(ulong idx, ulong probes, ulong secondaryStep)
    {
        if (!Sse.IsSupported)
        {
            return;
        }

        ulong nextIdx = probes >= SecondaryProbeStart
            ? (idx + secondaryStep) & _mask
            : (idx + 1) & _mask;
        Sse.Prefetch0(GetSlotPtr(nextIdx));
    }

    private void InitializeOrValidateHeader()
    {
        ulong magic = ReadUInt64(HeaderMagicOffset);
        if (magic == 0)
        {
            WriteUInt64(HeaderMagicOffset, Magic);
            WriteUInt64(HeaderVersionOffset, CurrentVersion);
            WriteUInt64(HeaderTableSizeOffset, _tableSize);
            WriteUInt64(HeaderUsedSlotsOffset, 0);
            WriteUInt64(HeaderMaxLoadFactorOffset, _maxLoadFactor);
            WriteUInt64(HeaderMaxOccupiedSlotIndexOffset, 0);
            WriteUInt32(HeaderSlotRegionCrc32Offset, ComputeSlotRegionCrc32());
            return;
        }

        if (magic != Magic)
        {
            throw new InvalidDataException("Shard file magic does not match HISTDB01.");
        }

        ulong version = ReadUInt64(HeaderVersionOffset);
        ulong existingTableSize = ReadUInt64(HeaderTableSizeOffset);
        if (existingTableSize != _tableSize)
        {
            throw new InvalidDataException($"Shard tableSize mismatch. File={existingTableSize}, Requested={_tableSize}.");
        }

        if (version == 1)
        {
            WriteUInt64(HeaderVersionOffset, CurrentVersion);
            WriteUInt64(HeaderMaxOccupiedSlotIndexOffset, 0);
            WriteUInt32(HeaderSlotRegionCrc32Offset, ComputeSlotRegionCrc32());
            return;
        }

        if (version == 2)
        {
            WriteUInt64(HeaderVersionOffset, CurrentVersion);
            WriteUInt32(HeaderSlotRegionCrc32Offset, ComputeSlotRegionCrc32());
            return;
        }

        if (version != CurrentVersion)
        {
            throw new InvalidDataException($"Unsupported shard version {version}.");
        }

        ulong maxOccupied = ReadUInt64(HeaderMaxOccupiedSlotIndexOffset);
        if (maxOccupied >= _tableSize)
        {
            throw new InvalidDataException($"Shard maxOccupiedSlotIndex invalid. File={maxOccupied}, TableSize={_tableSize}.");
        }

        uint storedCrc = ReadUInt32(HeaderSlotRegionCrc32Offset);
        uint actualCrc = ComputeSlotRegionCrc32();
        if (storedCrc != actualCrc)
        {
            throw new InvalidDataException(
                $"Shard slot-region CRC mismatch (header {storedCrc:X8}, computed {actualCrc:X8}). File may be corrupted.");
        }
    }

    private uint ComputeSlotRegionCrc32()
    {
        long byteLen = checked((long)(_tableSize * (ulong)EntrySize));
        return Crc32Ieee.ComputeUnmanaged((nint)(_basePtr + HeaderSize), byteLen);
    }

    private void FlushViewBestEffort()
    {
        try
        {
            MemoryMappedViewFlush.TryFlush((nint)_basePtr, _viewByteLength);
        }
        catch (Exception ex)
        {
            Trace.TraceWarning("[HistoryDB] MemoryMappedViewFlush failed: {0}", ex.Message);
        }
    }

    private uint ReadUInt32(int offset) => *(uint*)(_basePtr + offset);

    private void WriteUInt32(int offset, uint value) => *(uint*)(_basePtr + offset) = value;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void TryAdvanceMaxOccupiedSlotIndex(ulong slotIndex)
    {
        ref long headerWord = ref Unsafe.As<byte, long>(ref *(_basePtr + HeaderMaxOccupiedSlotIndexOffset));

        while (true)
        {
            ulong current = ReadUInt64(HeaderMaxOccupiedSlotIndexOffset);
            if (slotIndex <= current || slotIndex >= _tableSize)
            {
                return;
            }

            long currentBits = unchecked((long)current);
            long desiredBits = unchecked((long)slotIndex);
            long observedBits = Interlocked.CompareExchange(ref headerWord, desiredBits, currentBits);
            if (observedBits == currentBits)
            {
                return;
            }
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(HistoryShard));
        }
    }

    #endregion

    #region Nested types

    private enum SlotProbeResult : byte
    {
        Empty = 0,
        Match = 1,
        Occupied = 2
    }

    #endregion
}
