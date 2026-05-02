// HistoryShard.cs -- memory-mapped 128-bit hash table shard with linear-then-secondary probing, optional SSE2 slot compares, and a 64-byte binary header (on-disk format version 1).
// Hash visibility uses hash_hi as the published flag (written last). Consumed by ShardedHistoryWriter per generation shard file. SSE2 is used when supported (typical Windows/Linux x64); otherwise scalar compares apply.

using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.IO.Hashing;
using System.Runtime.Intrinsics.X86;
using System.Threading;

using HistoryDB.Interop;
using HistoryDB.Utilities;

namespace HistoryDB;

/// <summary>
/// Controls how <see cref="HistoryShard"/> opens the backing file (normal read/write mmap vs read-only shared scan).
/// </summary>
public enum HistoryShardOpenMode
{
    /// <summary>Default: create or open with read-write mmap (writer / repair mutations).</summary>
    ReadWrite = 0,

    /// <summary>
    /// Open an existing shard with <see cref="FileAccess.Read"/> and <see cref="FileShare.ReadWrite"/> for concurrent
    /// read-only inspection while another process may hold the file (for example <see cref="Repair.HistoryRepairEngine"/> shared scan).
    /// </summary>
    SharedReadOnlyScan = 1,
}

/// <summary>
/// Optional access metadata to preserve when relocating a slot (for example compaction copying into a sibling shard).
/// </summary>
internal readonly record struct PreservedSlotAccess(long DateObtainedTicks, long DateAccessedTicks, ulong AccessedCounter);

/// <summary>
/// Result of a best-effort volatile consistency check on one mmap slot (hygiene / scrubber).
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
    ProbeLimitExceeded = 3,

    /// <summary>Lookup path updated <c>date_accessed</c> and <c>accessed_counter</c> on a matched slot (writer-only).</summary>
    AccessRecorded = 4,

    /// <summary>Lookup path did not find the hash in this shard (or touch lost a race).</summary>
    AccessNotFound = 5,

    /// <summary>Lookup saw the hash but skipped mutating access metadata (rare race with tombstone/retire).</summary>
    AccessSkipped = 6,
}

/// <summary>
/// Result of <see cref="HistoryShard.TryTombstone"/>.
/// </summary>
internal enum ShardTombstoneResult
{
    /// <summary>The matching slot was found and overwritten with the tombstone sentinel.</summary>
    Tombstoned = 0,

    /// <summary>No slot in the probe path matched the requested hash.</summary>
    NotFound = 1,

    /// <summary>Probing exceeded the soft probe budget before finding a match or terminating slot.</summary>
    ProbeLimitExceeded = 2
}

/// <summary>
/// Result of <see cref="HistoryShard.TryRecordAccessOnMatch"/> (writer-only metadata update on a matched slot).
/// </summary>
internal enum ShardAccessRecordResult
{
    /// <summary><c>date_accessed</c> and <c>accessed_counter</c> were updated.</summary>
    Recorded = 0,

    /// <summary>No matching slot was found in the probe path.</summary>
    NotFound = 1,

    /// <summary>The slot no longer matched the hash when the writer applied the update (concurrent tombstone/retire race).</summary>
    RaceLost = 2,

    /// <summary>Probing exceeded the soft probe budget.</summary>
    ProbeLimitExceeded = 3,
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
/// <b>SIMD:</b> On x64, when <see cref="Vector128.IsHardwareAccelerated"/> is true, the 128-bit hash compare uses <see cref="Vector128.LoadUnsafe{T}(ref T)"/> (unaligned-safe) plus a vector equality; otherwise scalar quadword compares are used. Linear batch probing prefetches the paired slot and the next two indices via <see cref="Sse.Prefetch0"/> when available.
/// </para>
/// <para>
/// <b>Hygiene:</b> <see cref="TryScrubSlotLayout"/> double-samples the slot payload for volatile consistency (used by optional background scrubbers in <see cref="ShardedHistoryWriter"/>).
/// </para>
/// </remarks>
internal unsafe sealed class HistoryShard : IDisposable
{
    #region Constants
    private const ulong Magic = 0x3130424454534948UL; // "HISTDB01"

    /// <summary>On-disk header format version (only value supported).</summary>
    /// <remarks>
    /// Physical stride is inferred from file length: <see cref="SlotStrideLegacy"/> (56) or <see cref="SlotStrideWithCrc"/> (60) for the same header version.
    /// </remarks>
    internal const ulong DiskFormatVersion1 = 1UL;

    /// <summary>Byte length of the hash + server quadwords within one slot (tombstone path clears only bytes after this prefix).</summary>
    internal const int LegacyEntryByteLength = 32;

    /// <summary>Logical slot payload (CRC input) byte length — hash pair, server pair, access metadata.</summary>
    internal const int SlotPayloadByteLength = 56;

    /// <summary>Legacy on-disk stride per slot (same as <see cref="SlotPayloadByteLength"/>).</summary>
    internal const int SlotStrideLegacy = SlotPayloadByteLength;

    /// <summary>On-disk stride including little-endian CRC32 of the first <see cref="SlotPayloadByteLength"/> bytes.</summary>
    internal const int SlotStrideWithCrc = SlotPayloadByteLength + sizeof(uint);

    /// <summary>Byte offset of <c>date_obtained</c> UTC ticks (Int64) within one slot.</summary>
    internal const int SlotDateObtainedTicksOffset = 32;

    /// <summary>Byte offset of <c>date_accessed</c> UTC ticks (Int64) within one slot.</summary>
    internal const int SlotDateAccessedTicksOffset = 40;

    /// <summary>Byte offset of <c>accessed_counter</c> (UInt64) within one slot.</summary>
    internal const int SlotAccessedCounterOffset = 48;

    /// <summary>Little-endian CRC32 of <see cref="SlotPayloadByteLength"/> slot bytes (when <see cref="SlotStrideWithCrc"/> is the physical stride).</summary>
    internal const int SlotCrc32Offset = SlotPayloadByteLength;

    /// <summary>Reserved <c>hash_hi</c> value of a tombstone slot (paired with <see cref="TombstoneHashLoMarker"/>).</summary>
    /// <remarks>Real input hashes that collide with the marker are nudged by <see cref="NormalizeReservedHashes"/> so the marker is unambiguous.</remarks>
    private const ulong TombstoneHashHiMarker = 1UL;

    /// <summary>Reserved <c>hash_lo</c> value of a tombstone slot.</summary>
    private const ulong TombstoneHashLoMarker = 0UL;

    private const int HeaderSize = 64;

    private const int HeaderMagicOffset = 0;
    private const int HeaderVersionOffset = 8;
    private const int HeaderTableSizeOffset = 16;
    private const int HeaderUsedSlotsOffset = 24;
    private const int HeaderMaxLoadFactorOffset = 32;
    private const int HeaderMaxOccupiedSlotIndexOffset = 40;

    /// <summary>Expected magic for a HistoryDB shard data file (<c>HISTDB01</c>).</summary>
    internal const ulong ExpectedFileMagic = Magic;

    /// <summary>Header file format version written for new shard files in this build.</summary>
    internal const ulong ExpectedFileVersion = DiskFormatVersion1;

    /// <summary>Header byte length (constant for all versions).</summary>
    internal const int HeaderByteLength = HeaderSize;

    /// <summary>Logical slot payload length (alias for CRC-covered region; legacy name kept for callers).</summary>
    internal const int EntryByteLength = SlotPayloadByteLength;

    /// <summary>Byte offset of the magic field within the header.</summary>
    internal const int MagicOffset = HeaderMagicOffset;

    /// <summary>Byte offset of the version field within the header.</summary>
    internal const int VersionOffset = HeaderVersionOffset;

    /// <summary>Byte offset of the table-size field within the header.</summary>
    internal const int TableSizeOffset = HeaderTableSizeOffset;

    /// <summary>Byte offset of the used-slots counter within the header.</summary>
    internal const int UsedSlotsOffset = HeaderUsedSlotsOffset;

    /// <summary>Byte offset of the max-load-factor field within the header.</summary>
    internal const int MaxLoadFactorOffset = HeaderMaxLoadFactorOffset;

    /// <summary>Byte offset of the max-occupied-slot-index watermark within the header.</summary>
    internal const int MaxOccupiedSlotIndexOffset = HeaderMaxOccupiedSlotIndexOffset;
    private const ulong ProbeSoftLimit = 64;
    private const ulong SecondaryProbeStart = 16;
    private const ulong LinearProbeWindow = 4;
    private const ulong LinearProbeBatchSize = 2;

    #endregion

    /// <summary>
    /// For header format <see cref="DiskFormatVersion1"/>, selects 56-byte vs 60-byte (CRC suffix) slot stride from total file length.
    /// </summary>
    internal static bool TryResolveSlotStrideFromFileLength(long fileLengthBytes, ulong tableOnDisk, out int slotStride, out string? errorDetail)
    {
        errorDetail = null;
        try
        {
            long legacyBytes = checked(HeaderSize + (long)tableOnDisk * SlotStrideLegacy);
            long withCrcBytes = checked(HeaderSize + (long)tableOnDisk * SlotStrideWithCrc);
            if (fileLengthBytes == withCrcBytes)
            {
                slotStride = SlotStrideWithCrc;
                return true;
            }

            if (fileLengthBytes == legacyBytes)
            {
                slotStride = SlotStrideLegacy;
                return true;
            }

            slotStride = 0;
            errorDetail = $"length {fileLengthBytes} is neither expected legacy {legacyBytes} nor CRC-extended {withCrcBytes} bytes for tableSize={tableOnDisk}.";
            return false;
        }
        catch (OverflowException ex)
        {
            slotStride = 0;
            errorDetail = ex.Message;
            return false;
        }
    }

    #region Fields

    private readonly MemoryMappedFile _mmf;
    private readonly MemoryMappedViewAccessor _accessor;
    private readonly byte* _basePtr;
    private readonly ulong _tableSize;
    private readonly ulong _mask;
    private readonly ulong _maxLoadFactor;
    private readonly ulong _loadLimitSlots;
    private readonly bool _sharedReadOnlyScan;
    private readonly int _slotStride;
    private bool _disposed;

    #endregion

    #region Properties

    /// <summary>Number of slots in the table (power of two).</summary>
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
    public ulong MaxOccupiedSlotIndex => ReadUInt64(HeaderMaxOccupiedSlotIndexOffset);

    /// <summary>Bytes per table slot on disk (56 legacy, 60 with CRC suffix).</summary>
    public int PhysicalSlotStride => _slotStride;

    #endregion

    #region Constructors

    /// <summary>
    /// Opens or creates the mmap shard file at <paramref name="path"/> with the given logical table size.
    /// </summary>
    /// <param name="path">Absolute or relative path to the shard data file (normalized to a rooted path).</param>
    /// <param name="tableSize">Slot count; must be a power of two greater than zero.</param>
    /// <param name="maxLoadFactor">Maximum fill percentage before inserts return <see cref="ShardInsertResult.Full"/> (1..99).</param>
    /// <param name="openMode">Normal read/write open, or read-only shared scan (existing file only).</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="tableSize"/> or <paramref name="maxLoadFactor"/> is invalid.</exception>
    /// <exception cref="InvalidDataException">Thrown when an existing file header does not match this instance's parameters.</exception>
    public HistoryShard(string path, ulong tableSize, ulong maxLoadFactor = 75, HistoryShardOpenMode openMode = HistoryShardOpenMode.ReadWrite)
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

        _tableSize = tableSize;
        _mask = tableSize - 1;
        _maxLoadFactor = maxLoadFactor;
        _loadLimitSlots = (tableSize * maxLoadFactor) / 100;
        _sharedReadOnlyScan = openMode == HistoryShardOpenMode.SharedReadOnlyScan;

        if (_sharedReadOnlyScan)
        {
            if (!File.Exists(path))
            {
                throw new FileNotFoundException("Shared read-only scan requires an existing shard file.", path);
            }

            FileStream fs = new(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 4096, FileOptions.RandomAccess);
            long len = fs.Length;
            if (len < HeaderSize)
            {
                fs.Dispose();
                throw new InvalidDataException($"Shard '{path}' length {len} is shorter than the {HeaderSize}-byte header.");
            }

            Span<byte> hdr = stackalloc byte[HeaderSize];
            fs.Seek(0, SeekOrigin.Begin);
            if (fs.Read(hdr) != HeaderSize)
            {
                fs.Dispose();
                throw new InvalidDataException($"Shard '{path}' header could not be read.");
            }

                ulong magic = BinaryPrimitives.ReadUInt64LittleEndian(hdr[HeaderMagicOffset..]);
                ulong version = BinaryPrimitives.ReadUInt64LittleEndian(hdr[HeaderVersionOffset..]);
                ulong tableOnDisk = BinaryPrimitives.ReadUInt64LittleEndian(hdr[HeaderTableSizeOffset..]);
            if (magic != Magic || version != DiskFormatVersion1 || tableOnDisk != tableSize)
            {
                fs.Dispose();
                throw new InvalidDataException($"Shard '{path}' header is incompatible with this open (magic=0x{magic:X16}, version={version}, table={tableOnDisk}).");
            }

            if (!TryResolveSlotStrideFromFileLength(len, tableOnDisk, out int slotStride, out string? strideError))
            {
                fs.Dispose();
                throw new InvalidDataException($"Shard '{path}' {strideError}");
            }

            long expectedSize = checked(HeaderSize + (long)tableSize * slotStride);
            if (len != expectedSize)
            {
                fs.Dispose();
                throw new InvalidDataException($"Shard '{path}' length {len} != expected {expectedSize} for tableSize={tableSize}.");
            }

            _slotStride = slotStride;
            _mmf = MemoryMappedFile.CreateFromFile(
                fs,
                mapName: null,
                capacity: 0,
                access: MemoryMappedFileAccess.Read,
                inheritability: HandleInheritability.None,
                leaveOpen: false);
            _accessor = _mmf.CreateViewAccessor(0, len, MemoryMappedFileAccess.Read);
        }
        else
        {
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            FileStream lease = new(
                path,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.ReadWrite,
                bufferSize: 4096,
                FileOptions.RandomAccess);

            long len = lease.Length;
            ulong magic = 0;
            ulong versionOnDisk = 0;
            ulong tableOnDisk = 0;
            if (len >= HeaderSize)
            {
                Span<byte> hdr = stackalloc byte[HeaderSize];
                lease.Seek(0, SeekOrigin.Begin);
                if (lease.Read(hdr) != HeaderSize)
                {
                    lease.Dispose();
                    throw new InvalidDataException($"Shard '{path}' header could not be read.");
                }

                magic = BinaryPrimitives.ReadUInt64LittleEndian(hdr[HeaderMagicOffset..]);
                versionOnDisk = BinaryPrimitives.ReadUInt64LittleEndian(hdr[HeaderVersionOffset..]);
                tableOnDisk = BinaryPrimitives.ReadUInt64LittleEndian(hdr[HeaderTableSizeOffset..]);
            }

            int slotStride;
            long expectedSize;
            if (magic == 0)
            {
                slotStride = SlotStrideWithCrc;
                expectedSize = checked(HeaderSize + (long)tableSize * slotStride);
                if (len > expectedSize)
                {
                    lease.Dispose();
                    throw new InvalidDataException($"Shard '{path}' length {len} exceeds expected {expectedSize} for a new or zeroed header.");
                }

                if (len < expectedSize)
                {
                    lease.SetLength(expectedSize);
                }
            }
            else
            {
                if (magic != Magic)
                {
                    lease.Dispose();
                    throw new InvalidDataException($"Shard '{path}' magic 0x{magic:X16} does not match HISTDB01.");
                }

                if (versionOnDisk != DiskFormatVersion1)
                {
                    lease.Dispose();
                    throw new InvalidDataException($"Unsupported shard format version {versionOnDisk}.");
                }

                if (tableOnDisk != tableSize)
                {
                    lease.Dispose();
                    throw new InvalidDataException($"Shard tableSize mismatch. File={tableOnDisk}, Requested={tableSize}.");
                }

                if (!TryResolveSlotStrideFromFileLength(len, tableOnDisk, out slotStride, out string? strideError))
                {
                    lease.Dispose();
                    throw new InvalidDataException($"Shard '{path}' {strideError}");
                }

                expectedSize = checked(HeaderSize + (long)tableSize * slotStride);
                if (len != expectedSize)
                {
                    lease.Dispose();
                    throw new InvalidDataException($"Shard '{path}' length {len} != expected {expectedSize} for tableSize={tableSize}.");
                }
            }

            _slotStride = slotStride;
            _mmf = MemoryMappedFile.CreateFromFile(
                lease,
                mapName: null,
                capacity: expectedSize,
                access: MemoryMappedFileAccess.ReadWrite,
                inheritability: HandleInheritability.None,
                leaveOpen: false);
            _accessor = _mmf.CreateViewAccessor(0, expectedSize, MemoryMappedFileAccess.ReadWrite);
        }

        byte* ptr = null;
        _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
        _basePtr = ptr + _accessor.PointerOffset;

        if (_sharedReadOnlyScan)
        {
            ValidateExistingHeaderReadOnly();
        }
        else
        {
            InitializeOrValidateHeader();
        }
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

        NormalizeReservedHashes(hashHi, ref hashLo);

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
        ThrowIfReadOnlyScan();
        NormalizeReservedHashes(hashHi, ref hashLo);

        ulong usedSlots = UsedSlots;
        if (usedSlots >= _loadLimitSlots)
        {
            return ShardInsertResult.Full;
        }

        ShardInsertResult result = TryExistsOrInsertUncheckedInternal(hashHi, hashLo, serverHi, serverLo, out bool reclaimedTombstone, preserveAccessMetadata: null);
        if (result == ShardInsertResult.Inserted && !reclaimedTombstone)
        {
            // Tombstone reclamation reuses an already-counted slot, so the header is only bumped for a fresh empty insert.
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
    /// <remarks>
    /// <para>
    /// On <see cref="ShardInsertResult.Inserted"/>, the entry may have reclaimed a tombstoned slot earlier in the probe chain
    /// (in which case the writer should not bump <c>usedSlots</c>) or a fresh empty slot. Use <see cref="TryExistsOrInsertUncheckedInternal"/>
    /// to distinguish the two without allocating.
    /// </para>
    /// </remarks>
    public ShardInsertResult TryExistsOrInsertUnchecked(ulong hashHi, ulong hashLo, ulong serverHi, ulong serverLo) =>
        TryExistsOrInsertUncheckedInternal(hashHi, hashLo, serverHi, serverLo, out _, preserveAccessMetadata: null);

    /// <summary>
    /// Variant of <see cref="TryExistsOrInsertUnchecked"/> that also reports whether a tombstoned slot was reclaimed.
    /// </summary>
    /// <param name="reclaimedTombstone">When the result is <see cref="ShardInsertResult.Inserted"/>, set to <see langword="true"/> if an existing tombstoned slot was reused (caller should NOT bump <c>usedSlots</c>); <see langword="false"/> for a fresh empty slot.</param>
    internal ShardInsertResult TryExistsOrInsertUncheckedInternal(
        ulong hashHi,
        ulong hashLo,
        ulong serverHi,
        ulong serverLo,
        out bool reclaimedTombstone,
        PreservedSlotAccess? preserveAccessMetadata = null)
    {
        ThrowIfDisposed();
        ThrowIfReadOnlyScan();
        NormalizeReservedHashes(hashHi, ref hashLo);
        reclaimedTombstone = false;

        void Publish(byte* s)
        {
            if (preserveAccessMetadata is PreservedSlotAccess p)
            {
                PublishSlot(s, hashHi, hashLo, serverHi, serverLo, p.DateObtainedTicks, p.DateAccessedTicks, p.AccessedCounter);
            }
            else
            {
                PublishSlot(s, hashHi, hashLo, serverHi, serverLo);
            }
        }

        ulong idx = hashLo & _mask;
        ulong secondaryStep = (hashHi | 1UL) & _mask;
        if (secondaryStep == 0)
        {
            secondaryStep = 1;
        }

        ulong probes = 0;
        bool haveTombstone = false;
        ulong tombstoneIdx = 0;

        // Linear segment.
        while (probes < _tableSize && probes < SecondaryProbeStart)
        {
            ulong candidateIdx = (idx + probes) & _mask;
            byte* slot = GetSlotPtr(candidateIdx);
            SlotProbeResult probe = ProbeSlot(slot, hashHi, hashLo);
            if (probe == SlotProbeResult.Match)
            {
                return ShardInsertResult.Duplicate;
            }

            if (probe == SlotProbeResult.Empty)
            {
                if (haveTombstone)
                {
                    Publish(GetSlotPtr(tombstoneIdx));
                    TryAdvanceMaxOccupiedSlotIndex(tombstoneIdx);
                    reclaimedTombstone = true;
                    return ShardInsertResult.Inserted;
                }

                Publish(slot);
                TryAdvanceMaxOccupiedSlotIndex(candidateIdx);
                return ShardInsertResult.Inserted;
            }

            if (probe == SlotProbeResult.Tombstone && !haveTombstone)
            {
                haveTombstone = true;
                tombstoneIdx = candidateIdx;
            }

            probes++;
            if (probes > ProbeSoftLimit)
            {
                if (haveTombstone)
                {
                    Publish(GetSlotPtr(tombstoneIdx));
                    TryAdvanceMaxOccupiedSlotIndex(tombstoneIdx);
                    reclaimedTombstone = true;
                    return ShardInsertResult.Inserted;
                }

                return ShardInsertResult.ProbeLimitExceeded;
            }
        }

        // Secondary-step segment.
        if (probes < _tableSize)
        {
            idx = (idx + probes) & _mask;
        }

        while (probes < _tableSize)
        {
            byte* slot = GetSlotPtr(idx);
            SlotProbeResult probe = ProbeSlot(slot, hashHi, hashLo);
            if (probe == SlotProbeResult.Match)
            {
                return ShardInsertResult.Duplicate;
            }

            if (probe == SlotProbeResult.Empty)
            {
                if (haveTombstone)
                {
                    Publish(GetSlotPtr(tombstoneIdx));
                    TryAdvanceMaxOccupiedSlotIndex(tombstoneIdx);
                    reclaimedTombstone = true;
                    return ShardInsertResult.Inserted;
                }

                Publish(slot);
                TryAdvanceMaxOccupiedSlotIndex(idx);
                return ShardInsertResult.Inserted;
            }

            if (probe == SlotProbeResult.Tombstone && !haveTombstone)
            {
                haveTombstone = true;
                tombstoneIdx = idx;
            }

            PrefetchProbeSlot(idx, probes, secondaryStep);
            idx = probes >= SecondaryProbeStart
                ? (idx + secondaryStep) & _mask
                : (idx + 1) & _mask;
            probes++;

            if (probes > ProbeSoftLimit)
            {
                if (haveTombstone)
                {
                    Publish(GetSlotPtr(tombstoneIdx));
                    TryAdvanceMaxOccupiedSlotIndex(tombstoneIdx);
                    reclaimedTombstone = true;
                    return ShardInsertResult.Inserted;
                }

                return ShardInsertResult.ProbeLimitExceeded;
            }
        }

        if (haveTombstone)
        {
            Publish(GetSlotPtr(tombstoneIdx));
            TryAdvanceMaxOccupiedSlotIndex(tombstoneIdx);
            reclaimedTombstone = true;
            return ShardInsertResult.Inserted;
        }

        return ShardInsertResult.Full;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void PublishSlot(byte* slot, ulong hashHi, ulong hashLo, ulong serverHi, ulong serverLo)
    {
        long ticks = DateTime.UtcNow.Ticks;
        PublishSlot(slot, hashHi, hashLo, serverHi, serverLo, ticks, ticks, 0UL);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void PublishSlot(
        byte* slot,
        ulong hashHi,
        ulong hashLo,
        ulong serverHi,
        ulong serverLo,
        long dateObtainedTicks,
        long dateAccessedTicks,
        ulong accessedCounter)
    {
        // Publish protocol: metadata + server payload + hash_lo first, hash_hi last. Readers treat hash_hi == 0 as EMPTY.
        *(long*)(slot + SlotDateObtainedTicksOffset) = dateObtainedTicks;
        *(long*)(slot + SlotDateAccessedTicksOffset) = dateAccessedTicks;
        *(ulong*)(slot + SlotAccessedCounterOffset) = accessedCounter;
        *(ulong*)(slot + 16) = serverHi;
        *(ulong*)(slot + 24) = serverLo;
        *(ulong*)(slot + 8) = hashLo;
        Volatile.Write(ref *(ulong*)slot, hashHi);
        WriteSlotCrc(slot);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void WriteSlotCrc(byte* slot)
    {
        if (_slotStride != SlotStrideWithCrc)
        {
            return;
        }

        uint crc = Crc32.HashToUInt32(new ReadOnlySpan<byte>(slot, SlotPayloadByteLength));
        *(uint*)(slot + SlotCrc32Offset) = crc;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryVerifySlotCrc(byte* slot)
    {
        if (_slotStride != SlotStrideWithCrc)
        {
            return true;
        }

        uint stored = *(uint*)(slot + SlotCrc32Offset);
        uint computed = Crc32.HashToUInt32(new ReadOnlySpan<byte>(slot, SlotPayloadByteLength));
        return stored == computed;
    }

    /// <summary>Best-effort flush of this shard's mmap view (call after header/slot writes on sync boundaries).</summary>
    internal unsafe void FlushMappedViewToDisk()
    {
        nuint len = (nuint)checked(HeaderSize + (long)_tableSize * _slotStride);
        MappedMemoryFlush.TryFlush(_basePtr, len);
    }

    /// <summary>
    /// Probes for an existing entry matching <paramref name="hashHi"/>/<paramref name="hashLo"/> and overwrites it with the tombstone sentinel.
    /// </summary>
    /// <param name="hashHi">High 64 bits of the content hash.</param>
    /// <param name="hashLo">Low 64 bits of the content hash.</param>
    /// <returns>
    /// <see cref="ShardTombstoneResult.Tombstoned"/> when a matching slot was overwritten,
    /// <see cref="ShardTombstoneResult.NotFound"/> when probing terminated at an empty slot or table boundary without a match,
    /// <see cref="ShardTombstoneResult.ProbeLimitExceeded"/> when the soft probe budget was exhausted.
    /// </returns>
    /// <remarks>
    /// <para>
    /// <b>Thread safety:</b> Safe concurrent with the per-shard writer protocol because tombstoning is processed by the same
    /// single-writer loop that handles inserts (see <see cref="ShardedHistoryWriter"/>).
    /// </para>
    /// <para>
    /// <b>Header maintenance:</b> The caller is responsible for decrementing the in-memory used-slot counter by one and flushing it
    /// via <see cref="FlushUsedSlots"/>; this method only mutates the slot bytes.
    /// </para>
    /// </remarks>
    internal ShardTombstoneResult TryTombstone(ulong hashHi, ulong hashLo)
    {
        ThrowIfDisposed();
        ThrowIfReadOnlyScan();

        if (hashHi == 0 && hashLo == 0)
        {
            return ShardTombstoneResult.NotFound;
        }

        NormalizeReservedHashes(hashHi, ref hashLo);

        ulong idx = hashLo & _mask;
        ulong secondaryStep = (hashHi | 1UL) & _mask;
        if (secondaryStep == 0)
        {
            secondaryStep = 1;
        }

        ulong probes = 0;
        while (probes < _tableSize)
        {
            byte* slot = GetSlotPtr(idx);
            SlotProbeResult probe = ProbeSlot(slot, hashHi, hashLo);
            if (probe == SlotProbeResult.Empty)
            {
                return ShardTombstoneResult.NotFound;
            }

            if (probe == SlotProbeResult.Match)
            {
                // Tombstone publish protocol mirrors the insert publish protocol: payload first (zeroed), then sentinel
                // hash_lo, then hash_hi. Concurrent readers either see the live entry, then the tombstone -- never a torn mid-state.
                Unsafe.InitBlockUnaligned(slot + LegacyEntryByteLength, 0, (uint)(_slotStride - LegacyEntryByteLength));
                *(ulong*)(slot + 16) = 0;
                *(ulong*)(slot + 24) = 0;
                *(ulong*)(slot + 8) = TombstoneHashLoMarker;
                Volatile.Write(ref *(ulong*)slot, TombstoneHashHiMarker);
                WriteSlotCrc(slot);
                return ShardTombstoneResult.Tombstoned;
            }

            PrefetchProbeSlot(idx, probes, secondaryStep);
            idx = probes >= SecondaryProbeStart
                ? (idx + secondaryStep) & _mask
                : (idx + 1) & _mask;
            probes++;

            if (probes > ProbeSoftLimit)
            {
                return ShardTombstoneResult.ProbeLimitExceeded;
            }
        }

        return ShardTombstoneResult.NotFound;
    }

    /// <summary>
    /// Writer-only: finds a published non-tombstone slot matching the hash and updates <c>date_accessed</c> (UTC ticks) and increments
    /// <c>accessed_counter</c>. <c>date_obtained</c> is left unchanged.
    /// </summary>
    internal ShardAccessRecordResult TryRecordAccessOnMatch(ulong hashHi, ulong hashLo)
    {
        ThrowIfDisposed();
        ThrowIfReadOnlyScan();

        if (hashHi == 0 && hashLo == 0)
        {
            return ShardAccessRecordResult.NotFound;
        }

        NormalizeReservedHashes(hashHi, ref hashLo);

        ulong idx = hashLo & _mask;
        ulong secondaryStep = (hashHi | 1UL) & _mask;
        if (secondaryStep == 0)
        {
            secondaryStep = 1;
        }

        ulong probes = 0;
        while (probes < _tableSize)
        {
            byte* slot = GetSlotPtr(idx);
            SlotProbeResult probe = ProbeSlot(slot, hashHi, hashLo);
            if (probe == SlotProbeResult.Empty)
            {
                return ShardAccessRecordResult.NotFound;
            }

            if (probe == SlotProbeResult.Match)
            {
                ulong hi2 = Volatile.Read(ref *(ulong*)slot);
                ulong lo2 = *(ulong*)(slot + 8);
                if (hi2 != hashHi || lo2 != hashLo)
                {
                    return ShardAccessRecordResult.RaceLost;
                }

                if (hi2 == TombstoneHashHiMarker && lo2 == TombstoneHashLoMarker)
                {
                    return ShardAccessRecordResult.RaceLost;
                }

                long nowTicks = DateTime.UtcNow.Ticks;
                Volatile.Write(ref *(long*)(slot + SlotDateAccessedTicksOffset), nowTicks);
                ref ulong counter = ref *(ulong*)(slot + SlotAccessedCounterOffset);
                ulong observed;
                do
                {
                    observed = Volatile.Read(ref counter);
                    ulong next = observed + 1UL;
                    if (next == 0UL)
                    {
                        next = ulong.MaxValue;
                    }

                    if (Interlocked.CompareExchange(ref counter, next, observed) == observed)
                    {
                        break;
                    }
                }
                while (true);

                WriteSlotCrc(slot);
                return ShardAccessRecordResult.Recorded;
            }

            PrefetchProbeSlot(idx, probes, secondaryStep);
            idx = probes >= SecondaryProbeStart
                ? (idx + secondaryStep) & _mask
                : (idx + 1) & _mask;
            probes++;

            if (probes > ProbeSoftLimit)
            {
                return ShardAccessRecordResult.ProbeLimitExceeded;
            }
        }

        return ShardAccessRecordResult.NotFound;
    }

    /// <summary>
    /// Persists the header used-slot counter (best-effort flush point for writers).
    /// </summary>
    /// <param name="usedSlots">Monotonic writer-side occupancy count.</param>
    public void FlushUsedSlots(ulong usedSlots)
    {
        ThrowIfDisposed();
        ThrowIfReadOnlyScan();
        WriteUInt64(HeaderUsedSlotsOffset, usedSlots);
    }

    /// <summary>
    /// Overwrites the header <c>maxOccupiedSlotIndex</c> watermark to <paramref name="value"/>.
    /// </summary>
    /// <param name="value">The corrected watermark; must be strictly less than <see cref="TableSize"/>.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="value"/> is outside the legal range.</exception>
    /// <remarks>
    /// <para>
    /// <b>Repair-only contract:</b> safe only when the caller holds the shard file exclusively (for example, the offline repair engine).
    /// Concurrent writers may otherwise observe a non-monotonic decrease.
    /// </para>
    /// </remarks>
    internal void OverwriteMaxOccupiedSlotIndexForRepair(ulong value)
    {
        ThrowIfDisposed();
        ThrowIfReadOnlyScan();
        if (value >= _tableSize)
        {
            throw new ArgumentOutOfRangeException(nameof(value), "maxOccupiedSlotIndex must be < TableSize.");
        }

        WriteUInt64(HeaderMaxOccupiedSlotIndexOffset, value);
    }

    /// <summary>
    /// Resets a slot to the empty state by zeroing all four 64-bit words; <c>hash_hi</c> is written last to keep the publish protocol intact.
    /// </summary>
    /// <param name="index">Slot index in <c>[0, TableSize)</c>.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="index"/> is out of range.</exception>
    /// <remarks>
    /// <para>
    /// <b>Repair-only contract:</b> safe only when the caller holds the shard file exclusively. Used to discard torn slots whose
    /// <c>hash_hi</c> visibility flag was published over inconsistent payload bytes; data in zeroed slots is irrecoverable either way.
    /// </para>
    /// </remarks>
    internal void ResetSlotForRepair(ulong index)
    {
        ThrowIfDisposed();
        ThrowIfReadOnlyScan();
        if (index >= _tableSize)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        byte* slot = GetSlotPtr(index);
        Unsafe.InitBlockUnaligned(slot, 0, (uint)_slotStride);
        WriteSlotCrc(slot);
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
    /// Returns <see langword="true"/> when the slot at <paramref name="index"/> contains a tombstone marker (hash_hi == 1, hash_lo == 0).
    /// </summary>
    /// <param name="index">Slot index in <c>[0, TableSize)</c>.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="index"/> is out of range.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsTombstoneAt(ulong index)
    {
        ThrowIfDisposed();
        if (index >= _tableSize)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        byte* slot = GetSlotPtr(index);
        ulong hi = Volatile.Read(ref *(ulong*)slot);
        if (hi != TombstoneHashHiMarker)
        {
            return false;
        }

        ulong lo = *(ulong*)(slot + 8);
        return lo == TombstoneHashLoMarker;
    }

    /// <summary>
    /// Reads the full slot (hash + server payload) at <paramref name="index"/>, returning <see langword="true"/> only for real occupied entries
    /// (Empty and Tombstone slots return <see langword="false"/>).
    /// </summary>
    /// <param name="index">Slot index in <c>[0, TableSize)</c>.</param>
    /// <param name="hashHi">High 64 bits of the entry hash on success.</param>
    /// <param name="hashLo">Low 64 bits of the entry hash on success.</param>
    /// <param name="serverHi">High 64 bits of the server payload on success.</param>
    /// <param name="serverLo">Low 64 bits of the server payload on success.</param>
    /// <returns><see langword="true"/> if the slot is a published, non-tombstoned entry.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="index"/> is out of range.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadOccupiedSlotAt(ulong index, out ulong hashHi, out ulong hashLo, out ulong serverHi, out ulong serverLo) =>
        TryReadOccupiedSlotAt(index, out hashHi, out hashLo, out serverHi, out serverLo, out _, out _, out _);

    /// <summary>
    /// Like <see cref="TryReadOccupiedSlotAt(ulong, out ulong, out ulong, out ulong, out ulong)"/> but also returns access metadata when the slot is occupied.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadOccupiedSlotAt(
        ulong index,
        out ulong hashHi,
        out ulong hashLo,
        out ulong serverHi,
        out ulong serverLo,
        out long dateObtainedTicks,
        out long dateAccessedTicks,
        out ulong accessedCounter)
    {
        ThrowIfDisposed();
        if (index >= _tableSize)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        byte* slot = GetSlotPtr(index);
        hashHi = Volatile.Read(ref *(ulong*)slot);
        dateObtainedTicks = 0;
        dateAccessedTicks = 0;
        accessedCounter = 0;
        if (hashHi == 0)
        {
            hashLo = 0;
            serverHi = 0;
            serverLo = 0;
            return false;
        }

        hashLo = *(ulong*)(slot + 8);
        if (hashHi == TombstoneHashHiMarker && hashLo == TombstoneHashLoMarker)
        {
            serverHi = 0;
            serverLo = 0;
            return false;
        }

        serverHi = *(ulong*)(slot + 16);
        serverLo = *(ulong*)(slot + 24);
        dateObtainedTicks = Volatile.Read(ref *(long*)(slot + SlotDateObtainedTicksOffset));
        dateAccessedTicks = Volatile.Read(ref *(long*)(slot + SlotDateAccessedTicksOffset));
        accessedCounter = Volatile.Read(ref *(ulong*)(slot + SlotAccessedCounterOffset));
        if (!TryVerifySlotCrc(slot))
        {
            hashHi = 0;
            hashLo = 0;
            serverHi = 0;
            serverLo = 0;
            dateObtainedTicks = 0;
            dateAccessedTicks = 0;
            accessedCounter = 0;
            return false;
        }

        return true;
    }

    /// <summary>
    /// Double-samples the slot with volatile reads and bounded retries to detect torn or inconsistent mmap layout.
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
            long obtA = Volatile.Read(ref *(long*)(slot + SlotDateObtainedTicksOffset));
            long accA = Volatile.Read(ref *(long*)(slot + SlotDateAccessedTicksOffset));
            ulong ctrA = Volatile.Read(ref *(ulong*)(slot + SlotAccessedCounterOffset));
            Thread.MemoryBarrier();
            ulong hiB = Volatile.Read(ref *(ulong*)slot);
            ulong loB = Volatile.Read(ref *(ulong*)(slot + 8));
            ulong sHiB = Volatile.Read(ref *(ulong*)(slot + 16));
            ulong sLoB = Volatile.Read(ref *(ulong*)(slot + 24));
            long obtB = Volatile.Read(ref *(long*)(slot + SlotDateObtainedTicksOffset));
            long accB = Volatile.Read(ref *(long*)(slot + SlotDateAccessedTicksOffset));
            ulong ctrB = Volatile.Read(ref *(ulong*)(slot + SlotAccessedCounterOffset));
            if (hiA == hiB && loA == loB && sHiA == sHiB && sLoA == sLoB && obtA == obtB && accA == accB && ctrA == ctrB)
            {
                if (hiA == 0)
                {
                    outcome = SlotScrubOutcome.OkEmpty;
                }
                else if (!TryVerifySlotCrc(slot))
                {
                    outcome = SlotScrubOutcome.VolatileLayoutMismatch;
                }
                else
                {
                    outcome = SlotScrubOutcome.OkOccupiedStable;
                }

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
    private byte* GetSlotPtr(ulong index) => _basePtr + HeaderSize + ((long)index * _slotStride);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ulong ReadUInt64(int offset)
    {
        ThrowIfDisposed();
        return *(ulong*)(_basePtr + offset);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void WriteUInt64(int offset, ulong value)
    {
        if (_sharedReadOnlyScan)
        {
            throw new InvalidOperationException("Cannot write to a shard opened in SharedReadOnlyScan mode.");
        }

        *(ulong*)(_basePtr + offset) = value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void NormalizeReservedHashes(ulong hashHi, ref ulong hashLo)
    {
        // (0, 0) collides with the EMPTY marker; (1, 0) collides with the TOMBSTONE marker. Both pairs are
        // 2^-128-improbable for random inputs but are mapped onto a non-reserved variant so the reserved sentinels
        // remain unambiguous on disk.
        if (hashLo == 0 && (hashHi == 0 || hashHi == TombstoneHashHiMarker))
        {
            hashLo = 1;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private SlotProbeResult ProbeSlot(byte* slot, ulong hashHi, ulong hashLo)
    {
        // Preserve publish ordering semantics: hash_hi is the visibility flag.
        ulong existingHi = Volatile.Read(ref *(ulong*)slot);
        if (existingHi == 0)
        {
            return SlotProbeResult.Empty;
        }

        // Tombstone fast-detect: hash_hi == 1, hash_lo == 0. Real input hashes are funneled away from this pattern
        // by NormalizeReservedHashes, so a stored (1, 0) is unambiguously a tombstone marker.
        if (existingHi == TombstoneHashHiMarker)
        {
            ulong existingLoFast = *(ulong*)(slot + 8);
            if (existingLoFast == TombstoneHashLoMarker)
            {
                return SlotProbeResult.Tombstone;
            }
        }

        if (Vector128.IsHardwareAccelerated)
        {
            return ProbeSlotVector128(slot, hashHi, hashLo);
        }

        ulong existingLo = *(ulong*)(slot + 8);
        if (existingHi == hashHi && existingLo == hashLo)
        {
            return TryVerifySlotCrc(slot) ? SlotProbeResult.Match : SlotProbeResult.Occupied;
        }

        return SlotProbeResult.Occupied;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private SlotProbeResult ProbeSlotVector128(byte* slot, ulong hashHi, ulong hashLo)
    {
        ulong* hashPair = (ulong*)slot;
        Vector128<ulong> loaded = Vector128.LoadUnsafe(ref hashPair[0]);
        Vector128<ulong> expected = Vector128.Create(hashHi, hashLo);
        Vector128<ulong> eq = Vector128.Equals(loaded, expected);
        if ((eq.GetElement(0) & eq.GetElement(1)) != ulong.MaxValue)
        {
            return SlotProbeResult.Occupied;
        }

        return TryVerifySlotCrc(slot) ? SlotProbeResult.Match : SlotProbeResult.Occupied;
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
        if (Sse.IsSupported)
        {
            Sse.Prefetch0(secondSlot);
        }

        ulong secondHi = Volatile.Read(ref *(ulong*)secondSlot);
        if (secondHi == 0)
        {
            matchedFirstSlot = true;
            return ProbeSlot(firstSlot, hashHi, hashLo) == SlotProbeResult.Match
                ? SlotProbeResult.Match
                : SlotProbeResult.Empty;
        }

        if (Sse.IsSupported)
        {
            ulong next0 = (secondIdx + 1UL) & _mask;
            ulong next1 = (secondIdx + 2UL) & _mask;
            Sse.Prefetch0(GetSlotPtr(next0));
            Sse.Prefetch0(GetSlotPtr(next1));
        }

        // Both slots are non-empty. Each slot is PhysicalSlotStride bytes (56-byte logical payload plus optional CRC suffix).
        // ProbeSlot uses hardware-accelerated Vector128 for the 16-byte hash compare when available, and tombstone fast-detect
        // (hash_hi == 1, hash_lo == 0). Tombstones never count as Match because NormalizeReservedHashes excludes (1, 0) from real input hashes.
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
            WriteUInt64(HeaderVersionOffset, DiskFormatVersion1);
            WriteUInt64(HeaderTableSizeOffset, _tableSize);
            WriteUInt64(HeaderUsedSlotsOffset, 0);
            WriteUInt64(HeaderMaxLoadFactorOffset, _maxLoadFactor);
            WriteUInt64(HeaderMaxOccupiedSlotIndexOffset, 0);
            WriteUInt32(HeaderSlotRegionCrc32Offset, ComputeSlotRegionCrc32());
            return;
        }

        ValidateExistingHeaderReadOnly();
    }

    /// <summary>
    /// Validates a non-empty on-disk header without mutating bytes (used by shared read-only scan opens).
    /// </summary>
    private void ValidateExistingHeaderReadOnly()
    {
        ulong magic = ReadUInt64(HeaderMagicOffset);
        if (magic == 0)
        {
            throw new InvalidDataException("Shard header magic is zero; shared read-only scan cannot initialize a new file.");
        }

        if (magic != Magic)
        {
            throw new InvalidDataException("Shard file magic does not match HISTDB01.");
        }

        ulong version = ReadUInt64(HeaderVersionOffset);
        if (version != DiskFormatVersion1)
        {
            throw new InvalidDataException($"Unsupported shard format version {version}.");
        }

        ulong existingTableSize = ReadUInt64(HeaderTableSizeOffset);
        if (existingTableSize != _tableSize)
        {
            throw new InvalidDataException($"Shard tableSize mismatch. File={existingTableSize}, Requested={_tableSize}.");
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfReadOnlyScan()
    {
        if (_sharedReadOnlyScan)
        {
            throw new InvalidOperationException("This operation is not supported on a shard opened in SharedReadOnlyScan mode.");
        }
    }

    #endregion

    #region Nested types

    private enum SlotProbeResult : byte
    {
        Empty = 0,
        Match = 1,
        Occupied = 2,

        /// <summary>
        /// Slot was previously occupied and explicitly tombstoned via <see cref="HistoryShard.TryTombstone"/>. Probes continue past
        /// tombstones (they do not terminate); inserts may reclaim the first tombstone encountered along their probe path.
        /// </summary>
        Tombstone = 3,
    }

    #endregion
}
