// HistoryRepairOptions.cs -- offline scan and repair option records for HistoryDatabase artifacts.
// Used by HistoryDatabase.ScanAsync and HistoryDatabase.RepairAsync; consumed by the internal HistoryRepairEngine.

namespace HistoryDB.Repair;

/// <summary>
/// Behavior selector for how the repair engine treats unrecoverable shard data files.
/// </summary>
/// <remarks>
/// <para>
/// <b>Non-destructive fixes</b> (Bloom sidecar rebuild, header watermark/used-slot recomputation, expired-set tail truncation,
/// and optional journal rewrite) are applied under both policies because they recompute from intact data without discarding records.
/// </para>
/// <para>
/// The policy only changes how the engine reacts to <c>.dat</c> files whose payload cannot be recovered (bad magic, unsupported version,
/// file length inconsistent with any plausible header).
/// </para>
/// </remarks>
public enum RepairPolicy
{
    /// <summary>
    /// Default. Report unrecoverable shard data as <see cref="RepairSeverity.Error"/> and leave the file untouched for operator inspection.
    /// </summary>
    FailFast = 0,

    /// <summary>
    /// Rename unrecoverable shard <c>.dat</c> (and matching <c>.idx</c>) to <c>{name}.corrupt-{utcTicks:x}</c>. The next normal
    /// <see cref="HistoryDatabase"/> open recreates the empty replacement via <c>MemoryMappedFile.CreateFromFile(..., OpenOrCreate, ...)</c>.
    /// </summary>
    QuarantineAndRecreate = 1,
}

/// <summary>
/// Options for <see cref="HistoryDatabase.ScanAsync"/>. Read-only; the engine never mutates files in scan mode.
/// </summary>
public sealed record HistoryScanOptions
{
    /// <summary>
    /// When set, asserts the on-disk shard count per generation. Mismatches surface as findings without aborting the scan.
    /// </summary>
    public int? ExpectedShardCount { get; init; }

    /// <summary>
    /// When set, asserts every shard's on-disk <c>tableSize</c> header field equals this value.
    /// </summary>
    public ulong? ExpectedSlotsPerShard { get; init; }

    /// <summary>
    /// When set, asserts every shard's on-disk <c>maxLoadFactor</c> header field equals this value.
    /// </summary>
    public ulong? ExpectedMaxLoadFactorPercent { get; init; }

    /// <summary>
    /// When set (default), classifies suspect slots via <see cref="HistoryShard.TryScrubSlotLayout"/> and emits one finding per torn slot.
    /// Disable to keep large scans purely sequential without volatile probes.
    /// </summary>
    public bool ScanForTornSlots { get; init; } = true;

    /// <summary>
    /// When <see langword="true"/>, scan-only opens shard and sidecar files with shared read access so a live
    /// <see cref="HistoryDatabase"/> may keep the tree open. Mutating repair actions remain disabled in scan mode.
    /// </summary>
    public bool UseSharedRead { get; init; }
}

/// <summary>
/// Options for <see cref="HistoryDatabase.RepairAsync"/>. Mutating fields are gated by individual booleans so operators can apply
/// the minimal set of fixes needed for their environment.
/// </summary>
/// <remarks>
/// <para>
/// <b>Concurrency:</b> the repair engine attempts an exclusive open on every shard <c>.dat</c> and <c>.idx</c> before any mutation. If
/// any open fails the run aborts with a <see cref="RepairFindingCode.LockedByOtherProcess"/> finding and zero changes are applied,
/// ensuring the operation is safe to invoke against a directory that an open <see cref="HistoryDatabase"/> instance might still own.
/// </para>
/// </remarks>
public sealed record HistoryRepairOptions
{
    /// <summary>
    /// Behavior for unrecoverable shard data files. Defaults to <see cref="RepairPolicy.FailFast"/>.
    /// </summary>
    public RepairPolicy Policy { get; init; } = RepairPolicy.FailFast;

    /// <inheritdoc cref="HistoryScanOptions.ExpectedShardCount"/>
    public int? ExpectedShardCount { get; init; }

    /// <inheritdoc cref="HistoryScanOptions.ExpectedSlotsPerShard"/>
    public ulong? ExpectedSlotsPerShard { get; init; }

    /// <inheritdoc cref="HistoryScanOptions.ExpectedMaxLoadFactorPercent"/>
    public ulong? ExpectedMaxLoadFactorPercent { get; init; }

    /// <inheritdoc cref="HistoryScanOptions.ScanForTornSlots"/>
    public bool ScanForTornSlots { get; init; } = true;

    /// <inheritdoc cref="HistoryScanOptions.UseSharedRead"/>
    public bool UseSharedRead { get; init; }

    /// <summary>
    /// When <see langword="true"/> (default), atomically rewrites Bloom sidecars whose contents diverge from a fresh scan or whose metadata is invalid.
    /// </summary>
    public bool RebuildBloomSidecars { get; init; } = true;

    /// <summary>
    /// When <see langword="true"/> (default), writes the corrected <c>usedSlots</c> count back into the shard header when scan disagrees with header.
    /// </summary>
    public bool RecomputeUsedSlotsHeader { get; init; } = true;

    /// <summary>
    /// When <see langword="true"/> (default), writes the corrected <c>maxOccupiedSlotIndex</c> watermark back into the shard header when scan disagrees with header.
    /// </summary>
    public bool RecomputeMaxOccupiedSlotIndexHeader { get; init; } = true;

    /// <summary>
    /// When <see langword="true"/> (default), truncates <c>expired-md5.bin</c> trailing partial records (file length not a multiple of 16 bytes).
    /// </summary>
    public bool TruncateExpiredSetTrailingPartial { get; init; } = true;

    /// <summary>
    /// When <see langword="true"/>, atomically rewrites <c>history-journal.log</c> to drop unparseable lines. Default is <see langword="false"/>
    /// so that operators must opt in to mutating the durable journal.
    /// </summary>
    public bool RewriteJournalDroppingInvalidLines { get; init; }

    /// <summary>
    /// When <see langword="true"/> in <see cref="RepairPolicy.QuarantineAndRecreate"/>, also quarantines orphan <c>.idx</c> sidecars whose matching <c>.dat</c> file is missing.
    /// Default is <see langword="true"/>.
    /// </summary>
    public bool QuarantineOrphanBloomSidecars { get; init; } = true;

    /// <summary>
    /// Optional override for the Bloom sidecar bit count used when the existing <c>.idx</c> is missing or unreadable.
    /// When unset, missing sidecars are reported (informational) and left for the next normal open to rebuild via warmup.
    /// </summary>
    public int? OverrideBloomBitsPerShard { get; init; }

    /// <summary>
    /// Optional override for the Bloom sidecar hash function count when rebuilding without a readable existing <c>.idx</c>. Defaults to the writer's compile-time constant (7).
    /// </summary>
    public int? OverrideBloomHashFunctions { get; init; }
}
