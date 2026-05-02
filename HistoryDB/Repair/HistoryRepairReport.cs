// HistoryRepairReport.cs -- structured findings and actions emitted by HistoryDatabase.ScanAsync / RepairAsync.
// Findings describe what the engine observed; actions describe what the engine actually wrote (or would have written in scan mode).

using System.Collections.ObjectModel;

namespace HistoryDB.Repair;

/// <summary>
/// Severity levels for <see cref="RepairFinding"/>.
/// </summary>
public enum RepairSeverity
{
    /// <summary>Informational; no fix required.</summary>
    Info = 0,

    /// <summary>Recoverable inconsistency that the engine can repair without data loss.</summary>
    Warning = 1,

    /// <summary>Unrecoverable corruption. Under <see cref="RepairPolicy.FailFast"/> the file is left untouched.</summary>
    Error = 2,
}

/// <summary>
/// Stable codes assigned to each finding; usable for dashboards and tests without parsing prose.
/// </summary>
public enum RepairFindingCode
{
    /// <summary>Generic informational finding.</summary>
    Info = 0,

    /// <summary>Shard <c>.dat</c> magic field does not match <c>HISTDB01</c>.</summary>
    HeaderMagicMismatch = 1,

    /// <summary>Shard <c>.dat</c> on-disk version is unsupported.</summary>
    HeaderVersionMismatch = 2,

    /// <summary>Shard <c>.dat</c> header <c>tableSize</c> field disagrees with caller-supplied expectation or is not a power of two.</summary>
    TableSizeMismatch = 3,

    /// <summary>Shard <c>.dat</c> file length does not equal <c>HeaderSize + tableSize * EntrySize</c>.</summary>
    FileLengthMismatch = 4,

    /// <summary>Shard <c>.dat</c> header <c>maxOccupiedSlotIndex</c> watermark disagrees with sequential scan.</summary>
    MaxOccupiedSlotIndexHeaderStale = 5,

    /// <summary>Shard <c>.dat</c> header <c>usedSlots</c> count disagrees with sequential scan.</summary>
    UsedSlotsHeaderStale = 6,

    /// <summary>Sequential scan observed a slot whose 32 bytes failed volatile double-sample consistency.</summary>
    TornSlotDetected = 7,

    /// <summary>Bloom <c>.idx</c> sidecar file is missing for an existing shard.</summary>
    BloomSidecarMissing = 8,

    /// <summary>Bloom <c>.idx</c> sidecar header (magic / version / bit count / hash count / word count) is invalid.</summary>
    BloomSidecarMetadataMismatch = 9,

    /// <summary>Bloom <c>.idx</c> sidecar bits diverge from a fresh scan of the shard.</summary>
    BloomSidecarBitsDivergent = 10,

    /// <summary>Bloom <c>.idx</c> sidecar present without a matching <c>.dat</c> shard data file.</summary>
    OrphanBloomSidecar = 11,

    /// <summary>An unexpected file was found inside a generation directory or the root.</summary>
    UnknownFile = 12,

    /// <summary><c>expired-md5.bin</c> length is not a multiple of 16 bytes (trailing partial record).</summary>
    ExpiredSetTrailingPartial = 13,

    /// <summary><c>history-journal.log</c> contains a line that does not parse under either of the supported field shapes.</summary>
    JournalInvalidLine = 14,

    /// <summary>The engine could not acquire an exclusive open on a shard artifact; another process likely has it open.</summary>
    LockedByOtherProcess = 15,

    /// <summary>Shard <c>.dat</c> header reports a <c>maxLoadFactor</c> outside the legal range or unexpected by the caller.</summary>
    MaxLoadFactorMismatch = 16,

    /// <summary>The number of <c>.dat</c> shards in a generation differs from <see cref="HistoryRepairOptions.ExpectedShardCount"/>.</summary>
    ShardCountMismatch = 17,

    /// <summary>A directory whose name does not match the generation token format was found beneath the root.</summary>
    UnknownGenerationDirectory = 18,
}

/// <summary>
/// Stable codes describing each repair action that was actually applied (or would be applied if scan mode upgraded to repair).
/// </summary>
public enum RepairActionCode
{
    /// <summary>Atomically rewrote a Bloom sidecar to match a fresh scan.</summary>
    BloomSidecarRewritten = 0,

    /// <summary>Wrote the corrected <c>usedSlots</c> value into the shard header.</summary>
    UsedSlotsHeaderRewritten = 1,

    /// <summary>Wrote the corrected <c>maxOccupiedSlotIndex</c> watermark into the shard header.</summary>
    MaxOccupiedSlotIndexHeaderRewritten = 2,

    /// <summary>Truncated <c>expired-md5.bin</c> to drop a trailing partial record.</summary>
    ExpiredSetTruncated = 3,

    /// <summary>Atomically rewrote <c>history-journal.log</c> dropping unparseable lines.</summary>
    JournalRewritten = 4,

    /// <summary>Renamed a shard data or sidecar file to <c>{name}.corrupt-{utcTicks:x}</c> under the quarantine policy.</summary>
    QuarantinedFile = 5,

    /// <summary>Zeroed a torn slot's hash header (slot becomes EMPTY for future inserts).</summary>
    TornSlotZeroed = 6,
}

/// <summary>
/// One observation produced by the scan / repair pipeline.
/// </summary>
/// <param name="Severity">Severity of the observation.</param>
/// <param name="Code">Stable machine-readable identifier.</param>
/// <param name="Path">File or directory the finding refers to (may be empty for global findings).</param>
/// <param name="Detail">Human-readable narrative.</param>
public readonly record struct RepairFinding(
    RepairSeverity Severity,
    RepairFindingCode Code,
    string Path,
    string Detail);

/// <summary>
/// One mutation that the repair engine applied (or would have applied in scan / verify-only mode).
/// </summary>
/// <param name="Code">Stable machine-readable identifier.</param>
/// <param name="Path">File path the action targeted.</param>
/// <param name="Detail">Human-readable narrative.</param>
/// <param name="Applied">When <see langword="true"/>, the engine actually wrote the change. When <see langword="false"/>, the action was suppressed (scan mode, lock contention, or operator-disabled toggle).</param>
public readonly record struct RepairAction(
    RepairActionCode Code,
    string Path,
    string Detail,
    bool Applied);

/// <summary>
/// Aggregate report from <see cref="HistoryDatabase.ScanAsync"/> or <see cref="HistoryDatabase.RepairAsync"/>.
/// </summary>
public sealed class HistoryRepairReport
{
    private readonly List<RepairFinding> _findings = [];
    private readonly List<RepairAction> _actions = [];

    /// <summary>
    /// Findings produced by the scan in source order.
    /// </summary>
    public IReadOnlyList<RepairFinding> Findings { get; }

    /// <summary>
    /// Actions emitted by the repair pipeline in source order.
    /// </summary>
    public IReadOnlyList<RepairAction> Actions { get; }

    /// <summary>Number of generation directories enumerated.</summary>
    public int GenerationsScanned { get; internal set; }

    /// <summary>Number of shard <c>.dat</c> files inspected (including unrecoverable ones).</summary>
    public int ShardsScanned { get; internal set; }

    /// <summary>Number of slots scanned across all shards.</summary>
    public ulong SlotsScanned { get; internal set; }

    /// <summary>Number of occupied slots observed across all shards.</summary>
    public ulong OccupiedSlotsObserved { get; internal set; }

    /// <summary>Number of Bloom sidecars rewritten.</summary>
    public int BloomSidecarsRewritten { get; internal set; }

    /// <summary>Number of shard headers fixed (any of <c>usedSlots</c> / <c>maxOccupiedSlotIndex</c>).</summary>
    public int HeadersRewritten { get; internal set; }

    /// <summary>Number of files quarantined under <see cref="RepairPolicy.QuarantineAndRecreate"/>.</summary>
    public int FilesQuarantined { get; internal set; }

    /// <summary>True when the engine ran in scan-only mode (<see cref="HistoryDatabase.ScanAsync"/>).</summary>
    public bool ScanOnly { get; internal set; }

    /// <summary>True when the engine aborted because of a <see cref="RepairFindingCode.LockedByOtherProcess"/> finding.</summary>
    public bool AbortedDueToLockContention { get; internal set; }

    /// <summary>True if and only if no <see cref="RepairSeverity.Error"/> findings remain after all repairs were applied.</summary>
    public bool IsHealthy
    {
        get
        {
            for (int i = 0; i < _findings.Count; i++)
            {
                if (_findings[i].Severity == RepairSeverity.Error)
                {
                    return false;
                }
            }

            return true;
        }
    }

    internal HistoryRepairReport()
    {
        Findings = new ReadOnlyCollection<RepairFinding>(_findings);
        Actions = new ReadOnlyCollection<RepairAction>(_actions);
    }

    internal void AddFinding(RepairSeverity severity, RepairFindingCode code, string path, string detail) =>
        _findings.Add(new RepairFinding(severity, code, path, detail));

    internal void AddAction(RepairActionCode code, string path, string detail, bool applied) =>
        _actions.Add(new RepairAction(code, path, detail, applied));
}
