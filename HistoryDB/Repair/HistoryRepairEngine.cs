// HistoryRepairEngine.cs -- scan and repair pipeline for HistoryDatabase on-disk artifacts.
// Exclusive repair probes use FileShare.None. Scan-only runs may use HistoryScanOptions.UseSharedRead for shared read opens
// so a live HistoryDatabase can coexist; mutating repair paths always require exclusive access.

using System.Buffers.Binary;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

using HistoryDB.Utilities;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HistoryDB.Repair;

/// <summary>
/// Static-style engine implementing <see cref="HistoryDatabase.ScanAsync"/> and <see cref="HistoryDatabase.RepairAsync"/>.
/// </summary>
internal sealed partial class HistoryRepairEngine
{
    /// <summary>Default Bloom hash function count produced by the writer.</summary>
    private const int DefaultBloomHashFunctions = 7;

    /// <summary>Yield interval for the shard slot scan loop (matches WarmupOneShardBloomAsync cadence).</summary>
    private const ulong WarmupYieldMask = 0xFFFFUL;

    /// <summary>Five-hex-digit lower-case generation directory regex (matches FormatGenerationToken).</summary>
    private static readonly Regex GenerationDirRegex = new("^[0-9a-f]{5}$", RegexOptions.Compiled);

    /// <summary>Four-hex-digit lower-case shard token regex (matches FormatShardSequenceToken).</summary>
    private static readonly Regex ShardTokenRegex = new("^[0-9a-f]{4}$", RegexOptions.Compiled);

    private readonly string _rootPath;
    private readonly HistoryRepairOptions _options;
    private readonly bool _scanOnly;
    private readonly ILogger _logger;
    private readonly HistoryRepairReport _report = new();
    private readonly bool _useSharedRead;

    private HistoryRepairEngine(string rootPath, HistoryRepairOptions options, bool scanOnly, ILogger? logger)
    {
        _rootPath = rootPath;
        _options = options;
        _scanOnly = scanOnly;
        _logger = logger ?? NullLogger.Instance;
        _report.ScanOnly = scanOnly;
        _useSharedRead = scanOnly && options.UseSharedRead;
    }

    /// <summary>Runs the engine in scan-only mode with the supplied options projected to a non-mutating <see cref="HistoryRepairOptions"/>.</summary>
    internal static async Task<HistoryRepairReport> ScanAsync(
        string rootPath,
        HistoryScanOptions? scanOptions,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootPath);

        HistoryScanOptions effectiveScan = scanOptions ?? new HistoryScanOptions();
        HistoryRepairOptions repairOptions = new()
        {
            ExpectedShardCount = effectiveScan.ExpectedShardCount,
            ExpectedSlotsPerShard = effectiveScan.ExpectedSlotsPerShard,
            ExpectedMaxLoadFactorPercent = effectiveScan.ExpectedMaxLoadFactorPercent,
            ScanForTornSlots = effectiveScan.ScanForTornSlots,
            UseSharedRead = effectiveScan.UseSharedRead,
        };

        HistoryRepairEngine engine = new(rootPath, repairOptions, scanOnly: true, logger);
        await engine.RunAsync(cancellationToken).ConfigureAwait(false);
        return engine._report;
    }

    /// <summary>Runs the engine in repair mode with mutations gated by <see cref="HistoryRepairOptions"/>.</summary>
    internal static async Task<HistoryRepairReport> RepairAsync(
        string rootPath,
        HistoryRepairOptions options,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootPath);
        ArgumentNullException.ThrowIfNull(options);

        HistoryRepairEngine engine = new(rootPath, options, scanOnly: false, logger);
        await engine.RunAsync(cancellationToken).ConfigureAwait(false);
        return engine._report;
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        LogEngineStarting(_logger, _rootPath, _scanOnly, _options.Policy);

        if (!Directory.Exists(_rootPath))
        {
            _report.AddFinding(
                RepairSeverity.Error,
                RepairFindingCode.UnknownFile,
                _rootPath,
                "Root directory does not exist.");
            LogEngineCompleted(
                _logger,
                _report.GenerationsScanned,
                _report.ShardsScanned,
                _report.SlotsScanned,
                _report.OccupiedSlotsObserved,
                _report.HeadersRewritten,
                _report.BloomSidecarsRewritten,
                _report.FilesQuarantined,
                _report.IsHealthy);
            return;
        }

        List<string> generationDirs = EnumerateGenerationDirectories();
        List<(string GenDir, string DatPath, string IdxPath)> shardArtifacts = [];

        foreach (string genDir in generationDirs)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CollectShardArtifacts(genDir, shardArtifacts);
        }

        if (!TryAcquireInitialFileLocks(shardArtifacts))
        {
            _report.AbortedDueToLockContention = true;
            LogEngineCompleted(
                _logger,
                _report.GenerationsScanned,
                _report.ShardsScanned,
                _report.SlotsScanned,
                _report.OccupiedSlotsObserved,
                _report.HeadersRewritten,
                _report.BloomSidecarsRewritten,
                _report.FilesQuarantined,
                _report.IsHealthy);
            return;
        }

        foreach (string genDir in generationDirs)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await ProcessGenerationAsync(genDir, cancellationToken).ConfigureAwait(false);
        }

        ProcessExpiredSet(cancellationToken);
        ProcessJournal(cancellationToken);

        LogEngineCompleted(
            _logger,
            _report.GenerationsScanned,
            _report.ShardsScanned,
            _report.SlotsScanned,
            _report.OccupiedSlotsObserved,
            _report.HeadersRewritten,
            _report.BloomSidecarsRewritten,
            _report.FilesQuarantined,
            _report.IsHealthy);
    }

    private List<string> EnumerateGenerationDirectories()
    {
        List<string> generationDirs = [];

        foreach (string subDir in Directory.EnumerateDirectories(_rootPath))
        {
            string name = Path.GetFileName(subDir);
            if (GenerationDirRegex.IsMatch(name))
            {
                generationDirs.Add(subDir);
            }
            else
            {
                _report.AddFinding(
                    RepairSeverity.Warning,
                    RepairFindingCode.UnknownGenerationDirectory,
                    subDir,
                    $"Directory name '{name}' does not match the 5-hex generation token format.");
            }
        }

        generationDirs.Sort(StringComparer.OrdinalIgnoreCase);
        _report.GenerationsScanned = generationDirs.Count;
        return generationDirs;
    }

    private void CollectShardArtifacts(string genDir, List<(string GenDir, string DatPath, string IdxPath)> shardArtifacts)
    {
        foreach (string filePath in Directory.EnumerateFiles(genDir))
        {
            string fileName = Path.GetFileName(filePath);
            string ext = Path.GetExtension(fileName);
            string token = Path.GetFileNameWithoutExtension(fileName);

            // Tolerate quarantined artifacts and do not report them as unknown.
            if (fileName.IndexOf(".corrupt-", StringComparison.Ordinal) >= 0)
            {
                continue;
            }

            if (string.Equals(ext, ".dat", StringComparison.OrdinalIgnoreCase) && ShardTokenRegex.IsMatch(token))
            {
                string datPath = filePath;
                string idxPath = Path.Combine(genDir, $"{token}.idx");
                shardArtifacts.Add((genDir, datPath, idxPath));
            }
            else if (string.Equals(ext, ".idx", StringComparison.OrdinalIgnoreCase) && ShardTokenRegex.IsMatch(token))
            {
                // .idx files are added implicitly when their .dat is enumerated; record orphans below in the per-generation pass.
            }
            else
            {
                _report.AddFinding(
                    RepairSeverity.Warning,
                    RepairFindingCode.UnknownFile,
                    filePath,
                    $"Unexpected file '{fileName}' inside generation directory.");
            }
        }
    }

    private bool TryAcquireInitialFileLocks(IReadOnlyList<(string GenDir, string DatPath, string IdxPath)> shardArtifacts)
    {
        foreach ((string _, string datPath, string idxPath) in shardArtifacts)
        {
            if (_useSharedRead)
            {
                if (!TryProbeSharedRead(datPath))
                {
                    LogAbortLocked(_logger, datPath);
                    return false;
                }

                if (File.Exists(idxPath) && !TryProbeSharedRead(idxPath))
                {
                    LogAbortLocked(_logger, idxPath);
                    return false;
                }
            }
            else
            {
                if (!TryProbeExclusive(datPath))
                {
                    LogAbortLocked(_logger, datPath);
                    return false;
                }

                if (File.Exists(idxPath) && !TryProbeExclusive(idxPath))
                {
                    LogAbortLocked(_logger, idxPath);
                    return false;
                }
            }
        }

        string expiredPath = Path.Combine(_rootPath, "expired-md5.bin");
        if (File.Exists(expiredPath))
        {
            if (_useSharedRead)
            {
                if (!TryProbeSharedRead(expiredPath))
                {
                    LogAbortLocked(_logger, expiredPath);
                    return false;
                }
            }
            else if (!TryProbeExclusive(expiredPath))
            {
                LogAbortLocked(_logger, expiredPath);
                return false;
            }
        }

        string journalPath = Path.Combine(_rootPath, "history-journal.log");
        if (File.Exists(journalPath))
        {
            if (_useSharedRead)
            {
                if (!TryProbeSharedRead(journalPath))
                {
                    LogAbortLocked(_logger, journalPath);
                    return false;
                }
            }
            else if (!TryProbeExclusive(journalPath))
            {
                LogAbortLocked(_logger, journalPath);
                return false;
            }
        }

        return true;
    }

    private bool TryProbeSharedRead(string path)
    {
        try
        {
            using FileStream stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            return true;
        }
        catch (IOException ex)
        {
            _report.AddFinding(
                RepairSeverity.Error,
                RepairFindingCode.LockedByOtherProcess,
                path,
                ex.Message);
            return false;
        }
        catch (UnauthorizedAccessException ex)
        {
            _report.AddFinding(
                RepairSeverity.Error,
                RepairFindingCode.LockedByOtherProcess,
                path,
                ex.Message);
            return false;
        }
    }

    private bool TryProbeExclusive(string path)
    {
        try
        {
            using FileStream stream = File.Open(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            return true;
        }
        catch (IOException ex)
        {
            _report.AddFinding(
                RepairSeverity.Error,
                RepairFindingCode.LockedByOtherProcess,
                path,
                ex.Message);
            return false;
        }
        catch (UnauthorizedAccessException ex)
        {
            _report.AddFinding(
                RepairSeverity.Error,
                RepairFindingCode.LockedByOtherProcess,
                path,
                ex.Message);
            return false;
        }
    }

    private async Task ProcessGenerationAsync(string genDir, CancellationToken cancellationToken)
    {
        SortedDictionary<string, (string DatPath, bool DatExists, string IdxPath, bool IdxExists)> shardEntries = new(StringComparer.OrdinalIgnoreCase);

        foreach (string filePath in Directory.EnumerateFiles(genDir))
        {
            string fileName = Path.GetFileName(filePath);
            if (fileName.IndexOf(".corrupt-", StringComparison.Ordinal) >= 0)
            {
                continue;
            }

            string ext = Path.GetExtension(fileName);
            string token = Path.GetFileNameWithoutExtension(fileName);

            if (!ShardTokenRegex.IsMatch(token))
            {
                continue;
            }

            (string DatPath, bool DatExists, string IdxPath, bool IdxExists) entry = shardEntries.TryGetValue(token, out var existing)
                ? existing
                : (Path.Combine(genDir, $"{token}.dat"), false, Path.Combine(genDir, $"{token}.idx"), false);

            if (string.Equals(ext, ".dat", StringComparison.OrdinalIgnoreCase))
            {
                entry.DatExists = true;
            }
            else if (string.Equals(ext, ".idx", StringComparison.OrdinalIgnoreCase))
            {
                entry.IdxExists = true;
            }

            shardEntries[token] = entry;
        }

        if (_options.ExpectedShardCount is int expected)
        {
            int datCount = 0;
            foreach (var pair in shardEntries)
            {
                if (pair.Value.DatExists)
                {
                    datCount++;
                }
            }

            if (datCount != expected)
            {
                _report.AddFinding(
                    RepairSeverity.Warning,
                    RepairFindingCode.ShardCountMismatch,
                    genDir,
                    $"Generation has {datCount} shard(s); expected {expected}.");
            }
        }

        foreach (var pair in shardEntries)
        {
            cancellationToken.ThrowIfCancellationRequested();
            (string datPath, bool datExists, string idxPath, bool idxExists) = pair.Value;

            if (!datExists && idxExists)
            {
                HandleOrphanIdx(idxPath);
                continue;
            }

            if (!datExists)
            {
                continue;
            }

            await ProcessShardAsync(datPath, idxPath, idxExists, cancellationToken).ConfigureAwait(false);
        }
    }

    private void HandleOrphanIdx(string idxPath)
    {
        _report.AddFinding(
            RepairSeverity.Warning,
            RepairFindingCode.OrphanBloomSidecar,
            idxPath,
            "Bloom sidecar without matching shard data file.");

        if (_scanOnly || _options.Policy != RepairPolicy.QuarantineAndRecreate || !_options.QuarantineOrphanBloomSidecars)
        {
            return;
        }

        string quarantinePath = MakeQuarantinePath(idxPath);
        try
        {
            File.Move(idxPath, quarantinePath);
            _report.FilesQuarantined++;
            _report.AddAction(RepairActionCode.QuarantinedFile, idxPath, $"Renamed orphan sidecar to {Path.GetFileName(quarantinePath)}.", applied: true);
            LogQuarantined(_logger, idxPath, quarantinePath);
        }
        catch (IOException ex)
        {
            LogIoError(_logger, ex, idxPath, ex.Message);
            _report.AddAction(RepairActionCode.QuarantinedFile, idxPath, $"Quarantine failed: {ex.Message}.", applied: false);
        }
    }

    private async Task ProcessShardAsync(string datPath, string idxPath, bool idxExists, CancellationToken cancellationToken)
    {
        _report.ShardsScanned++;

        ShardHeaderProbe probe = ProbeShardHeader(datPath);
        if (!probe.IsRecoverable)
        {
            HandleUnrecoverableShard(datPath, idxPath, idxExists, probe);
            return;
        }

        if (_options.ExpectedSlotsPerShard is ulong expectedSlots && probe.TableSize != expectedSlots)
        {
            _report.AddFinding(
                RepairSeverity.Warning,
                RepairFindingCode.TableSizeMismatch,
                datPath,
                $"On-disk tableSize {probe.TableSize} differs from expected {expectedSlots}.");
        }

        if (_options.ExpectedMaxLoadFactorPercent is ulong expectedLoad && probe.MaxLoadFactor != expectedLoad)
        {
            _report.AddFinding(
                RepairSeverity.Warning,
                RepairFindingCode.MaxLoadFactorMismatch,
                datPath,
                $"On-disk maxLoadFactor {probe.MaxLoadFactor} differs from expected {expectedLoad}.");
        }

        ulong scannedUsed = 0;
        ulong scannedMaxOccupied = 0;
        bool sawAnyOccupied = false;
        int zeroedTornSlots = 0;
        BloomBuilder? bloom = TryCreateBloomBuilder(idxPath, idxExists, datPath);

        try
        {
            using HistoryShard shard = new(
                datPath,
                probe.TableSize,
                probe.MaxLoadFactor,
                _useSharedRead ? HistoryShardOpenMode.SharedReadOnlyScan : HistoryShardOpenMode.ReadWrite);

            for (ulong i = 0; i < probe.TableSize; i++)
            {
                if ((i & WarmupYieldMask) == 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await Task.Yield();
                }

                _report.SlotsScanned++;
                if (shard.TryReadOccupiedSlotAt(i, out ulong hashHi, out ulong hashLo, out _, out _))
                {
                    scannedUsed++;
                    if (i > scannedMaxOccupied)
                    {
                        scannedMaxOccupied = i;
                    }
                    sawAnyOccupied = true;
                    bloom?.Add(hashHi, hashLo);
                }
                else if (shard.IsTombstoneAt(i))
                {
                    // Tombstones consume a slot for watermark purposes (their hash_hi is non-zero, so they affect the linear scan upper bound)
                    // but they are NOT counted as "used" (the writer's TryTombstone explicitly decrements UsedSlots) and they are NOT added to
                    // the rebuilt Bloom filter (otherwise a future un-expire+re-add would see a stale Bloom hit and skip lookups via MayContain).
                    if (i > scannedMaxOccupied)
                    {
                        scannedMaxOccupied = i;
                    }
                    sawAnyOccupied = true;
                }
                else if (_options.ScanForTornSlots && shard.TryScrubSlotLayout(i, out SlotScrubOutcome outcome) && outcome == SlotScrubOutcome.VolatileLayoutMismatch)
                {
                    _report.AddFinding(
                        RepairSeverity.Warning,
                        RepairFindingCode.TornSlotDetected,
                        datPath,
                        $"slot {i}");

                    if (!_scanOnly && _options.Policy == RepairPolicy.QuarantineAndRecreate)
                    {
                        shard.ResetSlotForRepair(i);
                        zeroedTornSlots++;
                        _report.AddAction(
                            RepairActionCode.TornSlotZeroed,
                            datPath,
                            $"slot {i} zeroed",
                            applied: true);
                    }
                }
            }

            _report.OccupiedSlotsObserved += scannedUsed;

            ulong watermark = sawAnyOccupied ? scannedMaxOccupied : 0UL;
            bool headerRewritten = false;

            if (probe.UsedSlots != scannedUsed)
            {
                _report.AddFinding(
                    RepairSeverity.Warning,
                    RepairFindingCode.UsedSlotsHeaderStale,
                    datPath,
                    $"header.usedSlots={probe.UsedSlots} differs from scan {scannedUsed}.");

                if (!_scanOnly && _options.RecomputeUsedSlotsHeader)
                {
                    shard.FlushUsedSlots(scannedUsed);
                    _report.AddAction(
                        RepairActionCode.UsedSlotsHeaderRewritten,
                        datPath,
                        $"usedSlots {probe.UsedSlots} -> {scannedUsed}",
                        applied: true);
                    headerRewritten = true;
                }
            }

            if (probe.MaxOccupiedSlotIndex != watermark)
            {
                _report.AddFinding(
                    RepairSeverity.Warning,
                    RepairFindingCode.MaxOccupiedSlotIndexHeaderStale,
                    datPath,
                    $"header.maxOccupiedSlotIndex={probe.MaxOccupiedSlotIndex} differs from scan {watermark}.");

                if (!_scanOnly && _options.RecomputeMaxOccupiedSlotIndexHeader)
                {
                    shard.OverwriteMaxOccupiedSlotIndexForRepair(watermark);
                    _report.AddAction(
                        RepairActionCode.MaxOccupiedSlotIndexHeaderRewritten,
                        datPath,
                        $"maxOccupiedSlotIndex {probe.MaxOccupiedSlotIndex} -> {watermark}",
                        applied: true);
                    headerRewritten = true;
                }
            }

            if (headerRewritten)
            {
                _report.HeadersRewritten++;
                LogHeaderRewritten(_logger, datPath, probe.UsedSlots, scannedUsed, probe.MaxOccupiedSlotIndex, watermark);
            }

            if (zeroedTornSlots > 0)
            {
                LogTornSlotsZeroed(_logger, datPath, zeroedTornSlots);
            }
        }
        catch (IOException ex)
        {
            _report.AddFinding(RepairSeverity.Error, RepairFindingCode.FileLengthMismatch, datPath, ex.Message);
            LogIoError(_logger, ex, datPath, ex.Message);
            return;
        }
        catch (InvalidDataException ex)
        {
            _report.AddFinding(RepairSeverity.Error, RepairFindingCode.HeaderMagicMismatch, datPath, ex.Message);
            LogIoError(_logger, ex, datPath, ex.Message);
            return;
        }

        TryRebuildBloom(datPath, idxPath, idxExists, bloom);
    }

    private void HandleUnrecoverableShard(string datPath, string idxPath, bool idxExists, ShardHeaderProbe probe)
    {
        _report.AddFinding(
            RepairSeverity.Error,
            probe.RootCauseCode,
            datPath,
            probe.RootCauseDetail);

        LogUnrecoverableShard(_logger, datPath, probe.RootCauseDetail);

        if (_scanOnly || _options.Policy != RepairPolicy.QuarantineAndRecreate)
        {
            return;
        }

        string datQuarantine = MakeQuarantinePath(datPath);
        try
        {
            File.Move(datPath, datQuarantine);
            _report.FilesQuarantined++;
            _report.AddAction(RepairActionCode.QuarantinedFile, datPath, $"Renamed shard data to {Path.GetFileName(datQuarantine)}.", applied: true);
            LogQuarantined(_logger, datPath, datQuarantine);
        }
        catch (IOException ex)
        {
            LogIoError(_logger, ex, datPath, ex.Message);
            _report.AddAction(RepairActionCode.QuarantinedFile, datPath, $"Quarantine failed: {ex.Message}.", applied: false);
            return;
        }

        if (idxExists)
        {
            string idxQuarantine = MakeQuarantinePath(idxPath);
            try
            {
                File.Move(idxPath, idxQuarantine);
                _report.FilesQuarantined++;
                _report.AddAction(RepairActionCode.QuarantinedFile, idxPath, $"Renamed sidecar to {Path.GetFileName(idxQuarantine)}.", applied: true);
                LogQuarantined(_logger, idxPath, idxQuarantine);
            }
            catch (IOException ex)
            {
                LogIoError(_logger, ex, idxPath, ex.Message);
                _report.AddAction(RepairActionCode.QuarantinedFile, idxPath, $"Quarantine failed: {ex.Message}.", applied: false);
            }
        }
    }

    private BloomBuilder? TryCreateBloomBuilder(string idxPath, bool idxExists, string datPath)
    {
        FileShare bloomShare = _useSharedRead ? FileShare.ReadWrite : FileShare.None;
        if (idxExists && BloomSidecarHeaderReader.TryReadHeader(idxPath, out int existingBitCount, out int existingHashCount, bloomShare))
        {
            return new BloomBuilder(existingBitCount, existingHashCount);
        }

        if (idxExists)
        {
            _report.AddFinding(
                RepairSeverity.Warning,
                RepairFindingCode.BloomSidecarMetadataMismatch,
                idxPath,
                "Bloom sidecar header is invalid or unreadable.");
        }
        else
        {
            _report.AddFinding(
                RepairSeverity.Info,
                RepairFindingCode.BloomSidecarMissing,
                idxPath,
                "No Bloom sidecar present; will be rebuilt on next open by warmup.");
        }

        if (_options.OverrideBloomBitsPerShard is int overrideBits && overrideBits > 0)
        {
            int hashes = _options.OverrideBloomHashFunctions ?? DefaultBloomHashFunctions;
            return new BloomBuilder(overrideBits, hashes);
        }

        // No reliable bit count to repair against; let normal-open warmup handle it.
        _ = datPath;
        return null;
    }

    private void TryRebuildBloom(string datPath, string idxPath, bool idxExists, BloomBuilder? bloom)
    {
        if (bloom is null)
        {
            return;
        }

        bool needsWrite;
        if (idxExists)
        {
            FileShare bloomShare = _useSharedRead ? FileShare.ReadWrite : FileShare.None;
            ulong[]? existingWords = BloomSidecarHeaderReader.TryReadWords(idxPath, bloom.BitCount, bloom.HashCount, bloomShare);
            if (existingWords is null)
            {
                _report.AddFinding(
                    RepairSeverity.Warning,
                    RepairFindingCode.BloomSidecarBitsDivergent,
                    idxPath,
                    "Existing sidecar payload is unreadable.");
                needsWrite = true;
            }
            else if (!WordsEqual(existingWords, bloom.Words))
            {
                _report.AddFinding(
                    RepairSeverity.Warning,
                    RepairFindingCode.BloomSidecarBitsDivergent,
                    idxPath,
                    "Existing sidecar bits diverge from a fresh scan.");
                needsWrite = true;
            }
            else
            {
                needsWrite = false;
            }
        }
        else
        {
            needsWrite = true;
        }

        if (!needsWrite || _scanOnly || !_options.RebuildBloomSidecars)
        {
            if (needsWrite)
            {
                _report.AddAction(
                    RepairActionCode.BloomSidecarRewritten,
                    idxPath,
                    "Rewrite suppressed by options or scan-only mode.",
                    applied: false);
            }

            return;
        }

        try
        {
            BloomSidecarFile.WriteSidecar(idxPath, bloom.Words, bloom.BitCount, bloom.HashCount);
            _report.BloomSidecarsRewritten++;
            _report.AddAction(
                RepairActionCode.BloomSidecarRewritten,
                idxPath,
                $"Atomic rewrite ({bloom.BitCount} bits, {bloom.HashCount} hashes).",
                applied: true);
            LogBloomRewritten(_logger, idxPath, _report.OccupiedSlotsObserved, bloom.BitCount, bloom.HashCount);
        }
        catch (IOException ex)
        {
            LogIoError(_logger, ex, idxPath, ex.Message);
            _report.AddAction(RepairActionCode.BloomSidecarRewritten, idxPath, $"Atomic rewrite failed: {ex.Message}.", applied: false);
        }

        _ = datPath;
    }

    private static bool WordsEqual(ulong[] a, ulong[] b)
    {
        if (a.Length != b.Length)
        {
            return false;
        }

        for (int i = 0; i < a.Length; i++)
        {
            if (a[i] != b[i])
            {
                return false;
            }
        }

        return true;
    }

    private void ProcessExpiredSet(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string path = Path.Combine(_rootPath, "expired-md5.bin");
        if (!File.Exists(path))
        {
            return;
        }

        long length;
        try
        {
            length = new FileInfo(path).Length;
        }
        catch (IOException ex)
        {
            LogIoError(_logger, ex, path, ex.Message);
            return;
        }

        if (length >= ExpiredSetFile.HeaderByteLength)
        {
            try
            {
                using FileStream fs = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                Span<byte> hdr = stackalloc byte[ExpiredSetFile.HeaderByteLength];
                if (fs.Read(hdr) == ExpiredSetFile.HeaderByteLength)
                {
                    uint magic = BinaryPrimitives.ReadUInt32LittleEndian(hdr[..4]);
                    if (magic == ExpiredSetFile.FileMagic)
                    {
                        ulong count = BinaryPrimitives.ReadUInt64LittleEndian(hdr.Slice(8, 8));
                        long expectedLength = ExpiredSetFile.HeaderByteLength + checked((long)count * 16L);
                        if (expectedLength != length)
                        {
                            _report.AddFinding(
                                RepairSeverity.Warning,
                                RepairFindingCode.ExpiredSetTrailingPartial,
                                path,
                                $"Formatted expired set: expected {expectedLength} bytes for count={count}, file is {length} bytes.");
                        }

                        return;
                    }
                }
            }
            catch (IOException ex)
            {
                LogIoError(_logger, ex, path, ex.Message);
                return;
            }
        }

        const int RecordSize = 16;
        long remainder = length % RecordSize;
        if (remainder == 0)
        {
            return;
        }

        _report.AddFinding(
            RepairSeverity.Warning,
            RepairFindingCode.ExpiredSetTrailingPartial,
            path,
            $"File length {length} is not a multiple of {RecordSize}; {remainder} trailing byte(s) drop on next load.");

        if (_scanOnly || !_options.TruncateExpiredSetTrailingPartial)
        {
            _report.AddAction(
                RepairActionCode.ExpiredSetTruncated,
                path,
                "Truncation suppressed by options or scan-only mode.",
                applied: false);
            return;
        }

        long targetLength = length - remainder;
        try
        {
            using FileStream stream = File.Open(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            stream.SetLength(targetLength);
            stream.Flush(flushToDisk: true);
            _report.AddAction(
                RepairActionCode.ExpiredSetTruncated,
                path,
                $"Truncated to {targetLength} bytes.",
                applied: true);
            LogTruncatedTail(_logger, path, (int)remainder);
        }
        catch (IOException ex)
        {
            LogIoError(_logger, ex, path, ex.Message);
            _report.AddAction(RepairActionCode.ExpiredSetTruncated, path, $"Truncation failed: {ex.Message}.", applied: false);
        }
    }

    private void ProcessJournal(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string path = Path.Combine(_rootPath, "history-journal.log");
        if (!File.Exists(path))
        {
            return;
        }

        List<string> validLines = [];
        int invalidCount = 0;
        int lineNumber = 0;
        bool sawAnyInvalid = false;

        try
        {
            foreach (string line in File.ReadLines(path))
            {
                cancellationToken.ThrowIfCancellationRequested();
                lineNumber++;
                if (TryParseJournalLine(line))
                {
                    validLines.Add(line);
                }
                else
                {
                    invalidCount++;
                    sawAnyInvalid = true;
                    _report.AddFinding(
                        RepairSeverity.Warning,
                        RepairFindingCode.JournalInvalidLine,
                        path,
                        $"Line {lineNumber} is unparseable; will be {(_options.RewriteJournalDroppingInvalidLines ? "dropped" : "skipped on load")}.");
                }
            }
        }
        catch (IOException ex)
        {
            LogIoError(_logger, ex, path, ex.Message);
            return;
        }

        if (!sawAnyInvalid)
        {
            return;
        }

        if (_scanOnly || !_options.RewriteJournalDroppingInvalidLines)
        {
            _report.AddAction(
                RepairActionCode.JournalRewritten,
                path,
                "Rewrite suppressed by options or scan-only mode.",
                applied: false);
            return;
        }

        string tempPath = path + ".repair.tmp";
        try
        {
            using (FileStream stream = File.Open(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
            using (StreamWriter writer = new(stream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false)) { NewLine = "\n" })
            {
                foreach (string keepLine in validLines)
                {
                    writer.Write(keepLine);
                    writer.Write('\n');
                }

                writer.Flush();
                stream.Flush(flushToDisk: true);
            }

            File.Move(tempPath, path, overwrite: true);
            _report.AddAction(
                RepairActionCode.JournalRewritten,
                path,
                $"Atomic rewrite kept {validLines.Count} line(s); dropped {invalidCount}.",
                applied: true);
            LogJournalRewritten(_logger, path, invalidCount, validLines.Count);
        }
        catch (IOException ex)
        {
            LogIoError(_logger, ex, path, ex.Message);
            _report.AddAction(RepairActionCode.JournalRewritten, path, $"Rewrite failed: {ex.Message}.", applied: false);
            try
            {
                if (File.Exists(tempPath))
                {
                    File.Delete(tempPath);
                }
            }
            catch (IOException)
            {
                // Best-effort cleanup; original journal is intact.
            }
        }
    }

    private static bool TryParseJournalLine(string line)
    {
        if (string.IsNullOrEmpty(line))
        {
            return false;
        }

        string[] parts = line.Split('|');
        if (parts.Length is not 3 and not 5)
        {
            return false;
        }

        try
        {
            _ = Convert.FromBase64String(parts[0]);
            _ = Convert.FromBase64String(parts[1]);
        }
        catch (FormatException)
        {
            return false;
        }

        if (!long.TryParse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
        {
            return false;
        }

        if (parts.Length == 5)
        {
            if (!ulong.TryParse(parts[3], NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
            {
                return false;
            }

            if (!ulong.TryParse(parts[4], NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
            {
                return false;
            }
        }

        return true;
    }

    private static string MakeQuarantinePath(string original)
    {
        string suffix = DateTime.UtcNow.Ticks.ToString("x", CultureInfo.InvariantCulture);
        return $"{original}.corrupt-{suffix}";
    }

    private ShardHeaderProbe ProbeShardHeader(string datPath)
    {
        long fileLength;
        try
        {
            fileLength = new FileInfo(datPath).Length;
        }
        catch (IOException ex)
        {
            return ShardHeaderProbe.Unrecoverable(RepairFindingCode.FileLengthMismatch, ex.Message);
        }

        if (fileLength < HistoryShard.HeaderByteLength)
        {
            return ShardHeaderProbe.Unrecoverable(
                RepairFindingCode.FileLengthMismatch,
                $"File length {fileLength} is shorter than the {HistoryShard.HeaderByteLength}-byte header.");
        }

        Span<byte> headerBytes = stackalloc byte[HistoryShard.HeaderByteLength];
        FileShare headerShare = _useSharedRead ? FileShare.ReadWrite : FileShare.None;
        try
        {
            using FileStream stream = File.Open(datPath, FileMode.Open, FileAccess.Read, headerShare);
            int read = stream.Read(headerBytes);
            if (read != HistoryShard.HeaderByteLength)
            {
                return ShardHeaderProbe.Unrecoverable(
                    RepairFindingCode.FileLengthMismatch,
                    $"Read {read} of {HistoryShard.HeaderByteLength} header bytes.");
            }
        }
        catch (IOException ex)
        {
            return ShardHeaderProbe.Unrecoverable(RepairFindingCode.FileLengthMismatch, ex.Message);
        }
        catch (UnauthorizedAccessException ex)
        {
            return ShardHeaderProbe.Unrecoverable(RepairFindingCode.FileLengthMismatch, ex.Message);
        }

        ulong magic = BinaryPrimitives.ReadUInt64LittleEndian(headerBytes[HistoryShard.MagicOffset..]);
        ulong version = BinaryPrimitives.ReadUInt64LittleEndian(headerBytes[HistoryShard.VersionOffset..]);
        ulong tableSize = BinaryPrimitives.ReadUInt64LittleEndian(headerBytes[HistoryShard.TableSizeOffset..]);
        ulong usedSlots = BinaryPrimitives.ReadUInt64LittleEndian(headerBytes[HistoryShard.UsedSlotsOffset..]);
        ulong maxLoadFactor = BinaryPrimitives.ReadUInt64LittleEndian(headerBytes[HistoryShard.MaxLoadFactorOffset..]);
        ulong watermark = BinaryPrimitives.ReadUInt64LittleEndian(headerBytes[HistoryShard.MaxOccupiedSlotIndexOffset..]);

        // Treat an all-zero header (magic == 0) as a freshly-created file: HistoryShard initializes it on next open.
        if (magic == 0 && fileLength == HistoryShard.HeaderByteLength)
        {
            return ShardHeaderProbe.Unrecoverable(
                RepairFindingCode.FileLengthMismatch,
                "Header bytes are zero and file is header-only; treating as orphan stub.");
        }

        if (magic == 0)
        {
            // A zeroed header on a non-empty file means the writer was killed before initializing; HistoryShard would
            // initialize it from scratch losing any existing slots. We refuse to interpret it.
            return ShardHeaderProbe.Unrecoverable(
                RepairFindingCode.HeaderMagicMismatch,
                "Header magic is zero but file contains slot bytes; refusing to overwrite header.");
        }

        if (magic != HistoryShard.ExpectedFileMagic)
        {
            return ShardHeaderProbe.Unrecoverable(
                RepairFindingCode.HeaderMagicMismatch,
                $"Header magic 0x{magic:X16} != HISTDB01.");
        }

        if (version != HistoryShard.DiskFormatVersion1)
        {
            return ShardHeaderProbe.Unrecoverable(
                RepairFindingCode.HeaderVersionMismatch,
                $"Unsupported shard format version {version}.");
        }

        if (tableSize == 0 || (tableSize & (tableSize - 1)) != 0)
        {
            return ShardHeaderProbe.Unrecoverable(
                RepairFindingCode.TableSizeMismatch,
                $"tableSize {tableSize} is not a power of two.");
        }

        if (!HistoryShard.TryResolveSlotStrideFromFileLength(fileLength, tableSize, out int slotStride, out string? strideError))
        {
            return ShardHeaderProbe.Unrecoverable(
                RepairFindingCode.FileLengthMismatch,
                strideError ?? "Could not resolve slot stride from file length.");
        }

        long expectedLength = HistoryShard.HeaderByteLength + (long)tableSize * slotStride;
        if (expectedLength != fileLength)
        {
            return ShardHeaderProbe.Unrecoverable(
                RepairFindingCode.FileLengthMismatch,
                $"File length {fileLength} != expected {expectedLength} for tableSize {tableSize}.");
        }

        if (maxLoadFactor == 0 || maxLoadFactor >= 100)
        {
            return ShardHeaderProbe.Unrecoverable(
                RepairFindingCode.MaxLoadFactorMismatch,
                $"maxLoadFactor {maxLoadFactor} is not in [1, 99].");
        }

        if (watermark >= tableSize)
        {
            return ShardHeaderProbe.Unrecoverable(
                RepairFindingCode.MaxOccupiedSlotIndexHeaderStale,
                $"maxOccupiedSlotIndex {watermark} >= tableSize {tableSize}.");
        }

        return new ShardHeaderProbe(
            isRecoverable: true,
            tableSize: tableSize,
            usedSlots: usedSlots,
            maxLoadFactor: maxLoadFactor,
            maxOccupiedSlotIndex: watermark,
            rootCauseCode: RepairFindingCode.Info,
            rootCauseDetail: string.Empty);
    }

    private readonly struct ShardHeaderProbe
    {
        public ShardHeaderProbe(
            bool isRecoverable,
            ulong tableSize,
            ulong usedSlots,
            ulong maxLoadFactor,
            ulong maxOccupiedSlotIndex,
            RepairFindingCode rootCauseCode,
            string rootCauseDetail)
        {
            IsRecoverable = isRecoverable;
            TableSize = tableSize;
            UsedSlots = usedSlots;
            MaxLoadFactor = maxLoadFactor;
            MaxOccupiedSlotIndex = maxOccupiedSlotIndex;
            RootCauseCode = rootCauseCode;
            RootCauseDetail = rootCauseDetail;
        }

        public bool IsRecoverable { get; }
        public ulong TableSize { get; }
        public ulong UsedSlots { get; }
        public ulong MaxLoadFactor { get; }
        public ulong MaxOccupiedSlotIndex { get; }
        public RepairFindingCode RootCauseCode { get; }
        public string RootCauseDetail { get; }

        public static ShardHeaderProbe Unrecoverable(RepairFindingCode code, string detail) =>
            new(false, 0, 0, 0, 0, code, detail);
    }

    private sealed class BloomBuilder
    {
        private readonly ulong[] _words;
        private readonly int _hashCount;
        private readonly int _bitMask;

        public int BitCount => _bitMask + 1;
        public int HashCount => _hashCount;
        public ulong[] Words => _words;

        public BloomBuilder(int bitCount, int hashCount)
        {
            if (bitCount <= 0 || (bitCount & (bitCount - 1)) != 0)
            {
                throw new ArgumentOutOfRangeException(nameof(bitCount));
            }

            if (hashCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(hashCount));
            }

            _words = new ulong[bitCount >> 6];
            _hashCount = hashCount;
            _bitMask = bitCount - 1;
        }

        public void Add(ulong hashHi, ulong hashLo)
        {
            // Mirror BloomFilter64.Mix / Add so a repaired sidecar matches what the live writer would produce.
            ulong h1 = Mix(hashLo);
            ulong h2 = Mix(hashHi ^ 0x9E3779B97F4A7C15UL) | 1UL;
            for (int i = 0; i < _hashCount; i++)
            {
                ulong combined = h1 + ((ulong)i * h2);
                int bitIndex = (int)(combined & (uint)_bitMask);
                int wordIndex = bitIndex >> 6;
                ulong bit = 1UL << (bitIndex & 63);
                _words[wordIndex] |= bit;
            }
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

    private static class BloomSidecarHeaderReader
    {
        public static bool TryReadHeader(string path, out int bitCount, out int hashCount, FileShare share = FileShare.None)
        {
            bitCount = 0;
            hashCount = 0;
            try
            {
                using FileStream stream = File.Open(path, FileMode.Open, FileAccess.Read, share);
                using BinaryReader reader = new(stream);
                if (stream.Length < 5 * sizeof(int))
                {
                    return false;
                }

                uint magic = reader.ReadUInt32();
                uint version = reader.ReadUInt32();
                int onDiskBitCount = reader.ReadInt32();
                int onDiskHashCount = reader.ReadInt32();
                int onDiskWordCount = reader.ReadInt32();
                if (magic != BloomSidecarFile.FileMagic || version != BloomSidecarFile.FileVersion)
                {
                    return false;
                }

                if (onDiskBitCount <= 0 || (onDiskBitCount & (onDiskBitCount - 1)) != 0)
                {
                    return false;
                }

                if (onDiskHashCount <= 0 || onDiskWordCount != (onDiskBitCount >> 6))
                {
                    return false;
                }

                long expectedLength = (5L * sizeof(int)) + ((long)onDiskWordCount * sizeof(ulong));
                if (stream.Length != expectedLength)
                {
                    return false;
                }

                bitCount = onDiskBitCount;
                hashCount = onDiskHashCount;
                return true;
            }
            catch (IOException)
            {
                return false;
            }
            catch (UnauthorizedAccessException)
            {
                return false;
            }
        }

        public static ulong[]? TryReadWords(string path, int expectedBitCount, int expectedHashCount, FileShare share = FileShare.None)
        {
            try
            {
                using FileStream stream = File.Open(path, FileMode.Open, FileAccess.Read, share);
                using BinaryReader reader = new(stream);
                if (stream.Length < 5 * sizeof(int))
                {
                    return null;
                }

                uint magic = reader.ReadUInt32();
                uint version = reader.ReadUInt32();
                int bitCount = reader.ReadInt32();
                int hashCount = reader.ReadInt32();
                int wordCount = reader.ReadInt32();
                if (magic != BloomSidecarFile.FileMagic ||
                    version != BloomSidecarFile.FileVersion ||
                    bitCount != expectedBitCount ||
                    hashCount != expectedHashCount ||
                    wordCount != (expectedBitCount >> 6))
                {
                    return null;
                }

                ulong[] words = new ulong[wordCount];
                for (int i = 0; i < wordCount; i++)
                {
                    words[i] = reader.ReadUInt64();
                }

                return words;
            }
            catch (IOException)
            {
                return null;
            }
            catch (UnauthorizedAccessException)
            {
                return null;
            }
        }
    }
}
