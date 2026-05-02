using HisBench.Contracts;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace HisBench.Infrastructure;

internal sealed partial class ColdRestartCoordinator : IColdRestartCoordinator
{
    private readonly ILogger<ColdRestartCoordinator> _logger;

    public ColdRestartCoordinator(ILogger<ColdRestartCoordinator> logger)
    {
        _logger = logger;
    }

    public async Task TryColdRestartAsync(string historyDirectory, CancellationToken cancellationToken)
    {
        if (OperatingSystem.IsLinux())
        {
            await TryLinuxDropCaches(cancellationToken).ConfigureAwait(false);
            return;
        }

        if (OperatingSystem.IsWindows())
        {
            await TryWindowsStandbyPurge(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task TryLinuxDropCaches(CancellationToken ct)
    {
        // Prefer /bin/sh (POSIX, present on every Linux distro including Alpine/busybox); falls back to bash if /bin/sh
        // is missing for some reason. The drop_caches write requires root: when sudo is unavailable we just log and continue.
        string shell = File.Exists("/bin/sh") ? "/bin/sh" : "bash";
        try
        {
            using Process proc = Process.Start(new ProcessStartInfo
            {
                FileName = shell,
                ArgumentList = { "-c", "sync; (echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null) || (echo 3 > /proc/sys/vm/drop_caches)" },
                UseShellExecute = false,
                CreateNoWindow = true
            })!;
            await proc.WaitForExitAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogLinuxColdRestartFailed(_logger, ex);
        }
    }

    private async Task TryWindowsStandbyPurge(CancellationToken ct)
    {
        try
        {
            string sysRoot = Environment.GetFolderPath(Environment.SpecialFolder.Windows);
            string ramMapPath = Path.Combine(sysRoot, "System32", "RAMMap.exe");
            if (!File.Exists(ramMapPath))
            {
                LogRamMapNotFound(_logger);
                return;
            }

            using Process proc = Process.Start(new ProcessStartInfo
            {
                FileName = ramMapPath,
                Arguments = "-Ew",
                UseShellExecute = false,
                CreateNoWindow = true
            })!;
            await proc.WaitForExitAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogWindowsColdRestartFailed(_logger, ex);
        }
    }

    [LoggerMessage(EventId = 3000, Level = LogLevel.Warning, Message = "Linux cold restart cache drop failed.")]
    private static partial void LogLinuxColdRestartFailed(ILogger logger, Exception ex);

    [LoggerMessage(EventId = 3001, Level = LogLevel.Information, Message = "RAMMap.exe not found, skipping Windows standby list purge.")]
    private static partial void LogRamMapNotFound(ILogger logger);

    [LoggerMessage(EventId = 3002, Level = LogLevel.Warning, Message = "Windows cold restart purge failed.")]
    private static partial void LogWindowsColdRestartFailed(ILogger logger, Exception ex);
}
