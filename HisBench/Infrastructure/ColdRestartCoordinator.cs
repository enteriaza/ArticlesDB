using HisBench.Contracts;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace HisBench.Infrastructure;

internal sealed class ColdRestartCoordinator : IColdRestartCoordinator
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
        try
        {
            using Process proc = Process.Start(new ProcessStartInfo
            {
                FileName = "bash",
                ArgumentList = { "-lc", "sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null" },
                UseShellExecute = false,
                CreateNoWindow = true
            })!;
            await proc.WaitForExitAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Linux cold restart cache drop failed.");
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
                _logger.LogInformation("RAMMap.exe not found, skipping Windows standby list purge.");
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
            _logger.LogWarning(ex, "Windows cold restart purge failed.");
        }
    }
}
