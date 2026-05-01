// SystemMemoryReader.cs -- best-effort installed physical RAM for Bloom sizing on Windows (GlobalMemoryStatusEx), Linux (/proc/meminfo MemTotal), and other platforms (GC / conservative default).
// P/Invoke is Windows-only; Linux uses procfs text parsing. No ARM-specific intrinsics.

using System.Globalization;
using System.Runtime.InteropServices;

namespace HistoryDB.Utilities;

/// <summary>
/// Reads total physical memory for optional Bloom filter memory budgeting when no explicit byte budget is configured.
/// </summary>
/// <remarks>
/// <para>
/// <b>Cross-platform:</b> Windows uses <c>GlobalMemoryStatusEx</c>. Linux reads <c>/proc/meminfo</c> <c>MemTotal</c> (kB).
/// Other hosts use <see cref="GC.GetGCMemoryInfo()"/> total available memory, then a conservative default if unknown.
/// </para>
/// <para>
/// <b>Thread safety:</b> Static methods are safe for concurrent calls; OS APIs are process-wide snapshots.
/// </para>
/// </remarks>
internal static class SystemMemoryReader
{
    private const ulong DefaultFallbackBytes = 16UL * 1024 * 1024 * 1024;

    /// <summary>
    /// Returns an estimate of installed physical memory in bytes for Bloom budget derivation.
    /// </summary>
    /// <returns>Positive byte count; never zero.</returns>
    /// <remarks>
    /// <para>
    /// Callers multiply by a fractional budget (for example 0.8) to cap Bloom bit arrays without requiring manual tuning on each machine.
    /// </para>
    /// </remarks>
    internal static ulong GetInstalledMemoryBytesForBloomBudget()
    {
        if (OperatingSystem.IsWindows() && TryGetWindowsPhysicalMemory(out ulong windowsBytes) && windowsBytes > 0)
        {
            return windowsBytes;
        }

        if (OperatingSystem.IsLinux() && TryGetLinuxMemTotalKilobytes(out ulong memTotalKb))
        {
            try
            {
                ulong bytes = checked(memTotalKb * 1024UL);
                if (bytes > 0)
                {
                    return bytes;
                }
            }
            catch (OverflowException)
            {
                // Fall through to GC / default.
            }
        }

        long available = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
        if (available > 0)
        {
            return (ulong)available;
        }

        return DefaultFallbackBytes;
    }

    private static bool TryGetLinuxMemTotalKilobytes(out ulong memTotalKb)
    {
        memTotalKb = 0;
        try
        {
            const string path = "/proc/meminfo";
            if (!File.Exists(path))
            {
                return false;
            }

            using FileStream stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using StreamReader reader = new(stream, leaveOpen: false);
            for (int lineIndex = 0; lineIndex < 48 && !reader.EndOfStream; lineIndex++)
            {
                string? line = reader.ReadLine();
                if (line is null)
                {
                    break;
                }

                ReadOnlySpan<char> span = line.AsSpan().TrimStart();
                if (!span.StartsWith("MemTotal:", StringComparison.Ordinal))
                {
                    continue;
                }

                span = span["MemTotal:".Length..].TrimStart();
                int space = span.IndexOf(' ');
                ReadOnlySpan<char> numberSpan = space >= 0 ? span[..space] : span;
                if (ulong.TryParse(numberSpan, NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong kb))
                {
                    memTotalKb = kb;
                    return true;
                }

                return false;
            }
        }
        catch (IOException)
        {
            return false;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }

        return false;
    }

    private static bool TryGetWindowsPhysicalMemory(out ulong totalPhysicalMemory)
    {
        MemoryStatusEx status = new()
        {
            dwLength = (uint)Marshal.SizeOf<MemoryStatusEx>()
        };

        if (GlobalMemoryStatusEx(ref status))
        {
            totalPhysicalMemory = status.ullTotalPhys;
            return true;
        }

        totalPhysicalMemory = 0;
        return false;
    }

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Auto)]
    private struct MemoryStatusEx
    {
        public uint dwLength;
        public uint dwMemoryLoad;
        public ulong ullTotalPhys;
        public ulong ullAvailPhys;
        public ulong ullTotalPageFile;
        public ulong ullAvailPageFile;
        public ulong ullTotalVirtual;
        public ulong ullAvailVirtual;
        public ulong ullAvailExtendedVirtual;
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GlobalMemoryStatusEx(ref MemoryStatusEx lpBuffer);
}
