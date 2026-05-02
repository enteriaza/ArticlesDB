// MemoryMappedViewFlush.cs -- best-effort mapped view sync for crash-consistency hints on Sync/Dispose.

using System.Runtime.InteropServices;

namespace HistoryDB.Utilities;

internal static class MemoryMappedViewFlush
{
    internal static void TryFlush(IntPtr address, long byteLength)
    {
        if (byteLength <= 0)
        {
            return;
        }

        if (OperatingSystem.IsWindows())
        {
            _ = NativeWindows.FlushViewOfFile(address, (nuint)byteLength);
            return;
        }

        if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            _ = NativePosix.msync(address, (nuint)byteLength, NativePosix.MS_SYNC);
        }
    }

    private static class NativeWindows
    {
        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        internal static extern bool FlushViewOfFile(IntPtr lpBaseAddress, nuint dwNumberOfBytesToFlush);
    }

    private static class NativePosix
    {
        internal const int MS_SYNC = 4;

        [DllImport("libc", EntryPoint = "msync")]
        internal static extern int msync(IntPtr addr, nuint len, int flags);
    }
}
