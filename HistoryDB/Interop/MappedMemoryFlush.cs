using System.Runtime.InteropServices;

namespace HistoryDB.Interop;

/// <summary>
/// Best-effort flush of mmap-backed ranges to durable media (storage-engine sync path).
/// </summary>
internal static unsafe class MappedMemoryFlush
{
    private const int MS_SYNC = 4;

    public static bool TryFlush(void* basePointer, nuint byteLength)
    {
        if (byteLength == 0)
        {
            return true;
        }

        if (OperatingSystem.IsWindows())
        {
            return FlushViewOfFile(basePointer, byteLength);
        }

        if (OperatingSystem.IsLinux())
        {
            return msync_linux(basePointer, byteLength, MS_SYNC) == 0;
        }

        if (OperatingSystem.IsMacOS())
        {
            return msync_macos(basePointer, byteLength, MS_SYNC) == 0;
        }

        if (OperatingSystem.IsFreeBSD())
        {
            return msync_freebsd(basePointer, byteLength, MS_SYNC) == 0;
        }

        return false;
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool FlushViewOfFile(void* lpBaseAddress, nuint dwNumberOfBytesToFlush);

    [DllImport("libc.so.6", EntryPoint = "msync")]
    private static extern int msync_linux(void* addr, nuint len, int flags);

    [DllImport("libSystem.dylib", EntryPoint = "msync")]
    private static extern int msync_macos(void* addr, nuint len, int flags);

    [DllImport("libc", EntryPoint = "msync")]
    private static extern int msync_freebsd(void* addr, nuint len, int flags);
}
