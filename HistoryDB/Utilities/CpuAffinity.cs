// CpuAffinity.cs -- best-effort logical CPU pinning for background threads (shard writers, Bloom I/O) to improve cache and first-touch NUMA locality with mmap-backed shards.
// Windows uses SetThreadAffinityMask when logical CPU count is 64 or fewer (single-group assumption). Linux uses pthread_setaffinity_np with a 1024-bit cpu_set_t,
// resolved against libpthread.so.0 first and libc.so.6 as a fallback (glibc 2.34+ folded pthread into libc; libpthread.so.0 is a stub on minimal containers).

using System.Runtime.InteropServices;

namespace HistoryDB.Utilities;

/// <summary>
/// Applies best-effort processor affinity to the <b>current</b> OS thread (not the whole process).
/// </summary>
/// <remarks>
/// <para>
/// <b>Windows:</b> Uses <c>SetThreadAffinityMask</c> when <see cref="Environment.ProcessorCount"/> is at most 64 (single processor-group mask). Larger machines skip pinning and return <see langword="false"/>.
/// </para>
/// <para>
/// <b>Linux:</b> Uses <c>pthread_setaffinity_np</c> from <c>libpthread.so.0</c> with a 128-byte <c>cpu_set_t</c> (glibc layout) for logical CPUs 0..1023. Falls back to
/// <c>libc.so.6</c> on glibc 2.34+ distributions where pthread symbols are merged into libc and <c>libpthread.so.0</c> may be missing on minimal containers.
/// </para>
/// <para>
/// <b>Thread safety:</b> Must be called from the thread to pin; safe to call concurrently on different threads.
/// </para>
/// </remarks>
internal static unsafe class CpuAffinity
{
    private const int LinuxCpuSetBytes = 128;

    /// <summary>
    /// Attempts to pin the current thread to a single logical processor index in <c>[0, ProcessorCount)</c>.
    /// </summary>
    /// <param name="logicalProcessorIndex">Zero-based logical index (same numbering as <see cref="Environment.ProcessorCount"/> ordering).</param>
    /// <returns><see langword="true"/> when the host reports success.</returns>
    internal static bool TryPinCurrentThreadToLogicalProcessor(int logicalProcessorIndex)
    {
        if (logicalProcessorIndex < 0 || logicalProcessorIndex >= Environment.ProcessorCount)
        {
            return false;
        }

        if (OperatingSystem.IsWindows())
        {
            return TryPinWindows(logicalProcessorIndex);
        }

        if (OperatingSystem.IsLinux())
        {
            return TryPinLinux(logicalProcessorIndex);
        }

        return false;
    }

    private static bool TryPinWindows(int logicalProcessorIndex)
    {
        int count = Environment.ProcessorCount;
        if (count > 64)
        {
            return false;
        }

        UIntPtr mask = (UIntPtr)(1UL << logicalProcessorIndex);
        UIntPtr previous = SetThreadAffinityMask(GetCurrentThread(), mask);
        return previous != UIntPtr.Zero;
    }

    private static bool TryPinLinux(int logicalProcessorIndex)
    {
        if (logicalProcessorIndex >= 1024)
        {
            return false;
        }

        Span<byte> set = stackalloc byte[LinuxCpuSetBytes];
        set.Clear();
        int word = logicalProcessorIndex / 64;
        int bit = logicalProcessorIndex % 64;
        fixed (byte* p = set)
        {
            ulong* words = (ulong*)p;
            words[word] = 1UL << bit;

            // Try libpthread.so.0 first; if it cannot be loaded (glibc 2.34+ minimal containers) or returns ENOENT,
            // fall back to libc.so.6 where the symbols actually live on modern glibc.
            if (TryLinuxPThread(p, out bool succeededPThread))
            {
                return succeededPThread;
            }

            if (TryLinuxLibc(p, out bool succeededLibc))
            {
                return succeededLibc;
            }

            return false;
        }
    }

    private static bool TryLinuxPThread(byte* cpuSet, out bool succeeded)
    {
        succeeded = false;
        try
        {
            nint self = pthread_self();
            int rc = pthread_setaffinity_np(self, (nuint)LinuxCpuSetBytes, cpuSet);
            succeeded = rc == 0;
            return true;
        }
        catch (DllNotFoundException)
        {
            return false;
        }
        catch (EntryPointNotFoundException)
        {
            return false;
        }
    }

    private static bool TryLinuxLibc(byte* cpuSet, out bool succeeded)
    {
        succeeded = false;
        try
        {
            nint self = pthread_self_libc();
            int rc = pthread_setaffinity_np_libc(self, (nuint)LinuxCpuSetBytes, cpuSet);
            succeeded = rc == 0;
            return true;
        }
        catch (DllNotFoundException)
        {
            return false;
        }
        catch (EntryPointNotFoundException)
        {
            return false;
        }
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern nint GetCurrentThread();

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern UIntPtr SetThreadAffinityMask(nint hThread, UIntPtr dwThreadAffinityMask);

    [DllImport("libpthread.so.0", EntryPoint = "pthread_self")]
    private static extern nint pthread_self();

    [DllImport("libpthread.so.0", EntryPoint = "pthread_setaffinity_np")]
    private static extern int pthread_setaffinity_np(nint thread, nuint cpusetsize, byte* cpuset);

    [DllImport("libc.so.6", EntryPoint = "pthread_self")]
    private static extern nint pthread_self_libc();

    [DllImport("libc.so.6", EntryPoint = "pthread_setaffinity_np")]
    private static extern int pthread_setaffinity_np_libc(nint thread, nuint cpusetsize, byte* cpuset);
}
