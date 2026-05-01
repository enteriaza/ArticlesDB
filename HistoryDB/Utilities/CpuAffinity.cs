// CpuAffinity.cs -- best-effort logical CPU pinning for background threads (shard writers, Bloom I/O) to improve cache and first-touch NUMA locality with mmap-backed shards.
// Windows uses SetThreadAffinityMask when logical CPU count is 64 or fewer (single-group assumption). Linux uses pthread_setaffinity_np with a 1024-bit cpu_set_t.

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
/// <b>Linux:</b> Uses <c>pthread_setaffinity_np</c> from <c>libpthread.so.0</c> with a 128-byte <c>cpu_set_t</c> (glibc layout) for logical CPUs 0..1023.
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

        nint self;
        try
        {
            self = pthread_self();
        }
        catch (DllNotFoundException)
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
            int rc = pthread_setaffinity_np(self, (nuint)LinuxCpuSetBytes, p);
            return rc == 0;
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
}
