// WindowsNumaProcessorOrder.cs -- builds a logical processor index list ordered by ascending NUMA node (group 0 only) for shard-writer pinning heuristics on Windows.
// Falls back silently when APIs are unavailable or layout cannot be determined; callers use round-robin across Environment.ProcessorCount.

using System.Runtime.InteropServices;

namespace HistoryDB.Utilities;

/// <summary>
/// Windows-only helper that enumerates logical processors in NUMA node order (node 0, then node 1, ...) using group-0 affinity masks.
/// </summary>
/// <remarks>
/// <para>
/// <b>Limitations:</b> Only processors reported in <see cref="GROUP_AFFINITY"/> for each NUMA node on <b>group 0</b> are included. Multi-group topologies may omit processors outside group 0.
/// </para>
/// </remarks>
internal static class WindowsNumaProcessorOrder
{
    [StructLayout(LayoutKind.Sequential)]
    private struct GroupAffinity
    {
        public UIntPtr Mask;
        public ushort Group;
        public ushort Reserved0;
        public ushort Reserved1;
        public ushort Reserved2;
    }

    /// <summary>
    /// Tries to build a list of logical processor indices (bit positions in group 0) sorted by NUMA node id ascending.
    /// </summary>
    /// <param name="processors">When successful, a non-empty array suitable for round-robin shard assignment.</param>
    /// <returns><see langword="true"/> when at least one processor was enumerated.</returns>
    internal static bool TryGetProcessorsNumaNodeOrderGroup0(out int[]? processors)
    {
        processors = null;
        if (!OperatingSystem.IsWindows())
        {
            return false;
        }

        if (!GetNumaHighestNodeNumber(out byte highestNode))
        {
            return false;
        }

        List<int> list = [];
        int maxNode = highestNode;
        for (int node = 0; node <= maxNode; node++)
        {
            GroupAffinity ga = default;
            if (!GetNumaNodeProcessorMaskEx((ushort)node, ref ga))
            {
                continue;
            }

            if (ga.Group != 0)
            {
                continue;
            }

            ulong mask = ga.Mask.ToUInt64();
            for (int bit = 0; bit < 64; bit++)
            {
                if ((mask & (1UL << bit)) != 0)
                {
                    list.Add(bit);
                }
            }
        }

        if (list.Count == 0)
        {
            return false;
        }

        processors = list.ToArray();
        return true;
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool GetNumaHighestNodeNumber(out byte HighestNodeNumber);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool GetNumaNodeProcessorMaskEx(ushort NodeNumber, ref GroupAffinity ProcessorMask);
}
