// CpuTopologyServices.cs -- infrastructure adapters for CPU affinity and NUMA topology enumeration.

using HistoryDB.Contracts;
using HistoryDB.Utilities;

namespace HistoryDB.Infrastructure.System;

internal sealed class ThreadAffinityService : IThreadAffinityService
{
    public bool TryPinCurrentThreadToLogicalProcessor(int logicalProcessorIndex) =>
        CpuAffinity.TryPinCurrentThreadToLogicalProcessor(logicalProcessorIndex);
}

internal sealed class NumaTopologyService : INumaTopologyService
{
    public bool TryGetProcessorsNumaNodeOrderGroup0(out int[]? processors) =>
        WindowsNumaProcessorOrder.TryGetProcessorsNumaNodeOrderGroup0(out processors);
}
