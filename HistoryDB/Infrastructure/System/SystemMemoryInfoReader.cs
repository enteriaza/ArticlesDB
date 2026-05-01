// SystemMemoryInfoReader.cs -- infrastructure adapter exposing installed memory metrics.

using HistoryDB.Contracts;
using HistoryDB.Utilities;

namespace HistoryDB.Infrastructure.System;

internal sealed class SystemMemoryInfoReader : ISystemMemoryInfo
{
    public ulong GetInstalledMemoryBytes() => SystemMemoryReader.GetInstalledMemoryBytesForBloomBudget();
}
