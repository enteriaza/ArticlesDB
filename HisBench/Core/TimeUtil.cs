using HdrHistogram;
using System.Diagnostics;

namespace HisBench.Core;

internal static class TimeUtil
{
    internal static long ToNanoseconds(long stopwatchTicks) => (long)(stopwatchTicks * (1_000_000_000.0 / Stopwatch.Frequency));

    internal static long ElapsedMilliseconds(long startTicks) =>
        (long)((Stopwatch.GetTimestamp() - startTicks) * 1000.0 / Stopwatch.Frequency);

    internal static double P99Microseconds(LongHistogram histogram) =>
        histogram.TotalCount == 0 ? 0 : histogram.GetValueAtPercentile(99) / 1000.0;

    internal static double P50Microseconds(LongHistogram histogram) =>
        histogram.TotalCount == 0 ? 0 : histogram.GetValueAtPercentile(50) / 1000.0;
}
