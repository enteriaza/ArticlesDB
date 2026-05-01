using System.Text;

namespace HisBench.Application;

internal static class BenchmarkUsageFormatter
{
    internal static string Format()
    {
        StringBuilder sb = new();
        sb.AppendLine("A simple history performance tester");
        sb.AppendLine();
        sb.AppendLine("Usage: hisbench [--add-count n] [--add-forks n] [--history-dir dir] [--lookup-count n] [--lookup-forks n]");
        sb.AppendLine("                [--random-msgids] [--duration sec] [--target-generations n] [--fill-factor f]");
        sb.AppendLine("                [--dataset-size n] [--zipfian] [--mix i:l:e] [--restart-test] [--cold-restart]");
        sb.AppendLine("                [--burst-mode] [--burst-profile spike|ramp|wave] [--burst-duty-pct n] [--burst-sleep-ms n]");
        sb.AppendLine("                [--correlation-ms n] [--saturation-queue-depth n] [--degraded-p99-us n] [--mapping-file file]");
        sb.AppendLine("                [--shards n] [--slots-per-shard n] [--load-factor n] [--queue-capacity n] [--retained-generations n]");
        sb.AppendLine("                [--bloom-checkpoint-interval n] [--scrub-samples n] [--scrub-interval-ms n]");
        sb.AppendLine("                [--queue-threshold n] [--probe-threshold n] [--auto] [--help]");
        sb.AppendLine();
        sb.AppendLine("  --add-count N                 Number of history additions (default: 2000000)");
        sb.AppendLine("  --add-forks N                 Number of addition workers (default: 8)");
        sb.AppendLine("  --history-dir DIR             Root history directory (default: ./hisbench-data)");
        sb.AppendLine("  --lookup-count N              Number of lookup operations (default: 2000000)");
        sb.AppendLine("  --lookup-forks N              Number of lookup workers (default: 8)");
        sb.AppendLine("  --mapping-file FILE           Compatibility option (ignored)");
        sb.AppendLine("  --random-msgids               Generate random message IDs");
        sb.AppendLine("  --duration SEC                Sustained mode duration in seconds (default: 0; when 0, phase lengths follow --add-count / --lookup-count)");
        sb.AppendLine("  --target-generations N        Insert enough data for N generations (default: 0)");
        sb.AppendLine("  --fill-factor F               Fill factor for generation forcing (0.01-0.99, default: 0.80)");
        sb.AppendLine("  --dataset-size N              Logical dataset cardinality cap (default: 0 = unlimited)");
        sb.AppendLine("  --zipfian                     Use 80/20 hot-cold lookup skew");
        sb.AppendLine("  --mix I:L:E                   Mixed workload ratio (default when --mix omitted: 70:20:10)");
        sb.AppendLine("  --restart-test                Reopen DB and measure restart lookups");
        sb.AppendLine("  --cold-restart                Best effort cache drop before reopen");
        sb.AppendLine("  --burst-mode                  Enable burst traffic shaping");
        sb.AppendLine("  --burst-profile P             spike | ramp | wave (default: spike)");
        sb.AppendLine("  --burst-duty-pct N            Active duty percent in burst mode (default: 10)");
        sb.AppendLine("  --burst-sleep-ms N            Sleep delay during inactive period (default: 50)");
        sb.AppendLine("  --correlation-ms N            Queue/failure correlation sample interval (default: 250)");
        sb.AppendLine("  --saturation-queue-depth N    Queue depth threshold for saturation status (default: 0 = disabled)");
        sb.AppendLine("  --degraded-p99-us N           Insert p99 threshold for degraded status (default: 0 = disabled)");
        sb.AppendLine("  --shards N                    Number of shard files per generation (default: 128)");
        sb.AppendLine("  --slots-per-shard N           Open-addressing slots per shard; must be a power of two (default: 4194304, ~128 MiB mmap per shard)");
        sb.AppendLine("  --load-factor N               Max load factor percent for inserts (default: 75)");
        sb.AppendLine("  --queue-capacity N            Bounded channel capacity per shard writer (default: 1000000)");
        sb.AppendLine("  --retained-generations N      Generations retained before retiring oldest (default: 8)");
        sb.AppendLine("  --bloom-checkpoint-interval N Inserts between Bloom checkpoint attempts (default: 1000000)");
        sb.AppendLine("  --scrub-samples N             Slot scrubber samples per tick (default: 256)");
        sb.AppendLine("  --scrub-interval-ms N         Slot scrubber interval in ms (default: 30000)");
        sb.AppendLine("  --queue-threshold N           Proactive rollover queue depth threshold (0 disables; default: 0). Applied when --target-generations > 0");
        sb.AppendLine("  --probe-threshold N           Proactive rollover aggregate probe-failure threshold across shards (<=0 disables; default: 0). Applied when --target-generations > 0");
        sb.AppendLine("  --auto                        Run automatic multi-trial tuning (forces add/lookup counts to 4000000)");
        sb.AppendLine("  --help                        Show help");
        return sb.ToString();
    }
}
