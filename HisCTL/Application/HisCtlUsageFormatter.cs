namespace HisCTL.Application;

internal static class HisCtlUsageFormatter
{
    public static string Format() =>
        """
        HisCTL -- HistoryDB operator utility (scan, repair, defrag, bulk expire).

        Usage:
          hisctl <verb> [options]

        Verbs:
          scan    Read-only consistency scan (exclusive by default; use --shared for live server).
          repair  Apply repair actions (exclusive lock; stop the server first).
          defrag  Online compaction via HistoryDatabase (stop other writers recommended).
          expire  Select articles by age or recycle ranking (dry-run unless --apply).

        Common options:
          --history-dir <path>     Root directory (default: ./hisctl-data)
          --shards <n>             Expected shard count for scan/repair assertions (default: 128)
          --slots-per-shard <n>    Slots per shard (default: 4194304)
          --load-factor <n>        Max load factor percent (default: 75)
          --queue-capacity <n>     Per-shard writer queue capacity (default: 1000000)
          --retained-generations <n> (default: 8)
          --help                   Show this help

        scan:
          --shared                 Use shared read opens (may run while HistoryDatabase holds the tree).

        repair:
          --policy failfast|quarantine   (default: failfast)

        defrag:
          --min-expired-fraction <0..1>   (default: 0.05)
          --max-parallel-generations <n> (default: from HistoryDefragOptions)
          --force-rollover-active        Force rollover before compaction
          --no-prune-expired-set         Set PruneExpiredSetAfterCompaction=false

        expire (first argument after verb is sub-mode: time | recycle):
          hisctl expire time --older-than "INTERVAL 1 DAY" [--by obtained|accessed] [--apply]
          hisctl expire recycle --limit <n> [--apply]

          --older-than "INTERVAL n UNIT"   MySQL-style; UNIT: MICROSECOND SECOND MINUTE HOUR DAY WEEK MONTH YEAR
          --by obtained|accessed           Which slot tick field to compare for time mode (default: obtained)
          --limit <n>                      Recycle mode: expire this many lowest-priority slots (staleness then hit count)
          --apply                          Perform expire + tombstone + persist (default is dry-run)
          --expire-parallel <n>            Tombstone fan-out parallelism (default: 64)

        Exit codes: 0 success, 1 usage/parse error, 2 operational failure (e.g. unhealthy scan).
        """;
}
