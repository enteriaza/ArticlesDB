using System.Globalization;
using HisCTL.Contracts;
using HistoryDB.Defrag;
using HistoryDB.Repair;

namespace HisCTL.Application;

internal sealed class HisCtlOptionsParser
{
    public HisCtlParseResult Parse(string verbToken, string[] args)
    {
        try
        {
            HisCtlVerb verb = verbToken.ToLowerInvariant() switch
            {
                "scan" => HisCtlVerb.Scan,
                "repair" => HisCtlVerb.Repair,
                "defrag" => HisCtlVerb.Defrag,
                "expire" => HisCtlVerb.Expire,
                "help" or "--help" or "-h" or "?" => HisCtlVerb.None,
                _ => throw new ArgumentException($"Unknown verb '{verbToken}'."),
            };

            if (verb == HisCtlVerb.None)
            {
                return new HisCtlParseResult(true, null, new HisCtlOptions { Verb = HisCtlVerb.None, ShowHelp = true });
            }

            string historyDirectory = "./hisctl-data";
            bool sharedScan = false;
            int shardCount = 128;
            ulong slotsPerShard = 1UL << 22;
            ulong maxLoadFactorPercent = 75;
            int queueCapacityPerShard = 1_000_000;
            int maxRetainedGenerations = 8;
            RepairPolicy repairPolicy = RepairPolicy.FailFast;
            double minExpiredFraction = 0.05;
            int maxParallelGenerations = 1;
            bool forceRolloverActive = true;
            bool pruneExpiredSet = true;
            string expireMode = "time";
            string? olderThan = null;
            string expireByField = "obtained";
            int recycleLimit = 0;
            bool applyExpire = false;
            int expireParallel = 64;

            int i = 0;
            if (verb == HisCtlVerb.Expire)
            {
                if (i >= args.Length)
                {
                    throw new ArgumentException("expire requires a sub-mode: time | recycle");
                }

                expireMode = args[i++].ToLowerInvariant();
                if (expireMode is not ("time" or "recycle"))
                {
                    throw new ArgumentException("expire sub-mode must be 'time' or 'recycle'.");
                }
            }

            for (; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "--history-dir":
                        historyDirectory = Next(args, ref i, "--history-dir");
                        break;
                    case "--shared":
                        sharedScan = true;
                        break;
                    case "--shards":
                        shardCount = ParsePositiveInt(args, ref i, "--shards");
                        break;
                    case "--slots-per-shard":
                        slotsPerShard = (ulong)ParsePositiveLong(args, ref i, "--slots-per-shard");
                        break;
                    case "--load-factor":
                        maxLoadFactorPercent = (ulong)ParsePositiveLong(args, ref i, "--load-factor");
                        break;
                    case "--queue-capacity":
                        queueCapacityPerShard = ParsePositiveInt(args, ref i, "--queue-capacity");
                        break;
                    case "--retained-generations":
                        maxRetainedGenerations = ParsePositiveInt(args, ref i, "--retained-generations");
                        break;
                    case "--policy":
                    {
                        string p = Next(args, ref i, "--policy").ToLowerInvariant();
                        repairPolicy = p switch
                        {
                            "failfast" => RepairPolicy.FailFast,
                            "quarantine" => RepairPolicy.QuarantineAndRecreate,
                            _ => throw new ArgumentException("--policy must be failfast or quarantine."),
                        };
                        break;
                    }

                    case "--min-expired-fraction":
                        minExpiredFraction = ParseDoubleRange(args, ref i, "--min-expired-fraction", 0.0, 1.0);
                        break;
                    case "--max-parallel-generations":
                        maxParallelGenerations = ParsePositiveInt(args, ref i, "--max-parallel-generations");
                        break;
                    case "--force-rollover-active":
                        forceRolloverActive = true;
                        break;
                    case "--no-force-rollover-active":
                        forceRolloverActive = false;
                        break;
                    case "--no-prune-expired-set":
                        pruneExpiredSet = false;
                        break;
                    case "--older-than":
                        olderThan = Next(args, ref i, "--older-than");
                        break;
                    case "--by":
                    {
                        string b = Next(args, ref i, "--by").ToLowerInvariant();
                        expireByField = b switch
                        {
                            "obtained" or "accessed" => b,
                            _ => throw new ArgumentException("--by must be obtained or accessed."),
                        };
                        break;
                    }

                    case "--limit":
                        recycleLimit = ParsePositiveInt(args, ref i, "--limit");
                        break;
                    case "--apply":
                        applyExpire = true;
                        break;
                    case "--expire-parallel":
                        expireParallel = ParsePositiveInt(args, ref i, "--expire-parallel");
                        break;
                    case "--help":
                        return new HisCtlParseResult(
                            true,
                            null,
                            new HisCtlOptions { Verb = verb, ShowHelp = true, HistoryDirectory = historyDirectory });
                    default:
                        throw new ArgumentException($"Unknown option: {args[i]}");
                }
            }

            HistoryDefragOptions defrag = new()
            {
                MinExpiredFractionToCompact = minExpiredFraction,
                MaxParallelGenerations = maxParallelGenerations,
                ForceRolloverActiveGeneration = forceRolloverActive,
                PruneExpiredSetAfterCompaction = pruneExpiredSet,
            };

            HisCtlOptions options = new()
            {
                Verb = verb,
                HistoryDirectory = historyDirectory,
                SharedScan = sharedScan,
                ShardCount = shardCount,
                SlotsPerShard = slotsPerShard,
                MaxLoadFactorPercent = maxLoadFactorPercent,
                QueueCapacityPerShard = queueCapacityPerShard,
                MaxRetainedGenerations = maxRetainedGenerations,
                RepairPolicy = repairPolicy,
                DefragOptions = defrag,
                ExpireMode = expireMode,
                OlderThanInterval = olderThan,
                ExpireByField = expireByField,
                RecycleLimit = recycleLimit,
                ApplyExpire = applyExpire,
                ExpireParallelTombstones = expireParallel,
            };

            if (verb == HisCtlVerb.Expire)
            {
                if (expireMode == "time" && string.IsNullOrWhiteSpace(olderThan))
                {
                    throw new ArgumentException("expire time requires --older-than \"INTERVAL ...\".");
                }

                if (expireMode == "recycle" && recycleLimit <= 0)
                {
                    throw new ArgumentException("expire recycle requires --limit <positive int>.");
                }
            }

            return new HisCtlParseResult(true, null, options);
        }
        catch (Exception ex)
        {
            return new HisCtlParseResult(false, ex.Message, null);
        }
    }

    private static string Next(string[] args, ref int i, string flag)
    {
        if (i + 1 >= args.Length)
        {
            throw new ArgumentException($"Missing value for {flag}.");
        }

        return args[++i];
    }

    private static int ParsePositiveInt(string[] args, ref int i, string flag)
    {
        string s = Next(args, ref i, flag);
        if (!int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out int v) || v <= 0)
        {
            throw new ArgumentException($"{flag} requires a positive integer.");
        }

        return v;
    }

    private static long ParsePositiveLong(string[] args, ref int i, string flag)
    {
        string s = Next(args, ref i, flag);
        if (!long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out long v) || v <= 0)
        {
            throw new ArgumentException($"{flag} requires a positive integer.");
        }

        return v;
    }

    private static double ParseDoubleRange(string[] args, ref int i, string flag, double min, double max)
    {
        string s = Next(args, ref i, flag);
        if (!double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out double v) || v < min || v > max)
        {
            throw new ArgumentException($"{flag} must be a number in [{min}, {max}].");
        }

        return v;
    }
}
