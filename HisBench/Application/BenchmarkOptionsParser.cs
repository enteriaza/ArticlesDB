using System.Globalization;
using HisBench.Contracts;
using HistoryDB;

namespace HisBench.Application;

internal sealed class BenchmarkOptionsParser : IBenchmarkOptionsParser
{
    private const long DefaultAddCount = 2_000_000;
    private const int DefaultAddForks = 8;
    private const long DefaultLookupCount = 2_000_000;
    private const int DefaultLookupForks = 8;
    private const string DefaultHistoryDir = "./hisbench-data";

    public ParseResult Parse(string[] args)
    {
        try
        {
            long addCount = DefaultAddCount;
            int addForks = DefaultAddForks;
            string historyDirectory = DefaultHistoryDir;
            long lookupCount = DefaultLookupCount;
            int lookupForks = DefaultLookupForks;
            bool randomMessageIds = false;
            bool showHelp = false;
            int durationSeconds = 0;
            int targetGenerations = 0;
            double fillFactor = 0.80;
            long datasetSize = 0;
            bool zipfianSkew = false;
            bool mixedMode = false;
            int mixInsert = 70;
            int mixLookup = 20;
            int mixExpire = 10;
            bool restartTest = false;
            bool coldRestart = false;
            bool burstMode = false;
            BurstProfile burstProfile = BurstProfile.Spike;
            int burstDutyPercent = 10;
            int burstSleepMs = 50;
            int correlationSampleMs = 250;
            long saturationQueueDepthThreshold = 0;
            double degradedP99Microseconds = 0;
            int shardCount = 128;
            ulong slotsPerShard = 1UL << 22;
            ulong maxLoadFactorPercent = 75;
            int queueCapacityPerShard = 1_000_000;
            int maxRetainedGenerations = 8;
            ulong bloomCheckpointInsertInterval = 1_000_000;
            int scrubberSamplesPerTick = 256;
            int scrubberIntervalMs = 30_000;
            int queueDepthThreshold = 0;
            long probeFailureThreshold = 0;
            bool autoTune = false;
            BloomCheckpointPersistMode bloomPersistMode = BloomCheckpointPersistMode.Throughput;
            HistoryCrossGenerationDuplicateCheck enqueueDupCheck = HistoryCrossGenerationDuplicateCheck.Full;
            int insertBatchSize = 0;

            for (int i = 0; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "--add-count": addCount = ParsePositiveLong(args, ref i, "--add-count"); break;
                    case "--add-forks": addForks = ParsePositiveInt(args, ref i, "--add-forks"); break;
                    case "--history-dir": historyDirectory = Next(args, ref i, "--history-dir"); break;
                    case "--lookup-count": lookupCount = ParsePositiveLong(args, ref i, "--lookup-count"); break;
                    case "--lookup-forks": lookupForks = ParsePositiveInt(args, ref i, "--lookup-forks"); break;
                    case "--mapping-file": _ = Next(args, ref i, "--mapping-file"); break;
                    case "--random-msgids": randomMessageIds = true; break;
                    case "--duration": durationSeconds = ParsePositiveInt(args, ref i, "--duration"); break;
                    case "--target-generations": targetGenerations = ParsePositiveInt(args, ref i, "--target-generations"); break;
                    case "--fill-factor": fillFactor = ParseDoubleRange(args, ref i, "--fill-factor", 0.01, 0.99); break;
                    case "--dataset-size": datasetSize = ParsePositiveLong(args, ref i, "--dataset-size"); break;
                    case "--zipfian": zipfianSkew = true; break;
                    case "--mix":
                        (mixInsert, mixLookup, mixExpire) = ParseMix(Next(args, ref i, "--mix"));
                        mixedMode = true;
                        break;
                    case "--restart-test": restartTest = true; break;
                    case "--cold-restart": coldRestart = true; break;
                    case "--burst-mode": burstMode = true; break;
                    case "--burst-profile": burstProfile = ParseBurstProfile(Next(args, ref i, "--burst-profile")); break;
                    case "--burst-duty-pct": burstDutyPercent = ParseIntRange(args, ref i, "--burst-duty-pct", 1, 99); break;
                    case "--burst-sleep-ms": burstSleepMs = ParsePositiveInt(args, ref i, "--burst-sleep-ms"); break;
                    case "--correlation-ms": correlationSampleMs = ParseNonNegativeInt(args, ref i, "--correlation-ms"); break;
                    case "--saturation-queue-depth": saturationQueueDepthThreshold = ParseNonNegativeLong(args, ref i, "--saturation-queue-depth"); break;
                    case "--degraded-p99-us": degradedP99Microseconds = ParseNonNegativeDouble(args, ref i, "--degraded-p99-us"); break;
                    case "--shards": shardCount = ParsePositiveInt(args, ref i, "--shards"); break;
                    case "--slots-per-shard": slotsPerShard = (ulong)ParsePositiveLong(args, ref i, "--slots-per-shard"); break;
                    case "--load-factor": maxLoadFactorPercent = (ulong)ParsePositiveLong(args, ref i, "--load-factor"); break;
                    case "--queue-capacity": queueCapacityPerShard = ParsePositiveInt(args, ref i, "--queue-capacity"); break;
                    case "--retained-generations": maxRetainedGenerations = ParsePositiveInt(args, ref i, "--retained-generations"); break;
                    case "--bloom-checkpoint-interval":
                        bloomCheckpointInsertInterval = (ulong)ParseNonNegativeLong(args, ref i, "--bloom-checkpoint-interval");
                        break;
                    case "--scrub-samples": scrubberSamplesPerTick = ParsePositiveInt(args, ref i, "--scrub-samples"); break;
                    case "--scrub-interval-ms": scrubberIntervalMs = ParsePositiveInt(args, ref i, "--scrub-interval-ms"); break;
                    case "--queue-threshold": queueDepthThreshold = ParsePositiveInt(args, ref i, "--queue-threshold"); break;
                    case "--probe-threshold": probeFailureThreshold = ParsePositiveLong(args, ref i, "--probe-threshold"); break;
                    case "--bloom-persist":
                        bloomPersistMode = ParseBloomPersistMode(Next(args, ref i, "--bloom-persist"));
                        break;
                    case "--enqueue-dup-check":
                        enqueueDupCheck = ParseEnqueueDupCheck(Next(args, ref i, "--enqueue-dup-check"));
                        break;
                    case "--insert-batch":
                        insertBatchSize = 128;
                        if (i + 1 < args.Length)
                        {
                            string next = args[i + 1];
                            if (!next.StartsWith("-", StringComparison.Ordinal) &&
                                long.TryParse(next, NumberStyles.Integer, CultureInfo.InvariantCulture, out long v))
                            {
                                i++;
                                if (v == 0)
                                {
                                    insertBatchSize = 0;
                                }
                                else if (v is >= 1 and <= HistoryDatabase.MaxHistoryAddBatch)
                                {
                                    insertBatchSize = (int)v;
                                }
                                else
                                {
                                    throw new ArgumentException(
                                        $"Invalid value for --insert-batch; expected 0..{HistoryDatabase.MaxHistoryAddBatch} (0 = writer coalesce 1, omit value for default 128).");
                                }
                            }
                        }

                        break;
                    case "--auto": autoTune = true; break;
                    case "--help":
                        showHelp = true;
                        break;
                    default:
                        throw new ArgumentException($"Unknown option: {args[i]}");
                }
            }

            return ParseResult.Ok(new BenchmarkOptions(
                addCount,
                addForks,
                historyDirectory,
                lookupCount,
                lookupForks,
                randomMessageIds,
                showHelp,
                durationSeconds,
                targetGenerations,
                fillFactor,
                datasetSize,
                zipfianSkew,
                mixedMode,
                mixInsert,
                mixLookup,
                mixExpire,
                restartTest,
                coldRestart,
                burstMode,
                burstProfile,
                burstDutyPercent,
                burstSleepMs,
                correlationSampleMs,
                saturationQueueDepthThreshold,
                degradedP99Microseconds,
                shardCount,
                slotsPerShard,
                maxLoadFactorPercent,
                queueCapacityPerShard,
                maxRetainedGenerations,
                bloomCheckpointInsertInterval,
                scrubberSamplesPerTick,
                scrubberIntervalMs,
                queueDepthThreshold,
                probeFailureThreshold,
                autoTune,
                bloomPersistMode,
                enqueueDupCheck,
                insertBatchSize));
        }
        catch (ArgumentException ex)
        {
            return ParseResult.Fail(ex.Message);
        }
    }

    private static string Next(string[] args, ref int i, string option)
    {
        if (i + 1 >= args.Length)
        {
            throw new ArgumentException($"Missing value for {option}.");
        }

        return args[++i];
    }

    private static int ParsePositiveInt(string[] args, ref int i, string option)
    {
        if (!int.TryParse(Next(args, ref i, option), NumberStyles.Integer, CultureInfo.InvariantCulture, out int value) || value <= 0)
        {
            throw new ArgumentException($"Invalid value for {option}.");
        }

        return value;
    }

    private static int ParseNonNegativeInt(string[] args, ref int i, string option)
    {
        if (!int.TryParse(Next(args, ref i, option), NumberStyles.Integer, CultureInfo.InvariantCulture, out int value) || value < 0)
        {
            throw new ArgumentException($"Invalid value for {option}.");
        }

        return value;
    }

    private static int ParseIntRange(string[] args, ref int i, string option, int min, int max)
    {
        int value = ParsePositiveInt(args, ref i, option);
        if (value < min || value > max)
        {
            throw new ArgumentException($"Invalid value for {option}.");
        }

        return value;
    }

    private static long ParsePositiveLong(string[] args, ref int i, string option)
    {
        if (!long.TryParse(Next(args, ref i, option), NumberStyles.Integer, CultureInfo.InvariantCulture, out long value) || value <= 0)
        {
            throw new ArgumentException($"Invalid value for {option}.");
        }

        return value;
    }

    private static long ParseNonNegativeLong(string[] args, ref int i, string option)
    {
        if (!long.TryParse(Next(args, ref i, option), NumberStyles.Integer, CultureInfo.InvariantCulture, out long value) || value < 0)
        {
            throw new ArgumentException($"Invalid value for {option}.");
        }

        return value;
    }

    private static double ParseDoubleRange(string[] args, ref int i, string option, double min, double max)
    {
        if (!double.TryParse(Next(args, ref i, option), NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out double value) ||
            value < min || value > max)
        {
            throw new ArgumentException($"Invalid value for {option}.");
        }

        return value;
    }

    private static double ParseNonNegativeDouble(string[] args, ref int i, string option)
    {
        if (!double.TryParse(Next(args, ref i, option), NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out double value) ||
            value < 0)
        {
            throw new ArgumentException($"Invalid value for {option}.");
        }

        return value;
    }

    private static (int Insert, int Lookup, int Expire) ParseMix(string text)
    {
        string[] parts = text.Split(':');
        if (parts.Length != 3 ||
            !int.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out int insert) ||
            !int.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out int lookup) ||
            !int.TryParse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out int expire) ||
            insert < 0 || lookup < 0 || expire < 0 || insert + lookup + expire <= 0)
        {
            throw new ArgumentException("Invalid --mix format; expected I:L:E.");
        }

        return (insert, lookup, expire);
    }

    private static BurstProfile ParseBurstProfile(string raw) => raw.ToLowerInvariant() switch
    {
        "spike" => BurstProfile.Spike,
        "ramp" => BurstProfile.Ramp,
        "wave" => BurstProfile.Wave,
        _ => throw new ArgumentException("Invalid --burst-profile. Expected: spike|ramp|wave.")
    };

    private static BloomCheckpointPersistMode ParseBloomPersistMode(string raw) => raw.ToLowerInvariant() switch
    {
        "throughput" => BloomCheckpointPersistMode.Throughput,
        "durability" => BloomCheckpointPersistMode.Durability,
        _ => throw new ArgumentException("Invalid --bloom-persist. Expected: throughput|durability.")
    };

    private static HistoryCrossGenerationDuplicateCheck ParseEnqueueDupCheck(string raw) => raw.ToLowerInvariant() switch
    {
        "full" => HistoryCrossGenerationDuplicateCheck.Full,
        "active-generation-only" => HistoryCrossGenerationDuplicateCheck.ActiveGenerationOnly,
        _ => throw new ArgumentException("Invalid --enqueue-dup-check. Expected: full|active-generation-only.")
    };
}
