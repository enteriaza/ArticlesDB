using HisBench.Application;
using HisBench.Contracts;
using HisBench.Infrastructure;
using Microsoft.Extensions.Logging;
using System.Globalization;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;
        CultureInfo.DefaultThreadCurrentUICulture = CultureInfo.InvariantCulture;
        CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
        CultureInfo.CurrentUICulture = CultureInfo.InvariantCulture;

        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .SetMinimumLevel(LogLevel.Information)
                .AddSimpleConsole(options =>
                {
                    options.TimestampFormat = "HH:mm:ss ";
                    options.SingleLine = true;
                });
        });

        IBenchmarkOptionsParser parser = new BenchmarkOptionsParser();
        ParseResult parseResult = parser.Parse(args);
        if (!parseResult.Success)
        {
            Console.Error.WriteLine(parseResult.ErrorMessage);
            Console.Error.WriteLine(BenchmarkUsageFormatter.Format());
            return 1;
        }

        BenchmarkOptions options = parseResult.Options!;
        if (options.ShowHelp)
        {
            Console.WriteLine(BenchmarkUsageFormatter.Format());
            return 0;
        }

        int desiredWorkers = Math.Max(64, Math.Max(options.AddForks, options.LookupForks) * 4);
        ThreadPool.GetMinThreads(out int minWorker, out int minIo);
        if (minWorker < desiredWorkers)
        {
            ThreadPool.SetMinThreads(desiredWorkers, minIo);
        }

        IBenchmarkOrchestrator orchestrator = new BenchmarkOrchestrator(
            new HistoryDatabaseFactory(),
            new ColdRestartCoordinator(loggerFactory.CreateLogger<ColdRestartCoordinator>()),
            new ProcessTelemetryReader(),
            new ConsoleBenchmarkReporter(),
            loggerFactory.CreateLogger<BenchmarkOrchestrator>());

        return await orchestrator.RunAsync(options, CancellationToken.None).ConfigureAwait(false);
    }
}
