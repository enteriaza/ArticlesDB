using HistoryDB;

namespace HisBench.Contracts;

internal interface IBenchmarkOptionsParser
{
    ParseResult Parse(string[] args);
}

internal interface IBenchmarkOrchestrator
{
    Task<int> RunAsync(BenchmarkOptions options, CancellationToken cancellationToken);
}

internal interface IHistoryDatabaseFactory
{
    HistoryDatabase Create(BenchmarkOptions options);
}

internal interface IColdRestartCoordinator
{
    Task TryColdRestartAsync(string historyDirectory, CancellationToken cancellationToken);
}

internal interface IProcessTelemetryReader
{
    ProcessResourceSnapshot ReadCurrent();
}

internal interface IBenchmarkReporter
{
    void PrintBanner(BenchmarkOptions options);

    void PrintReport(BenchmarkRunReport report);
}
