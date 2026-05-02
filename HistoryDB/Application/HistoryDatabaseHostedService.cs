// HistoryDatabaseHostedService.cs -- optional IHostedService adapter for Generic Host shutdown ordering.

using Microsoft.Extensions.Hosting;

namespace HistoryDB.Application;

/// <summary>
/// Calls <see cref="HistoryDatabase.DisposeAsync"/> when the host stops so journal and mmap-backed writers flush before process exit.
/// </summary>
/// <remarks>
/// Register after the database itself, for example:
/// <code>
/// services.AddSingleton(_ => new HistoryDatabase(rootPath, logger: logger));
/// services.AddHostedService(sp => new HistoryDatabaseHostedService(sp.GetRequiredService&lt;HistoryDatabase&gt;()));
/// </code>
/// </remarks>
public sealed class HistoryDatabaseHostedService : IHostedService
{
    private readonly HistoryDatabase _database;

    public HistoryDatabaseHostedService(HistoryDatabase database)
    {
        ArgumentNullException.ThrowIfNull(database);
        _database = database;
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) =>
        _database.DisposeAsync().AsTask();
}
