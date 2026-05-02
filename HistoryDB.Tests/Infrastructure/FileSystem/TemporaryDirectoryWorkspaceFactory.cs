using HistoryDB.Tests.Infrastructure.Configuration;
using HistoryDB.Tests.Infrastructure.Contracts;
using Microsoft.Extensions.Logging;

namespace HistoryDB.Tests.Infrastructure.FileSystem;

internal sealed class TemporaryDirectoryWorkspaceFactory : ITemporaryDirectoryWorkspaceFactory
{
    private readonly TestWorkspaceLayoutOptions _layoutOptions;
    private readonly ILoggerFactory _loggerFactory;

    public TemporaryDirectoryWorkspaceFactory(TestWorkspaceLayoutOptions layoutOptions, ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(layoutOptions);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        _layoutOptions = layoutOptions;
        _loggerFactory = loggerFactory;
    }

    /// <inheritdoc />
    public ITemporaryDirectoryWorkspace Create(string purposeFolderName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(purposeFolderName);

        string rootPath = Path.Combine(
            Path.GetTempPath(),
            _layoutOptions.RootFolderName,
            purposeFolderName,
            Guid.NewGuid().ToString("N"));

        ILogger<TemporaryDirectoryWorkspace> logger = _loggerFactory.CreateLogger<TemporaryDirectoryWorkspace>();
        return new TemporaryDirectoryWorkspace(rootPath, logger);
    }
}
