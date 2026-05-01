using HistoryDB.Tests.Infrastructure.Contracts;
using Microsoft.Extensions.Logging;

namespace HistoryDB.Tests.Infrastructure.FileSystem;

internal sealed class TemporaryDirectoryWorkspace : ITemporaryDirectoryWorkspace
{
    private readonly ILogger<TemporaryDirectoryWorkspace> _logger;

    public TemporaryDirectoryWorkspace(string rootPath, ILogger<TemporaryDirectoryWorkspace> logger)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootPath);
        ArgumentNullException.ThrowIfNull(logger);
        RootPath = rootPath;
        _logger = logger;
    }

    /// <inheritdoc />
    public string RootPath { get; }

    /// <inheritdoc />
    public string Combine(params string[] relativeSegments)
    {
        ArgumentNullException.ThrowIfNull(relativeSegments);

        if (relativeSegments.Length == 0)
        {
            return RootPath;
        }

        string[] segments = new string[relativeSegments.Length + 1];
        segments[0] = RootPath;
        relativeSegments.CopyTo(segments, 1);
        return Path.Combine(segments);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        try
        {
            if (Directory.Exists(RootPath))
            {
                Directory.Delete(RootPath, recursive: true);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete temporary workspace directory at {RootPath}", RootPath);
        }
    }
}
