using HistoryDB.Tests.Infrastructure.Bloom;
using HistoryDB.Tests.Infrastructure.Configuration;
using HistoryDB.Tests.Infrastructure.Contracts;
using HistoryDB.Tests.Infrastructure.FileSystem;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HistoryDB.Tests.Infrastructure.Fixtures;

/// <summary>
/// Shared composition root for HistoryDB.Tests (DIP: concrete adapters wired once per test collection/class fixture scope).
/// </summary>
/// <remarks>
/// xUnit class fixtures must expose exactly one public constructor; customize layout by deriving from this type and overriding <see cref="CreateLayoutOptions"/>.
/// </remarks>
public class TestHostFixture
{
    public TestHostFixture()
    {
        LayoutOptions = CreateLayoutOptions();
        LoggerFactory = NullLoggerFactory.Instance;

        WorkspaceFactory = new TemporaryDirectoryWorkspaceFactory(LayoutOptions, LoggerFactory);

        BloomSidecarReader = new BloomSidecarFormatReader(
            LoggerFactory.CreateLogger<BloomSidecarFormatReader>());
    }

    /// <summary>
    /// Layout configuration for temporary workspaces.
    /// </summary>
    public TestWorkspaceLayoutOptions LayoutOptions { get; }

    /// <summary>
    /// Logger factory used by workspace cleanup and Bloom decode diagnostics (defaults to no-op).
    /// </summary>
    public ILoggerFactory LoggerFactory { get; }

    public ITemporaryDirectoryWorkspaceFactory WorkspaceFactory { get; }

    public IBloomSidecarFormatReader BloomSidecarReader { get; }

    /// <summary>
    /// Override in a derived fixture to inject CI-specific path prefixes or retention policies.
    /// </summary>
    protected virtual TestWorkspaceLayoutOptions CreateLayoutOptions()
    {
        return new TestWorkspaceLayoutOptions
        {
            RootFolderName = "HistoryDB.Tests",
        };
    }
}
