namespace HistoryDB.Tests.Infrastructure.Configuration;

/// <summary>
/// Naming layout for isolated temporary workspaces created during tests (under <see cref="System.IO.Path.GetTempPath"/>).
/// </summary>
public sealed record TestWorkspaceLayoutOptions
{
    /// <summary>
    /// Top-level folder name beneath the process temp directory (for example <c>HistoryDB.Tests</c>).
    /// </summary>
    public required string RootFolderName { get; init; }

    /// <summary>
    /// Relative segment naming the Bloom sidecar file-system scenarios.
    /// </summary>
    public string BloomSidecarPurposeFolderName { get; init; } = "bloom-sidecar";

    /// <summary>
    /// Relative segment naming <see cref="HistoryDB.ShardedHistoryWriter"/> validation scenarios.
    /// </summary>
    public string WriterValidationPurposeFolderName { get; init; } = "writer-validation";
}
