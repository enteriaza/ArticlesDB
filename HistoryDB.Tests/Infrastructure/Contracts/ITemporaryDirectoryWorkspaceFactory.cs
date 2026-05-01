namespace HistoryDB.Tests.Infrastructure.Contracts;

/// <summary>
/// Creates unique temporary workspaces under the configured layout options (DIP: tests depend on this abstraction).
/// </summary>
public interface ITemporaryDirectoryWorkspaceFactory
{
    /// <summary>
    /// Creates a new workspace rooted under <paramref name="purposeFolderName"/> (for example <c>bloom-sidecar</c>).
    /// </summary>
    ITemporaryDirectoryWorkspace Create(string purposeFolderName);
}
