namespace HistoryDB.Tests.Infrastructure.Contracts;

/// <summary>
/// An isolated directory intended for short-lived test artifacts; deletes best-effort on dispose.
/// </summary>
public interface ITemporaryDirectoryWorkspace : IDisposable
{
    /// <summary>
    /// Absolute root path for this workspace (the directory may not exist until a writer creates it).
    /// </summary>
    string RootPath { get; }

    /// <summary>
    /// Combines <see cref="RootPath"/> with relative segments using <see cref="System.IO.Path.Combine(string[])"/>.
    /// </summary>
    string Combine(params string[] relativeSegments);
}
