// WritableDirectory.cs -- validates and normalizes user-supplied root paths for on-disk history generations (traversal and control-character hardening).
// Callers pass host-configured directory strings; this type rejects embedded NULs and non-rooted paths before CreateDirectory / mmap sidecars.

using System.IO;

namespace HistoryDB.Utilities;

/// <summary>
/// Validates directory paths used as the root for generation folders (<c>00001/</c>, shard files, Bloom sidecars).
/// </summary>
/// <remarks>
/// <para>
/// <b>Threat model:</b> Paths come from application configuration. Rejecting NUL bytes and requiring rooted paths reduces
/// oddities with <see cref="Path.Combine(string, string)"/> and accidental relative roots when the process working directory changes.
/// </para>
/// <para>
/// <b>Thread safety:</b> Static methods are re-entrant; no mutable shared state.
/// </para>
/// </remarks>
internal static class WritableDirectory
{
    /// <summary>
    /// Maximum path length accepted for the root directory, in characters (not bytes), before normalization.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Chosen to stay below common OS limits while allowing deep UNC paths; adjust only if deployment paths require it.
    /// </para>
    /// </remarks>
    internal const int MaxRootPathLength = 32_767;

    /// <summary>
    /// Returns a fully qualified, normalized path for <paramref name="rootPath"/> after validation.
    /// </summary>
    /// <param name="rootPath">User or configuration supplied directory.</param>
    /// <returns>The full normalized path (trailing separators may be trimmed by the OS normalization rules).</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="rootPath"/> is null, empty, whitespace, contains NUL, is too long, or is not rooted.</exception>
    internal static string ValidateRootDirectory(string rootPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootPath);

        if (rootPath.Length > MaxRootPathLength)
        {
            throw new ArgumentException($"rootPath exceeds maximum length ({MaxRootPathLength}).", nameof(rootPath));
        }

        if (rootPath.AsSpan().Contains('\0'))
        {
            throw new ArgumentException("rootPath must not contain NUL (U+0000) characters.", nameof(rootPath));
        }

        string full = Path.GetFullPath(rootPath);
        if (!Path.IsPathRooted(full))
        {
            throw new ArgumentException("rootPath must resolve to a rooted path.", nameof(rootPath));
        }

        return full;
    }

    /// <summary>
    /// Validates a rooted file path for memory-mapped shard (<c>.dat</c>) or Bloom (<c>.bloom</c>) data files.
    /// </summary>
    /// <param name="path">Path passed to <see cref="FileStream"/> / mmap APIs.</param>
    /// <returns>Fully qualified normalized path.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="path"/> is invalid, contains NUL, is too long, or is not rooted.</exception>
    internal static string ValidateMmapDataFilePath(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        if (path.Length > MaxRootPathLength)
        {
            throw new ArgumentException($"path exceeds maximum length ({MaxRootPathLength}).", nameof(path));
        }

        if (path.AsSpan().Contains('\0'))
        {
            throw new ArgumentException("path must not contain NUL (U+0000) characters.", nameof(path));
        }

        string full = Path.GetFullPath(path);
        if (!Path.IsPathRooted(full))
        {
            throw new ArgumentException("path must resolve to a rooted path.", nameof(path));
        }

        return full;
    }
}
