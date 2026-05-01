// PathValidator.cs -- infrastructure adapter for validating and normalizing filesystem paths.

using HistoryDB.Contracts;
using HistoryDB.Utilities;

namespace HistoryDB.Infrastructure.IO;

internal sealed class PathValidator : IPathValidator
{
    public string ValidateRootDirectory(string rootPath) => WritableDirectory.ValidateRootDirectory(rootPath);

    public string ValidateDataFilePath(string path) => WritableDirectory.ValidateMmapDataFilePath(path);
}
