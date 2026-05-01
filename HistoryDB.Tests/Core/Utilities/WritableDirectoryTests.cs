using HistoryDB.Utilities;

namespace HistoryDB.Tests.Core.Utilities;

public sealed class WritableDirectoryTests
{
    [Fact]
    public void ValidateRootDirectory_AcceptsProcessTemporaryDirectory()
    {
        string temporaryRoot = Path.GetTempPath();
        string normalized = WritableDirectory.ValidateRootDirectory(temporaryRoot);
        Assert.True(Path.IsPathRooted(normalized));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void ValidateRootDirectory_RejectsNullOrWhiteSpace(string? rootPath)
    {
        Assert.ThrowsAny<ArgumentException>(() => WritableDirectory.ValidateRootDirectory(rootPath!));
    }

    [Fact]
    public void ValidateRootDirectory_RejectsEmbeddedNullCharacter()
    {
        Assert.Throws<ArgumentException>(() => WritableDirectory.ValidateRootDirectory("C:\\temp\u0000folder"));
    }

    [Fact]
    public void ValidateMmapDataFilePath_RejectsEmbeddedNullCharacter()
    {
        string candidatePath = Path.Combine(Path.GetTempPath(), "shard\u00001.dat");
        Assert.Throws<ArgumentException>(() => WritableDirectory.ValidateMmapDataFilePath(candidatePath));
    }

    [Fact]
    public void ValidateMmapDataFilePath_AcceptsRootedFileUnderTemporaryDirectory()
    {
        string candidatePath = Path.Combine(Path.GetTempPath(), "historydb-test mmap validation.dat");
        string normalized = WritableDirectory.ValidateMmapDataFilePath(candidatePath);
        Assert.True(Path.IsPathRooted(normalized));
    }

    [Fact]
    public void ValidateRootDirectory_RejectsPathsLongerThanMaximumAllowedLength()
    {
        string prefix = Path.Combine(Path.GetTempPath(), new string('a', 100));
        string rootPath = prefix + new string('b', WritableDirectory.MaxRootPathLength);
        Assert.Throws<ArgumentException>(() => WritableDirectory.ValidateRootDirectory(rootPath));
    }
}
