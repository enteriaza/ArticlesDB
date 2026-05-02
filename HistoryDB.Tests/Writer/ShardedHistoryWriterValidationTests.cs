using HistoryDB.Contracts;
using HistoryDB.Tests.Infrastructure.Contracts;
using HistoryDB.Tests.Infrastructure.Fixtures;

namespace HistoryDB.Tests.Writer;

public sealed class ShardedHistoryWriterValidationTests : IClassFixture<TestHostFixture>
{
    private readonly TestHostFixture _host;

    public ShardedHistoryWriterValidationTests(TestHostFixture host)
    {
        ArgumentNullException.ThrowIfNull(host);
        _host = host;
    }

    [Fact]
    public void Constructor_RejectsNonPositiveShardCount()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        ArgumentOutOfRangeException exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ShardedHistoryWriter(rootPath: workspace.RootPath, shardCount: 0));
        Assert.Equal("shardCount", exception.ParamName);
    }

    [Fact]
    public void Constructor_RejectsNonPowerOfTwoSlotsPerShard()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        ArgumentOutOfRangeException exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ShardedHistoryWriter(rootPath: workspace.RootPath, shardCount: 1, slotsPerShard: 3));
        Assert.Equal("slotsPerShard", exception.ParamName);
    }

    [Fact]
    public void Constructor_RejectsZeroSlotsPerShard()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        ArgumentOutOfRangeException exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ShardedHistoryWriter(rootPath: workspace.RootPath, shardCount: 1, slotsPerShard: 0));
        Assert.Equal("slotsPerShard", exception.ParamName);
    }

    [Fact]
    public void Constructor_RejectsNonPositiveQueueCapacity()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        ArgumentOutOfRangeException exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ShardedHistoryWriter(rootPath: workspace.RootPath, shardCount: 1, queueCapacityPerShard: 0));
        Assert.Equal("queueCapacityPerShard", exception.ParamName);
    }

    [Fact]
    public void Constructor_RejectsNonPositiveMaxRetainedGenerations()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        ArgumentOutOfRangeException exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ShardedHistoryWriter(rootPath: workspace.RootPath, shardCount: 1, maxRetainedGenerations: 0));
        Assert.Equal("maxRetainedGenerations", exception.ParamName);
    }

    [Theory]
    [InlineData(0.0)]
    [InlineData(0.96)]
    public void Constructor_RejectsBloomMemoryFractionOutsideOpenZeroToPointNineFive(double bloomMemoryFraction)
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        ArgumentOutOfRangeException exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ShardedHistoryWriter(rootPath: workspace.RootPath, shardCount: 1, bloomMemoryFraction: bloomMemoryFraction));
        Assert.Equal("bloomMemoryFraction", exception.ParamName);
    }

    [Theory]
    [InlineData(3.9)]
    [InlineData(32.1)]
    public void Constructor_RejectsBloomTargetBitsPerEntryOutsideFourToThirtyTwo(double bloomTargetBitsPerEntry)
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        ArgumentOutOfRangeException exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ShardedHistoryWriter(
                rootPath: workspace.RootPath,
                shardCount: 1,
                bloomTargetBitsPerEntry: bloomTargetBitsPerEntry));
        Assert.Equal("bloomTargetBitsPerEntry", exception.ParamName);
    }

    [Fact]
    public void Constructor_RejectsProactiveRolloverPercentNotStrictlyBelowMaxLoadFactor()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        ArgumentOutOfRangeException exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ShardedHistoryWriter(
                rootPath: workspace.RootPath,
                shardCount: 1,
                maxLoadFactorPercent: 75,
                proactiveRolloverLoadFactorPercent: 75));
        Assert.Equal("proactiveRolloverLoadFactorPercent", exception.ParamName);
    }

    [Fact]
    public void Constructor_RejectsProactiveRolloverWhenComputedThresholdReachesHardLimit()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        ArgumentOutOfRangeException exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ShardedHistoryWriter(
                rootPath: workspace.RootPath,
                shardCount: 1,
                slotsPerShard: 2,
                maxLoadFactorPercent: 51,
                proactiveRolloverLoadFactorPercent: 50));
        Assert.Equal("proactiveRolloverLoadFactorPercent", exception.ParamName);
    }

    [Fact]
    public void Constructor_RejectsProactiveRolloverWhenThresholdSlotsWouldBeZero()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        ArgumentOutOfRangeException exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ShardedHistoryWriter(
                rootPath: workspace.RootPath,
                shardCount: 1,
                slotsPerShard: 64,
                maxLoadFactorPercent: 75,
                proactiveRolloverLoadFactorPercent: 1));
        Assert.Equal("proactiveRolloverLoadFactorPercent", exception.ParamName);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(100)]
    public void Constructor_RejectsQueueWatermarkPercentOutsideZeroToNinetyNine(int watermarkPercent)
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        ArgumentOutOfRangeException exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ShardedHistoryWriter(
                rootPath: workspace.RootPath,
                shardCount: 1,
                proactiveRolloverQueueWatermarkPercent: watermarkPercent));
        Assert.Equal("proactiveRolloverQueueWatermarkPercent", exception.ParamName);
    }

    [Fact]
    public void Constructor_RejectsNegativeAggregateProbeFailuresThreshold()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        ArgumentOutOfRangeException exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ShardedHistoryWriter(
                rootPath: workspace.RootPath,
                shardCount: 1,
                proactiveRolloverAggregateProbeFailuresThreshold: -1));
        Assert.Equal("proactiveRolloverAggregateProbeFailuresThreshold", exception.ParamName);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(100_001)]
    public void Constructor_RejectsSlotScrubberSamplesPerTickOutsideRange(int samplesPerTick)
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        ArgumentOutOfRangeException exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ShardedHistoryWriter(rootPath: workspace.RootPath, shardCount: 1, slotScrubberSamplesPerTick: samplesPerTick));
        Assert.Equal("slotScrubberSamplesPerTick", exception.ParamName);
    }

    [Theory]
    [InlineData(249)]
    [InlineData(86_400_001)]
    public void Constructor_RejectsSlotScrubberIntervalOutsideRange(int intervalMilliseconds)
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        ArgumentOutOfRangeException exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ShardedHistoryWriter(
                rootPath: workspace.RootPath,
                shardCount: 1,
                slotScrubberIntervalMilliseconds: intervalMilliseconds));
        Assert.Equal("slotScrubberIntervalMilliseconds", exception.ParamName);
    }

    [Fact]
    public void Constructor_FromHistoryWriterOptions_DelegatesValidation()
    {
        using ITemporaryDirectoryWorkspace workspace = _host.WorkspaceFactory.Create(_host.LayoutOptions.WriterValidationPurposeFolderName);
        ArgumentOutOfRangeException exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ShardedHistoryWriter(new HistoryWriterOptions
            {
                RootPath = workspace.RootPath,
                ShardCount = -1,
            }));
        Assert.Equal("shardCount", exception.ParamName);
    }
}
