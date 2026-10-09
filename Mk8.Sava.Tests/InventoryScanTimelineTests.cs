using System.Diagnostics;

namespace Mk8.Sava.Tests;

public sealed class InventoryScanTimelineTests
{
    [Theory]
    [InlineData(0, 10, 0, 0)]
    [InlineData(10, 20, 1, 10)]
    [InlineData(20, 30, 0, 0)]
    [InlineData(15, 35, 2, 10)]
    [InlineData(12, 61, 3, 29)]
    [InlineData(0, 100, 3, 35)]
    [InlineData(15, 15, 0, 0)]
    [InlineData(40, 50, 0, 0)]
    [InlineData(65, 70, 0, 0)]
    [InlineData(60, 70, 1, 5)]
    public void HalfOpenIntersectionsClipEachPassWithoutCountingGaps(
        long started, long finished, int passes, long ticks)
    {
        var timeline = new InventoryScanTimeline([
            new(10, 20), new(30, 40), new(40, 40), new(50, 65)]);

        var actual = timeline.Correlate(started, finished);

        Assert.Equal(new InventoryScanTimeline.Correlation(passes, ticks), actual);
        Assert.Equal(Stopwatch.GetElapsedTime(0, ticks), actual.Elapsed);
    }

    [Fact]
    public void SnapshotOwnsItsInputAndRefusesCollectionMutation()
    {
        InventoryScanTimeline.ScanInterval[] input = [new(10, 20), new(30, 40)];
        var timeline = new InventoryScanTimeline(input);
        var snapshot = timeline.Passes;
        input[0] = new(0, 100);
        input[1] = default;

        Assert.Equal(new InventoryScanTimeline.ScanInterval(10, 20), snapshot[0]);
        Assert.Equal(new InventoryScanTimeline.ScanInterval(30, 40), snapshot[1]);
        Assert.Throws<NotSupportedException>(() => ((IList<InventoryScanTimeline.ScanInterval>)snapshot)[0] = default);
        Assert.Equal(new InventoryScanTimeline.Correlation(2, 20), timeline.Correlate(0, 100));
    }

    [Fact]
    public void InvalidOrderingOverlapDurationAndPassCountAreRefused()
    {
        Assert.Throws<ArgumentException>(() => new InventoryScanTimeline([new(20, 10)]));
        Assert.Throws<ArgumentException>(() => new InventoryScanTimeline([new(10, 20), new(15, 30)]));
        Assert.Throws<ArgumentException>(() => new InventoryScanTimeline([new(30, 40), new(10, 20)]));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InventoryScanTimeline(
            new InventoryScanTimeline.ScanInterval[InventoryScanTimeline.MaximumPasses + 1]));
        var timeline = new InventoryScanTimeline([]);
        var reversed = Assert.Throws<ArgumentOutOfRangeException>(() => timeline.Correlate(20, 10));
        Assert.Equal("finished", reversed.ParamName);
    }

    [Fact]
    public void EmptyAndInstantaneousPassesDoNotInventPositiveOverlap()
    {
        var empty = new InventoryScanTimeline([]);
        var instantaneous = new InventoryScanTimeline([new(10, 10), new(20, 20)]);

        Assert.Empty(empty.Passes);
        Assert.Equal(default, empty.Correlate(0, 30));
        Assert.Equal(2, instantaneous.Passes.Count);
        Assert.Equal(default, instantaneous.Correlate(0, 30));
        Assert.Equal(default, instantaneous.Correlate(20, 20));
    }

    [Fact]
    public void FullExistingPassBudgetIsRetainedWithoutLossOrDoubleCounting()
    {
        var input = Enumerable.Range(0, InventoryScanTimeline.MaximumPasses)
            .Select(static index => new InventoryScanTimeline.ScanInterval(2 * index, 2 * index + 1)).ToArray();
        var timeline = new InventoryScanTimeline(input);

        Assert.Equal(110, timeline.Passes.Count);
        Assert.Equal(new InventoryScanTimeline.Correlation(110, 110), timeline.Correlate(0, 220));
        Assert.Equal(default, timeline.Correlate(220, 230));
    }
}
