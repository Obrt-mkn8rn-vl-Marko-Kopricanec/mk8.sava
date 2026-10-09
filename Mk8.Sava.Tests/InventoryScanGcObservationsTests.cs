using System.Diagnostics;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

// Forced collection controls must not run alongside the existing scale measurements.
[Collection("Physical inventory scale")]
public sealed class InventoryScanGcObservationsTests(ITestOutputHelper output)
{
    [Fact]
    public void ActualProcessCountersRetainAControlledBlockingCollection()
    {
        var before = InventoryScanGcObservations.Capture();
        var started = Stopwatch.GetTimestamp();
        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: false);
        var finished = Stopwatch.GetTimestamp();
        var after = InventoryScanGcObservations.Capture();
        var observations = new InventoryScanGcObservations(
            new InventoryScanTimeline([new(started, finished)]), [new(before, after)]);
        var actual = Assert.Single(observations.Passes);

        Assert.Equal(before, actual.Before);
        Assert.Equal(after, actual.After);
        Assert.True(actual.Generation2Difference >= 1);
        Assert.True(actual.PauseTicksDifference > 0);
        output.WriteLine(FormattableString.Invariant(
            $"inventory_gc_control,gen0_difference={actual.Generation0Difference},gen1_difference={actual.Generation1Difference},gen2_difference={actual.Generation2Difference},pause_timespan_ticks_difference={actual.PauseTicksDifference},before_started_ticks={before.Started},before_finished_ticks={before.Finished},body_started_ticks={started},body_finished_ticks={finished},after_started_ticks={after.Started},after_finished_ticks={after.Finished}"));
    }

    [Fact]
    public void SnapshotOwnsItsInputAndCannotBeMutatedThroughTheReturnedView()
    {
        var first = Observation(0);
        var second = Observation(10);
        InventoryScanGcObservations.Observation[] input = [first, second];
        var observations = new InventoryScanGcObservations(
            new InventoryScanTimeline([new(4, 5), new(14, 15)]), input);
        input[0] = default;
        input[1] = default;

        Assert.Equal(first, observations.Passes[0]);
        Assert.Equal(second, observations.Passes[1]);
        Assert.Throws<NotSupportedException>(() =>
            ((IList<InventoryScanGcObservations.Observation>)observations.Passes)[0] = default);
    }

    [Theory]
    [InlineData(3, 2, 6, 7)]
    [InlineData(1, 2, 8, 7)]
    [InlineData(1, 5, 6, 7)]
    [InlineData(1, 2, 4, 7)]
    public void CounterReadWindowsMustBracketTheExistingMeasuredCall(
        long beforeStarted, long beforeFinished, long afterStarted, long afterFinished)
    {
        var timeline = new InventoryScanTimeline([new(4, 5)]);
        var error = Assert.Throws<ArgumentException>(() => new InventoryScanGcObservations(timeline,
            [new(new(beforeStarted, beforeFinished, 0, 0, 0, 0), new(afterStarted, afterFinished, 0, 0, 0, 0))]));

        Assert.Equal("observations", error.ParamName);
    }

    [Fact]
    public void MissingExtraAndOutOfOrderObservationsAreRejected()
    {
        var one = new InventoryScanTimeline([new(4, 5)]);
        Assert.Throws<ArgumentException>(() => new InventoryScanGcObservations(one, []));
        Assert.Throws<ArgumentException>(() => new InventoryScanGcObservations(one, [Observation(0), Observation(10)]));
        var two = new InventoryScanTimeline([new(4, 5), new(14, 15)]);
        Assert.Throws<ArgumentException>(() => new InventoryScanGcObservations(two,
            [Observation(0), new(new(6, 12, 0, 0, 0, 0), new(16, 17, 0, 0, 0, 0))]));
        Assert.Throws<ArgumentNullException>(() => new InventoryScanGcObservations(null!, []));
    }

    [Fact]
    public void EmptyControlHasNoInventedScannerOrCounterObservation()
    {
        var observations = new InventoryScanGcObservations(new InventoryScanTimeline([]), []);

        Assert.Empty(observations.Passes);
        Assert.Throws<ArgumentException>(() =>
            new InventoryScanGcObservations(new InventoryScanTimeline([]), [Observation(0)]));
    }

    [Fact]
    public void FullExistingPassBudgetRemainsBoundedAndIsRetainedWithoutLoss()
    {
        var passes = new InventoryScanTimeline.ScanInterval[InventoryScanTimeline.MaximumPasses];
        var input = new InventoryScanGcObservations.Observation[passes.Length];
        for (var index = 0; index < passes.Length; index++)
        {
            passes[index] = new(10 * index + 4, 10 * index + 5);
            input[index] = Observation(10 * index);
        }
        var observations = new InventoryScanGcObservations(new InventoryScanTimeline(passes), input);

        Assert.Equal(110, observations.Passes.Count);
        Assert.Equal(input, observations.Passes);
    }

    [Fact]
    public void RawProcessPauseDifferenceIsNotClippedToOrAttributedToTheMeasuredInterval()
    {
        var observation = new InventoryScanGcObservations.Observation(
            new(1, 2, 10, 8, 3, 1), new(6, 7, 1_000_010, 9, 3, 1));
        var observations = new InventoryScanGcObservations(new InventoryScanTimeline([new(4, 5)]), [observation]);
        var actual = Assert.Single(observations.Passes);

        Assert.Equal(1_000_000, actual.PauseTicksDifference);
        Assert.Equal(1, actual.Generation0Difference);
        Assert.Equal(0, actual.Generation1Difference);
        Assert.Equal(0, actual.Generation2Difference);
    }

    [Fact]
    public void SignedCounterDecreasesAndIndependentGenerationReadsAreNotNormalized()
    {
        var observation = new InventoryScanGcObservations.Observation(
            new(1, 2, 100, int.MaxValue, 10, 30), new(6, 7, 50, int.MinValue, 9, 31));
        var observations = new InventoryScanGcObservations(new InventoryScanTimeline([new(4, 5)]), [observation]);
        var actual = Assert.Single(observations.Passes);

        Assert.Equal(-50, actual.PauseTicksDifference);
        Assert.Equal(-4_294_967_295, actual.Generation0Difference);
        Assert.Equal(-1, actual.Generation1Difference);
        Assert.Equal(1, actual.Generation2Difference);
    }

    private static InventoryScanGcObservations.Observation Observation(long offset) =>
        new(new(offset + 1, offset + 2, 0, 0, 0, 0), new(offset + 6, offset + 7, 0, 0, 0, 0));
}
