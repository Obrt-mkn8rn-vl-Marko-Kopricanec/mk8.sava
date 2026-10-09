using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Mk8.Sava.Tests;

// Process-wide counter reads are sequential. Their differences are observations,
// not pause intersections, time attributable to Advance, or causal explanations.
internal sealed class InventoryScanGcObservations
{
    private readonly ReadOnlyCollection<Observation> _passes;

    internal InventoryScanGcObservations(InventoryScanTimeline timeline, ReadOnlySpan<Observation> observations)
    {
        ArgumentNullException.ThrowIfNull(timeline);
        if (observations.Length != timeline.Passes.Count)
            throw new ArgumentException("Every actual scanner interval must have exactly one GC observation.", nameof(observations));

        var owned = observations.ToArray();
        for (var index = 0; index < owned.Length; index++)
        {
            var observation = owned[index];
            var pass = timeline.Passes[index];
            if (observation.Before.Finished < observation.Before.Started ||
                observation.After.Finished < observation.After.Started ||
                observation.Before.Finished > pass.Started ||
                observation.After.Started < pass.Finished ||
                (index > 0 && observation.Before.Started < owned[index - 1].After.Finished))
                throw new ArgumentException("Ordered counter-read windows must bracket their actual scanner intervals.", nameof(observations));
        }
        _passes = Array.AsReadOnly(owned);
    }

    internal IReadOnlyList<Observation> Passes => _passes;

    internal static Snapshot Capture()
    {
        var started = Stopwatch.GetTimestamp();
        var pauseTicks = GC.GetTotalPauseDuration().Ticks;
        var generation0 = GC.CollectionCount(0);
        var generation1 = GC.CollectionCount(1);
        var generation2 = GC.CollectionCount(2);
        return new Snapshot(started, Stopwatch.GetTimestamp(), pauseTicks, generation0, generation1, generation2);
    }

    [StructLayout(LayoutKind.Auto)]
    internal readonly record struct Snapshot(
        long Started, long Finished, long PauseTicks, int Generation0, int Generation1, int Generation2);

    [StructLayout(LayoutKind.Auto)]
    internal readonly record struct Observation(Snapshot Before, Snapshot After)
    {
        // Keep signed raw differences, including an observed counter decrease;
        // do not normalize it into a successful zero-collection observation.
        internal long PauseTicksDifference => checked(After.PauseTicks - Before.PauseTicks);
        internal long Generation0Difference => (long)After.Generation0 - Before.Generation0;
        internal long Generation1Difference => (long)After.Generation1 - Before.Generation1;
        internal long Generation2Difference => (long)After.Generation2 - Before.Generation2;
    }
}
