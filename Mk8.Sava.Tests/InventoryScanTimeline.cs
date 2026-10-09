using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Mk8.Sava.Tests;

// Intersections of observed Stopwatch intervals are chronology, not causal attribution.
internal sealed class InventoryScanTimeline
{
    internal const int MaximumPasses = 110;
    private readonly ReadOnlyCollection<ScanInterval> _passes;

    internal InventoryScanTimeline(ReadOnlySpan<ScanInterval> passes)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(passes.Length, MaximumPasses, nameof(passes));
        var owned = passes.ToArray();
        for (var index = 0; index < owned.Length; index++)
        {
            var pass = owned[index];
            if (pass.Finished < pass.Started || (index > 0 && pass.Started < owned[index - 1].Finished))
                throw new ArgumentException("Scan intervals must be ordered, non-overlapping and nonnegative in duration.", nameof(passes));
        }
        _passes = Array.AsReadOnly(owned);
    }

    internal IReadOnlyList<ScanInterval> Passes => _passes;

    internal Correlation Correlate(long started, long finished)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(finished, started);
        var count = 0;
        long ticks = 0;
        foreach (var pass in _passes)
        {
            if (pass.Finished <= started)
                continue;
            if (pass.Started >= finished)
                break;
            var intersection = checked(Math.Min(finished, pass.Finished) - Math.Max(started, pass.Started));
            if (intersection > 0)
            {
                count++;
                ticks = checked(ticks + intersection);
            }
        }
        return new Correlation(count, ticks);
    }

    [StructLayout(LayoutKind.Auto)]
    internal readonly record struct ScanInterval(long Started, long Finished);

    [StructLayout(LayoutKind.Auto)]
    internal readonly record struct Correlation(int PassCount, long TimestampTicks)
    {
        internal TimeSpan Elapsed => Stopwatch.GetElapsedTime(0, TimestampTicks);
    }
}
