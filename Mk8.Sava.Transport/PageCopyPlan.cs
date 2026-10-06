using System.Collections.ObjectModel;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Transport;

// A wire DTO's IReadOnlyList can still alias a mutable List/array. Only this
// privately constructed snapshot may drive page-copy framing after validation.
internal sealed class PageCopyPlan
{
    private const long MaximumPageBlobBytes = 8L * 1024 * 1024 * 1024 * 1024;
    private const long RangeFrameBytes = 4 * 1024 * 1024;

    private PageCopyPlan(PageRangeDiff descriptor, long dataLength)
    {
        Descriptor = descriptor;
        DataLength = dataLength;
    }

    internal PageRangeDiff Descriptor { get; }
    internal long DataLength { get; }

    internal static PageCopyPlan Create(PageRangeDiff changes, long sourceLength)
    {
        ArgumentNullException.ThrowIfNull(changes);
        if (sourceLength < 0 || sourceLength > MaximumPageBlobBytes || sourceLength % 512 != 0)
            throw new InvalidDataException("The application page-copy source length is invalid.");
        // Validate the snapshot, not an alias that the producer can subsequently
        // change. Immutable PageRange records need no second object-level copy.
        var pages = Snapshot(changes.PageRanges);
        var clears = Snapshot(changes.ClearRanges);
        _ = ValidateRanges(clears, MaximumPageBlobBytes);
        var dataLength = ValidateRanges(pages, sourceLength);
        return new PageCopyPlan(new PageRangeDiff(pages, clears), dataLength);
    }

    private static ReadOnlyCollection<PageRange> Snapshot(IReadOnlyList<PageRange>? ranges)
    {
        if (ranges is null)
            throw new InvalidDataException("The application page-copy ranges are missing.");
        return Array.AsReadOnly(ranges.ToArray());
    }

    private static long ValidateRanges(IReadOnlyList<PageRange> ranges, long maximumLength)
    {
        long total = 0;
        long lastEnd = -1;
        foreach (var range in ranges)
        {
            if (range is null || range.Start < 0 || range.Start <= lastEnd || range.End < range.Start ||
                range.End >= maximumLength || range.Start % 512 != 0 || (range.End + 1) % 512 != 0)
                throw new InvalidDataException("The application page-copy ranges are invalid or out of order.");
            total = checked(total + range.End - range.Start + 1);
            lastEnd = range.End;
        }
        return total;
    }

    internal IEnumerable<PageRange> EnumerateRanges()
    {
        foreach (var range in Descriptor.PageRanges)
        {
            for (var start = range.Start; start <= range.End;)
            {
                var end = Math.Min(range.End, start + RangeFrameBytes - 1);
                yield return new PageRange(start, end);
                start = end + 1;
            }
        }
    }
}
