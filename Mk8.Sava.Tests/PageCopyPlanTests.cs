using System.Diagnostics;
using Mk8.Sava.Storage;
using Mk8.Sava.Transport;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class PageCopyPlanTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(-512)]
    [InlineData(1)]
    [InlineData(8L * 1024 * 1024 * 1024 * 1024 + 512)]
    public void InvalidSourceLengthsCannotBecomeAPlan(long sourceLength) =>
        Assert.Throws<InvalidDataException>(() => PageCopyPlan.Create(new PageRangeDiff([], []), sourceLength));

    [Theory]
    [InlineData(-512, -1)]
    [InlineData(512, 511)]
    [InlineData(1, 512)]
    [InlineData(0, 512)]
    [InlineData(0, 1023)]
    [InlineData(0, long.MaxValue)]
    public void InvalidOccupiedRangesCannotBecomeAPlan(long start, long end) =>
        Assert.Throws<InvalidDataException>(() =>
            PageCopyPlan.Create(new PageRangeDiff([new PageRange(start, end)], []), 512));

    [Theory]
    [InlineData(0)]
    [InlineData(256)]
    [InlineData(512)]
    public void OverlappingUnalignedAndUnorderedRangesCannotBecomeAPlan(long secondStart) =>
        Assert.Throws<InvalidDataException>(() =>
            PageCopyPlan.Create(new PageRangeDiff([new(512, 1023), new(secondStart, secondStart + 511)], []), 2048));

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void MissingRangeCollectionsCannotBecomeAPlan(bool pages)
    {
        var changes = pages ? new PageRangeDiff(null!, []) : new PageRangeDiff([], null!);

        Assert.Throws<InvalidDataException>(() => PageCopyPlan.Create(changes, 512));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void NullEntriesCannotBecomeAPlan(bool pages)
    {
        var changes = pages ? new PageRangeDiff([null!], []) : new PageRangeDiff([], [null!]);

        Assert.Throws<InvalidDataException>(() => PageCopyPlan.Create(changes, 512));
    }

    [Fact]
    public void ClearRangesMayExtendBeyondShrunkSourceButNotBeyondPageBlobCapacity()
    {
        var clears = new PageRangeDiff([], [new PageRange(1024, 1535)]);

        var plan = PageCopyPlan.Create(clears, 512);

        Assert.Equal(0, plan.DataLength);
        Assert.Empty(plan.EnumerateRanges());
        Assert.Equal(new PageRange(1024, 1535), Assert.Single(plan.Descriptor.ClearRanges));
        Assert.Throws<InvalidDataException>(() => PageCopyPlan.Create(new PageRangeDiff([], [
            new PageRange(8L * 1024 * 1024 * 1024 * 1024, 8L * 1024 * 1024 * 1024 * 1024 + 511)]), 512));
    }

    [Fact]
    public void MaximumSourceIsValidAndSplitsWithoutAllocatingItsContent()
    {
        const long maximum = 8L * 1024 * 1024 * 1024 * 1024;
        var plan = PageCopyPlan.Create(new PageRangeDiff([new PageRange(0, maximum - 1)], []), maximum);

        Assert.Equal(maximum, plan.DataLength);
        Assert.Equal([new PageRange(0, 4194303), new PageRange(4194304, 8388607)], plan.EnumerateRanges().Take(2));
    }

    [Fact]
    public void RangeSplittingPreservesEveryByteAndSparseGap()
    {
        var plan = PageCopyPlan.Create(new PageRangeDiff([new(0, 8389119), new(8390144, 8390655)], []), 8391168);

        Assert.Equal(8389632, plan.DataLength);
        Assert.Equal([new PageRange(0, 4194303), new PageRange(4194304, 8388607),
            new PageRange(8388608, 8389119), new PageRange(8390144, 8390655)], plan.EnumerateRanges());
    }

    [Fact]
    public void SnapshotDoesNotExposeAliasesOrMutableCollections()
    {
        var pages = new PageRange[] { new(0, 511) };
        var clears = new List<PageRange> { new(1024, 1535) };
        var plan = PageCopyPlan.Create(new PageRangeDiff(pages, clears), 512);
        pages[0] = new PageRange(-512, -1);
        clears.Clear();

        Assert.Equal(new PageRange(0, 511), Assert.Single(plan.EnumerateRanges()));
        Assert.Equal(new PageRange(1024, 1535), Assert.Single(plan.Descriptor.ClearRanges));
        Assert.Throws<NotSupportedException>(() =>
            ((IList<PageRange>)plan.Descriptor.PageRanges)[0] = new PageRange(0, 1023));
    }

    [Fact]
    public void SnapshotAllocationIsLinearInDescriptorsNotBlobLength()
    {
        const int count = 5000;
        var pages = Enumerable.Range(0, count).Select(index => new PageRange(index * 1024L, index * 1024L + 511)).ToArray();
        var clears = Enumerable.Range(0, count).Select(index => new PageRange(index * 1024L + 512, index * 1024L + 1023)).ToArray();
        var changes = new PageRangeDiff(pages, clears);
        _ = PageCopyPlan.Create(changes, count * 1024L);
        var watch = Stopwatch.StartNew();
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var iteration = 0; iteration < 100; iteration++)
            _ = PageCopyPlan.Create(changes, count * 1024L);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        watch.Stop();

        output.WriteLine("100 plans / 10,000 descriptors each: {0} bytes allocated; {1:F3} ms", allocated, watch.Elapsed.TotalMilliseconds);
        Assert.InRange(allocated, 100L * count * 2 * IntPtr.Size, 100L * 128 * 1024);
    }
}
