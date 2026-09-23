namespace Mk8.Sava.Storage;

public sealed record PageRangeDiff(
    IReadOnlyList<PageRange> PageRanges,
    IReadOnlyList<PageRange> ClearRanges);
