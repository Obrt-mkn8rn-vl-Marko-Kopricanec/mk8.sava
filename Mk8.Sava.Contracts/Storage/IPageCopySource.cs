namespace Mk8.Sava.Storage;

public interface IPageCopySource
{
    Task<PageRangeDiff> ReadChangesAsync(
        string? previousSnapshot, long previousLength, CancellationToken cancellationToken);

    Task ReadRangeAsync(
        PageRange range, Func<Stream, CancellationToken, Task> consume, CancellationToken cancellationToken);
}
