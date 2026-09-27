using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

internal sealed class UrlPageCopySource(
    UrlTransferClient transfers, HttpRequest request, Uri sourceUri, UrlSource properties) : IPageCopySource
{
    public UrlSource Properties { get; } = properties;

    public Task<PageRangeDiff> ReadChangesAsync(
        string? previousSnapshot, long previousLength, CancellationToken cancellationToken) =>
        previousSnapshot is null
            ? Task.FromResult(new PageRangeDiff(Properties.PageRanges, []))
            : transfers.ReadPageDiffAsync(request, sourceUri, Properties.ETag!,
                Properties.ContentLength!.Value, previousLength, previousSnapshot, cancellationToken);

    public Task ReadRangeAsync(
        PageRange range, Func<Stream, CancellationToken, Task> consume, CancellationToken cancellationToken) =>
        transfers.ReadPinnedPageRangeAsync(
            request, sourceUri, Properties.ETag!, Properties.ContentLength!.Value, range, consume, cancellationToken);
}
