using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

// Public Azure pages still contain up to 5,000 entries. Private control frames
// fetch smaller batches so metadata volume cannot grow a single RPC unchecked.
internal static class GatewayListing
{
    internal const int MaximumBatchSize = 128;

    public static async Task<ContainerListPage> ListContainersAsync(IBlobApplication application,
        string account, bool includeDeleted, bool includeSystem, string prefix, string marker,
        int maximum, CancellationToken cancellationToken)
    {
        var items = new List<ContainerRecord>(maximum);
        var visited = new HashSet<string>(StringComparer.Ordinal) { marker };
        var hasMore = false;
        while (items.Count < maximum)
        {
            var batch = Math.Min(MaximumBatchSize, maximum - items.Count);
            var page = await application.ListContainersPageAsync(account, includeDeleted, includeSystem,
                prefix, marker, batch, cancellationToken).ConfigureAwait(false);
            ValidateProgress(page.Items.Count, page.HasMore, batch);
            items.AddRange(page.Items);
            hasMore = page.HasMore;
            if (!hasMore)
                break;
            var next = page.Items[^1].Name;
            if (!visited.Add(next))
                throw new InvalidDataException("Application container listing did not advance.");
            marker = next;
        }
        return new ContainerListPage(items, hasMore);
    }

    public static async Task<BlobListPage> ListBlobsAsync(IBlobApplication application,
        string account, string container, BlobListShowOnly showOnly, bool includeVersions,
        bool includeSnapshots, bool includeDeleted, bool includeUncommitted, string prefix,
        string startFrom, string endBefore, string delimiter, BlobListingMarker marker,
        int maximum, CancellationToken cancellationToken)
    {
        var items = new List<BlobListEntry>(maximum);
        var visited = new HashSet<BlobListingMarker> { marker };
        var hasMore = false;
        while (items.Count < maximum)
        {
            var batch = Math.Min(MaximumBatchSize, maximum - items.Count);
            var page = await application.ListBlobsPageAsync(account, container, showOnly, includeVersions,
                includeSnapshots, includeDeleted, includeUncommitted, prefix, startFrom, endBefore, delimiter,
                marker, batch, cancellationToken).ConfigureAwait(false);
            ValidateProgress(page.Items.Count, page.HasMore, batch);
            items.AddRange(page.Items);
            hasMore = page.HasMore;
            if (!hasMore)
                break;
            var next = new BlobListingMarker(page.Items[^1].Cursor, LegacyOffset: 0);
            if (!visited.Add(next))
                throw new InvalidDataException("Application blob listing did not advance.");
            marker = next;
        }
        return new BlobListPage(items, hasMore);
    }

    public static async Task<TaggedBlobPage> FindByTagsAsync(IBlobApplication application,
        string account, BlobTagFilter filter, BlobTagCursor? cursor, int maximum,
        CancellationToken cancellationToken)
    {
        var items = new List<BlobRecord>(maximum);
        var visited = new HashSet<BlobTagCursor>();
        if (cursor is not null)
            visited.Add(cursor);
        var hasMore = false;
        while (items.Count < maximum)
        {
            var batch = Math.Min(MaximumBatchSize, maximum - items.Count);
            var page = await application.FindBlobsByTagsPageAsync(account, filter, cursor,
                batch, cancellationToken).ConfigureAwait(false);
            ValidateProgress(page.Items.Count, page.HasMore, batch);
            items.AddRange(page.Items);
            hasMore = page.HasMore;
            if (!hasMore)
                break;
            var last = page.Items[^1];
            var next = new BlobTagCursor(last.Container, last.Name, last.GenerationId);
            if (!visited.Add(next))
                throw new InvalidDataException("Application tag listing did not advance.");
            cursor = next;
        }
        return new TaggedBlobPage(items, hasMore);
    }

    private static void ValidateProgress(int count, bool hasMore, int maximum)
    {
        if (hasMore && count == 0)
            throw new InvalidDataException("Application listing returned an empty continuation page.");
        if (count > maximum)
            throw new InvalidDataException("Application listing exceeded its private batch size.");
    }
}
