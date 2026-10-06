using Mk8.Sava.Storage;

namespace Mk8.Sava.Transport;

internal static class RpcListingProjection
{
    internal static object? Apply(RpcMethod method, object? result) => (method.Method.Name, result) switch
    {
        ("ListBlobsAsync", IReadOnlyList<BlobRecord> blobs) => blobs.Select(Project).ToArray(),
        ("ListBlobsPageAsync", BlobListPage page) => new BlobListPage(
            page.Items.Select(item => item.Blob is null ? item : item with { Blob = Project(item.Blob) }).ToArray(), page.HasMore),
        ("FindBlobsByTagsPageAsync", TaggedBlobPage page) => new TaggedBlobPage(
            page.Items.Select(Project).ToArray(), page.HasMore),
        (_, BlobEncryption encryption) => encryption with { CustomerProvidedKey = null },
        _ => result,
    };

    private static BlobRecord Project(BlobRecord record) => record with
    {
        CommittedBlocks = [],
        PendingCopyCommittedBlocks = null,
        PageRanges = [],
        PageMutationRanges = null,
        PendingCopyPageRanges = null,
        PendingCopyContent = null,
        PendingCopyAppendBlockCount = null,
        PendingCopyIsSealed = null,
    };
}
