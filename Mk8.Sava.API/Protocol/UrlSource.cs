using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

internal sealed record UrlSource(
    Stream Content,
    long? ContentLength,
    BlobHttpProperties Http,
    Dictionary<string, string> Metadata,
    Dictionary<string, string> Tags,
    BlobKind? Kind,
    string? AccessTier,
    string? ETag,
    long SequenceNumber,
    bool IsSealed,
    int AppendBlockCount,
    IReadOnlyList<CopySourceBlock> CommittedBlocks,
    IReadOnlyList<PageRange> PageRanges,
    DateTimeOffset? CreatedAt);
