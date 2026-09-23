namespace Mk8.Sava.Storage;

public sealed record StagedBlockRecord
{
    public required string Account { get; init; }
    public required string Container { get; init; }
    public required string BlobName { get; init; }
    public required string BlockId { get; init; }
    public required ContentManifest Content { get; init; }
    public required DateTimeOffset CreatedAt { get; init; }
}
