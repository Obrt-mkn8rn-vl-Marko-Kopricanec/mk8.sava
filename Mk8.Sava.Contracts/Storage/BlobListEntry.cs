namespace Mk8.Sava.Storage;

internal sealed record BlobListEntry(BlobRecord? Blob, string? Prefix, string? UncommittedBlobName = null)
{
    public bool IsUncommitted => UncommittedBlobName is not null;

    public string Name => Prefix ?? Blob?.Name ?? UncommittedBlobName!;

    public BlobListCursor Cursor
    {
        get
        {
            if (Prefix is not null)
                return new BlobListCursor(Name, false, true, -1, string.Empty, string.Empty);
            if (IsUncommitted)
                return new BlobListCursor(Name, false, false, -1, string.Empty, string.Empty);
            var blob = Blob ?? throw new InvalidOperationException("A blob list entry has no value.");
            return new BlobListCursor(
                blob.Name,
                false,
                false,
                blob switch
                {
                    { IsCurrent: true } => 0,
                    { VersionId: not null } => 1,
                    { Snapshot: null } => 2,
                    _ => 3
                },
                blob.VersionId ?? blob.Snapshot ?? string.Empty,
                blob.GenerationId);
        }
    }
}
