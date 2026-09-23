namespace Mk8.Sava.Storage;

internal sealed record TaggedBlobPage(IReadOnlyList<BlobRecord> Items, bool HasMore);
