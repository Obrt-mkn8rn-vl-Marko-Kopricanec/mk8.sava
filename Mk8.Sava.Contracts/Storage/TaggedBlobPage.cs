namespace Mk8.Sava.Storage;

public sealed record TaggedBlobPage(IReadOnlyList<BlobRecord> Items, bool HasMore);
