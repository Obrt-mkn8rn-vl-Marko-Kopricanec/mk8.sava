namespace Mk8.Sava.Storage;

public sealed record BlobListPage(IReadOnlyList<BlobListEntry> Items, bool HasMore);
