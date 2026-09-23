namespace Mk8.Sava.Storage;

internal sealed record BlobListPage(IReadOnlyList<BlobListEntry> Items, bool HasMore);
