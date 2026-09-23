namespace Mk8.Sava.Storage;

internal sealed record ChunkPackPage(IReadOnlyList<ChunkPackRecord> Items, bool HasMore);
