namespace Mk8.Sava.Storage;

internal sealed record ChunkIdPage(IReadOnlyList<string> Items, bool HasMore);
