namespace Mk8.Sava.Storage;

internal sealed record PhysicalChunkPage(IReadOnlyList<string> Items, bool HasMore);
