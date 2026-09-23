namespace Mk8.Sava.Storage;

public sealed record ChunkReference(string Id, long Offset, long Length);
