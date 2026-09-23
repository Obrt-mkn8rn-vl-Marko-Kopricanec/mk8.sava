namespace Mk8.Sava.Storage;

internal sealed record ChunkPackRecord(
    string PackId,
    string Domain,
    DateTimeOffset CreatedAt,
    bool Sealed);
