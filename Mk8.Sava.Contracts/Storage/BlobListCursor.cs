namespace Mk8.Sava.Storage;

public sealed record BlobListCursor(
    string Name,
    bool NameComplete,
    bool IsPrefix,
    int Rank,
    string OrderedId,
    string GenerationId);
