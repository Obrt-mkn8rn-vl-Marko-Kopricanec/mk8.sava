namespace Mk8.Sava.Storage;

internal sealed record BlobListCursor(
    string Name,
    bool NameComplete,
    bool IsPrefix,
    int Rank,
    string OrderedId,
    string GenerationId);
