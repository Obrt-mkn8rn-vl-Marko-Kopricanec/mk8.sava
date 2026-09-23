namespace Mk8.Sava.Storage;

internal sealed record BlobRecordMutation(
    string GenerationId,
    string ExpectedRevision,
    BlobRecord? Replacement);
