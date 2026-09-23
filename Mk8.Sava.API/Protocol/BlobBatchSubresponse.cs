namespace Mk8.Sava.Protocol;

internal sealed record BlobBatchSubresponse(
    int StatusCode,
    IReadOnlyDictionary<string, string> Headers,
    byte[] Body,
    string? ContentId);
