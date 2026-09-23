using Microsoft.Extensions.Primitives;

namespace Mk8.Sava.Protocol;

internal sealed record BlobBatchSubrequest(
    BlobBatchOperationKind Kind,
    string Method,
    string RawPath,
    QueryString QueryString,
    IReadOnlyDictionary<string, StringValues> Headers,
    string? ContentId);
