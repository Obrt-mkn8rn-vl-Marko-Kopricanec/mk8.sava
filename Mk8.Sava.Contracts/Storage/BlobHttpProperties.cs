namespace Mk8.Sava.Storage;

public sealed record BlobHttpProperties
{
    public string ContentType { get; init; } = "application/octet-stream";
    public string? ContentEncoding { get; init; }
    public string? ContentLanguage { get; init; }
    public string? CacheControl { get; init; }
    public string? ContentDisposition { get; init; }
    public string? ContentMd5 { get; init; }
}
