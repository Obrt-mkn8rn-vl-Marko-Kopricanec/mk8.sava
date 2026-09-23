namespace Mk8.Sava.Storage;

public sealed record CopyState
{
    public required string Id { get; init; }
    public required string Source { get; init; }
    public required string Status { get; init; }
    public required long BytesCopied { get; init; }
    public required long TotalBytes { get; init; }
    public DateTimeOffset? CompletedAt { get; init; }
    public DateTimeOffset? ReadyAt { get; init; }
    public DateTimeOffset? ExpiresAt { get; init; }
    public string? Description { get; init; }
    public bool IsIncremental { get; init; }
    public string? SourceSnapshot { get; init; }
}
