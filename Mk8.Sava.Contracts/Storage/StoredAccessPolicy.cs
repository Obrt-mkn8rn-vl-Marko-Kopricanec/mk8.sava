namespace Mk8.Sava.Storage;

public sealed record StoredAccessPolicy
{
    public DateTimeOffset? StartsAt { get; init; }
    public DateTimeOffset? ExpiresAt { get; init; }
    public required string Permission { get; init; }
}
