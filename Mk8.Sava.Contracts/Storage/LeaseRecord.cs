namespace Mk8.Sava.Storage;

public sealed record LeaseRecord
{
    public static LeaseRecord Available { get; } = new();

    public string? Id { get; init; }
    public LeaseState State { get; init; } = LeaseState.Available;
    public int? DurationSeconds { get; init; }
    public DateTimeOffset? AcquiredAt { get; init; }
    public DateTimeOffset? ExpiresAt { get; init; }
    public DateTimeOffset? BreakEndsAt { get; init; }
}
