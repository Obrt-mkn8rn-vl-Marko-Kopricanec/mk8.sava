namespace Mk8.Sava.Storage;

public sealed record StorageAnalyticsRetentionPolicy
{
    public bool Enabled { get; init; }
    public int? Days { get; init; }
}
