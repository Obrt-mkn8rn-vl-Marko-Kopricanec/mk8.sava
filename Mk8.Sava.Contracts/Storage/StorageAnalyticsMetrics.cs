namespace Mk8.Sava.Storage;

public sealed record StorageAnalyticsMetrics
{
    public string Version { get; init; } = "1.0";
    public bool Enabled { get; init; }
    public bool? IncludeApis { get; init; }
    public StorageAnalyticsRetentionPolicy RetentionPolicy { get; init; } = new();
}
