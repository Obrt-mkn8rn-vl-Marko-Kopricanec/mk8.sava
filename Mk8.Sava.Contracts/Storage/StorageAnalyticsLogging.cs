namespace Mk8.Sava.Storage;

public sealed record StorageAnalyticsLogging
{
    public string Version { get; init; } = "1.0";
    public bool Delete { get; init; }
    public bool Read { get; init; }
    public bool Write { get; init; }
    public StorageAnalyticsRetentionPolicy RetentionPolicy { get; init; } = new();
}
