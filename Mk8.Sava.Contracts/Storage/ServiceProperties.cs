namespace Mk8.Sava.Storage;

public sealed record ServiceProperties
{
    public StorageAnalyticsLogging Logging { get; init; } = new();
    public StorageAnalyticsMetrics HourMetrics { get; init; } = new();
    public StorageAnalyticsMetrics MinuteMetrics { get; init; } = new();
    public bool VersioningEnabled { get; init; }
    public bool ContainerSoftDeleteEnabled { get; init; }
    public int ContainerSoftDeleteRetentionDays { get; init; } = 7;
    public bool BlobSoftDeleteEnabled { get; init; }
    public int BlobSoftDeleteRetentionDays { get; init; } = 7;
    public bool BlobPermanentDeleteEnabled { get; init; }
    public string? DefaultServiceVersion { get; init; }
    public List<CorsRule> Cors { get; init; } = [];
    public StaticWebsiteProperties StaticWebsite { get; init; } = new();
}
