namespace Mk8.Sava.Storage;

public interface IStorageAnalyticsSink
{
    Task RecordAsync(StorageAnalyticsRequest request, CancellationToken cancellationToken);
}
