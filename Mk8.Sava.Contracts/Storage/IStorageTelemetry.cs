namespace Mk8.Sava.Storage;

public interface IStorageTelemetry
{
    StorageUsageSnapshot Usage { get; }

    StorageIntegritySnapshot Integrity { get; }

    void RecordRequest(int statusCode, long elapsedStopwatchTicks);

    void RecordMaintenance(StorageMaintenanceResult result, StorageUsageSnapshot usage);

    void RecordMaintenanceFailure();

    void RecordIntegrity(StorageIntegritySnapshot integrity);

    string RenderPrometheus();
}
