using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Storage;

public sealed class StorageMaintenanceService(
    BlobService blobs,
    IStorageTelemetry telemetry,
    IOptions<SavaOptions> configuredOptions,
    ILogger<StorageMaintenanceService> logger) : BackgroundService
{
    private static readonly Action<ILogger, StorageMaintenanceResult, Exception?> MaintenanceCompleted =
        LoggerMessage.Define<StorageMaintenanceResult>(
            LogLevel.Information,
            new EventId(3100, nameof(MaintenanceCompleted)),
            "Storage maintenance completed: {MaintenanceResult}");

    private static readonly Action<ILogger, Exception?> MaintenanceFailed =
        LoggerMessage.Define(
            LogLevel.Error,
            new EventId(3101, nameof(MaintenanceFailed)),
            "Storage maintenance pass failed.");

    private readonly TimeSpan _interval = configuredOptions.Value.MaintenanceScanInterval;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = await blobs.RunMaintenanceAsync(stoppingToken).ConfigureAwait(false);
                if (result is not
                    {
                        CompletedCopies: 0, CompletedObjectReplications: 0, FailedObjectReplications: 0,
                        RemovedObjectReplicas: 0, CompletedRehydrations: 0, CompletedSmartTierTransitions: 0,
                        ExpiredBlobs: 0,
                        PurgedSoftDeletedBlobs: 0, PurgedSoftDeletedContainers: 0,
                        ExpiredUncommittedBlocks: 0, ReclaimedChunks: 0, ReclaimedStagingFiles: 0,
                        RecompressedChunks: 0, CompactedChunkPacks: 0
                    })
                {
                    MaintenanceCompleted(logger, result, null);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            // A maintenance pass must be isolated so a later pass can retry safely.
#pragma warning disable CA1031
            catch (Exception exception)
            {
#pragma warning restore CA1031
                telemetry.RecordMaintenanceFailure();
                MaintenanceFailed(logger, exception);
            }

            await Task.Delay(_interval, stoppingToken).ConfigureAwait(false);
        }
    }

}
