using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Storage;

public sealed class StorageMaintenanceService(
    BlobService blobs,
    StorageTelemetry telemetry,
    IOptions<SavaOptions> configuredOptions,
    ILogger<StorageMaintenanceService> logger) : BackgroundService
{
    private readonly TimeSpan _interval = configuredOptions.Value.MaintenanceScanInterval;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = await blobs.RunMaintenanceAsync(stoppingToken);
                if (result is not { CompletedCopies: 0, CompletedRehydrations: 0, ExpiredBlobs: 0,
                        PurgedSoftDeletedBlobs: 0, PurgedSoftDeletedContainers: 0,
                        ExpiredUncommittedBlocks: 0, ReclaimedChunks: 0, ReclaimedStagingFiles: 0 })
                {
                    logger.LogInformation(
                        "Storage maintenance completed: {CompletedCopies} copies, {CompletedRehydrations} rehydrations, " +
                        "{ExpiredBlobs} expired blobs, {PurgedBlobs} purged blobs, {PurgedContainers} purged containers, " +
                        "{ExpiredBlocks} expired blocks, {ReclaimedChunks} reclaimed chunks, and " +
                        "{ReclaimedStagingFiles} reclaimed staging files.",
                        result.CompletedCopies,
                        result.CompletedRehydrations,
                        result.ExpiredBlobs,
                        result.PurgedSoftDeletedBlobs,
                        result.PurgedSoftDeletedContainers,
                        result.ExpiredUncommittedBlocks,
                        result.ReclaimedChunks,
                        result.ReclaimedStagingFiles);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                telemetry.RecordMaintenanceFailure();
                logger.LogError(exception, "Storage maintenance pass failed.");
            }

            await Task.Delay(_interval, stoppingToken);
        }
    }
}
