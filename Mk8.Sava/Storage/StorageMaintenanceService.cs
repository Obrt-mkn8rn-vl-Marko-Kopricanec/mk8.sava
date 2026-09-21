using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Storage;

public sealed class StorageMaintenanceService(
    BlobService blobs,
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
                        ExpiredUncommittedBlocks: 0, ReclaimedChunks: 0 })
                {
                    logger.LogInformation(
                        "Storage maintenance completed: {CompletedCopies} copies, {CompletedRehydrations} rehydrations, " +
                        "{ExpiredBlobs} expired blobs, {PurgedBlobs} purged blobs, {PurgedContainers} purged containers, " +
                        "{ExpiredBlocks} expired blocks, and {ReclaimedChunks} reclaimed chunks.",
                        result.CompletedCopies,
                        result.CompletedRehydrations,
                        result.ExpiredBlobs,
                        result.PurgedSoftDeletedBlobs,
                        result.PurgedSoftDeletedContainers,
                        result.ExpiredUncommittedBlocks,
                        result.ReclaimedChunks);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Storage maintenance pass failed.");
            }

            await Task.Delay(_interval, stoppingToken);
        }
    }
}
