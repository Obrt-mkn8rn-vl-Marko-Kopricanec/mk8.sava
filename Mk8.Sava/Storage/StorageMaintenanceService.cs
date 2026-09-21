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
                await blobs.CompletePendingCopiesAsync(stoppingToken);
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
