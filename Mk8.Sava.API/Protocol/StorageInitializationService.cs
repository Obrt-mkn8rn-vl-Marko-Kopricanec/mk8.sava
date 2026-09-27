using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

internal sealed class StorageInitializationService(
    MetadataStore metadata,
    StorageDataKeyContinuity dataKeys,
    BlobService blobs,
    StoragePaths paths,
    ILogger<StorageInitializationService> logger) : IHostedLifecycleService
{
    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await metadata.InitializeAsync(cancellationToken).ConfigureAwait(false);
        await dataKeys.EnsureAsync(cancellationToken).ConfigureAwait(false);
        await blobs.ApplyConfiguredAccountCapabilitiesAsync(cancellationToken).ConfigureAwait(false);
        var prunedChunkDirectories = paths.PruneLegacyEmptyChunkDirectories();
        if (prunedChunkDirectories > 0)
            StorageLogMessages.LegacyChunkDirectoriesPruned(logger, prunedChunkDirectories);
    }

    // All StartingAsync callbacks finish before any hosted service starts,
    // including when the host starts services concurrently.
    public Task StartingAsync(CancellationToken cancellationToken) => InitializeAsync(cancellationToken);

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
