using Mk8.Sava.Storage;

namespace Mk8.Sava.Application;

internal sealed class ApplicationInitializationService(
    MetadataStore metadata, StorageDataKeyContinuity dataKeys, BlobService blobs,
    StoragePaths paths, ILogger<ApplicationInitializationService> logger) : IHostedLifecycleService
{
    private static readonly Action<ILogger, int, Exception?> LegacyDirectoriesPruned =
        LoggerMessage.Define<int>(LogLevel.Information, new EventId(3200, nameof(LegacyDirectoriesPruned)),
            "Pruned {Count} legacy empty chunk directories.");
    private int _initialized;
    public bool Initialized => Volatile.Read(ref _initialized) != 0;

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await metadata.InitializeAsync(cancellationToken).ConfigureAwait(false);
        await dataKeys.EnsureAsync(cancellationToken).ConfigureAwait(false);
        await blobs.ApplyConfiguredAccountCapabilitiesAsync(cancellationToken).ConfigureAwait(false);
        var pruned = paths.PruneLegacyEmptyChunkDirectories();
        if (pruned > 0)
            LegacyDirectoriesPruned(logger, pruned, null);
        Volatile.Write(ref _initialized, 1);
    }

    public Task StartingAsync(CancellationToken cancellationToken) => InitializeAsync(cancellationToken);
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
