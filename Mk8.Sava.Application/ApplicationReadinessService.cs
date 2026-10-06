using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Application;

internal sealed class ApplicationReadinessService(
    MetadataStore metadata, IStorageTelemetry telemetry, ChunkStore chunks,
    ApplicationInitializationService initialization, ApplicationRpcAdmission admission,
    ILogger<ApplicationReadinessService> logger) : IApplicationReadiness
{
    private static readonly Action<ILogger, string, Exception?> ProbeFailed = LoggerMessage.Define<string>(
        LogLevel.Warning, new EventId(3300, nameof(ProbeFailed)),
        "Application readiness probe failed with {ExceptionType}.");

    public async Task<ApplicationReadiness> GetAsync(CancellationToken cancellationToken)
    {
        try
        {
            var metadataReady = initialization.Initialized &&
                await metadata.IsReadyAsync(cancellationToken).ConfigureAwait(false);
            var integrity = telemetry.Integrity;
            return new ApplicationReadiness(metadataReady && integrity.Healthy, metadataReady, integrity);
        }
#pragma warning disable CA1031 // Readiness is an availability signal; ordinary probe failures must report unavailable, not HTTP 500.
        catch (Exception exception) when (!cancellationToken.IsCancellationRequested &&
                                          !CatastrophicExceptionPolicy.Contains(exception))
        {
#pragma warning restore CA1031
            ProbeFailed(logger, exception.GetType().Name, null);
            return new ApplicationReadiness(false, false, StorageIntegritySnapshot.Pending);
        }
    }

    public Task<string> RenderStorageMetricsAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var metrics = telemetry is StorageTelemetry concrete
            ? concrete.RenderStoragePrometheus()
            : telemetry.RenderPrometheus();
        return Task.FromResult(metrics + chunks.Admission.RenderPrometheus() + admission.RenderMetrics());
    }
}
