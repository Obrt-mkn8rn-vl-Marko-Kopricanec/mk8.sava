using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed partial class AzureSdkCompatibilityTests
{
    [Fact]
    public async Task UnsupportedOperatorPathsReturnNotFoundWithoutAStorageContext()
    {
        using var client = factory.CreateClient();

        foreach (var path in new[] { "/health", "/HEALTH", "/health/unknown", "/health/live/unknown", "/metrics/unknown" })
        {
            using var response = await client.GetAsync(new Uri(path, UriKind.RelativeOrAbsolute))
                .ConfigureAwait(true);
            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        }

        using var live = await client.GetAsync(new Uri("/health/live", UriKind.RelativeOrAbsolute))
            .ConfigureAwait(true);
        Assert.Equal(HttpStatusCode.OK, live.StatusCode);
        using var ready = await client.GetAsync(new Uri("/health/ready", UriKind.RelativeOrAbsolute))
            .ConfigureAwait(true);
        Assert.Equal(HttpStatusCode.OK, ready.StatusCode);
        using var metrics = await client.GetAsync(new Uri("/metrics", UriKind.RelativeOrAbsolute))
            .ConfigureAwait(true);
        Assert.Equal(HttpStatusCode.OK, metrics.StatusCode);
    }

    [Fact]
    public async Task HealthFailuresAppearInHttpServerErrorMetric()
    {
        var application = new SavaWebApplicationFactory(
            Path.Combine(Path.GetTempPath(), $"mk8-sava-health-metrics-{Guid.NewGuid():N}"),
            new NullStorageFaultInjector(),
            analyticsSink: null,
            configurationOverrides: null,
            deleteDataPath: true,
            disableMaintenance: true);
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(true);
        var telemetry = application.Services.GetRequiredService<StorageTelemetry>();
        Assert.Contains("mk8_sava_http_requests_total 0", telemetry.RenderPrometheus(), StringComparison.Ordinal);
        Assert.Contains("mk8_sava_http_server_errors_total 0", telemetry.RenderPrometheus(), StringComparison.Ordinal);

        using var client = application.CreateClient();
        using var unknown = await client.GetAsync(new Uri("/health", UriKind.RelativeOrAbsolute))
            .ConfigureAwait(true);
        Assert.Equal(HttpStatusCode.NotFound, unknown.StatusCode);
        Assert.Contains("mk8_sava_http_requests_total 1", telemetry.RenderPrometheus(), StringComparison.Ordinal);
        Assert.Contains("mk8_sava_http_server_errors_total 0", telemetry.RenderPrometheus(), StringComparison.Ordinal);

        telemetry.RecordIntegrity(new StorageIntegritySnapshot(
            ReachableChunks: 1,
            CheckedChunks: 1,
            VerifiedChunks: 0,
            CustomerKeyChunks: 0,
            MissingChunks: 1,
            CorruptChunks: 0,
            Complete: true,
            CheckedAt: DateTimeOffset.UtcNow));
        using var unavailable = await client.GetAsync(new Uri("/health/ready", UriKind.RelativeOrAbsolute))
            .ConfigureAwait(true);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, unavailable.StatusCode);
        Assert.Contains("mk8_sava_http_requests_total 2", telemetry.RenderPrometheus(), StringComparison.Ordinal);
        Assert.Contains("mk8_sava_http_server_errors_total 1", telemetry.RenderPrometheus(), StringComparison.Ordinal);

        using var metrics = await client.GetAsync(new Uri("/metrics", UriKind.RelativeOrAbsolute))
            .ConfigureAwait(true);
        Assert.Equal(HttpStatusCode.OK, metrics.StatusCode);
        Assert.Contains("mk8_sava_http_requests_total 3", telemetry.RenderPrometheus(), StringComparison.Ordinal);
    }
}
