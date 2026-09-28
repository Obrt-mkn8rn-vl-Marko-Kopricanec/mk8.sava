using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class FatalPipelineTests
{
    [Fact]
    public async Task BatchSubrequestFatalFailureEscapesTheActualHttpPipeline()
    {
#pragma warning disable CA2201 // Inject a runtime-reserved failure to verify that Blob Batch cannot normalize it.
        var failure = new InvalidOperationException("injected batch fatal", new OutOfMemoryException());
#pragma warning restore CA2201
        var armed = 0;
        var factory = new SavaWebApplicationFactory();
        await using var factoryDisposal = factory.ConfigureAwait(false);
        var application = factory.WithWebHostBuilder(builder => builder.ConfigureServices(services =>
        {
            services.RemoveAll<StorageAuthenticator>();
            services.AddTransient(provider =>
            {
                if (Volatile.Read(ref armed) != 0)
                    throw failure;
                return ActivatorUtilities.CreateInstance<StorageAuthenticator>(provider);
            });
        }));
        await using var applicationDisposal = application.ConfigureAwait(false);
        using var transport = application.CreateClient();
        var service = CreateServiceClient(transport);
        var container = service.GetBlobContainerClient($"batch-fatal-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("source.bin");
        await blob.UploadAsync(BinaryData.FromString("must survive"));

        Volatile.Write(ref armed, 1);
        var batchClient = service.GetBlobBatchClient();
        using var batch = batchClient.CreateBatch();
        batch.DeleteBlob(container.Name, blob.Name);

        // TestServer's developer exception page renders an unhandled failure as plain text;
        // a Batch subresponse or AzureExceptionMiddleware would instead return Azure XML.
        var escaped = await Assert.ThrowsAsync<RequestFailedException>(() =>
            batchClient.SubmitBatchAsync(batch, throwOnAnyFailure: false));

        Assert.Equal(500, escaped.Status);
        Assert.Contains("injected batch fatal", escaped.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("<Error>", escaped.Message, StringComparison.Ordinal);
        Volatile.Write(ref armed, 0);
        Assert.True((await blob.ExistsAsync()).Value);
    }

    [Fact]
    public async Task AnalyticsFatalFailureEscapesAfterStoragePublication()
    {
#pragma warning disable CA2201 // Inject a runtime-reserved failure into the best-effort analytics boundary.
        var failure = new AccessViolationException("injected analytics fatal");
#pragma warning restore CA2201
        var sink = new OneShotFatalAnalyticsSink(failure);
        var application = new SavaWebApplicationFactory(
            Path.Combine(Path.GetTempPath(), $"mk8-sava-analytics-fatal-{Guid.NewGuid():N}"),
            new NullStorageFaultInjector(),
            sink,
            configurationOverrides: null,
            deleteDataPath: true);
        await using var applicationDisposal = application.ConfigureAwait(false);
        await application.InitializeAsync();
        using var transport = application.CreateClient();
        var service = CreateServiceClient(transport);
        var container = service.GetBlobContainerClient($"analytics-fatal-{Guid.NewGuid():N}");
        sink.Arm();

        // An unhandled fatal is rendered by TestServer, not swallowed by analytics
        // or converted to an Azure XML error by the storage exception middleware.
        var escaped = await Assert.ThrowsAsync<RequestFailedException>(() => container.CreateAsync());

        Assert.Equal(500, escaped.Status);
        Assert.Contains("injected analytics fatal", escaped.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("<Error>", escaped.Message, StringComparison.Ordinal);
        Assert.True((await container.ExistsAsync()).Value);
    }

    private static BlobServiceClient CreateServiceClient(HttpClient transport)
    {
        var options = new BlobClientOptions
        {
            Transport = new HttpClientTransport(transport),
            Retry = { MaxRetries = 0 }
        };
        return new BlobServiceClient(
            new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost"),
            new StorageSharedKeyCredential(
                SavaWebApplicationFactory.AccountName,
                SavaWebApplicationFactory.AccountKey),
            options);
    }

    private sealed class OneShotFatalAnalyticsSink(Exception failure) : IStorageAnalyticsSink
    {
        private int _armed;

        public void Arm() => Interlocked.Exchange(ref _armed, 1);

        public Task RecordAsync(StorageAnalyticsRequest request, CancellationToken cancellationToken) =>
            Interlocked.Exchange(ref _armed, 0) != 0
                ? Task.FromException(failure)
                : Task.CompletedTask;
    }
}
