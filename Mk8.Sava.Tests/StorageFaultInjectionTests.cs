using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using System.Security.Cryptography;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageFaultInjectionTests
{
    private static readonly IReadOnlyDictionary<string, string?> FaultTestConfiguration =
        new Dictionary<string, string?>
        {
            ["Sava:EnableSmallChunkPacking"] = "false",
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        };

    [Fact]
    public async Task ChunkPublicationFailureLeavesNoBlobOrDurableExtent()
    {
        var faultInjector = new ArmableStorageFaultInjector();
        var application = CreateApplication(faultInjector);
        try
        {
            await application.InitializeAsync();
            var container = CreateClient(application).GetBlobContainerClient($"chunk-fault-{Guid.NewGuid():N}");
            await container.CreateAsync();
            var blob = container.GetBlobClient("payload.bin");
            var content = RandomNumberGenerator.GetBytes(64 * 1024);

            faultInjector.Arm(StorageFaultPoint.BeforeChunkPublication);
            var failure = await Assert.ThrowsAsync<RequestFailedException>(() =>
                blob.UploadAsync(BinaryData.FromBytes(content), overwrite: true));

            Assert.Equal(500, failure.Status);
            Assert.Equal("InternalError", failure.ErrorCode);
            Assert.False((await blob.ExistsAsync()).Value);
            Assert.Empty(EnumerateContentFiles(application.DataPath));
            Assert.Empty(EnumerateStagingFiles(application.DataPath));
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task MetadataCommitAndReclamationFaultsPreservePublicationBoundary()
    {
        var faultInjector = new ArmableStorageFaultInjector();
        var application = CreateApplication(faultInjector);
        try
        {
            await application.InitializeAsync();
            var container = CreateClient(application).GetBlobContainerClient($"commit-fault-{Guid.NewGuid():N}");
            await container.CreateAsync();
            var blob = container.GetBlobClient("payload.bin");
            var content = RandomNumberGenerator.GetBytes(64 * 1024);

            faultInjector.Arm(StorageFaultPoint.BeforeBlobMetadataCommit);
            var failure = await Assert.ThrowsAsync<RequestFailedException>(() =>
                blob.UploadAsync(BinaryData.FromBytes(content), overwrite: true));

            Assert.Equal(500, failure.Status);
            Assert.Equal("InternalError", failure.ErrorCode);
            Assert.False((await blob.ExistsAsync()).Value);
            var unreachable = EnumerateContentFiles(application.DataPath);
            Assert.NotEmpty(unreachable);

            var service = application.Services.GetRequiredService<BlobService>();
            faultInjector.Arm(StorageFaultPoint.BeforeGarbageCollectionDelete);
            await Assert.ThrowsAsync<IOException>(() => service.CollectGarbageAsync(CancellationToken.None));
            Assert.Equal(unreachable, EnumerateContentFiles(application.DataPath));

            Assert.True(await service.CollectGarbageAsync(CancellationToken.None) > 0);
            Assert.Empty(EnumerateContentFiles(application.DataPath));
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task PostCommitFailureLeavesExactBlobRecoverableAfterRestart()
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-fault-restart-{Guid.NewGuid():N}");
        var faultInjector = new ArmableStorageFaultInjector();
        var containerName = $"lost-response-{Guid.NewGuid():N}";
        var content = RandomNumberGenerator.GetBytes(96 * 1024);
        var first = new SavaWebApplicationFactory(
            dataPath,
            faultInjector,
            analyticsSink: null,
            configurationOverrides: FaultTestConfiguration,
            deleteDataPath: false);
        try
        {
            await first.InitializeAsync();
            var container = CreateClient(first).GetBlobContainerClient(containerName);
            await container.CreateAsync();
            var blob = container.GetBlobClient("durable.bin");

            faultInjector.Arm(StorageFaultPoint.AfterBlobMetadataCommit);
            var failure = await Assert.ThrowsAsync<RequestFailedException>(() =>
                blob.UploadAsync(BinaryData.FromBytes(content), overwrite: true));

            Assert.Equal(500, failure.Status);
            Assert.Equal("InternalError", failure.ErrorCode);
        }
        finally
        {
            await first.DisposeAsync();
        }

        var restarted = new SavaWebApplicationFactory(dataPath, deleteDataPath: true);
        try
        {
            await restarted.InitializeAsync();
            var recovered = await CreateClient(restarted)
                .GetBlobContainerClient(containerName)
                .GetBlobClient("durable.bin")
                .DownloadContentAsync();
            Assert.Equal(content, recovered.Value.Content.ToArray());
        }
        finally
        {
            await restarted.DisposeAsync();
        }
    }

    [Fact]
    public async Task AnalyticsFailureCannotChangeCommittedResponses()
    {
        var analytics = new ThrowingAnalyticsSink();
        var application = new SavaWebApplicationFactory(
            Path.Combine(Path.GetTempPath(), $"mk8-sava-analytics-fault-{Guid.NewGuid():N}"),
            new NullStorageFaultInjector(),
            analytics,
            configurationOverrides: FaultTestConfiguration,
            deleteDataPath: true);
        try
        {
            await application.InitializeAsync();
            var container = CreateClient(application).GetBlobContainerClient($"analytics-fault-{Guid.NewGuid():N}");
            Assert.Equal(201, (await container.CreateAsync()).GetRawResponse().Status);

            var content = RandomNumberGenerator.GetBytes(32 * 1024);
            var blob = container.GetBlobClient("committed.bin");
            Assert.Equal(
                201,
                (await blob.UploadAsync(BinaryData.FromBytes(content), overwrite: true)).GetRawResponse().Status);
            Assert.Equal(content, (await blob.DownloadContentAsync()).Value.Content.ToArray());
            Assert.True(analytics.Attempts >= 3);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    private static SavaWebApplicationFactory CreateApplication(IStorageFaultInjector faultInjector) =>
        new(
            Path.Combine(Path.GetTempPath(), $"mk8-sava-fault-{Guid.NewGuid():N}"),
            faultInjector,
            analyticsSink: null,
            configurationOverrides: FaultTestConfiguration,
            deleteDataPath: true);

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory application)
    {
        var endpoint = new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost");
        var transportClient = new HttpClient(application.Server.CreateHandler())
        {
            BaseAddress = endpoint
        };
        return new BlobServiceClient(
            endpoint,
            new StorageSharedKeyCredential(
                SavaWebApplicationFactory.AccountName,
                SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions
            {
                Transport = new HttpClientTransport(transportClient),
                Retry = { MaxRetries = 0 }
            });
    }

    private static string[] EnumerateContentFiles(string dataPath) =>
        new[] { Path.Combine(dataPath, "chunks"), Path.Combine(dataPath, "packs") }
            .Where(Directory.Exists)
            .SelectMany(path => Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories))
            .Order(StringComparer.Ordinal)
            .ToArray();

    private static string[] EnumerateStagingFiles(string dataPath)
    {
        var path = Path.Combine(dataPath, "staging");
        return Directory.Exists(path)
            ? Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories).Order(StringComparer.Ordinal).ToArray()
            : [];
    }

    private sealed class ArmableStorageFaultInjector : IStorageFaultInjector
    {
        private readonly object _gate = new();
        private StorageFaultPoint _point;
        private bool _armed;

        public void Arm(StorageFaultPoint point)
        {
            lock (_gate)
            {
                _point = point;
                _armed = true;
            }
        }

        public void Inject(StorageFaultPoint point)
        {
            lock (_gate)
            {
                if (!_armed || point != _point)
                    return;
                _armed = false;
            }
            throw new IOException($"Injected storage failure at {point}.");
        }
    }

    private sealed class ThrowingAnalyticsSink : IStorageAnalyticsSink
    {
        private int _attempts;

        public int Attempts => Volatile.Read(ref _attempts);

        public Task RecordAsync(StorageAnalyticsRequest request, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _attempts);
            throw new IOException("Injected analytics persistence failure.");
        }
    }
}
