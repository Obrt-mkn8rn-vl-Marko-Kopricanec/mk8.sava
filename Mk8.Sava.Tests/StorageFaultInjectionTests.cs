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
        new Dictionary<string, string?>(StringComparer.Ordinal)
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
    public async Task PartialStagingWriteFailureLeavesNoPublishedChunkAndCanRetry()
    {
        var faultInjector = new ArmableStorageFaultInjector();
        var application = CreateApplication(faultInjector);
        try
        {
            await application.InitializeAsync();
            var container = CreateClient(application).GetBlobContainerClient($"staging-write-{Guid.NewGuid():N}");
            await container.CreateAsync();
            var blob = container.GetBlobClient("payload.bin");
            var content = RandomNumberGenerator.GetBytes(64 * 1024);

            faultInjector.Arm(StorageFaultPoint.DuringChunkStagingWrite);
            var failure = await Assert.ThrowsAsync<RequestFailedException>(() =>
                blob.UploadAsync(BinaryData.FromBytes(content), overwrite: true));

            Assert.Equal(500, failure.Status);
            Assert.False((await blob.ExistsAsync()).Value);
            Assert.Empty(EnumerateContentFiles(application.DataPath));
            Assert.Empty(EnumerateStagingFiles(application.DataPath));

            await blob.UploadAsync(BinaryData.FromBytes(content), overwrite: true);
            Assert.Equal(content, (await blob.DownloadContentAsync()).Value.Content.ToArray());
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task PartialPackAppendIsDiscardedBeforeTheNextAppendAfterRestart()
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-pack-write-{Guid.NewGuid():N}");
        var faultInjector = new ArmableStorageFaultInjector();
        var configuration = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00",
            ["Sava:SmallChunkPackingThresholdBytes"] = "4096"
        };
        var containerName = $"pack-write-{Guid.NewGuid():N}";
        var stableBytes = RandomNumberGenerator.GetBytes(1536);
        var retryBytes = RandomNumberGenerator.GetBytes(1536);
        string packId;
        long indexedLength;

        var first = new SavaWebApplicationFactory(
            dataPath,
            faultInjector,
            analyticsSink: null,
            configurationOverrides: configuration,
            deleteDataPath: false,
            disableMaintenance: true);
        try
        {
            await first.InitializeAsync();
            var container = CreateClient(first).GetBlobContainerClient(containerName);
            await container.CreateAsync();
            await container.GetBlobClient("stable.bin").UploadAsync(BinaryData.FromBytes(stableBytes));
            var metadata = first.Services.GetRequiredService<MetadataStore>();
            var pack = await metadata.GetActiveChunkPackAsync(
                SavaWebApplicationFactory.AccountName, CancellationToken.None);
            Assert.NotNull(pack);
            packId = pack.PackId;
            indexedLength = await metadata.GetPackIndexedLengthAsync(packId, CancellationToken.None);
            Assert.True(indexedLength > 0);

            faultInjector.Arm(StorageFaultPoint.DuringPackRecordAppend);
            var failedBlob = container.GetBlobClient("retry.bin");
            var failure = await Assert.ThrowsAsync<RequestFailedException>(() =>
                failedBlob.UploadAsync(BinaryData.FromBytes(retryBytes)));
            Assert.Equal(500, failure.Status);
            Assert.False((await failedBlob.ExistsAsync()).Value);
            Assert.Equal(stableBytes,
                (await container.GetBlobClient("stable.bin").DownloadContentAsync()).Value.Content.ToArray());

            var packPath = Path.Combine(dataPath, "packs", packId.Replace('/', Path.DirectorySeparatorChar) + ".pack");
            Assert.True(new FileInfo(packPath).Length > indexedLength);
            Assert.Equal(indexedLength, await metadata.GetPackIndexedLengthAsync(packId, CancellationToken.None));
        }
        finally
        {
            await first.DisposeAsync();
        }

        var restarted = new SavaWebApplicationFactory(
            dataPath,
            new NullStorageFaultInjector(),
            analyticsSink: null,
            configurationOverrides: configuration,
            deleteDataPath: true,
            disableMaintenance: true);
        try
        {
            await restarted.InitializeAsync();
            var container = CreateClient(restarted).GetBlobContainerClient(containerName);
            Assert.Equal(stableBytes,
                (await container.GetBlobClient("stable.bin").DownloadContentAsync()).Value.Content.ToArray());
            var retried = container.GetBlobClient("retry.bin");
            await retried.UploadAsync(BinaryData.FromBytes(retryBytes));
            Assert.Equal(retryBytes, (await retried.DownloadContentAsync()).Value.Content.ToArray());

            var metadata = restarted.Services.GetRequiredService<MetadataStore>();
            Assert.Equal(2, await metadata.CountPackedChunksAsync(CancellationToken.None).ConfigureAwait(true));
            var packPath = Path.Combine(dataPath, "packs", packId.Replace('/', Path.DirectorySeparatorChar) + ".pack");
            Assert.Equal(
                await metadata.GetPackIndexedLengthAsync(packId, CancellationToken.None).ConfigureAwait(true),
                new FileInfo(packPath).Length);
            Assert.Empty(EnumerateStagingFiles(dataPath));
        }
        finally
        {
            await restarted.DisposeAsync().ConfigureAwait(true);
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

    [Fact]
    public async Task ConcurrentIdenticalSmallChunksPublishOnePackRecord()
    {
        using var publicationBarrier = new Barrier(2);
        var injector = new BlockingPublicationInjector(publicationBarrier);
        var application = new SavaWebApplicationFactory(
            Path.Combine(Path.GetTempPath(), $"mk8-sava-pack-race-{Guid.NewGuid():N}"),
            injector,
            analyticsSink: null,
            configurationOverrides: new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sava:MaintenanceScanInterval"] = "01:00:00",
                ["Sava:SmallChunkPackingThresholdBytes"] = "4096"
            },
            deleteDataPath: true,
            disableMaintenance: true);
        try
        {
            await application.InitializeAsync();
            var store = application.Services.GetRequiredService<ChunkStore>();
            var metadata = application.Services.GetRequiredService<MetadataStore>();
            var payload = RandomNumberGenerator.GetBytes(1024);
            var encryption = new BlobEncryption(Scope: null, CustomerProvidedKeySha256: null);

            Task<StoredContent> StoreAsync() => Task.Run(() => store.StorePinnedAsync(
                SavaWebApplicationFactory.AccountName,
                encryption,
                new MemoryStream(payload, writable: false),
                CancellationToken.None));

            var pending = new[] { StoreAsync(), StoreAsync() };
            var stored = await Task.WhenAll(pending).WaitAsync(TimeSpan.FromSeconds(30));
            try
            {
                var chunkId = Assert.Single(stored[0].Manifest.Chunks).Id;
                Assert.Equal(chunkId, Assert.Single(stored[1].Manifest.Chunks).Id);
                var location = await metadata.GetPackedChunkLocationAsync(chunkId, CancellationToken.None);
                Assert.NotNull(location);
                Assert.Equal(1, await metadata.CountPackedChunksAsync(CancellationToken.None).ConfigureAwait(true));
                var packPath = Path.Combine(
                    application.DataPath,
                    "packs",
                    location.PackId.Replace('/', Path.DirectorySeparatorChar) + ".pack");
                Assert.Equal(location.RecordLength, new FileInfo(packPath).Length);

                using var reconstructed = new MemoryStream();
                await store.WriteRangeAsync(
                    stored[1].Manifest,
                    encryption,
                    0,
                    payload.Length,
                    reconstructed,
                    CancellationToken.None).ConfigureAwait(true);
                Assert.Equal(payload, reconstructed.ToArray());
            }
            finally
            {
                foreach (var item in stored)
                    item.Dispose();
            }
        }
        finally
        {
            await application.DisposeAsync().ConfigureAwait(true);
        }
    }

    [Theory]
    [InlineData(StorageFaultPoint.BeforePackMetadataCommit)]
    [InlineData(StorageFaultPoint.AfterPackMetadataCommit)]
    public async Task PackCompactionFailureKeepsTheAuthoritativePackReadable(StorageFaultPoint faultPoint)
    {
        var faultInjector = new ArmableStorageFaultInjector();
        var application = new SavaWebApplicationFactory(
            Path.Combine(Path.GetTempPath(), $"mk8-sava-pack-commit-{Guid.NewGuid():N}"),
            faultInjector,
            analyticsSink: null,
            configurationOverrides: new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sava:MaintenanceScanInterval"] = "01:00:00",
                ["Sava:SmallChunkPackingThresholdBytes"] = "4096",
                ["Sava:ChunkPackMaximumRecords"] = "2",
                ["Sava:ChunkPackSealAge"] = "00:00:00",
                ["Sava:ChunkPacksPerMaintenancePass"] = "16",
                ["Sava:ChunkPackCompactionMinimumSavingsBytes"] = "1",
                ["Sava:ChunkPackCompactionMinimumDeadRatio"] = "0.01"
            },
            deleteDataPath: true,
            disableMaintenance: true);
        try
        {
            await application.InitializeAsync();
            var container = CreateClient(application).GetBlobContainerClient($"pack-commit-{Guid.NewGuid():N}");
            await container.CreateAsync();
            var deleted = container.GetBlobClient("deleted.bin");
            var live = container.GetBlobClient("live.bin");
            var liveBytes = RandomNumberGenerator.GetBytes(1536);
            await deleted.UploadAsync(BinaryData.FromBytes(RandomNumberGenerator.GetBytes(1024)));
            await live.UploadAsync(BinaryData.FromBytes(liveBytes));
            await deleted.DeleteAsync();

            var service = application.Services.GetRequiredService<BlobService>();
            var metadata = application.Services.GetRequiredService<MetadataStore>();
            var liveRecord = await service.GetBlobAsync(
                SavaWebApplicationFactory.AccountName,
                container.Name,
                live.Name,
                versionId: null,
                snapshot: null,
                includeDeleted: false,
                CancellationToken.None);
            var chunkId = Assert.Single(liveRecord.Content.Chunks).Id;
            faultInjector.Arm(faultPoint);
            await Assert.ThrowsAsync<IOException>(() => service.RunMaintenanceAsync(CancellationToken.None));

            var committedLocation = await metadata.GetPackedChunkLocationAsync(chunkId, CancellationToken.None);
            Assert.NotNull(committedLocation);
            var committedPath = Path.Combine(
                application.DataPath,
                "packs",
                committedLocation.PackId.Replace('/', Path.DirectorySeparatorChar) + ".pack");
            Assert.True(File.Exists(committedPath));
            Assert.Equal(liveBytes, (await live.DownloadContentAsync()).Value.Content.ToArray());

            await service.RunMaintenanceAsync(CancellationToken.None);
            Assert.Equal(liveBytes, (await live.DownloadContentAsync()).Value.Content.ToArray());
            Assert.Single(EnumerateContentFiles(application.DataPath));
            var recoveredLocation = await metadata.GetPackedChunkLocationAsync(chunkId, CancellationToken.None);
            Assert.NotNull(recoveredLocation);
            var recoveredPath = Path.Combine(
                application.DataPath,
                "packs",
                recoveredLocation.PackId.Replace('/', Path.DirectorySeparatorChar) + ".pack");
            Assert.True(File.Exists(recoveredPath));
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
        return new BlobServiceClient(
            endpoint,
            new StorageSharedKeyCredential(
                SavaWebApplicationFactory.AccountName,
                SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions
            {
                Transport = new HttpClientTransport(application.Server.CreateHandler()),
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

    private sealed class BlockingPublicationInjector(Barrier barrier) : IStorageFaultInjector
    {
        public void Inject(StorageFaultPoint point)
        {
            if (point == StorageFaultPoint.BeforeChunkPublication &&
                !barrier.SignalAndWait(TimeSpan.FromSeconds(15)))
            {
                throw new TimeoutException("Both competing chunk writers did not reach publication.");
            }
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
