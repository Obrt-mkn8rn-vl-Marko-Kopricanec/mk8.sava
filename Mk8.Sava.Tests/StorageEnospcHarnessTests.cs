using System.Security.Cryptography;
using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

[CollectionDefinition("Isolated ENOSPC harness", DisableParallelization = true)]
public sealed class StorageEnospcHarnessCollection;

[Collection("Isolated ENOSPC harness")]
public sealed class StorageEnospcHarnessTests
{
    private const string DataPathVariable = "MK8_SAVA_ENOSPC_DATA_PATH";

    [Fact]
    public async Task ExhaustedFilesystemDoesNotPublishPartialUploadOrLoseEarlierBlob()
    {
        var dataPath = Environment.GetEnvironmentVariable(DataPathVariable);
        if (string.IsNullOrWhiteSpace(dataPath))
            return;

        var mountRoot = ValidateMountRoot(dataPath);
        dataPath = Path.GetFullPath(dataPath);

        var stableBytes = RandomNumberGenerator.GetBytes(32 * 1024);
        var attemptedBytes = RandomNumberGenerator.GetBytes(512 * 1024);
        const string containerName = "enospc-harness";
        var configuration = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00",
            ["Sava:EnableSmallChunkPacking"] = "false"
        };
        var fillerPath = Path.Combine(mountRoot, "filler.bin");

        var first = new SavaWebApplicationFactory(
            dataPath,
            new NullStorageFaultInjector(),
            analyticsSink: null,
            configurationOverrides: configuration,
            deleteDataPath: false,
            disableMaintenance: true);
        try
        {
            await first.InitializeAsync();
            var container = CreateClient(first).GetBlobContainerClient(containerName);
            await container.CreateAsync();
            var stable = container.GetBlobClient("stable.bin");
            var attempted = container.GetBlobClient("interrupted.bin");
            await stable.UploadAsync(BinaryData.FromBytes(stableBytes));
            var priorChunkCount = CountStandaloneChunks(dataPath);

            FillUntilNoSpace(fillerPath, 256 * 1024);
            var failure = await Assert.ThrowsAsync<RequestFailedException>(() =>
                attempted.UploadAsync(BinaryData.FromBytes(attemptedBytes)));
            Assert.Equal(500, failure.Status);
            Assert.True(CountStandaloneChunks(dataPath) > priorChunkCount);
            File.Delete(fillerPath);

            Assert.False((await attempted.ExistsAsync()).Value);
            Assert.Equal(stableBytes, (await stable.DownloadContentAsync()).Value.Content.ToArray());
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
            deleteDataPath: false,
            disableMaintenance: true);
        try
        {
            await restarted.InitializeAsync();
            var container = CreateClient(restarted).GetBlobContainerClient(containerName);
            Assert.Equal(stableBytes,
                (await container.GetBlobClient("stable.bin").DownloadContentAsync()).Value.Content.ToArray());
            var attempted = container.GetBlobClient("interrupted.bin");
            Assert.False((await attempted.ExistsAsync()).Value);
            Assert.True(await restarted.Services.GetRequiredService<BlobService>()
                .CollectGarbageAsync(CancellationToken.None) > 0);
            await attempted.UploadAsync(BinaryData.FromBytes(attemptedBytes));
            Assert.Equal(attemptedBytes, (await attempted.DownloadContentAsync()).Value.Content.ToArray());
        }
        finally
        {
            await restarted.DisposeAsync();
        }
    }

    [Fact]
    public async Task ExhaustedFilesystemRollsBackFailedContainerMetadataCommit()
    {
        var configuredPath = Environment.GetEnvironmentVariable(DataPathVariable);
        if (string.IsNullOrWhiteSpace(configuredPath))
            return;

        var mountRoot = ValidateMountRoot(configuredPath);
        var dataPath = Path.Combine(mountRoot, "metadata-data");
        var fillerPath = Path.Combine(mountRoot, "metadata-filler.bin");
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["payload"] = new string('x', 4096)
        };
        var acknowledged = new List<string>();
        string? failedName = null;
        var configuration = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        };
        var first = new SavaWebApplicationFactory(
            dataPath,
            new NullStorageFaultInjector(),
            analyticsSink: null,
            configurationOverrides: configuration,
            deleteDataPath: false,
            disableMaintenance: true);
        try
        {
            await first.InitializeAsync();
            var client = CreateClient(first);
            await client.GetBlobContainerClient("metadata-stable").CreateAsync();
            FillUntilNoSpace(fillerPath, 128 * 1024);

            for (var index = 0; index < 128; index++)
            {
                var name = $"metadata-commit-{index:D4}";
                try
                {
                    await client.GetBlobContainerClient(name).CreateAsync(metadata: metadata);
                    acknowledged.Add(name);
                }
                catch (RequestFailedException failure)
                {
                    Assert.Equal(500, failure.Status);
                    failedName = name;
                    break;
                }
            }
            Assert.NotNull(failedName);
            Assert.NotEmpty(acknowledged);
            File.Delete(fillerPath);

            Assert.False((await client.GetBlobContainerClient(failedName).ExistsAsync()).Value);
            Assert.True((await client.GetBlobContainerClient("metadata-stable").ExistsAsync()).Value);
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
            deleteDataPath: false,
            disableMaintenance: true);
        try
        {
            await restarted.InitializeAsync();
            var client = CreateClient(restarted);
            Assert.True((await client.GetBlobContainerClient("metadata-stable").ExistsAsync()).Value);
            foreach (var name in acknowledged)
            {
                var properties = await client.GetBlobContainerClient(name).GetPropertiesAsync();
                Assert.Equal(metadata["payload"], properties.Value.Metadata["payload"]);
            }
            var failed = client.GetBlobContainerClient(failedName!);
            Assert.False((await failed.ExistsAsync()).Value);
            await failed.CreateAsync(metadata: metadata);
            Assert.Equal(metadata["payload"], (await failed.GetPropertiesAsync()).Value.Metadata["payload"]);
        }
        finally
        {
            await restarted.DisposeAsync();
        }
    }

    [Fact]
    public async Task ExhaustedFilesystemDuringPackAppendPreservesIndexedRecords()
    {
        var configuredPath = Environment.GetEnvironmentVariable(DataPathVariable);
        if (string.IsNullOrWhiteSpace(configuredPath))
            return;

        var mountRoot = ValidateMountRoot(configuredPath);
        var dataPath = Path.Combine(mountRoot, "pack-data");
        var fillerPath = Path.Combine(mountRoot, "pack-filler.bin");
        var stableBytes = RandomNumberGenerator.GetBytes(1024);
        var attemptedBytes = RandomNumberGenerator.GetBytes(4096);
        var recorder = new RecordingStorageFaultInjector();
        var configuration = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00",
            ["Sava:SmallChunkPackingThresholdBytes"] = "4096"
        };
        string packId;
        long priorIndexedLength;
        var first = new SavaWebApplicationFactory(
            dataPath,
            recorder,
            analyticsSink: null,
            configurationOverrides: configuration,
            deleteDataPath: false,
            disableMaintenance: true);
        try
        {
            await first.InitializeAsync();
            var container = CreateClient(first).GetBlobContainerClient("enospc-pack");
            await container.CreateAsync();
            var stable = container.GetBlobClient("stable.bin");
            await stable.UploadAsync(BinaryData.FromBytes(stableBytes));
            var metadata = first.Services.GetRequiredService<MetadataStore>();
            var pack = await metadata.GetActiveChunkPackAsync(
                SavaWebApplicationFactory.AccountName, CancellationToken.None);
            Assert.NotNull(pack);
            packId = pack.PackId;
            priorIndexedLength = await metadata.GetPackIndexedLengthAsync(packId, CancellationToken.None);
            recorder.Reset();

            FillUntilNoSpace(fillerPath, 8 * 1024);
            var attempted = container.GetBlobClient("interrupted.bin");
            var failure = await Assert.ThrowsAsync<RequestFailedException>(() =>
                attempted.UploadAsync(BinaryData.FromBytes(attemptedBytes)));
            Assert.Equal(500, failure.Status);
            Assert.True(recorder.StagingCompleted);
            Assert.True(recorder.PackAppendStarted);
            Assert.Equal(1, metadata.CountPackedChunks());
            Assert.Equal(priorIndexedLength, await metadata.GetPackIndexedLengthAsync(packId, CancellationToken.None));
            File.Delete(fillerPath);

            Assert.False((await attempted.ExistsAsync()).Value);
            Assert.Equal(stableBytes, (await stable.DownloadContentAsync()).Value.Content.ToArray());
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
            deleteDataPath: false,
            disableMaintenance: true);
        try
        {
            await restarted.InitializeAsync();
            var container = CreateClient(restarted).GetBlobContainerClient("enospc-pack");
            Assert.Equal(stableBytes,
                (await container.GetBlobClient("stable.bin").DownloadContentAsync()).Value.Content.ToArray());
            var attempted = container.GetBlobClient("interrupted.bin");
            await attempted.UploadAsync(BinaryData.FromBytes(attemptedBytes));
            Assert.Equal(attemptedBytes, (await attempted.DownloadContentAsync()).Value.Content.ToArray());
            var metadata = restarted.Services.GetRequiredService<MetadataStore>();
            Assert.Equal(2, metadata.CountPackedChunks());
            var packPath = Path.Combine(dataPath, "packs", packId.Replace('/', Path.DirectorySeparatorChar) + ".pack");
            Assert.Equal(await metadata.GetPackIndexedLengthAsync(packId, CancellationToken.None), new FileInfo(packPath).Length);
        }
        finally
        {
            await restarted.DisposeAsync();
        }
    }

    [Fact]
    public async Task ExhaustedFilesystemDuringPackCompactionKeepsOldPackAuthoritative()
    {
        var configuredPath = Environment.GetEnvironmentVariable(DataPathVariable);
        if (string.IsNullOrWhiteSpace(configuredPath))
            return;

        var mountRoot = ValidateMountRoot(configuredPath);
        var dataPath = Path.Combine(mountRoot, "compaction-data");
        var fillerPath = Path.Combine(mountRoot, "compaction-filler.bin");
        var liveBytes = RandomNumberGenerator.GetBytes(4096);
        var configuration = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00",
            ["Sava:SmallChunkPackingThresholdBytes"] = "4096",
            ["Sava:ChunkPackMaximumRecords"] = "2",
            ["Sava:ChunkPackCompactionMinimumSavingsBytes"] = "1",
            ["Sava:ChunkPackCompactionMinimumDeadRatio"] = "0.01"
        };
        string packId;
        string chunkId;
        long oldPackLength;

        var first = new SavaWebApplicationFactory(
            dataPath,
            new NullStorageFaultInjector(),
            analyticsSink: null,
            configurationOverrides: configuration,
            deleteDataPath: false,
            disableMaintenance: true);
        try
        {
            await first.InitializeAsync();
            var container = CreateClient(first).GetBlobContainerClient("enospc-compaction");
            await container.CreateAsync();
            var deleted = container.GetBlobClient("deleted.bin");
            var live = container.GetBlobClient("live.bin");
            await deleted.UploadAsync(BinaryData.FromBytes(RandomNumberGenerator.GetBytes(4096)));
            await live.UploadAsync(BinaryData.FromBytes(liveBytes));
            await deleted.DeleteAsync();

            var service = first.Services.GetRequiredService<BlobService>();
            var metadata = first.Services.GetRequiredService<MetadataStore>();
            var chunks = first.Services.GetRequiredService<ChunkStore>();
            Assert.True(await service.CollectGarbageAsync(CancellationToken.None) > 0);
            var liveRecord = await service.GetBlobAsync(
                SavaWebApplicationFactory.AccountName,
                container.Name,
                live.Name,
                versionId: null,
                snapshot: null,
                includeDeleted: false,
                CancellationToken.None);
            chunkId = Assert.Single(liveRecord.Content.Chunks).Id;
            var location = await metadata.GetPackedChunkLocationAsync(chunkId, CancellationToken.None);
            Assert.NotNull(location);
            packId = location.PackId;
            var pack = Assert.Single((await metadata.ListSealedChunkPacksAsync(
                after: null,
                maximum: 16,
                CancellationToken.None)).Items);
            Assert.Equal(packId, pack.PackId);
            var packPath = Path.Combine(dataPath, "packs", packId.Replace('/', Path.DirectorySeparatorChar) + ".pack");
            oldPackLength = new FileInfo(packPath).Length;

            FillUntilNoSpace(fillerPath, 4096);
            var failure = await Assert.ThrowsAsync<IOException>(() =>
                chunks.TryCompactPackAsync(pack, CancellationToken.None));
            Assert.Contains("No space left on device", failure.Message, StringComparison.OrdinalIgnoreCase);
            File.Delete(fillerPath);

            Assert.Equal(oldPackLength, new FileInfo(packPath).Length);
            Assert.Equal(packId,
                (await metadata.GetPackedChunkLocationAsync(chunkId, CancellationToken.None))?.PackId);
            Assert.Empty(Directory.EnumerateFiles(
                Path.Combine(dataPath, "staging"), "pack-compact-*.tmp"));
            Assert.Equal(liveBytes, (await live.DownloadContentAsync()).Value.Content.ToArray());
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
            deleteDataPath: false,
            disableMaintenance: true);
        try
        {
            await restarted.InitializeAsync();
            var live = CreateClient(restarted)
                .GetBlobContainerClient("enospc-compaction")
                .GetBlobClient("live.bin");
            Assert.Equal(liveBytes, (await live.DownloadContentAsync()).Value.Content.ToArray());
            var metadata = restarted.Services.GetRequiredService<MetadataStore>();
            var pack = Assert.Single((await metadata.ListSealedChunkPacksAsync(
                after: null,
                maximum: 16,
                CancellationToken.None)).Items);
            Assert.Equal(packId, pack.PackId);
            var compacted = await restarted.Services.GetRequiredService<ChunkStore>()
                .TryCompactPackAsync(pack, CancellationToken.None);
            Assert.Equal(1, compacted.CompactedPacks);
            Assert.Equal(liveBytes, (await live.DownloadContentAsync()).Value.Content.ToArray());
            Assert.NotEqual(packId,
                (await metadata.GetPackedChunkLocationAsync(chunkId, CancellationToken.None))?.PackId, StringComparer.Ordinal);
        }
        finally
        {
            await restarted.DisposeAsync();
        }
    }

    private static string ValidateMountRoot(string dataPath)
    {
        var fullPath = Path.GetFullPath(dataPath);
        var mountRoot = Path.GetDirectoryName(fullPath)
                        ?? throw new InvalidOperationException("The ENOSPC data path has no mount root.");
        if (Path.GetFileName(fullPath) != "data" ||
            !Path.GetFileName(mountRoot).StartsWith("mk8-sava-enospc-", StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The ENOSPC test requires an isolated mk8-sava-enospc-* mount.");
        }
        return mountRoot;
    }

    private static void FillUntilNoSpace(string path, int releaseBytes)
    {
        var bytes = new byte[1024 * 1024];
        IOException? noSpace = null;
        using (var filler = new FileStream(
                   path,
                   FileMode.CreateNew,
                   FileAccess.ReadWrite,
                   FileShare.None,
                   bufferSize: 4096,
                   FileOptions.WriteThrough))
        {
            while (noSpace is null)
            {
                try
                {
                    filler.Write(bytes);
                }
                catch (IOException exception)
                {
                    noSpace = exception;
                }
            }
        }

        Assert.Contains("No space left on device", noSpace.Message, StringComparison.OrdinalIgnoreCase);
        using var release = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        Assert.True(release.Length >= 8 * 1024 * 1024);
        release.SetLength(release.Length - releaseBytes);
        release.Flush(flushToDisk: true);
    }

    private static int CountStandaloneChunks(string dataPath) =>
        Directory.EnumerateFiles(Path.Combine(dataPath, "chunks"), "*.chunk", SearchOption.AllDirectories).Count();

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
                Transport = new HttpClientTransport(new HttpClient(application.Server.CreateHandler())
                {
                    BaseAddress = endpoint
                }),
                Retry = { MaxRetries = 0 }
            });
    }

    private sealed class RecordingStorageFaultInjector : IStorageFaultInjector
    {
        private int _stagingCompleted;
        private int _packAppendStarted;

        public bool StagingCompleted => Volatile.Read(ref _stagingCompleted) != 0;
        public bool PackAppendStarted => Volatile.Read(ref _packAppendStarted) != 0;

        public void Reset()
        {
            Interlocked.Exchange(ref _stagingCompleted, 0);
            Interlocked.Exchange(ref _packAppendStarted, 0);
        }

        public void Inject(StorageFaultPoint point)
        {
            if (point == StorageFaultPoint.BeforeChunkPublication)
                Interlocked.Exchange(ref _stagingCompleted, 1);
            if (point == StorageFaultPoint.DuringPackRecordAppend)
                Interlocked.Exchange(ref _packAppendStarted, 1);
        }
    }
}
