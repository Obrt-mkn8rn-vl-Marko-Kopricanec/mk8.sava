using System.Security.Cryptography;
using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

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

        await AssertPartialUploadRejectedAsync(
            dataPath, fillerPath, containerName, configuration, stableBytes, attemptedBytes).ConfigureAwait(true);
        await AssertPartialUploadRecoversAsync(
            dataPath, containerName, configuration, stableBytes, attemptedBytes).ConfigureAwait(true);
    }

    private static async Task AssertPartialUploadRejectedAsync(
        string dataPath,
        string fillerPath,
        string containerName,
        Dictionary<string, string?> configuration,
        byte[] stableBytes,
        byte[] attemptedBytes)
    {
        var first = CreateHarness(dataPath, configuration);
        try
        {
            await first.InitializeAsync().ConfigureAwait(false);
            var container = CreateClient(first).GetBlobContainerClient(containerName);
            await container.CreateAsync().ConfigureAwait(false);
            var stable = container.GetBlobClient("stable.bin");
            var attempted = container.GetBlobClient("interrupted.bin");
            await stable.UploadAsync(BinaryData.FromBytes(stableBytes)).ConfigureAwait(false);
            var priorChunkCount = CountStandaloneChunks(dataPath);

            FillUntilNoSpace(fillerPath, 256 * 1024);
            var failure = await Assert.ThrowsAsync<RequestFailedException>(() =>
                attempted.UploadAsync(BinaryData.FromBytes(attemptedBytes))).ConfigureAwait(false);
            Assert.Equal(500, failure.Status);
            Assert.True(CountStandaloneChunks(dataPath) > priorChunkCount);
            File.Delete(fillerPath);

            Assert.False((await attempted.ExistsAsync().ConfigureAwait(false)).Value);
            Assert.Equal(stableBytes, (await stable.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        }
        finally
        {
            await first.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task AssertPartialUploadRecoversAsync(
        string dataPath,
        string containerName,
        Dictionary<string, string?> configuration,
        byte[] stableBytes,
        byte[] attemptedBytes)
    {
        var restarted = CreateHarness(dataPath, configuration);
        try
        {
            await restarted.InitializeAsync().ConfigureAwait(false);
            var container = CreateClient(restarted).GetBlobContainerClient(containerName);
            Assert.Equal(stableBytes,
                (await container.GetBlobClient("stable.bin").DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
            var attempted = container.GetBlobClient("interrupted.bin");
            Assert.False((await attempted.ExistsAsync().ConfigureAwait(false)).Value);
            Assert.True(await restarted.Services.GetRequiredService<BlobService>()
                .CollectGarbageAsync(CancellationToken.None).ConfigureAwait(false) > 0);
            await attempted.UploadAsync(BinaryData.FromBytes(attemptedBytes)).ConfigureAwait(false);
            Assert.Equal(attemptedBytes, (await attempted.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        }
        finally
        {
            await restarted.DisposeAsync().ConfigureAwait(false);
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
        var configuration = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        };

        var failure = await AssertMetadataCommitRejectedAsync(
            dataPath, fillerPath, configuration, metadata).ConfigureAwait(true);
        await AssertMetadataCommitRecoversAsync(dataPath, configuration, metadata, failure).ConfigureAwait(true);
    }

    private static async Task<MetadataFailureObservation> AssertMetadataCommitRejectedAsync(
        string dataPath,
        string fillerPath,
        Dictionary<string, string?> configuration,
        Dictionary<string, string> metadata)
    {
        var acknowledged = new List<string>();
        string? failedName = null;
        var first = CreateHarness(dataPath, configuration);
        try
        {
            await first.InitializeAsync().ConfigureAwait(false);
            var client = CreateClient(first);
            await client.GetBlobContainerClient("metadata-stable").CreateAsync().ConfigureAwait(false);
            FillUntilNoSpace(fillerPath, 128 * 1024);

            for (var index = 0; index < 128; index++)
            {
                var name = $"metadata-commit-{index:D4}";
                try
                {
                    await client.GetBlobContainerClient(name).CreateAsync(metadata: metadata).ConfigureAwait(false);
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

            Assert.False((await client.GetBlobContainerClient(failedName).ExistsAsync().ConfigureAwait(false)).Value);
            Assert.True((await client.GetBlobContainerClient("metadata-stable").ExistsAsync().ConfigureAwait(false)).Value);
        }
        finally
        {
            await first.DisposeAsync().ConfigureAwait(false);
        }

        return new MetadataFailureObservation(acknowledged, failedName!);
    }

    private static async Task AssertMetadataCommitRecoversAsync(
        string dataPath,
        Dictionary<string, string?> configuration,
        Dictionary<string, string> metadata,
        MetadataFailureObservation failure)
    {
        var restarted = CreateHarness(dataPath, configuration);
        try
        {
            await restarted.InitializeAsync().ConfigureAwait(false);
            var client = CreateClient(restarted);
            Assert.True((await client.GetBlobContainerClient("metadata-stable").ExistsAsync().ConfigureAwait(false)).Value);
            foreach (var name in failure.Acknowledged)
            {
                var properties = await client.GetBlobContainerClient(name).GetPropertiesAsync().ConfigureAwait(false);
                Assert.Equal(metadata["payload"], properties.Value.Metadata["payload"]);
            }
            var failed = client.GetBlobContainerClient(failure.FailedName);
            Assert.False((await failed.ExistsAsync().ConfigureAwait(false)).Value);
            await failed.CreateAsync(metadata: metadata).ConfigureAwait(false);
            Assert.Equal(metadata["payload"], (await failed.GetPropertiesAsync().ConfigureAwait(false)).Value.Metadata["payload"]);
        }
        finally
        {
            await restarted.DisposeAsync().ConfigureAwait(false);
        }
    }

    [Fact]
    public async Task ExhaustedFilesystemRollsBackFailedBlobMetadataUpdate()
    {
        var configuredPath = Environment.GetEnvironmentVariable(DataPathVariable);
        if (string.IsNullOrWhiteSpace(configuredPath))
            return;

        var mountRoot = ValidateMountRoot(configuredPath);
        var dataPath = Path.Combine(mountRoot, "blob-metadata-data");
        var fillerPath = Path.Combine(mountRoot, "blob-metadata-filler.bin");
        var stableBytes = RandomNumberGenerator.GetBytes(8192);
        var configuration = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        };

        var acknowledged = await AssertBlobMetadataUpdateRejectedAsync(
            dataPath, fillerPath, stableBytes, configuration).ConfigureAwait(true);
        await AssertBlobMetadataUpdateRecoversAsync(
            dataPath, stableBytes, acknowledged, configuration).ConfigureAwait(true);
    }

    private static async Task<BlobMetadataFailureObservation> AssertBlobMetadataUpdateRejectedAsync(
        string dataPath,
        string fillerPath,
        byte[] stableBytes,
        Dictionary<string, string?> configuration)
    {
        var first = CreateHarness(dataPath, configuration);
        try
        {
            await first.InitializeAsync().ConfigureAwait(false);
            var container = CreateClient(first).GetBlobContainerClient("enospc-blob-metadata");
            await container.CreateAsync().ConfigureAwait(false);
            var blob = container.GetBlobClient("stable.bin");
            await blob.UploadAsync(BinaryData.FromBytes(stableBytes), new Azure.Storage.Blobs.Models.BlobUploadOptions
            {
                Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["state"] = "seed" }
            }).ConfigureAwait(false);
            var acknowledgedState = "seed";
            var acknowledgedETag = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag;
            var acknowledgedUpdates = 0;
            FillUntilNoSpace(fillerPath, 256 * 1024);

            var failed = false;
            for (var index = 0; index < 256; index++)
            {
                var state = $"{index:D4}-{new string('m', 4096)}";
                try
                {
                    var updated = await blob.SetMetadataAsync(
                        new Dictionary<string, string>(StringComparer.Ordinal) { ["state"] = state }).ConfigureAwait(false);
                    acknowledgedState = state;
                    acknowledgedETag = updated.Value.ETag;
                    acknowledgedUpdates++;
                }
                catch (RequestFailedException failure)
                {
                    Assert.Equal(500, failure.Status);
                    failed = true;
                    break;
                }
            }
            Assert.True(failed);
            Assert.True(acknowledgedUpdates > 0);
            File.Delete(fillerPath);

            var properties = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
            Assert.Equal(acknowledgedState, properties.Metadata["state"]);
            Assert.Equal(acknowledgedETag, properties.ETag);
            Assert.Equal(stableBytes, (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
            return new BlobMetadataFailureObservation(acknowledgedState, acknowledgedETag);
        }
        finally
        {
            await first.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task AssertBlobMetadataUpdateRecoversAsync(
        string dataPath,
        byte[] stableBytes,
        BlobMetadataFailureObservation acknowledged,
        Dictionary<string, string?> configuration)
    {
        var restarted = CreateHarness(dataPath, configuration);
        try
        {
            await restarted.InitializeAsync().ConfigureAwait(false);
            var blob = CreateClient(restarted)
                .GetBlobContainerClient("enospc-blob-metadata")
                .GetBlobClient("stable.bin");
            var properties = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
            Assert.Equal(acknowledged.State, properties.Metadata["state"]);
            Assert.Equal(acknowledged.ETag, properties.ETag);
            Assert.Equal(stableBytes, (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
            await blob.SetMetadataAsync(
                new Dictionary<string, string>(StringComparer.Ordinal) { ["state"] = "recovered" }).ConfigureAwait(false);
            Assert.Equal("recovered", (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.Metadata["state"]);
            Assert.Equal(stableBytes, (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        }
        finally
        {
            await restarted.DisposeAsync().ConfigureAwait(false);
        }
    }

    [Fact]
    public async Task ExhaustedFilesystemAtBlobPublicationCommitKeepsEarlierBlob()
    {
        var configuredPath = Environment.GetEnvironmentVariable(DataPathVariable);
        if (string.IsNullOrWhiteSpace(configuredPath))
            return;

        var mountRoot = ValidateMountRoot(configuredPath);
        var dataPath = Path.Combine(mountRoot, "publication-data");
        var fillerPath = Path.Combine(mountRoot, "publication-filler.bin");
        var stableBytes = RandomNumberGenerator.GetBytes(32 * 1024);
        var attemptedBytes = RandomNumberGenerator.GetBytes(96 * 1024);
        var configuration = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00",
            ["Sava:EnableSmallChunkPacking"] = "false"
        };

        await AssertPublicationCommitRejectedAsync(
            dataPath, fillerPath, stableBytes, attemptedBytes, configuration).ConfigureAwait(true);
        await AssertPublicationCommitRecoversAsync(
            dataPath, stableBytes, attemptedBytes, configuration).ConfigureAwait(true);
    }

    private static async Task AssertPublicationCommitRejectedAsync(
        string dataPath,
        string fillerPath,
        byte[] stableBytes,
        byte[] attemptedBytes,
        Dictionary<string, string?> configuration)
    {
        var injector = new EnospcAtCommitFaultInjector(fillerPath);
        var first = CreateHarness(dataPath, configuration, injector);
        try
        {
            await first.InitializeAsync().ConfigureAwait(false);
            var container = CreateClient(first).GetBlobContainerClient("enospc-publication");
            await container.CreateAsync().ConfigureAwait(false);
            var stable = container.GetBlobClient("stable.bin");
            var attempted = container.GetBlobClient("attempted.bin");
            await stable.UploadAsync(BinaryData.FromBytes(stableBytes)).ConfigureAwait(false);
            var priorChunkCount = CountStandaloneChunks(dataPath);
            injector.Arm();

            try
            {
                var failure = await Assert.ThrowsAsync<RequestFailedException>(() =>
                    attempted.UploadAsync(BinaryData.FromBytes(attemptedBytes))).ConfigureAwait(false);
                Assert.Equal(500, failure.Status);
                Assert.True(injector.Filled);
                Assert.True(CountStandaloneChunks(dataPath) > priorChunkCount);
            }
            finally
            {
                if (File.Exists(fillerPath))
                    File.Delete(fillerPath);
            }
            Assert.False((await attempted.ExistsAsync().ConfigureAwait(false)).Value);
            Assert.Equal(stableBytes, (await stable.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        }
        finally
        {
            await first.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task AssertPublicationCommitRecoversAsync(
        string dataPath,
        byte[] stableBytes,
        byte[] attemptedBytes,
        Dictionary<string, string?> configuration)
    {
        var restarted = CreateHarness(dataPath, configuration);
        try
        {
            await restarted.InitializeAsync().ConfigureAwait(false);
            var container = CreateClient(restarted).GetBlobContainerClient("enospc-publication");
            Assert.Equal(stableBytes, (await container.GetBlobClient("stable.bin")
                .DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
            var attempted = container.GetBlobClient("attempted.bin");
            Assert.False((await attempted.ExistsAsync().ConfigureAwait(false)).Value);
            Assert.True(await restarted.Services.GetRequiredService<BlobService>()
                .CollectGarbageAsync(CancellationToken.None).ConfigureAwait(false) > 0);
            await attempted.UploadAsync(BinaryData.FromBytes(attemptedBytes)).ConfigureAwait(false);
            Assert.Equal(attemptedBytes, (await attempted.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        }
        finally
        {
            await restarted.DisposeAsync().ConfigureAwait(false);
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

        var packId = await AssertPackAppendRejectedAsync(
            dataPath, fillerPath, stableBytes, attemptedBytes, recorder, configuration).ConfigureAwait(true);
        await AssertPackAppendRecoversAsync(
            dataPath, packId, stableBytes, attemptedBytes, configuration).ConfigureAwait(true);
    }

    private static async Task<string> AssertPackAppendRejectedAsync(
        string dataPath,
        string fillerPath,
        byte[] stableBytes,
        byte[] attemptedBytes,
        RecordingStorageFaultInjector recorder,
        Dictionary<string, string?> configuration)
    {
        var first = CreateHarness(dataPath, configuration, recorder);
        try
        {
            await first.InitializeAsync().ConfigureAwait(false);
            var container = CreateClient(first).GetBlobContainerClient("enospc-pack");
            await container.CreateAsync().ConfigureAwait(false);
            var stable = container.GetBlobClient("stable.bin");
            await stable.UploadAsync(BinaryData.FromBytes(stableBytes)).ConfigureAwait(false);
            var metadata = first.Services.GetRequiredService<MetadataStore>();
            var pack = await metadata.GetActiveChunkPackAsync(
                SavaWebApplicationFactory.AccountName, CancellationToken.None).ConfigureAwait(false);
            Assert.NotNull(pack);
            var packId = pack.PackId;
            var priorIndexedLength = await metadata.GetPackIndexedLengthAsync(packId, CancellationToken.None).ConfigureAwait(false);
            recorder.Reset();

            FillUntilNoSpace(fillerPath, 8 * 1024);
            var attempted = container.GetBlobClient("interrupted.bin");
            var failure = await Assert.ThrowsAsync<RequestFailedException>(() =>
                attempted.UploadAsync(BinaryData.FromBytes(attemptedBytes))).ConfigureAwait(false);
            Assert.Equal(500, failure.Status);
            Assert.True(recorder.StagingCompleted);
            Assert.True(recorder.PackAppendStarted);
            Assert.Equal(1, await metadata.CountPackedChunksAsync(CancellationToken.None).ConfigureAwait(false));
            Assert.Equal(priorIndexedLength, await metadata.GetPackIndexedLengthAsync(packId, CancellationToken.None).ConfigureAwait(false));
            File.Delete(fillerPath);

            Assert.False((await attempted.ExistsAsync().ConfigureAwait(false)).Value);
            Assert.Equal(stableBytes, (await stable.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
            return packId;
        }
        finally
        {
            await first.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task AssertPackAppendRecoversAsync(
        string dataPath,
        string packId,
        byte[] stableBytes,
        byte[] attemptedBytes,
        Dictionary<string, string?> configuration)
    {
        var restarted = CreateHarness(dataPath, configuration);
        try
        {
            await restarted.InitializeAsync().ConfigureAwait(false);
            var container = CreateClient(restarted).GetBlobContainerClient("enospc-pack");
            Assert.Equal(stableBytes,
                (await container.GetBlobClient("stable.bin").DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
            var attempted = container.GetBlobClient("interrupted.bin");
            await attempted.UploadAsync(BinaryData.FromBytes(attemptedBytes)).ConfigureAwait(false);
            Assert.Equal(attemptedBytes, (await attempted.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
            var metadata = restarted.Services.GetRequiredService<MetadataStore>();
            Assert.Equal(2, await metadata.CountPackedChunksAsync(CancellationToken.None).ConfigureAwait(false));
            var packPath = Path.Combine(dataPath, "packs", packId.Replace('/', Path.DirectorySeparatorChar) + ".pack");
            Assert.Equal(await metadata.GetPackIndexedLengthAsync(packId, CancellationToken.None).ConfigureAwait(false), new FileInfo(packPath).Length);
        }
        finally
        {
            await restarted.DisposeAsync().ConfigureAwait(false);
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

        var seed = await AssertPackCompactionRejectedAsync(
            dataPath, fillerPath, liveBytes, configuration).ConfigureAwait(true);
        await AssertPackCompactionRecoversAsync(
            dataPath, liveBytes, seed, configuration).ConfigureAwait(true);
    }

    private static async Task<CompactionIdentity> AssertPackCompactionRejectedAsync(
        string dataPath,
        string fillerPath,
        byte[] liveBytes,
        Dictionary<string, string?> configuration)
    {
        var first = CreateHarness(dataPath, configuration);
        try
        {
            await first.InitializeAsync().ConfigureAwait(false);
            var seed = await SeedCompactionAsync(first, dataPath, liveBytes).ConfigureAwait(false);
            var metadata = first.Services.GetRequiredService<MetadataStore>();
            var chunks = first.Services.GetRequiredService<ChunkStore>();

            FillUntilNoSpace(fillerPath, 4096);
            var failure = await Assert.ThrowsAsync<IOException>(() =>
                chunks.TryCompactPackAsync(seed.Pack, CancellationToken.None)).ConfigureAwait(false);
            Assert.Contains("No space left on device", failure.Message, StringComparison.OrdinalIgnoreCase);
            File.Delete(fillerPath);

            Assert.Equal(seed.OldPackLength, new FileInfo(seed.PackPath).Length);
            Assert.Equal(seed.Identity.PackId,
                (await metadata.GetPackedChunkLocationAsync(seed.Identity.ChunkId, CancellationToken.None).ConfigureAwait(false))?.PackId);
            Assert.Empty(Directory.EnumerateFiles(
                Path.Combine(dataPath, "staging"), "pack-compact-*.tmp"));
            Assert.Equal(liveBytes, (await seed.Live.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
            return seed.Identity;
        }
        finally
        {
            await first.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task<CompactionSeed> SeedCompactionAsync(
        SavaWebApplicationFactory first,
        string dataPath,
        byte[] liveBytes)
    {
        var container = CreateClient(first).GetBlobContainerClient("enospc-compaction");
        await container.CreateAsync().ConfigureAwait(false);
        var deleted = container.GetBlobClient("deleted.bin");
        var live = container.GetBlobClient("live.bin");
        await deleted.UploadAsync(BinaryData.FromBytes(RandomNumberGenerator.GetBytes(4096))).ConfigureAwait(false);
        await live.UploadAsync(BinaryData.FromBytes(liveBytes)).ConfigureAwait(false);
        await deleted.DeleteAsync().ConfigureAwait(false);

        var service = first.Services.GetRequiredService<BlobService>();
        var metadata = first.Services.GetRequiredService<MetadataStore>();
        Assert.True(await service.CollectGarbageAsync(CancellationToken.None).ConfigureAwait(false) > 0);
        var liveRecord = await service.GetBlobAsync(
            SavaWebApplicationFactory.AccountName, container.Name, live.Name,
            versionId: null, snapshot: null, includeDeleted: false, CancellationToken.None).ConfigureAwait(false);
        var chunkId = Assert.Single(liveRecord.Content.Chunks).Id;
        var location = await metadata.GetPackedChunkLocationAsync(chunkId, CancellationToken.None).ConfigureAwait(false);
        Assert.NotNull(location);
        var pack = Assert.Single((await metadata.ListSealedChunkPacksAsync(
            after: null, maximum: 16, CancellationToken.None).ConfigureAwait(false)).Items);
        Assert.Equal(location.PackId, pack.PackId);
        var packPath = Path.Combine(dataPath, "packs", pack.PackId.Replace('/', Path.DirectorySeparatorChar) + ".pack");
        return new CompactionSeed(live, pack, new CompactionIdentity(pack.PackId, chunkId),
            packPath, new FileInfo(packPath).Length);
    }

    private static async Task AssertPackCompactionRecoversAsync(
        string dataPath,
        byte[] liveBytes,
        CompactionIdentity seed,
        Dictionary<string, string?> configuration)
    {
        var restarted = CreateHarness(dataPath, configuration);
        try
        {
            await restarted.InitializeAsync().ConfigureAwait(false);
            var live = CreateClient(restarted)
                .GetBlobContainerClient("enospc-compaction")
                .GetBlobClient("live.bin");
            Assert.Equal(liveBytes, (await live.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
            var metadata = restarted.Services.GetRequiredService<MetadataStore>();
            var pack = Assert.Single((await metadata.ListSealedChunkPacksAsync(
                after: null,
                maximum: 16,
                CancellationToken.None).ConfigureAwait(false)).Items);
            Assert.Equal(seed.PackId, pack.PackId);
            var compacted = await restarted.Services.GetRequiredService<ChunkStore>()
                .TryCompactPackAsync(pack, CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(1, compacted.CompactedPacks);
            Assert.Equal(liveBytes, (await live.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
            Assert.NotEqual(seed.PackId,
                (await metadata.GetPackedChunkLocationAsync(seed.ChunkId, CancellationToken.None).ConfigureAwait(false))?.PackId,
                StringComparer.Ordinal);
        }
        finally
        {
            await restarted.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static SavaWebApplicationFactory CreateHarness(
        string dataPath,
        Dictionary<string, string?> configuration,
        IStorageFaultInjector? injector = null) =>
        new(dataPath, injector ?? new NullStorageFaultInjector(), analyticsSink: null,
            configurationOverrides: configuration, deleteDataPath: false, disableMaintenance: true);

    private sealed record MetadataFailureObservation(IReadOnlyList<string> Acknowledged, string FailedName);

    private sealed record BlobMetadataFailureObservation(string State, ETag ETag);

    private sealed record CompactionIdentity(string PackId, string ChunkId);

    private sealed record CompactionSeed(
        BlobClient Live, ChunkPackRecord Pack, CompactionIdentity Identity, string PackPath, long OldPackLength);

    private static string ValidateMountRoot(string dataPath)
    {
        var fullPath = Path.GetFullPath(dataPath);
        var mountRoot = Path.GetDirectoryName(fullPath)
                        ?? throw new InvalidOperationException("The ENOSPC data path has no mount root.");
        if (!string.Equals(Path.GetFileName(fullPath), "data", StringComparison.Ordinal) ||
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
                Transport = new HttpClientTransport(application.Server.CreateHandler()),
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

    private sealed class EnospcAtCommitFaultInjector(string fillerPath) : IStorageFaultInjector
    {
        private int _armed;
        private int _filled;

        public bool Filled => Volatile.Read(ref _filled) != 0;

        public void Arm() => Interlocked.Exchange(ref _armed, 1);

        public void Inject(StorageFaultPoint point)
        {
            if (point != StorageFaultPoint.BeforeBlobMetadataCommit ||
                Interlocked.Exchange(ref _armed, 0) != 1)
                return;
            FillUntilNoSpace(fillerPath, releaseBytes: 0);
            Interlocked.Exchange(ref _filled, 1);
        }
    }
}
