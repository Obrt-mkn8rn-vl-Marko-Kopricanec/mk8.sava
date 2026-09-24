using System.Security.Cryptography;
using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageKeyRotationTests
{
    [Fact]
    public async Task FirstDataKeyBindingRejectsASecondPhysicalChunkEncryptedWithAnotherKey()
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-mixed-key-{Guid.NewGuid():N}");
        var foreignPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-foreign-key-{Guid.NewGuid():N}");
        var account = SavaWebApplicationFactory.AccountName;
        var containerName = $"mixed-key-{Guid.NewGuid():N}";
        var retainedBytes = RandomNumberGenerator.GetBytes(80);
        var orphanBytes = RandomNumberGenerator.GetBytes(80);
        var foreignKey = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
        string orphanChunkPath = string.Empty;
        try
        {
            var baseConfiguration = new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sava:EnableSmallChunkPacking"] = "false",
                ["Sava:MaintenanceScanInterval"] = "01:00:00"
            };
            {
                var first = new SavaWebApplicationFactory(dataPath, baseConfiguration, deleteDataPath: false);
                await using (first.ConfigureAwait(false))
                {
                    await first.InitializeAsync();
                    var container = CreateClient(first, SavaWebApplicationFactory.AccountKey)
                        .GetBlobContainerClient(containerName);
                    await container.CreateAsync();
                    await container.GetBlobClient("retained.bin").UploadAsync(BinaryData.FromBytes(retainedBytes));
                }
            }

            orphanChunkPath = await CopyForeignOrphanChunkAsync(
                dataPath, foreignPath, account, foreignKey, orphanBytes, baseConfiguration).ConfigureAwait(true);
            await ClearDataKeyFingerprintsAsync(dataPath).ConfigureAwait(true);

            {
                var rejected = new SavaWebApplicationFactory(dataPath, baseConfiguration, deleteDataPath: false);
                await using (rejected.ConfigureAwait(false))
                {
                    var failure = await Assert.ThrowsAsync<InvalidDataException>(rejected.InitializeAsync)
                        .ConfigureAwait(true);
                    Assert.Contains("data encryption key continuity", failure.Message, StringComparison.Ordinal);
                    Assert.Contains("Corrupt", failure.Message, StringComparison.Ordinal);
                }
            }

            File.Delete(orphanChunkPath);
            var recovered = new SavaWebApplicationFactory(dataPath, baseConfiguration, deleteDataPath: false);
            await using var recoveredDisposal1 = recovered.ConfigureAwait(false);
            await recovered.InitializeAsync().ConfigureAwait(true);
            var retained = await CreateClient(recovered, SavaWebApplicationFactory.AccountKey)
                .GetBlobContainerClient(containerName)
                .GetBlobClient("retained.bin")
                .DownloadContentAsync().ConfigureAwait(true);
            Assert.Equal(retainedBytes, retained.Value.Content.ToArray());
        }
        finally
        {
            if (Directory.Exists(dataPath))
                await SavaWebApplicationFactory.DeleteDataPathAsync(dataPath).ConfigureAwait(true);
            if (Directory.Exists(foreignPath))
                await SavaWebApplicationFactory.DeleteDataPathAsync(foreignPath).ConfigureAwait(true);
        }
    }

    private static async Task<string> CopyForeignOrphanChunkAsync(
        string dataPath,
        string foreignPath,
        string account,
        string foreignKey,
        byte[] orphanBytes,
        Dictionary<string, string?> baseConfiguration)
    {
        var foreign = new SavaWebApplicationFactory(
            foreignPath,
            new NullStorageFaultInjector(),
            analyticsSink: null,
            configurationOverrides: new Dictionary<string, string?>(baseConfiguration, StringComparer.Ordinal)
            {
                [$"Sava:DataEncryptionKeys:{account}"] = foreignKey
            },
            deleteDataPath: false,
            disableMaintenance: true);
        await using var disposal = foreign.ConfigureAwait(false);
        await foreign.InitializeAsync().ConfigureAwait(false);
        var chunks = foreign.Services.GetRequiredService<ChunkStore>();
        using var stored = await chunks.StorePinnedAsync(
            account,
            new BlobEncryption(Scope: null, CustomerProvidedKeySha256: null),
            new MemoryStream(orphanBytes, writable: false),
            CancellationToken.None).ConfigureAwait(false);
        var id = Assert.Single(stored.Manifest.Chunks).Id;
        var relative = id.Replace('/', Path.DirectorySeparatorChar) + ".chunk";
        var orphanChunkPath = Path.Combine(dataPath, "chunks", relative);
        Directory.CreateDirectory(Path.GetDirectoryName(orphanChunkPath)!);
        File.Copy(Path.Combine(foreignPath, "chunks", relative), orphanChunkPath);
        return orphanChunkPath;
    }

    private static async Task ClearDataKeyFingerprintsAsync(string dataPath)
    {
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = Path.Combine(dataPath, "metadata.db"),
            Pooling = false
        }.ToString());
        await using var disposal = connection.ConfigureAwait(false);
        await connection.OpenAsync().ConfigureAwait(false);
        var clear = connection.CreateCommand();
        await using var clearDisposal = clear.ConfigureAwait(false);
        clear.CommandText = "DELETE FROM data_encryption_keys;";
        await clear.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    [Fact]
    public async Task StartupRejectsChangedDataKeyWhileCrashOrphanExtentStillExists()
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-orphan-key-{Guid.NewGuid():N}");
        var containerName = $"orphan-key-{Guid.NewGuid():N}";
        var content = RandomNumberGenerator.GetBytes(64 * 1024);
        var rotatedDataKey = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
        try
        {
            await CreateCrashOrphanExtentAsync(dataPath, containerName, content).ConfigureAwait(true);

            await AssertRotatedKeyRejectedAsync(dataPath, rotatedDataKey).ConfigureAwait(true);

            {
                var cleanup = new SavaWebApplicationFactory(
                             dataPath,
                             new NullStorageFaultInjector(),
                             analyticsSink: null,
                             configurationOverrides: null,
                             deleteDataPath: false,
                             disableMaintenance: true);
                await using (cleanup.ConfigureAwait(false))
                {
                    await cleanup.InitializeAsync().ConfigureAwait(true);
                    var service = cleanup.Services.GetRequiredService<BlobService>();
                    Assert.True(await service.CollectGarbageAsync(CancellationToken.None).ConfigureAwait(true) > 0);
                    Assert.Empty(Directory.EnumerateFiles(
                        Path.Combine(dataPath, "chunks"),
                        "*.chunk",
                        SearchOption.AllDirectories));
                }
            }

            var rotated = new SavaWebApplicationFactory(
                dataPath,
                new Dictionary<string, string?>(StringComparer.Ordinal)
                {
                    [$"Sava:DataEncryptionKeys:{SavaWebApplicationFactory.AccountName}"] = rotatedDataKey
                },
                deleteDataPath: true);
            await using var rotatedDisposal2 = rotated.ConfigureAwait(false);
            await rotated.InitializeAsync().ConfigureAwait(true);
            var blob = CreateClient(rotated, SavaWebApplicationFactory.AccountKey)
                .GetBlobContainerClient(containerName)
                .GetBlobClient("payload.bin");
            await blob.UploadAsync(BinaryData.FromBytes(content)).ConfigureAwait(true);
            Assert.Equal(content, (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());
        }
        finally
        {
            if (Directory.Exists(dataPath))
                await SavaWebApplicationFactory.DeleteDataPathAsync(dataPath).ConfigureAwait(true);
        }
    }

    private static async Task AssertRotatedKeyRejectedAsync(string dataPath, string rotatedDataKey)
    {
        var wrong = new SavaWebApplicationFactory(
            dataPath,
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:DataEncryptionKeys:{SavaWebApplicationFactory.AccountName}"] = rotatedDataKey
            },
            deleteDataPath: false);
        await using var disposal = wrong.ConfigureAwait(false);
        var failure = await Assert.ThrowsAsync<InvalidDataException>(wrong.InitializeAsync).ConfigureAwait(false);
        Assert.Contains("data encryption key continuity", failure.Message, StringComparison.Ordinal);
    }

    private static async Task CreateCrashOrphanExtentAsync(string dataPath, string containerName, byte[] content)
    {
        var injector = new SingleMetadataCommitFaultInjector();
        var first = new SavaWebApplicationFactory(
            dataPath,
            injector,
            analyticsSink: null,
            configurationOverrides: new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sava:EnableSmallChunkPacking"] = "false",
                ["Sava:MaintenanceScanInterval"] = "01:00:00"
            },
            deleteDataPath: false,
            disableMaintenance: true);
        await using var disposal = first.ConfigureAwait(false);
        await first.InitializeAsync().ConfigureAwait(false);
        var container = CreateClient(first, SavaWebApplicationFactory.AccountKey)
            .GetBlobContainerClient(containerName);
        await container.CreateAsync().ConfigureAwait(false);
        injector.Arm();
        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient("payload.bin").UploadAsync(BinaryData.FromBytes(content))).ConfigureAwait(false);
        Assert.Equal(500, rejected.Status);
        Assert.False((await container.GetBlobClient("payload.bin").ExistsAsync().ConfigureAwait(false)).Value);
        Assert.Empty((await first.Services.GetRequiredService<MetadataStore>()
            .GetStorageInventoryAsync(CancellationToken.None).ConfigureAwait(false)).ReachableChunkIds);
        Assert.NotEmpty(Directory.EnumerateFiles(
            Path.Combine(dataPath, "chunks"), "*.chunk", SearchOption.AllDirectories));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StartupRejectsChangedDataKeyBeforeServingExistingContent(bool fingerprintRecorded)
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-key-continuity-{Guid.NewGuid():N}");
        var account = SavaWebApplicationFactory.AccountName;
        var containerName = $"key-continuity-{Guid.NewGuid():N}";
        var content = RandomNumberGenerator.GetBytes(64 * 1024);
        try
        {
            {
                var initial = new SavaWebApplicationFactory(dataPath, deleteDataPath: false);
                await using (initial.ConfigureAwait(false))
                {
                    await initial.InitializeAsync();
                    var container = CreateClient(initial, SavaWebApplicationFactory.AccountKey)
                        .GetBlobContainerClient(containerName);
                    await container.CreateAsync();
                    await container.GetBlobClient("retained.bin").UploadAsync(BinaryData.FromBytes(content));
                }
            }

            if (fingerprintRecorded)
            {
                var recorded = new SavaWebApplicationFactory(dataPath, deleteDataPath: false);
                await using var recordedDisposal3 = recorded.ConfigureAwait(false);
                await recorded.InitializeAsync();
                Assert.Equal(content, (await CreateClient(recorded, SavaWebApplicationFactory.AccountKey)
                    .GetBlobContainerClient(containerName)
                    .GetBlobClient("retained.bin")
                    .DownloadContentAsync()).Value.Content.ToArray());
            }

            {
                var wrong = new SavaWebApplicationFactory(
                             dataPath,
                             new Dictionary<string, string?>(StringComparer.Ordinal)
                             {
                                 [$"Sava:DataEncryptionKeys:{account}"] =
                                     Convert.ToBase64String(RandomNumberGenerator.GetBytes(32))
                             },
                             deleteDataPath: false);
                await using (wrong.ConfigureAwait(false))
                {
                    var failure = await Assert.ThrowsAsync<InvalidDataException>(wrong.InitializeAsync);
                    Assert.Contains("data encryption key", failure.Message, StringComparison.OrdinalIgnoreCase);
                }
            }

            var recovered = new SavaWebApplicationFactory(dataPath, deleteDataPath: true);
            await using var recoveredDisposal4 = recovered.ConfigureAwait(false);
            await recovered.InitializeAsync();
            Assert.Equal(content, (await CreateClient(recovered, SavaWebApplicationFactory.AccountKey)
                .GetBlobContainerClient(containerName)
                .GetBlobClient("retained.bin")
                .DownloadContentAsync()).Value.Content.ToArray());
        }
        finally
        {
            if (Directory.Exists(dataPath))
                await SavaWebApplicationFactory.DeleteDataPathAsync(dataPath).ConfigureAwait(true);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StableDataKeyPreservesBlobsAndDeduplicationAcrossAccountCredentialRotation(
        bool independentDataKeyFromFirstWrite)
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-key-rotation-{Guid.NewGuid():N}");
        var backupPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-key-rotation-backup-{Guid.NewGuid():N}");
        var account = SavaWebApplicationFactory.AccountName;
        var oldCredential = SavaWebApplicationFactory.AccountKey;
        var newCredential = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
        var dataKey = independentDataKeyFromFirstWrite
            ? Convert.ToBase64String(RandomNumberGenerator.GetBytes(32))
            : oldCredential;
        var firstConfiguration = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        };
        if (independentDataKeyFromFirstWrite)
            firstConfiguration[$"Sava:DataEncryptionKeys:{account}"] = dataKey;
        var content = RandomNumberGenerator.GetBytes(96 * 1024);
        var containerName = $"key-rotation-{Guid.NewGuid():N}";

        try
        {
            await CreateBeforeRotationAsync(
                dataPath, firstConfiguration, oldCredential, containerName, content).ConfigureAwait(true);

            await AssertCredentialRotationAsync(
                dataPath, backupPath, account, oldCredential, newCredential,
                dataKey, containerName, content).ConfigureAwait(true);
        }
        finally
        {
            if (Directory.Exists(dataPath))
                await SavaWebApplicationFactory.DeleteDataPathAsync(dataPath).ConfigureAwait(true);
            if (Directory.Exists(backupPath))
                Directory.Delete(backupPath, recursive: true);
        }
    }

    private static async Task AssertCredentialRotationAsync(
        string dataPath,
        string backupPath,
        string account,
        string oldCredential,
        string newCredential,
        string dataKey,
        string containerName,
        byte[] content)
    {
        var restarted = new SavaWebApplicationFactory(
            dataPath,
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:Accounts:{account}"] = newCredential,
                [$"Sava:DataEncryptionKeys:{account}"] = dataKey,
                ["Sava:MaintenanceScanInterval"] = "01:00:00"
            },
            deleteDataPath: true);
        await using var disposal = restarted.ConfigureAwait(false);
        await restarted.InitializeAsync().ConfigureAwait(false);
        var client = CreateClient(restarted, newCredential);
        var container = client.GetBlobContainerClient(containerName);
        var before = container.GetBlobClient("before.bin");
        Assert.Equal(content, (await before.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());

        var inventoryBefore = await restarted.Services.GetRequiredService<MetadataStore>()
            .GetStorageInventoryAsync(CancellationToken.None).ConfigureAwait(false);
        await container.GetBlobClient("after.bin").UploadAsync(BinaryData.FromBytes(content)).ConfigureAwait(false);
        var inventoryAfter = await restarted.Services.GetRequiredService<MetadataStore>()
            .GetStorageInventoryAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.True(inventoryBefore.ReachableChunkIds.SetEquals(inventoryAfter.ReachableChunkIds));
        Assert.Equal(content, (await container.GetBlobClient("after.bin").DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToArray());

        var denied = await Assert.ThrowsAsync<RequestFailedException>(() =>
            CreateClient(restarted, oldCredential)
                .GetBlobContainerClient(containerName)
                .GetBlobClient("before.bin")
                .DownloadContentAsync()).ConfigureAwait(false);
        Assert.Equal(403, denied.Status);
        await ValidateRotatedBackupAsync(restarted, backupPath, account, newCredential, dataKey).ConfigureAwait(false);
    }

    private static async Task CreateBeforeRotationAsync(
        string dataPath,
        Dictionary<string, string?> firstConfiguration,
        string oldCredential,
        string containerName,
        byte[] content)
    {
        var first = new SavaWebApplicationFactory(dataPath, firstConfiguration, deleteDataPath: false);
        await using var disposal = first.ConfigureAwait(false);
        await first.InitializeAsync().ConfigureAwait(false);
        var container = CreateClient(first, oldCredential).GetBlobContainerClient(containerName);
        await container.CreateAsync().ConfigureAwait(false);
        await container.GetBlobClient("before.bin").UploadAsync(BinaryData.FromBytes(content)).ConfigureAwait(false);
    }

    private static async Task ValidateRotatedBackupAsync(
        SavaWebApplicationFactory restarted,
        string backupPath,
        string account,
        string newCredential,
        string dataKey)
    {
        var backup = restarted.Services.GetRequiredService<StorageBackupService>();
        await backup.CreateAsync(backupPath, CancellationToken.None).ConfigureAwait(false);
        var validOptions = new SavaOptions
        {
            Accounts = new Dictionary<string, string>(StringComparer.Ordinal) { [account] = newCredential },
            DataEncryptionKeys = new Dictionary<string, string>(StringComparer.Ordinal) { [account] = dataKey }
        };
        await StorageBackupService.ValidateBackupAsync(backupPath, validOptions, CancellationToken.None).ConfigureAwait(false);
        var wrongOptions = new SavaOptions
        {
            Accounts = validOptions.Accounts,
            DataEncryptionKeys = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [account] = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32))
            }
        };
        await Assert.ThrowsAsync<InvalidDataException>(() =>
            StorageBackupService.ValidateBackupAsync(backupPath, wrongOptions, CancellationToken.None)).ConfigureAwait(false);
    }

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory app, string credential)
    {
        var account = SavaWebApplicationFactory.AccountName;
        var endpoint = new Uri($"http://{account}.localhost");
        return new BlobServiceClient(
            endpoint,
            new StorageSharedKeyCredential(account, credential),
            new BlobClientOptions
            {
                Transport = new HttpClientTransport(app.Server.CreateHandler()),
                Retry = { MaxRetries = 0 }
            });
    }

    private sealed class SingleMetadataCommitFaultInjector : IStorageFaultInjector
    {
        private int _armed;

        public void Arm() => Interlocked.Exchange(ref _armed, 1);

        public void Inject(StorageFaultPoint point)
        {
            if (point == StorageFaultPoint.BeforeBlobMetadataCommit && Interlocked.Exchange(ref _armed, 0) == 1)
                throw new IOException("Injected metadata commit failure after chunk publication.");
        }
    }
}
