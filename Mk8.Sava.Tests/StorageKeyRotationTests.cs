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
            await using (var first = new SavaWebApplicationFactory(dataPath, baseConfiguration, deleteDataPath: false))
            {
                await first.InitializeAsync();
                var container = CreateClient(first, SavaWebApplicationFactory.AccountKey)
                    .GetBlobContainerClient(containerName);
                await container.CreateAsync();
                await container.GetBlobClient("retained.bin").UploadAsync(BinaryData.FromBytes(retainedBytes));
            }

            await using (var foreign = new SavaWebApplicationFactory(
                             foreignPath,
                             new NullStorageFaultInjector(),
                             analyticsSink: null,
                             configurationOverrides: new Dictionary<string, string?>(baseConfiguration, StringComparer.Ordinal)
                             {
                                 [$"Sava:DataEncryptionKeys:{account}"] = foreignKey
                             },
                             deleteDataPath: false,
                             disableMaintenance: true))
            {
                await foreign.InitializeAsync();
                var chunks = foreign.Services.GetRequiredService<ChunkStore>();
                using var stored = await chunks.StorePinnedAsync(
                    account,
                    new BlobEncryption(Scope: null, CustomerProvidedKeySha256: null),
                    new MemoryStream(orphanBytes, writable: false),
                    CancellationToken.None);
                var id = Assert.Single(stored.Manifest.Chunks).Id;
                var relative = id.Replace('/', Path.DirectorySeparatorChar) + ".chunk";
                orphanChunkPath = Path.Combine(dataPath, "chunks", relative);
                Directory.CreateDirectory(Path.GetDirectoryName(orphanChunkPath)!);
                File.Copy(Path.Combine(foreignPath, "chunks", relative), orphanChunkPath);
            }

            await using (var connection = new SqliteConnection($"Data Source={Path.Combine(dataPath, "metadata.db")}"))
            {
                await connection.OpenAsync();
                await using var clear = connection.CreateCommand();
                clear.CommandText = "DELETE FROM data_encryption_keys;";
                await clear.ExecuteNonQueryAsync();
            }

            await using (var rejected = new SavaWebApplicationFactory(dataPath, baseConfiguration, deleteDataPath: false))
            {
                var failure = await Assert.ThrowsAsync<InvalidDataException>(rejected.InitializeAsync);
                Assert.Contains("data encryption key continuity", failure.Message, StringComparison.Ordinal);
                Assert.Contains("Corrupt", failure.Message, StringComparison.Ordinal);
            }

            File.Delete(orphanChunkPath);
            await using var recovered = new SavaWebApplicationFactory(dataPath, baseConfiguration, deleteDataPath: false);
            await recovered.InitializeAsync();
            var retained = await CreateClient(recovered, SavaWebApplicationFactory.AccountKey)
                .GetBlobContainerClient(containerName)
                .GetBlobClient("retained.bin")
                .DownloadContentAsync();
            Assert.Equal(retainedBytes, retained.Value.Content.ToArray());
        }
        finally
        {
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, recursive: true);
            if (Directory.Exists(foreignPath))
                Directory.Delete(foreignPath, recursive: true);
        }
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
            var injector = new SingleMetadataCommitFaultInjector();
            await using (var first = new SavaWebApplicationFactory(
                             dataPath,
                             injector,
                             analyticsSink: null,
                             configurationOverrides: new Dictionary<string, string?>(StringComparer.Ordinal)
                             {
                                 ["Sava:EnableSmallChunkPacking"] = "false",
                                 ["Sava:MaintenanceScanInterval"] = "01:00:00"
                             },
                             deleteDataPath: false,
                             disableMaintenance: true))
            {
                await first.InitializeAsync();
                var container = CreateClient(first, SavaWebApplicationFactory.AccountKey)
                    .GetBlobContainerClient(containerName);
                await container.CreateAsync();
                injector.Arm();
                var rejected = await Assert.ThrowsAsync<RequestFailedException>(() =>
                    container.GetBlobClient("payload.bin").UploadAsync(BinaryData.FromBytes(content)));
                Assert.Equal(500, rejected.Status);
                Assert.False((await container.GetBlobClient("payload.bin").ExistsAsync()).Value);
                Assert.Empty((await first.Services.GetRequiredService<MetadataStore>()
                    .GetStorageInventoryAsync(CancellationToken.None)).ReachableChunkIds);
                Assert.NotEmpty(Directory.EnumerateFiles(
                    Path.Combine(dataPath, "chunks"),
                    "*.chunk",
                    SearchOption.AllDirectories));
            }

            await using (var wrong = new SavaWebApplicationFactory(
                             dataPath,
                             new Dictionary<string, string?>(StringComparer.Ordinal)
                             {
                                 [$"Sava:DataEncryptionKeys:{SavaWebApplicationFactory.AccountName}"] =
                                     rotatedDataKey
                             },
                             deleteDataPath: false))
            {
                var failure = await Assert.ThrowsAsync<InvalidDataException>(wrong.InitializeAsync);
                Assert.Contains("data encryption key continuity", failure.Message, StringComparison.Ordinal);
            }

            await using (var cleanup = new SavaWebApplicationFactory(
                             dataPath,
                             new NullStorageFaultInjector(),
                             analyticsSink: null,
                             configurationOverrides: null,
                             deleteDataPath: false,
                             disableMaintenance: true))
            {
                await cleanup.InitializeAsync();
                var service = cleanup.Services.GetRequiredService<BlobService>();
                Assert.True(await service.CollectGarbageAsync(CancellationToken.None) > 0);
                Assert.Empty(Directory.EnumerateFiles(
                    Path.Combine(dataPath, "chunks"),
                    "*.chunk",
                    SearchOption.AllDirectories));
            }

            await using var rotated = new SavaWebApplicationFactory(
                dataPath,
                new Dictionary<string, string?>(StringComparer.Ordinal)
                {
                    [$"Sava:DataEncryptionKeys:{SavaWebApplicationFactory.AccountName}"] = rotatedDataKey
                },
                deleteDataPath: true);
            await rotated.InitializeAsync();
            var blob = CreateClient(rotated, SavaWebApplicationFactory.AccountKey)
                .GetBlobContainerClient(containerName)
                .GetBlobClient("payload.bin");
            await blob.UploadAsync(BinaryData.FromBytes(content));
            Assert.Equal(content, (await blob.DownloadContentAsync()).Value.Content.ToArray());
        }
        finally
        {
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, recursive: true);
        }
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
            await using (var initial = new SavaWebApplicationFactory(dataPath, deleteDataPath: false))
            {
                await initial.InitializeAsync();
                var container = CreateClient(initial, SavaWebApplicationFactory.AccountKey)
                    .GetBlobContainerClient(containerName);
                await container.CreateAsync();
                await container.GetBlobClient("retained.bin").UploadAsync(BinaryData.FromBytes(content));
            }

            if (fingerprintRecorded)
            {
                await using var recorded = new SavaWebApplicationFactory(dataPath, deleteDataPath: false);
                await recorded.InitializeAsync();
                Assert.Equal(content, (await CreateClient(recorded, SavaWebApplicationFactory.AccountKey)
                    .GetBlobContainerClient(containerName)
                    .GetBlobClient("retained.bin")
                    .DownloadContentAsync()).Value.Content.ToArray());
            }

            await using (var wrong = new SavaWebApplicationFactory(
                             dataPath,
                             new Dictionary<string, string?>(StringComparer.Ordinal)
                             {
                                 [$"Sava:DataEncryptionKeys:{account}"] =
                                     Convert.ToBase64String(RandomNumberGenerator.GetBytes(32))
                             },
                             deleteDataPath: false))
            {
                var failure = await Assert.ThrowsAsync<InvalidDataException>(wrong.InitializeAsync);
                Assert.Contains("data encryption key", failure.Message, StringComparison.OrdinalIgnoreCase);
            }

            await using var recovered = new SavaWebApplicationFactory(dataPath, deleteDataPath: true);
            await recovered.InitializeAsync();
            Assert.Equal(content, (await CreateClient(recovered, SavaWebApplicationFactory.AccountKey)
                .GetBlobContainerClient(containerName)
                .GetBlobClient("retained.bin")
                .DownloadContentAsync()).Value.Content.ToArray());
        }
        finally
        {
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, recursive: true);
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
            await using (var first = new SavaWebApplicationFactory(
                             dataPath,
                             firstConfiguration,
                             deleteDataPath: false))
            {
                await first.InitializeAsync();
                var container = CreateClient(first, oldCredential).GetBlobContainerClient(containerName);
                await container.CreateAsync();
                await container.GetBlobClient("before.bin").UploadAsync(BinaryData.FromBytes(content));
            }

            await using (var restarted = new SavaWebApplicationFactory(
                             dataPath,
                             new Dictionary<string, string?>(StringComparer.Ordinal)
                             {
                                 [$"Sava:Accounts:{account}"] = newCredential,
                                 [$"Sava:DataEncryptionKeys:{account}"] = dataKey,
                                 ["Sava:MaintenanceScanInterval"] = "01:00:00"
                             },
                             deleteDataPath: true))
            {
                await restarted.InitializeAsync();
                var client = CreateClient(restarted, newCredential);
                var container = client.GetBlobContainerClient(containerName);
                var before = container.GetBlobClient("before.bin");
                Assert.Equal(content, (await before.DownloadContentAsync()).Value.Content.ToArray());

                var inventoryBefore = await restarted.Services
                    .GetRequiredService<MetadataStore>()
                    .GetStorageInventoryAsync(CancellationToken.None);
                await container.GetBlobClient("after.bin").UploadAsync(BinaryData.FromBytes(content));
                var inventoryAfter = await restarted.Services
                    .GetRequiredService<MetadataStore>()
                    .GetStorageInventoryAsync(CancellationToken.None);
                Assert.True(inventoryBefore.ReachableChunkIds.SetEquals(inventoryAfter.ReachableChunkIds));
                Assert.Equal(content, (await container.GetBlobClient("after.bin").DownloadContentAsync())
                    .Value.Content.ToArray());

                var denied = await Assert.ThrowsAsync<RequestFailedException>(() =>
                    CreateClient(restarted, oldCredential)
                        .GetBlobContainerClient(containerName)
                        .GetBlobClient("before.bin")
                        .DownloadContentAsync());
                Assert.Equal(403, denied.Status);

                var backup = restarted.Services.GetRequiredService<StorageBackupService>();
                await backup.CreateAsync(backupPath, CancellationToken.None);
                var validOptions = new SavaOptions
                {
                    Accounts = new Dictionary<string, string>(StringComparer.Ordinal) { [account] = newCredential },
                    DataEncryptionKeys = new Dictionary<string, string>(StringComparer.Ordinal) { [account] = dataKey }
                };
                await StorageBackupService.ValidateBackupAsync(backupPath, validOptions, CancellationToken.None);
                var wrongOptions = new SavaOptions
                {
                    Accounts = validOptions.Accounts,
                    DataEncryptionKeys = new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        [account] = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32))
                    }
                };
                await Assert.ThrowsAsync<InvalidDataException>(() =>
                    StorageBackupService.ValidateBackupAsync(backupPath, wrongOptions, CancellationToken.None));
            }
        }
        finally
        {
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, recursive: true);
            if (Directory.Exists(backupPath))
                Directory.Delete(backupPath, recursive: true);
        }
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
                Transport = new HttpClientTransport(new HttpClient(app.Server.CreateHandler())
                {
                    BaseAddress = endpoint
                }),
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
