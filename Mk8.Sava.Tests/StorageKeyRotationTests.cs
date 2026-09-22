using System.Security.Cryptography;
using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageKeyRotationTests
{
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
        var firstConfiguration = new Dictionary<string, string?>
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
                             new Dictionary<string, string?>
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
}
