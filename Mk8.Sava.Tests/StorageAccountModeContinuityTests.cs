using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageAccountModeContinuityTests
{
    [Fact]
    public async Task BackupValidationRejectsConflictingNamespaceModeBeforeRestorePublication()
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-mode-backup-source-{Guid.NewGuid():N}");
        var backupPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-mode-backup-{Guid.NewGuid():N}");
        var restoredPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-mode-backup-target-{Guid.NewGuid():N}");
        var containerName = $"mode-backup-{Guid.NewGuid():N}";
        var content = new byte[] { 7, 0, 255, 8 };
        try
        {
            await using (var source = CreateFactory(dataPath, hierarchicalMode: true, deleteDataPath: false))
            {
                await source.InitializeAsync();
                var container = CreateClient(source).GetBlobContainerClient(containerName);
                await container.CreateAsync();
                await container.GetBlobClient("retained.bin").UploadAsync(BinaryData.FromBytes(content));
                await source.Services.GetRequiredService<StorageBackupService>()
                    .CreateAsync(backupPath, CancellationToken.None);
            }

            var wrong = CreateOptions(hierarchicalMode: false);
            var failure = await Assert.ThrowsAsync<InvalidDataException>(() =>
                StorageBackupService.ValidateBackupAsync(backupPath, wrong, CancellationToken.None));
            Assert.Contains("hierarchical namespace mode", failure.Message, StringComparison.Ordinal);
            await Assert.ThrowsAsync<InvalidDataException>(() =>
                StorageBackupService.RestoreAsync(backupPath, restoredPath, wrong, CancellationToken.None));
            Assert.False(Directory.Exists(restoredPath));

            var correct = CreateOptions(hierarchicalMode: true);
            await StorageBackupService.ValidateBackupAsync(backupPath, correct, CancellationToken.None);
            await StorageBackupService.RestoreAsync(backupPath, restoredPath, correct, CancellationToken.None);
            await using var recovered = CreateFactory(restoredPath, hierarchicalMode: true, deleteDataPath: true);
            await recovered.InitializeAsync();
            var downloaded = await CreateClient(recovered)
                .GetBlobContainerClient(containerName)
                .GetBlobClient("retained.bin")
                .DownloadContentAsync();
            Assert.Equal(content, downloaded.Value.Content.ToArray());
        }
        finally
        {
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, recursive: true);
            if (Directory.Exists(backupPath))
                Directory.Delete(backupPath, recursive: true);
            if (Directory.Exists(restoredPath))
                Directory.Delete(restoredPath, recursive: true);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RestartRejectsChangedHierarchicalNamespaceModeWithoutLosingData(bool initialMode)
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-account-mode-{Guid.NewGuid():N}");
        var containerName = $"mode-{Guid.NewGuid():N}";
        var content = new byte[] { 0, 1, 2, 255, 3 };
        try
        {
            await using (var first = CreateFactory(dataPath, initialMode, deleteDataPath: false))
            {
                await first.InitializeAsync();
                var container = CreateClient(first).GetBlobContainerClient(containerName);
                await container.CreateAsync();
                await container.GetBlobClient("original.bin").UploadAsync(BinaryData.FromBytes(content));
            }

            await using (var changed = CreateFactory(dataPath, !initialMode, deleteDataPath: false))
            {
                var failure = await Assert.ThrowsAsync<InvalidDataException>(changed.InitializeAsync);
                Assert.Contains("hierarchical namespace mode", failure.Message, StringComparison.Ordinal);
            }

            await using var recovered = CreateFactory(dataPath, initialMode, deleteDataPath: true);
            await recovered.InitializeAsync();
            var downloaded = await CreateClient(recovered)
                .GetBlobContainerClient(containerName)
                .GetBlobClient("original.bin")
                .DownloadContentAsync();
            Assert.Equal(content, downloaded.Value.Content.ToArray());
        }
        finally
        {
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, recursive: true);
        }
    }

    private static SavaWebApplicationFactory CreateFactory(string dataPath, bool hierarchicalMode, bool deleteDataPath) =>
        new(dataPath, new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] =
                hierarchicalMode.ToString()
        }, deleteDataPath);

    private static SavaOptions CreateOptions(bool hierarchicalMode) => new()
    {
        Accounts = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SavaWebApplicationFactory.AccountName] = SavaWebApplicationFactory.AccountKey
        },
        AccountCapabilities = new Dictionary<string, StorageAccountCapabilities>(StringComparer.Ordinal)
        {
            [SavaWebApplicationFactory.AccountName] = new()
            {
                HierarchicalNamespaceEnabled = hierarchicalMode
            }
        }
    };

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory app)
    {
        var account = SavaWebApplicationFactory.AccountName;
        var endpoint = new Uri($"http://{account}.localhost");
        return new BlobServiceClient(
            endpoint,
            new StorageSharedKeyCredential(account, SavaWebApplicationFactory.AccountKey),
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
