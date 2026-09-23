using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;

namespace Mk8.Sava.Tests;

public sealed class StorageAccountModeContinuityTests
{
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
        new(dataPath, new Dictionary<string, string?>
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] =
                hierarchicalMode.ToString()
        }, deleteDataPath);

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
