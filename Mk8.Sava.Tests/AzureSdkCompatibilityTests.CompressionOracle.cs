using System.Security.Cryptography;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;

namespace Mk8.Sava.Tests;

public sealed partial class AzureSdkCompatibilityTests
{
    [Fact]
    public async Task CompressionOracleIsIndependentOfPreviouslyStoredSharedRootContent()
    {
        var unrelated = RandomNumberGenerator.GetBytes(256 * 1024);
        using var transport = new HttpClientTransport(factory.Server.CreateHandler());
        var owner = new BlobServiceClient(new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });
        var container = owner.GetBlobContainerClient($"compression-noise-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("unrelated.bin");
        await blob.UploadAsync(BinaryData.FromBytes(unrelated)).ConfigureAwait(true);
        var before = EnumerateChunkFiles(factory.DataPath).Sum(file => file.Length);
        Assert.True(before > 512 * 1024 / 4);

        // Run the maintained oracle after genuine unrelated storage, not a
        // fabricated file-length list or an invented old suite failure cause.
        await IdenticalContentIsCompressedAndPhysicallyDeduplicated().ConfigureAwait(true);

        Assert.Equal(before, EnumerateChunkFiles(factory.DataPath).Sum(file => file.Length));
        Assert.Equal(unrelated, (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());
    }
}
