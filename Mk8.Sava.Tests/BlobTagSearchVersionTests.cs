using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Mk8.Sava.Tests;

public sealed class BlobTagSearchVersionTests
{
    [Theory]
    [InlineData(BlobClientOptions.ServiceVersion.V2019_12_12, false)]
    [InlineData(BlobClientOptions.ServiceVersion.V2020_12_06, false)]
    [InlineData(BlobClientOptions.ServiceVersion.V2021_04_10, true)]
    public async Task ContainerSearchHonorsApril2021BoundaryWithoutRestrictingAccountSearch(
        BlobClientOptions.ServiceVersion serviceVersion, bool containerSupported)
    {
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(application, BlobClientOptions.ServiceVersion.V2023_11_03)
            .GetBlobContainerClient($"tag-search-version-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("match.bin");
        await blob.UploadAsync(BinaryData.FromString("payload"), new BlobUploadOptions
        {
            Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["phase"] = "match" }
        }).ConfigureAwait(true);
        var older = CreateClient(application, serviceVersion);
        var expression = $"@container = '{container.Name}' AND \"phase\" = 'match'";
        Assert.Equal([blob.Name], await ReadNamesAsync(older.FindBlobsByTagsAsync(expression)).ConfigureAwait(true));

        var scoped = older.GetBlobContainerClient(container.Name);
        if (containerSupported)
        {
            Assert.Equal([blob.Name], await ReadNamesAsync(scoped.FindBlobsByTagsAsync("\"phase\" = 'match'"))
                .ConfigureAwait(true));
        }
        else
        {
            var rejected = await Assert.ThrowsAsync<RequestFailedException>(() =>
                ReadNamesAsync(scoped.FindBlobsByTagsAsync("\"phase\" = 'match'"))).ConfigureAwait(true);
            Assert.Equal(409, rejected.Status);
            Assert.Equal("FeatureVersionMismatch", rejected.ErrorCode);
        }
        Assert.Equal("match", (await blob.GetTagsAsync().ConfigureAwait(true)).Value.Tags["phase"]);
        Assert.Equal("payload", (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    private static async Task<string[]> ReadNamesAsync(IAsyncEnumerable<TaggedBlobItem> items)
    {
        var names = new List<string>();
        await foreach (var item in items.ConfigureAwait(false))
            names.Add(item.BlobName);
        return names.Order(StringComparer.Ordinal).ToArray();
    }

    private static BlobServiceClient CreateClient(
        SavaWebApplicationFactory application, BlobClientOptions.ServiceVersion serviceVersion) =>
        new(new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions(serviceVersion)
            {
                Transport = new HttpClientTransport(application.Server.CreateHandler()),
                Retry = { MaxRetries = 0 }
            });
}
