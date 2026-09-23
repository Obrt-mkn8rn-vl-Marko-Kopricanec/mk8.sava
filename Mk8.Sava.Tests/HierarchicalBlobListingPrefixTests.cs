using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Mk8.Sava.Tests;

public sealed class HierarchicalBlobListingPrefixTests
{
    [Fact]
    public async Task HnsListRejectsFileInTheMiddleOfThePrefixPath()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
        await using var applicationDisposal = application.ConfigureAwait(false);
        var account = SavaWebApplicationFactory.SecondAccountName;
        var container = CreateClient(application, account, SavaWebApplicationFactory.SecondAccountKey)
            .GetBlobContainerClient($"hns-prefix-{Guid.NewGuid():N}");
        await container.CreateAsync();
        await container.GetBlobClient("file.txt").UploadAsync(BinaryData.FromString("root"));
        await container.GetBlobClient("directory/file.txt").UploadAsync(BinaryData.FromString("nested"));

        await AssertPrefixRejectedAsync(container, "file.txt/child");
        await AssertPrefixRejectedAsync(container, "directory/file.txt/child");
        Assert.Equal(["file.txt"], await ListNamesAsync(container, "file.txt"));
        Assert.Empty(await ListNamesAsync(container, "missing/child"));
    }

    [Fact]
    public async Task FlatListKeepsFileAndDescendantNamesAsIndependentBlobs()
    {
        var application = new SavaWebApplicationFactory();
        await using var applicationDisposal = application.ConfigureAwait(false);
        var container = CreateClient(application,
                SavaWebApplicationFactory.AccountName,
                SavaWebApplicationFactory.AccountKey)
            .GetBlobContainerClient($"flat-prefix-{Guid.NewGuid():N}");
        await container.CreateAsync();
        await container.GetBlobClient("file.txt").UploadAsync(BinaryData.FromString("root"));
        await container.GetBlobClient("file.txt/child").UploadAsync(BinaryData.FromString("nested"));

        Assert.Equal(["file.txt/child"], await ListNamesAsync(container, "file.txt/"));
    }

    private static async Task AssertPrefixRejectedAsync(BlobContainerClient container, string prefix)
    {
        var failure = await Assert.ThrowsAsync<RequestFailedException>(async () =>
            await ListNamesAsync(container, prefix).ConfigureAwait(false)).ConfigureAwait(false);
        Assert.Equal(409, failure.Status);
        Assert.Equal("PathAlreadyExists", failure.ErrorCode);
    }

    private static async Task<string[]> ListNamesAsync(BlobContainerClient container, string prefix)
    {
        var names = new List<string>();
        await foreach (BlobItem blob in container.GetBlobsAsync(
                           new GetBlobsOptions { Prefix = prefix }).ConfigureAwait(false))
            names.Add(blob.Name);
        return [.. names];
    }

    private static BlobServiceClient CreateClient(
        SavaWebApplicationFactory application, string account, string key) =>
        new(new Uri($"http://{account}.localhost"),
            new StorageSharedKeyCredential(account, key),
            new BlobClientOptions
            {
                Transport = new HttpClientTransport(application.Server.CreateHandler()),
                Retry = { MaxRetries = 0 }
            });
}
