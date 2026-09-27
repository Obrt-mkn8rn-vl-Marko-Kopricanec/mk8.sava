using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Mk8.Sava.Tests;

public sealed class BlobCopyPropertyTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task CompletedCopyPropertiesSurviveMetadataAndTagChanges(bool versioning, bool updateTags)
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:VersioningEnabled"] =
                versioning ? "true" : "false"
        });
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(application).GetBlobContainerClient($"copy-properties-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var source = container.GetBlobClient("source.bin");
        await source.UploadAsync(BinaryData.FromString("copied payload"), new BlobUploadOptions
        {
            Metadata = Values("original"),
            HttpHeaders = new BlobHttpHeaders { ContentType = "application/x-copy-properties" }
        }).ConfigureAwait(true);
        var destination = container.GetBlobClient("destination.bin");
        var copy = await destination.StartCopyFromUriAsync(source.Uri, new BlobCopyFromUriOptions
        {
            Tags = Values("original")
        }).ConfigureAwait(true);
        await copy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None).ConfigureAwait(true);
        var before = (await destination.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(CopyStatus.Success, before.CopyStatus);
        Assert.False(string.IsNullOrEmpty(before.CopyId));

        if (updateTags)
            await destination.SetTagsAsync(Values("changed")).ConfigureAwait(true);
        else
            await destination.SetMetadataAsync(Values("changed")).ConfigureAwait(true);
        var after = (await destination.GetPropertiesAsync().ConfigureAwait(true)).Value;
        AssertCopyPropertiesEqual(before, after);
        Assert.Equal(before.ContentType, after.ContentType);
        Assert.Equal("copied payload", (await destination.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        Assert.Equal(updateTags ? "original" : "changed", after.Metadata["phase"]);
        Assert.Equal(updateTags ? "changed" : "original", (await destination.GetTagsAsync().ConfigureAwait(true)).Value.Tags["phase"]);
        Assert.Equal(updateTags, before.ETag == after.ETag);
        Assert.True(after.LastModified >= before.LastModified);
        if (updateTags)
            Assert.Equal(before.LastModified, after.LastModified);
        if (versioning)
            await AssertVersionIdentityAsync(destination, before, after, updateTags).ConfigureAwait(true);
    }

    private static async Task AssertVersionIdentityAsync(
        BlobClient destination, BlobProperties before, BlobProperties after, bool updateTags)
    {
        Assert.NotNull(before.VersionId);
        Assert.NotNull(after.VersionId);
        Assert.Equal(updateTags, string.Equals(before.VersionId, after.VersionId, StringComparison.Ordinal));
        var originalVersion = destination.WithVersion(before.VersionId);
        var originalProperties = (await originalVersion.GetPropertiesAsync().ConfigureAwait(false)).Value;
        AssertCopyPropertiesEqual(before, originalProperties);
        Assert.Equal("original", originalProperties.Metadata["phase"]);
        Assert.Equal("copied payload", (await originalVersion.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
    }

    private static void AssertCopyPropertiesEqual(BlobProperties expected, BlobProperties actual)
    {
        Assert.Equal(expected.CopyId, actual.CopyId);
        Assert.Equal(expected.CopyStatus, actual.CopyStatus);
        Assert.Equal(expected.CopySource, actual.CopySource);
        Assert.Equal(expected.CopyProgress, actual.CopyProgress);
        Assert.Equal(expected.CopyCompletedOn, actual.CopyCompletedOn);
        Assert.Equal(expected.CopyStatusDescription, actual.CopyStatusDescription);
        Assert.Equal(expected.ContentLength, actual.ContentLength);
    }

    private static Dictionary<string, string> Values(string phase) =>
        new(StringComparer.Ordinal) { ["phase"] = phase };

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory application) =>
        new(new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03)
            {
                Transport = new HttpClientTransport(application.Server.CreateHandler()),
                Retry = { MaxRetries = 0 }
            });
}
