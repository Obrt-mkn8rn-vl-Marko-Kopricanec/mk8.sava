using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;

namespace Mk8.Sava.Tests;

public sealed class BlobTagHistoryTests
{
    [Fact]
    public async Task SnapshotTagWritesHonorAugust2020ServiceVersionBoundary()
    {
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(application).GetBlobContainerClient($"snapshot-tag-version-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("payload.bin");
        await blob.UploadAsync(BinaryData.FromString("payload"), new BlobUploadOptions
        {
            Tags = Tags("original")
        }).ConfigureAwait(true);
        var snapshotId = (await blob.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;
        var older = CreateClient(application, BlobClientOptions.ServiceVersion.V2019_12_12)
            .GetBlobContainerClient(container.Name).GetBlobClient(blob.Name).WithSnapshot(snapshotId);
        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() => older.SetTagsAsync(Tags("rejected")))
            .ConfigureAwait(true);
        Assert.Equal(409, rejected.Status);
        Assert.Equal("FeatureVersionMismatch", rejected.ErrorCode);
        Assert.Equal("original", (await older.GetTagsAsync().ConfigureAwait(true)).Value.Tags["phase"]);

        var supported = CreateClient(application, BlobClientOptions.ServiceVersion.V2020_08_04)
            .GetBlobContainerClient(container.Name).GetBlobClient(blob.Name).WithSnapshot(snapshotId);
        Assert.Equal(204, (await supported.SetTagsAsync(Tags("supported")).ConfigureAwait(true)).Status);
        Assert.Equal("supported", (await supported.GetTagsAsync().ConfigureAwait(true)).Value.Tags["phase"]);
        Assert.Equal("original", (await blob.GetTagsAsync().ConfigureAwait(true)).Value.Tags["phase"]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task HistoricalTagsCanChangeWithoutMutatingContentOrEntityHeaders(bool targetVersion)
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:VersioningEnabled"] =
                targetVersion ? "true" : "false"
        });
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(application).GetBlobContainerClient($"tag-history-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var current = container.GetBlobClient("payload.bin");
        var uploaded = await current.UploadAsync(BinaryData.FromString("original"), new BlobUploadOptions
        {
            Tags = Tags("original")
        }).ConfigureAwait(true);
        var historical = targetVersion
            ? current.WithVersion(uploaded.Value.VersionId ?? throw new InvalidOperationException("The upload did not return a version ID."))
            : current.WithSnapshot((await current.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot);
        await current.UploadAsync(BinaryData.FromString("current"), new BlobUploadOptions
        {
            Tags = Tags("current")
        }).ConfigureAwait(true);

        var beforeHistorical = (await historical.GetPropertiesAsync().ConfigureAwait(true)).Value;
        var beforeCurrent = (await current.GetPropertiesAsync().ConfigureAwait(true)).Value;
        var identities = await ReadIdentitiesAsync(container, current.Name).ConfigureAwait(true);
        Assert.Equal("original", (await historical.GetTagsAsync().ConfigureAwait(true)).Value.Tags["phase"]);

        var changed = await historical.SetTagsAsync(Tags("retagged"), new BlobRequestConditions
        {
            TagConditions = "phase = 'original'"
        }).ConfigureAwait(true);
        Assert.Equal(204, changed.Status);
        Assert.Equal("retagged", (await historical.GetTagsAsync().ConfigureAwait(true)).Value.Tags["phase"]);
        Assert.Equal("current", (await current.GetTagsAsync().ConfigureAwait(true)).Value.Tags["phase"]);
        await AssertUnchangedEntityAsync(historical, beforeHistorical, "original").ConfigureAwait(true);
        await AssertUnchangedEntityAsync(current, beforeCurrent, "current").ConfigureAwait(true);

        var tagOnly = await AssertHistoricalTagSasAsync(application, historical).ConfigureAwait(true);
        var cleared = await tagOnly.SetTagsAsync(new Dictionary<string, string>(StringComparer.Ordinal))
            .ConfigureAwait(true);
        Assert.Equal(204, cleared.Status);
        Assert.Empty((await historical.GetTagsAsync().ConfigureAwait(true)).Value.Tags);
        Assert.Equal("current", (await current.GetTagsAsync().ConfigureAwait(true)).Value.Tags["phase"]);
        Assert.Equal(identities, await ReadIdentitiesAsync(container, current.Name).ConfigureAwait(true));
        await AssertUnchangedEntityAsync(historical, beforeHistorical, "original").ConfigureAwait(true);
    }

    private static async Task<BlobClient> AssertHistoricalTagSasAsync(
        SavaWebApplicationFactory application, BlobClient historical)
    {
        var writeOnly = CreateSasClient(application,
            historical.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)));
        var deniedWrite = await Assert.ThrowsAsync<RequestFailedException>(() => writeOnly.SetTagsAsync(Tags("denied")))
            .ConfigureAwait(false);
        Assert.Equal(403, deniedWrite.Status);
        Assert.Equal("AuthorizationPermissionMismatch", deniedWrite.ErrorCode);
        var tagOnly = CreateSasClient(application,
            historical.GenerateSasUri(BlobSasPermissions.Tag, DateTimeOffset.UtcNow.AddMinutes(5)));
        var stale = await Assert.ThrowsAsync<RequestFailedException>(() => tagOnly.SetTagsAsync(Tags("stale"),
            new BlobRequestConditions { TagConditions = "phase = 'original'" })).ConfigureAwait(false);
        Assert.Equal(412, stale.Status);
        Assert.Equal("ConditionNotMet", stale.ErrorCode);
        Assert.Equal("retagged", (await tagOnly.GetTagsAsync().ConfigureAwait(false)).Value.Tags["phase"]);
        var deniedContent = await Assert.ThrowsAsync<RequestFailedException>(() => tagOnly.DownloadContentAsync())
            .ConfigureAwait(false);
        Assert.Equal(403, deniedContent.Status);
        return tagOnly;
    }

    private static async Task AssertUnchangedEntityAsync(
        BlobClient blob, BlobProperties before, string expectedContent)
    {
        var after = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(before.ETag, after.ETag);
        Assert.Equal(before.LastModified, after.LastModified);
        Assert.Equal(before.ContentLength, after.ContentLength);
        Assert.Equal(expectedContent, (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
    }

    private static async Task<string> ReadIdentitiesAsync(BlobContainerClient container, string name)
    {
        var identities = new List<string>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
        {
            Prefix = name,
            States = BlobStates.Version | BlobStates.Snapshots
        }).ConfigureAwait(false))
        {
            identities.Add($"{item.Name}:{item.VersionId}:{item.Snapshot}");
        }
        return string.Join('|', identities.Order(StringComparer.Ordinal));
    }

    private static Dictionary<string, string> Tags(string phase) =>
        new(StringComparer.Ordinal) { ["phase"] = phase };

    private static BlobServiceClient CreateClient(
        SavaWebApplicationFactory application,
        BlobClientOptions.ServiceVersion serviceVersion = BlobClientOptions.ServiceVersion.V2023_11_03) =>
        new(new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            CreateOptions(application, serviceVersion));

    private static BlobClient CreateSasClient(SavaWebApplicationFactory application, Uri uri) =>
        new(uri, CreateOptions(application, BlobClientOptions.ServiceVersion.V2023_11_03));

    private static BlobClientOptions CreateOptions(
        SavaWebApplicationFactory application, BlobClientOptions.ServiceVersion serviceVersion) =>
        new(serviceVersion)
        {
            Transport = new HttpClientTransport(application.Server.CreateHandler()),
            Retry = { MaxRetries = 0 }
        };
}
