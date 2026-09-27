using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace Mk8.Sava.Tests;

public sealed class BlobTagLeaseTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ActiveAndBreakingLeasesRequireTheMatchingIdForTagReadsAndWrites(bool breaking)
    {
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(application).GetBlobContainerClient($"tag-lease-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("payload.bin");
        await blob.UploadAsync(BinaryData.FromString("payload"), new BlobUploadOptions
        {
            Tags = Tags("original")
        }).ConfigureAwait(true);
        var snapshot = blob.WithSnapshot((await blob.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value;
        var leaseId = Guid.NewGuid().ToString();
        var lease = blob.GetBlobLeaseClient(leaseId);
        await lease.AcquireAsync(BlobLeaseClient.InfiniteLeaseDuration).ConfigureAwait(true);
        if (breaking)
            await lease.BreakAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(true);

        await AssertTagLeaseFailureAsync(blob, null, 403, "LeaseIdMissing").ConfigureAwait(true);
        await AssertTagLeaseFailureAsync(blob, Guid.NewGuid().ToString(), 403, "LeaseIdMismatchWithBlobOperation")
            .ConfigureAwait(true);
        await AssertTagLeaseFailureAsync(blob, "not-a-guid", 400, "InvalidHeaderValue").ConfigureAwait(true);
        Assert.Equal("payload", (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        Assert.Equal("original", (await snapshot.GetTagsAsync().ConfigureAwait(true)).Value.Tags["phase"]);
        Assert.Equal(204, (await snapshot.SetTagsAsync(Tags("snapshot")).ConfigureAwait(true)).Status);
        await AssertTagLeaseFailureAsync(snapshot, leaseId, 412, "LeaseNotPresentWithBlobOperation").ConfigureAwait(true);

        var conditions = new BlobRequestConditions { LeaseId = leaseId };
        Assert.Equal("original", (await blob.GetTagsAsync(conditions).ConfigureAwait(true)).Value.Tags["phase"]);
        Assert.Equal(204, (await blob.SetTagsAsync(Tags("updated"), conditions).ConfigureAwait(true)).Status);
        Assert.Equal("updated", (await blob.GetTagsAsync(conditions).ConfigureAwait(true)).Value.Tags["phase"]);
        Assert.Equal("snapshot", (await snapshot.GetTagsAsync().ConfigureAwait(true)).Value.Tags["phase"]);
        var after = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(before.ETag, after.ETag);
        Assert.Equal(before.LastModified, after.LastModified);

        await lease.ReleaseAsync().ConfigureAwait(true);
        await AssertTagLeaseFailureAsync(blob, leaseId, 412, "LeaseNotPresentWithBlobOperation").ConfigureAwait(true);
        Assert.Equal("updated", (await blob.GetTagsAsync().ConfigureAwait(true)).Value.Tags["phase"]);
        Assert.Equal(204, (await blob.SetTagsAsync(Tags("released")).ConfigureAwait(true)).Status);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExpiredAndFullyBrokenLeasesNoLongerRestrictTagReadsOrWrites(bool broken)
    {
        var clock = new AdjustableTimeProvider(DateTimeOffset.UtcNow);
        var application = new SavaWebApplicationFactory(clock,
            new Dictionary<string, string?>(StringComparer.Ordinal));
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(application).GetBlobContainerClient($"tag-expired-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("payload.bin");
        await blob.UploadAsync(BinaryData.FromString("payload"), new BlobUploadOptions
        {
            Tags = Tags("original")
        }).ConfigureAwait(true);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value;
        var lease = blob.GetBlobLeaseClient();
        var id = (await lease.AcquireAsync(broken ? BlobLeaseClient.InfiniteLeaseDuration : TimeSpan.FromSeconds(15))
            .ConfigureAwait(true)).Value.LeaseId;
        if (broken)
            await blob.GetBlobLeaseClient(id).BreakAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(true);
        clock.Advance(TimeSpan.FromSeconds(61));

        await AssertTagLeaseFailureAsync(blob, id, 412, "LeaseNotPresentWithBlobOperation").ConfigureAwait(true);
        Assert.Equal("original", (await blob.GetTagsAsync().ConfigureAwait(true)).Value.Tags["phase"]);
        Assert.Equal(204, (await blob.SetTagsAsync(Tags("updated")).ConfigureAwait(true)).Status);
        Assert.Equal("updated", (await blob.GetTagsAsync().ConfigureAwait(true)).Value.Tags["phase"]);
        var after = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(before.ETag, after.ETag);
        Assert.Equal(before.LastModified, after.LastModified);
        Assert.Equal("payload", (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    private static async Task AssertTagLeaseFailureAsync(BlobClient blob, string? leaseId, int status, string code)
    {
        var conditions = new BlobRequestConditions { LeaseId = leaseId };
        var read = await Assert.ThrowsAsync<RequestFailedException>(() => blob.GetTagsAsync(conditions))
            .ConfigureAwait(false);
        Assert.Equal(status, read.Status);
        Assert.Equal(code, read.ErrorCode);
        var write = await Assert.ThrowsAsync<RequestFailedException>(() => blob.SetTagsAsync(Tags("rejected"), conditions))
            .ConfigureAwait(false);
        Assert.Equal(status, write.Status);
        Assert.Equal(code, write.ErrorCode);
    }

    private static Dictionary<string, string> Tags(string phase) =>
        new(StringComparer.Ordinal) { ["phase"] = phase };

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory application) =>
        new(new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03)
            {
                Transport = new HttpClientTransport(application.Server.CreateHandler()),
                Retry = { MaxRetries = 0 }
            });

    private sealed class AdjustableTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private long _utcTicks = utcNow.UtcDateTime.Ticks;

        public override DateTimeOffset GetUtcNow() =>
            new(Interlocked.Read(ref _utcTicks), TimeSpan.Zero);

        public void Advance(TimeSpan value) =>
            Interlocked.Add(ref _utcTicks, value.Ticks);
    }
}
