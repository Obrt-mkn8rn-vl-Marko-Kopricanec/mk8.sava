using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;

namespace Mk8.Sava.Tests;

public sealed class ExternalIncrementalCopyTests
{
    [Fact]
    public async Task PageDiffRejectsExternalSourceReplacementWithTheSameCreationTime()
    {
        SavaWebApplicationFactory? application = null;
        application = new SavaWebApplicationFactory(
            new FixedTimeProvider(DateTimeOffset.UtcNow),
            new Dictionary<string, string?>(StringComparer.Ordinal) { ["Sava:AsyncCopyCompletionDelay"] = "00:00:00" },
            () => application?.Server.CreateHandler() ?? throw new InvalidOperationException("Source is not initialized."));
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var sourceContainer = CreateClient(application, SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey).GetBlobContainerClient($"external-source-{Guid.NewGuid():N}");
        var targetContainer = CreateClient(application, SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey).GetBlobContainerClient($"external-target-{Guid.NewGuid():N}");
        await sourceContainer.CreateAsync().ConfigureAwait(true);
        await targetContainer.CreateAsync().ConfigureAwait(true);
        var source = sourceContainer.GetPageBlobClient("source.vhd");
        var target = targetContainer.GetPageBlobClient("backup.vhd");
        var original = Enumerable.Repeat((byte)0xA5, 512).ToArray();
        await ReplacePageAsync(source, original).ConfigureAwait(true);
        var createdOn = (await source.GetPropertiesAsync().ConfigureAwait(true)).Value.CreatedOn;
        var firstSnapshot = (await source.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;
        var sourceSas = source.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        var copy = await target.StartCopyIncrementalAsync(sourceSas, firstSnapshot).ConfigureAwait(true);
        await copy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None).ConfigureAwait(true);
        var before = (await target.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(CopyStatus.Success, before.CopyStatus);
        await AssertEarlierSnapshotRejectedAsync(target, sourceSas, firstSnapshot).ConfigureAwait(true);

        // Put Blob recreates the base but retains its old snapshots. A fixed clock
        // makes creation-time headers identical; the source page diff detects it.
        var replacement = Enumerable.Repeat((byte)0xB6, 512).ToArray();
        await ReplacePageAsync(source, replacement).ConfigureAwait(true);
        Assert.Equal(createdOn, (await source.GetPropertiesAsync().ConfigureAwait(true)).Value.CreatedOn);
        var replacementSnapshot = (await source.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;
        var sourceFailure = await Assert.ThrowsAsync<RequestFailedException>(() =>
            source.WithSnapshot(replacementSnapshot).GetPageRangesDiffAsync(previousSnapshot: firstSnapshot))
            .ConfigureAwait(true);
        Assert.Equal(409, sourceFailure.Status);
        Assert.Equal("BlobOverwritten", sourceFailure.ErrorCode);

        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() =>
            target.StartCopyIncrementalAsync(sourceSas, replacementSnapshot)).ConfigureAwait(true);
        Assert.Equal(409, rejected.Status);
        Assert.Equal("IncrementalCopyBlobMismatch", rejected.ErrorCode);
        var after = (await target.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(before.CopyId, after.CopyId);
        Assert.Equal(before.CopyStatus, after.CopyStatus);
        Assert.Equal(before.DestinationSnapshot, after.DestinationSnapshot);
        Assert.Equal(before.ETag, after.ETag);
        Assert.Equal(original, (await target.WithSnapshot(before.DestinationSnapshot!)
            .DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());
        Assert.Equal(original, (await source.WithSnapshot(firstSnapshot)
            .DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());
        Assert.Equal(replacement, (await source.WithSnapshot(replacementSnapshot)
            .DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());
    }

    private static async Task AssertEarlierSnapshotRejectedAsync(PageBlobClient target, Uri sourceSas, string snapshot)
    {
        var error = await Assert.ThrowsAsync<RequestFailedException>(() =>
            target.StartCopyIncrementalAsync(sourceSas, snapshot)).ConfigureAwait(false);
        Assert.Equal(409, error.Status);
        Assert.Equal("IncrementalCopyOfEarlierVersionSnapshotNotAllowed", error.ErrorCode);
    }

    private static async Task ReplacePageAsync(PageBlobClient source, byte[] bytes)
    {
        await source.CreateAsync(bytes.Length).ConfigureAwait(false);
        using var payload = new MemoryStream(bytes, writable: false);
        await source.UploadPagesAsync(payload, 0).ConfigureAwait(false);
    }

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory application, string account, string key) =>
        new(new Uri($"http://{account}.localhost"), new StorageSharedKeyCredential(account, key),
            new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03)
            {
                Transport = new HttpClientTransport(application.Server.CreateHandler()),
                Retry = { MaxRetries = 0 }
            });

    private sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => utcNow;
    }
}
