using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;

namespace Mk8.Sava.Tests;

public sealed class CrossAccountCopyTests
{
    [Fact]
    public async Task LocalCrossAccountIncrementalCopyRejectsRecreatedSourceUnderFixedClock()
    {
        var clock = new FixedTimeProvider(DateTimeOffset.UtcNow);
        var application = new SavaWebApplicationFactory(clock, new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:AsyncCopyCompletionDelay"] = "00:00:00"
        });
        await using var disposal = application.ConfigureAwait(true);
        var sourceAccount = CreatePathClient(
            application, SavaWebApplicationFactory.SecondAccountName, SavaWebApplicationFactory.SecondAccountKey);
        var destinationAccount = CreatePathClient(
            application, SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);
        var sourceContainer = sourceAccount.GetBlobContainerClient($"same-origin-source-{Guid.NewGuid():N}");
        var destinationContainer = destinationAccount.GetBlobContainerClient($"same-origin-target-{Guid.NewGuid():N}");
        await sourceContainer.CreateAsync().ConfigureAwait(true);
        await destinationContainer.CreateAsync().ConfigureAwait(true);
        var source = sourceContainer.GetPageBlobClient("source.vhd");
        var destination = destinationContainer.GetPageBlobClient("backup.vhd");
        await source.CreateAsync(512).ConfigureAwait(true);
        var original = Enumerable.Repeat((byte)0xA5, 512).ToArray();
        await source.UploadPagesAsync(new MemoryStream(original), 0).ConfigureAwait(true);
        var firstSnapshot = (await source.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;
        var sourceSas = source.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        var invalidSas = new Uri(sourceSas.AbsoluteUri.Replace("sig=", "sig=0", StringComparison.Ordinal));
        var denied = await Assert.ThrowsAsync<RequestFailedException>(async () =>
            await destination.StartCopyIncrementalAsync(invalidSas, firstSnapshot).ConfigureAwait(true))
            .ConfigureAwait(true);
        Assert.Equal(403, denied.Status);
        Assert.False((await destination.ExistsAsync().ConfigureAwait(true)).Value);

        var firstCopy = await destination.StartCopyIncrementalAsync(sourceSas, firstSnapshot).ConfigureAwait(true);
        await firstCopy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None)
            .ConfigureAwait(true);
        var destinationSnapshot = (await destination.GetPropertiesAsync().ConfigureAwait(true)).Value.DestinationSnapshot!;
        Assert.Equal(original, (await destination.WithSnapshot(destinationSnapshot)
            .DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());

        await source.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots).ConfigureAwait(true);
        await source.CreateAsync(512).ConfigureAwait(true);
        var replacementSnapshot = (await source.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;
        var replacementSas = source.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        var error = await Assert.ThrowsAsync<RequestFailedException>(async () =>
            await destination.StartCopyIncrementalAsync(replacementSas, replacementSnapshot).ConfigureAwait(true))
            .ConfigureAwait(true);
        Assert.Equal(409, error.Status);
        Assert.Equal("IncrementalCopyBlobMismatch", error.ErrorCode);
        Assert.Equal(original, (await destination.WithSnapshot(destinationSnapshot)
            .DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());
    }

    [Fact]
    public async Task IncrementalCopyReadsPrivatePageSnapshotInAnotherAccountWithSourceSas()
    {
        SavaWebApplicationFactory? application = null;
        application = new SavaWebApplicationFactory(() =>
            application?.Server.CreateHandler() ??
            throw new InvalidOperationException("The source server has not been initialized."));
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);

        var sourceAccount = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var destinationAccount = CreateClient(
            application,
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        var sourceContainer = sourceAccount.GetBlobContainerClient($"incremental-source-{Guid.NewGuid():N}");
        var destinationContainer = destinationAccount.GetBlobContainerClient($"incremental-target-{Guid.NewGuid():N}");
        await sourceContainer.CreateAsync().ConfigureAwait(true);
        await destinationContainer.CreateAsync().ConfigureAwait(true);

        var source = sourceContainer.GetPageBlobClient("source.vhd");
        await source.CreateAsync(1024).ConfigureAwait(true);
        var firstPage = Enumerable.Repeat((byte)0xB5, 512).ToArray();
        await source.UploadPagesAsync(new MemoryStream(firstPage), 0).ConfigureAwait(true);
        var sourceSnapshot = (await source.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;
        var sourceSas = source.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        using (var sourceTransport = new HttpClient(application.Server.CreateHandler()))
        {
            var snapshotSas = new Uri($"{sourceSas}&snapshot={Uri.EscapeDataString(sourceSnapshot)}");
            using var snapshotRead = await sourceTransport.GetAsync(snapshotSas).ConfigureAwait(true);
            Assert.Equal(System.Net.HttpStatusCode.OK, snapshotRead.StatusCode);
            using var pageList = await sourceTransport.GetAsync(new Uri($"{snapshotSas}&comp=pagelist"))
                .ConfigureAwait(true);
            Assert.Equal(System.Net.HttpStatusCode.OK, pageList.StatusCode);
        }
        var destination = destinationContainer.GetPageBlobClient("backup.vhd");

        var unsigned = await Assert.ThrowsAsync<RequestFailedException>(() =>
            destination.StartCopyIncrementalAsync(source.Uri, sourceSnapshot)).ConfigureAwait(true);
        Assert.Equal("CannotVerifyCopySource", unsigned.ErrorCode);
        Assert.False((await destination.ExistsAsync().ConfigureAwait(true)).Value);

        var operation = await destination.StartCopyIncrementalAsync(sourceSas, sourceSnapshot).ConfigureAwait(true);
        await operation.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None)
            .ConfigureAwait(true);
        var properties = (await destination.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(CopyStatus.Success, properties.CopyStatus);
        Assert.True(properties.IsIncrementalCopy);
        var firstDestinationSnapshot = properties.DestinationSnapshot!;
        var expected = new byte[1024];
        firstPage.CopyTo(expected, 0);
        Assert.Equal(expected, (await destination.WithSnapshot(firstDestinationSnapshot)
            .DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());

        await AssertSecondIncrementalCopyAsync(source, destination, firstDestinationSnapshot, firstPage)
            .ConfigureAwait(true);
    }

    private static async Task AssertSecondIncrementalCopyAsync(
        PageBlobClient source,
        PageBlobClient destination,
        string firstDestinationSnapshot,
        byte[] firstPage)
    {
        var secondPage = Enumerable.Repeat((byte)0xC6, 512).ToArray();
        await source.UploadPagesAsync(new MemoryStream(secondPage), 512).ConfigureAwait(true);
        var secondSourceSnapshot = (await source.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;
        var renewedSas = source.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(10));
        var secondOperation = await destination.StartCopyIncrementalAsync(renewedSas, secondSourceSnapshot)
            .ConfigureAwait(true);
        await secondOperation.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None)
            .ConfigureAwait(true);
        var secondProperties = (await destination.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(CopyStatus.Success, secondProperties.CopyStatus);
        Assert.NotEqual(firstDestinationSnapshot, secondProperties.DestinationSnapshot, StringComparer.Ordinal);
        var expected = new byte[1024];
        firstPage.CopyTo(expected, 0);
        secondPage.CopyTo(expected, 512);
        Assert.Equal(expected, (await destination.WithSnapshot(secondProperties.DestinationSnapshot!)
            .DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());
        Assert.Equal(firstPage.Concat(new byte[512]).ToArray(),
            (await destination.WithSnapshot(firstDestinationSnapshot).DownloadContentAsync().ConfigureAwait(true))
            .Value.Content.ToArray());
    }

    [Fact]
    public async Task SynchronousCopyFromAnotherAccountUsesSourceSasAndPreservesBlockShape()
    {
        SavaWebApplicationFactory? application = null;
        application = new SavaWebApplicationFactory(() =>
            application?.Server.CreateHandler() ??
            throw new InvalidOperationException("The source server has not been initialized."));
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);

        var sourceAccount = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var destinationAccount = CreateClient(
            application,
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        var sourceContainer = sourceAccount.GetBlobContainerClient($"copy-source-{Guid.NewGuid():N}");
        var destinationContainer = destinationAccount.GetBlobContainerClient($"copy-target-{Guid.NewGuid():N}");
        await sourceContainer.CreateAsync().ConfigureAwait(true);
        await destinationContainer.CreateAsync().ConfigureAwait(true);

        var bytes = new byte[32 * 1024];
        DeterministicTestBytes.Fill(0xC0A1, bytes);
        var source = sourceContainer.GetBlobClient("source.bin");
        await source.UploadAsync(BinaryData.FromBytes(bytes), new BlobUploadOptions
        {
            Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["origin"] = "other-account" }
        }).ConfigureAwait(true);
        var destination = destinationContainer.GetBlobClient("copied.bin");

        var unauthorized = await Assert.ThrowsAsync<RequestFailedException>(() =>
            destination.SyncCopyFromUriAsync(source.Uri)).ConfigureAwait(true);
        Assert.Equal(500, unauthorized.Status);
        Assert.Equal("CannotVerifyCopySource", unauthorized.ErrorCode);
        Assert.False((await destination.ExistsAsync().ConfigureAwait(true)).Value);

        var sourceSas = source.GenerateSasUri(
            BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        var copied = await destination.SyncCopyFromUriAsync(sourceSas).ConfigureAwait(true);
        Assert.Equal(202, copied.GetRawResponse().Status);
        Assert.Equal(CopyStatus.Success, copied.Value.CopyStatus);
        var properties = (await destination.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(bytes.Length, properties.ContentLength);
        Assert.Equal("other-account", properties.Metadata["origin"]);
        Assert.Equal(bytes, (await destination.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());
        Assert.Equal(bytes, (await source.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());
        var sourceBlocks = await sourceContainer.GetBlockBlobClient(source.Name)
            .GetBlockListAsync(BlockListTypes.Committed).ConfigureAwait(true);
        var destinationBlocks = await destinationContainer.GetBlockBlobClient(destination.Name)
            .GetBlockListAsync(BlockListTypes.Committed).ConfigureAwait(true);
        Assert.Empty(sourceBlocks.Value.CommittedBlocks);
        Assert.Empty(destinationBlocks.Value.CommittedBlocks);

        await AssertExplicitBlockListCopyAsync(sourceContainer, destinationContainer).ConfigureAwait(true);
    }

    private static async Task AssertExplicitBlockListCopyAsync(
        BlobContainerClient sourceContainer,
        BlobContainerClient destinationContainer)
    {
        var staged = sourceContainer.GetBlockBlobClient("staged.bin");
        var blockIds = new[]
        {
            Convert.ToBase64String("cross-account-1"u8),
            Convert.ToBase64String("cross-account-2"u8),
            Convert.ToBase64String("cross-account-3"u8)
        };
        var parts = new[] { "first-", "second", "-third" };
        for (var index = 0; index < blockIds.Length; index++)
        {
            using var part = BinaryData.FromString(parts[index]).ToStream();
            await staged.StageBlockAsync(blockIds[index], part).ConfigureAwait(true);
        }
        await staged.CommitBlockListAsync(blockIds).ConfigureAwait(true);
        var stagedDestination = destinationContainer.GetBlockBlobClient("staged-copy.bin");
        var stagedSas = staged.GenerateSasUri(
            BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        var stagedCopy = await stagedDestination.SyncCopyFromUriAsync(stagedSas).ConfigureAwait(true);
        Assert.Equal(202, stagedCopy.GetRawResponse().Status);
        Assert.Equal("first-second-third", (await stagedDestination.DownloadContentAsync().ConfigureAwait(true))
            .Value.Content.ToString());
        var committed = await stagedDestination.GetBlockListAsync(BlockListTypes.Committed).ConfigureAwait(true);
        Assert.Equal(blockIds, committed.Value.CommittedBlocks.Select(block => block.Name), StringComparer.Ordinal);

        var asynchronousDestination = destinationContainer.GetBlockBlobClient("async-staged-copy.bin");
        var operation = await asynchronousDestination.StartCopyFromUriAsync(stagedSas).ConfigureAwait(true);
        await operation.WaitForCompletionAsync().ConfigureAwait(true);
        var asynchronousProperties = (await asynchronousDestination.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(CopyStatus.Success, asynchronousProperties.CopyStatus);
        Assert.Equal("first-second-third", (await asynchronousDestination.DownloadContentAsync().ConfigureAwait(true))
            .Value.Content.ToString());
        var asynchronousBlocks = await asynchronousDestination.GetBlockListAsync(BlockListTypes.Committed)
            .ConfigureAwait(true);
        Assert.Equal(blockIds, asynchronousBlocks.Value.CommittedBlocks.Select(block => block.Name), StringComparer.Ordinal);
    }

    private static BlobServiceClient CreateClient(
        SavaWebApplicationFactory application, string accountName, string accountKey) => new(
        new Uri($"http://{accountName}.localhost"),
        new StorageSharedKeyCredential(accountName, accountKey),
        new BlobClientOptions
        {
            Transport = new HttpClientTransport(application.Server.CreateHandler()),
            Retry = { MaxRetries = 0 }
        });

    private static BlobServiceClient CreatePathClient(
        SavaWebApplicationFactory application, string accountName, string accountKey) => new(
        new Uri($"http://127.0.0.1:10000/{accountName}"),
        new StorageSharedKeyCredential(accountName, accountKey),
        new BlobClientOptions
        {
            Transport = new HttpClientTransport(application.Server.CreateHandler()),
            Retry = { MaxRetries = 0 }
        });

    private sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => utcNow;
    }
}
