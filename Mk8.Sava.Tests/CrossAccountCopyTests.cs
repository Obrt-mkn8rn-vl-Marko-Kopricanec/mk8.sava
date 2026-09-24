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
}
