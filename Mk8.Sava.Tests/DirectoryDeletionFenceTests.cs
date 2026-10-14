using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class DirectoryDeletionFenceTests
{
    [Theory]
    [InlineData(false, "live-child")]
    [InlineData(true, "live-child")]
    [InlineData(false, "deleted-child")]
    [InlineData(true, "deleted-child")]
    [InlineData(false, "prefix-sibling")]
    [InlineData(true, "prefix-sibling")]
    public async Task DeletionCommitRechecksDescendantsAfterAnEmptyDirectoryWasCaptured(bool softDelete, string publication)
    {
        var application = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true",
            }, disableMaintenance: true);
        await using var applicationLifetime = application.ConfigureAwait(false);
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        var owner = new BlobServiceClient(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });
        var container = owner.GetBlobContainerClient("directory-deletion");
        await container.CreateAsync().ConfigureAwait(true);
        var seed = container.GetBlobClient("directory/seed.bin");
        await seed.UploadAsync(BinaryData.FromString("seed")).ConfigureAwait(true);
        await seed.DeleteAsync().ConfigureAwait(true);
        var metadata = application.Services.GetRequiredService<MetadataStore>();
        var directory = Assert.IsType<BlobRecord>(await metadata.GetBlobAsync(SavaWebApplicationFactory.AccountName,
            container.Name, "directory", versionId: null, snapshot: null, includeDeleted: false,
            CancellationToken.None).ConfigureAwait(true));
        Assert.True(directory.IsDirectory);
        Assert.False(await metadata.HasActiveBlobDescendantsAsync(directory.Account, directory.Container,
            directory.Name, CancellationToken.None).ConfigureAwait(true));

        // This is the exact stale-decision interleaving: the empty check and
        // captured revision precede a committed child. No timing race is needed.
        var child = container.GetBlobClient(string.Equals(publication, "prefix-sibling", StringComparison.Ordinal)
            ? "directory-sibling.bin" : "directory/late.bin");
        await child.UploadAsync(BinaryData.FromString("late child")).ConfigureAwait(true);
        if (string.Equals(publication, "deleted-child", StringComparison.Ordinal))
            await child.DeleteAsync().ConfigureAwait(true);
        var beforeCommit = Assert.IsType<BlobRecord>(await metadata.GetBlobAsync(directory.Account, directory.Container,
            directory.Name, versionId: null, snapshot: null, includeDeleted: false,
            CancellationToken.None).ConfigureAwait(true));
        Assert.Equal(directory.Revision, beforeCommit.Revision);
        BlobRecord? replacement = softDelete ? directory with
        {
            IsDeleted = true,
            IsCurrent = false,
            DeletedAt = DateTimeOffset.UtcNow,
            Revision = MetadataStore.NewRevision(),
        } : null;
        var observed = await Record.ExceptionAsync(() => metadata.ApplyBlobRecordMutationsAsync(
            [new BlobRecordMutation(directory.GenerationId, directory.Revision, replacement)],
            CancellationToken.None, clearStagedBlocksForCurrentBlobs: true)).ConfigureAwait(true);
        var afterCommit = await metadata.GetBlobAsync(directory.Account, directory.Container, directory.Name,
            versionId: null, snapshot: null, includeDeleted: false, CancellationToken.None).ConfigureAwait(true);
        await VerifyCommitAsync(publication, observed, directory, afterCommit, child).ConfigureAwait(true);
    }

    private static async Task VerifyCommitAsync(string publication, Exception? observed, BlobRecord directory,
        BlobRecord? afterCommit, BlobClient child)
    {
        if (string.Equals(publication, "live-child", StringComparison.Ordinal))
        {
            var refusal = StorageExceptionMapper.Map(Assert.IsType<Exception>(observed, exactMatch: false));
            Assert.Equal(409, refusal.StatusCode);
            Assert.Equal("DirectoryIsNotEmpty", refusal.ErrorCode);
            // SQLite deserialization owns fresh dictionaries/collections. Compare
            // all typed metadata values, not record equality of those references.
            Assert.Equal(JsonSerializer.Serialize(directory), JsonSerializer.Serialize(afterCommit));
            Assert.Equal("late child", (await child.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        }
        else
        {
            Assert.Null(observed);
            Assert.Null(afterCommit);
            if (string.Equals(publication, "prefix-sibling", StringComparison.Ordinal))
                Assert.Equal("late child", (await child.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        }
    }
}
