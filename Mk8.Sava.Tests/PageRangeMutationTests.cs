using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;

namespace Mk8.Sava.Tests;

public sealed class PageRangeMutationTests(SavaWebApplicationFactory application)
    : IClassFixture<SavaWebApplicationFactory>
{
    [Fact]
    public async Task IdenticalPageRewriteIsReportedAsUpdatedSinceSnapshot()
    {
        var client = CreateClient(application);
        var container = client.GetBlobContainerClient($"same-page-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var page = container.GetPageBlobClient("disk.vhd");
        await page.CreateAsync(1024).ConfigureAwait(true);
        var bytes = Enumerable.Repeat((byte)0xA7, 512).ToArray();
        await page.UploadPagesAsync(new MemoryStream(bytes), 0).ConfigureAwait(true);
        var firstSnapshot = (await page.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;

        await page.UploadPagesAsync(new MemoryStream(bytes), 0).ConfigureAwait(true);
        var diff = (await page.GetPageRangesDiffAsync(previousSnapshot: firstSnapshot)
            .ConfigureAwait(true)).Value;
        var rewritten = Assert.Single(diff.PageRanges);
        Assert.Equal(0, rewritten.Offset);
        Assert.Equal(512, rewritten.Length);
        Assert.Empty(diff.ClearRanges);

        var secondSnapshot = (await page.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;
        var snapshotDiff = (await page.WithSnapshot(secondSnapshot)
            .GetPageRangesDiffAsync(previousSnapshot: firstSnapshot).ConfigureAwait(true)).Value;
        Assert.Equal(0, Assert.Single(snapshotDiff.PageRanges).Offset);
        Assert.Empty(snapshotDiff.ClearRanges);
    }

    [Fact]
    public async Task IdenticalPageRewriteRemainsVisibleAfterRestart()
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-page-diff-{Guid.NewGuid():N}");
        var containerName = $"restart-page-{Guid.NewGuid():N}";
        string firstSnapshot;
        try
        {
            var first = new SavaWebApplicationFactory(dataPath, deleteDataPath: false);
            await using (first.ConfigureAwait(true))
            {
                var page = CreateClient(first).GetBlobContainerClient(containerName).GetPageBlobClient("disk.vhd");
                await page.GetParentBlobContainerClient().CreateAsync().ConfigureAwait(true);
                await page.CreateAsync(1024).ConfigureAwait(true);
                var bytes = Enumerable.Repeat((byte)0xD4, 512).ToArray();
                await page.UploadPagesAsync(new MemoryStream(bytes), 0).ConfigureAwait(true);
                firstSnapshot = (await page.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;
                await page.UploadPagesAsync(new MemoryStream(bytes), 0).ConfigureAwait(true);
            }

            var restarted = new SavaWebApplicationFactory(dataPath, deleteDataPath: false);
            await using (restarted.ConfigureAwait(true))
            {
                var page = CreateClient(restarted).GetBlobContainerClient(containerName).GetPageBlobClient("disk.vhd");
                var diff = (await page.GetPageRangesDiffAsync(previousSnapshot: firstSnapshot)
                    .ConfigureAwait(true)).Value;
                Assert.Equal(0, Assert.Single(diff.PageRanges).Offset);
                Assert.Empty(diff.ClearRanges);
            }
        }
        finally
        {
            await SavaWebApplicationFactory.DeleteDataPathAsync(dataPath).ConfigureAwait(true);
        }
    }

    [Fact]
    public async Task PageDiffRespectsSeparateWritesAndSnapshotBoundary()
    {
        var container = CreateClient(application).GetBlobContainerClient($"split-page-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var page = container.GetPageBlobClient("disk.vhd");
        await page.CreateAsync(2048).ConfigureAwait(true);
        var bytes = Enumerable.Repeat((byte)0xE2, 2048).ToArray();
        await page.UploadPagesAsync(new MemoryStream(bytes), 0).ConfigureAwait(true);
        var firstSnapshot = (await page.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;

        await page.UploadPagesAsync(new MemoryStream(bytes[..512]), 0).ConfigureAwait(true);
        await page.UploadPagesAsync(new MemoryStream(bytes[..512]), 1024).ConfigureAwait(true);
        var secondSnapshot = (await page.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;
        await page.UploadPagesAsync(new MemoryStream(bytes[..512]), 512).ConfigureAwait(true);

        var recent = (await page.GetPageRangesDiffAsync(previousSnapshot: secondSnapshot)
            .ConfigureAwait(true)).Value;
        var recentRange = Assert.Single(recent.PageRanges);
        Assert.Equal(512, recentRange.Offset);
        Assert.Equal(512, recentRange.Length);

        var entire = (await page.GetPageRangesDiffAsync(previousSnapshot: firstSnapshot)
            .ConfigureAwait(true)).Value;
        var entireRange = Assert.Single(entire.PageRanges);
        Assert.Equal(0, entireRange.Offset);
        Assert.Equal(1536, entireRange.Length);
        Assert.Empty(entire.ClearRanges);
    }

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory factory) => new(
        new Uri($"http://127.0.0.1:10000/{SavaWebApplicationFactory.AccountName}"),
        new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey),
        new BlobClientOptions
        {
            Transport = new Azure.Core.Pipeline.HttpClientTransport(factory.Server.CreateHandler())
        });
}
