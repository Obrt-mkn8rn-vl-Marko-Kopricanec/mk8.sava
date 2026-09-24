using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using System.Net;

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

    [Fact]
    public async Task PageDiffParametersRequireTheirPublishedServiceVersions()
    {
        var container = CreateClient(application).GetBlobContainerClient($"version-page-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var page = container.GetPageBlobClient("disk.vhd");
        await page.CreateAsync(512).ConfigureAwait(true);
        var snapshot = (await page.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;
        var pageSas = page.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        using var transport = new HttpClient(application.Server.CreateHandler());

        var previousQuery = new Uri($"{pageSas}&comp=pagelist&prevsnapshot={Uri.EscapeDataString(snapshot)}");
        await AssertPageDiffVersionAsync(transport, previousQuery, "2014-02-14", null, HttpStatusCode.Conflict)
            .ConfigureAwait(true);
        await AssertPageDiffVersionAsync(transport, previousQuery, "2015-07-08", null, HttpStatusCode.OK)
            .ConfigureAwait(true);

        var pageList = new Uri($"{pageSas}&comp=pagelist");
        var previousUrl = $"{pageSas}&snapshot={Uri.EscapeDataString(snapshot)}";
        await AssertPageDiffVersionAsync(transport, pageList, "2018-11-09", previousUrl, HttpStatusCode.Conflict)
            .ConfigureAwait(true);
        await AssertPageDiffVersionAsync(transport, pageList, "2019-07-07", previousUrl, HttpStatusCode.OK)
            .ConfigureAwait(true);
    }

    private static async Task AssertPageDiffVersionAsync(
        HttpClient transport,
        Uri uri,
        string version,
        string? previousUrl,
        HttpStatusCode expectedStatus)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, uri);
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        if (previousUrl is not null)
            request.Headers.TryAddWithoutValidation("x-ms-previous-snapshot-url", previousUrl);
        using var response = await transport.SendAsync(request).ConfigureAwait(true);
        Assert.Equal(expectedStatus, response.StatusCode);
        if (expectedStatus == HttpStatusCode.Conflict)
        {
            var error = System.Xml.Linq.XDocument.Parse(
                await response.Content.ReadAsStringAsync().ConfigureAwait(true));
            Assert.Equal("FeatureVersionMismatch", error.Root?.Element("Code")?.Value);
        }
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
