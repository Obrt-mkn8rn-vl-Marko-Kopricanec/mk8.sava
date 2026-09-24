using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;
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
    public async Task PageDiffRejectsOverwriteEvenWhenCreationTimeIsUnchanged()
    {
        var clock = new FixedTimeProvider(DateTimeOffset.UtcNow);
        var fixedApplication = new SavaWebApplicationFactory(
            clock, new Dictionary<string, string?>(StringComparer.Ordinal));
        await using var fixedApplicationDisposal = fixedApplication.ConfigureAwait(true);
        var container = CreateClient(fixedApplication).GetBlobContainerClient($"overwrite-page-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var page = container.GetPageBlobClient("disk.vhd");
        await page.CreateAsync(512).ConfigureAwait(true);
        await page.UploadPagesAsync(new MemoryStream(new byte[512]), 0).ConfigureAwait(true);
        var originalSnapshot = (await page.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;

        await page.CreateAsync(512).ConfigureAwait(true);
        Assert.True((await page.WithSnapshot(originalSnapshot).ExistsAsync().ConfigureAwait(true)).Value);
        var error = await Assert.ThrowsAsync<Azure.RequestFailedException>(async () =>
            await page.GetPageRangesDiffAsync(previousSnapshot: originalSnapshot).ConfigureAwait(true))
            .ConfigureAwait(true);
        Assert.Equal(409, error.Status);
        Assert.Equal("BlobOverwritten", error.ErrorCode);
    }

    [Fact]
    public async Task IncrementalCopyRejectsRecreatedSourceWithTheSameCreationTime()
    {
        var clock = new FixedTimeProvider(DateTimeOffset.UtcNow);
        var fixedApplication = new SavaWebApplicationFactory(clock, new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:AsyncCopyCompletionDelay"] = "00:00:00"
        });
        await using var fixedApplicationDisposal = fixedApplication.ConfigureAwait(true);
        var container = CreateClient(fixedApplication).GetBlobContainerClient($"same-time-copy-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var source = container.GetPageBlobClient("source.vhd");
        await source.CreateAsync(512).ConfigureAwait(true);
        var firstSnapshot = (await source.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;
        var destination = container.GetPageBlobClient("backup.vhd");
        var copy = await destination.StartCopyIncrementalAsync(source.Uri, firstSnapshot).ConfigureAwait(true);
        await copy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None)
            .ConfigureAwait(true);

        await source.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots).ConfigureAwait(true);
        await source.CreateAsync(512).ConfigureAwait(true);
        var replacementSnapshot = (await source.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;
        var error = await Assert.ThrowsAsync<Azure.RequestFailedException>(async () =>
            await destination.StartCopyIncrementalAsync(source.Uri, replacementSnapshot).ConfigureAwait(true))
            .ConfigureAwait(true);
        Assert.Equal(409, error.Status);
        Assert.Equal("IncrementalCopyBlobMismatch", error.ErrorCode);
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

    [Fact]
    public async Task PageDiffPaginationPreservesAddressOrderAcrossUpdatedAndClearedRanges()
    {
        var container = CreateClient(application).GetBlobContainerClient($"paged-page-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var page = container.GetPageBlobClient("disk.vhd");
        await page.CreateAsync(2048).ConfigureAwait(true);
        var original = new byte[2048];
        await page.UploadPagesAsync(new MemoryStream(original), 0).ConfigureAwait(true);
        var snapshot = (await page.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;
        await page.ClearPagesAsync(new HttpRange(512, 512)).ConfigureAwait(true);
        await page.ClearPagesAsync(new HttpRange(1536, 512)).ConfigureAwait(true);
        await page.UploadPagesAsync(new MemoryStream(original[..512]), 0).ConfigureAwait(true);
        await page.UploadPagesAsync(new MemoryStream(original[..512]), 1024).ConfigureAwait(true);

        var pageSas = page.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        var listUri = new Uri($"{pageSas}&comp=pagelist&prevsnapshot={Uri.EscapeDataString(snapshot)}");
        using var transport = new HttpClient(application.Server.CreateHandler());
        var unpaged = await ReadPageListAsync(transport, listUri, "2023-11-03").ConfigureAwait(true);
        Assert.Equal(new[] { "PageRange:0", "ClearRange:512", "PageRange:1024", "ClearRange:1536" },
            DescribePageRanges(unpaged));

        var firstPage = await ReadPageListAsync(
            transport, new Uri($"{listUri}&maxresults=3"), "2023-11-03").ConfigureAwait(true);
        Assert.Equal(new[] { "PageRange:0", "ClearRange:512", "PageRange:1024" },
            DescribePageRanges(firstPage));
        Assert.Equal("3", firstPage.Root?.Element("NextMarker")?.Value);

        var lastPage = await ReadPageListAsync(
            transport, new Uri($"{listUri}&maxresults=3&marker=3"), "2023-11-03").ConfigureAwait(true);
        Assert.Equal(new[] { "ClearRange:1536" }, DescribePageRanges(lastPage));
        Assert.Equal(string.Empty, lastPage.Root?.Element("NextMarker")?.Value);

        await AssertPageListStatusAsync(
            transport, new Uri($"{listUri}&maxresults=1"), "2019-07-07", HttpStatusCode.Conflict)
            .ConfigureAwait(true);
        var firstVersionedPage = await ReadPageListAsync(
            transport, new Uri($"{listUri}&maxresults=1"), "2020-10-02").ConfigureAwait(true);
        Assert.Equal(new[] { "PageRange:0" }, DescribePageRanges(firstVersionedPage));
        Assert.Equal("1", firstVersionedPage.Root?.Element("NextMarker")?.Value);
        await AssertPageListStatusAsync(
            transport, new Uri($"{listUri}&maxresults=0"), "2023-11-03", HttpStatusCode.BadRequest)
            .ConfigureAwait(true);
    }

    [Fact]
    public async Task FragmentedPageListCapsResultsAtTenThousandAndContinues()
    {
        var container = CreateClient(application).GetBlobContainerClient($"fragmented-page-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var page = container.GetPageBlobClient("disk.vhd");
        await page.CreateAsync(10_001L * 1024).ConfigureAwait(true);

        // Seed fragmentation without 10,001 network writes; the public page-list route still reads persisted metadata.
        var metadata = application.Services.GetRequiredService<MetadataStore>();
        var record = await metadata.GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            container.Name,
            page.Name,
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None).ConfigureAwait(true);
        Assert.NotNull(record);
        var ranges = Enumerable.Range(0, 10_001)
            .Select(index => new PageRange(index * 1024L, index * 1024L + 511))
            .ToArray();
        await metadata.PutBlobRecordAsync(
            record with { PageRanges = ranges }, record.Revision, CancellationToken.None).ConfigureAwait(true);

        var pageSas = page.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        var listUri = new Uri($"{pageSas}&comp=pagelist&maxresults=20000");
        using var transport = new HttpClient(application.Server.CreateHandler());
        var firstPage = await ReadPageListAsync(transport, listUri, "2020-10-02").ConfigureAwait(true);
        Assert.Equal(10_000, DescribePageRanges(firstPage).Length);
        Assert.Equal("10000", firstPage.Root?.Element("NextMarker")?.Value);

        var lastPage = await ReadPageListAsync(
            transport, new Uri($"{listUri}&marker=10000"), "2020-10-02").ConfigureAwait(true);
        Assert.Equal(new[] { "PageRange:10240000" }, DescribePageRanges(lastPage));
        Assert.Equal(string.Empty, lastPage.Root?.Element("NextMarker")?.Value);
    }

    private static string[] DescribePageRanges(System.Xml.Linq.XDocument document) =>
        document.Root!.Elements()
            .Where(element => !string.Equals(element.Name.LocalName, "NextMarker", StringComparison.Ordinal))
            .Select(element => $"{element.Name.LocalName}:{element.Element("Start")?.Value}")
            .ToArray();

    private static async Task<System.Xml.Linq.XDocument> ReadPageListAsync(
        HttpClient transport, Uri uri, string version)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, uri);
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        using var response = await transport.SendAsync(request).ConfigureAwait(true);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        return System.Xml.Linq.XDocument.Parse(await response.Content.ReadAsStringAsync().ConfigureAwait(true));
    }

    private static async Task AssertPageListStatusAsync(
        HttpClient transport, Uri uri, string version, HttpStatusCode expectedStatus)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, uri);
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        using var response = await transport.SendAsync(request).ConfigureAwait(true);
        Assert.Equal(expectedStatus, response.StatusCode);
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

    private sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => utcNow;
    }
}
