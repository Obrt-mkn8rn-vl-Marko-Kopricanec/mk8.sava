using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Xml.Linq;
using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class ExternalIncrementalCopyTransferTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UnavailablePreviousSnapshotOrMalformedDiffKeepsTheCompletedDestination(bool deletePreviousSnapshot)
    {
        var reads = new ConcurrentQueue<SourceRead>();
        var control = new SourceReadControl();
        var application = CreateApplication(reads, control);
        await using var disposal = application.ConfigureAwait(true);
        var fixture = await CreateFixtureAsync(application, reads, control).ConfigureAwait(true);
        var first = await CopyInitialSparseSnapshotAsync(fixture).ConfigureAwait(true);
        var previousSourceSnapshot = control.LastSourceSnapshot!;
        var before = (await fixture.Target.GetPropertiesAsync().ConfigureAwait(true)).Value;
        using var payload = new MemoryStream(Enumerable.Repeat((byte)0xD8, 512).ToArray(), writable: false);
        await fixture.Source.UploadPagesAsync(payload, 0).ConfigureAwait(true);
        if (deletePreviousSnapshot)
            await fixture.Source.WithSnapshot(previousSourceSnapshot).DeleteAsync().ConfigureAwait(true);
        else
            control.MalformedPageDiff = true;
        reads.Clear();
        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() => CopyLatestSnapshotAsync(fixture))
            .ConfigureAwait(true);
        Assert.Equal("CannotVerifyCopySource", rejected.ErrorCode);
        Assert.Equal(500, rejected.Status);
        Assert.Empty(reads);
        var after = (await fixture.Target.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(before.CopyId, after.CopyId);
        Assert.Equal(before.ETag, after.ETag);
        Assert.Equal(first, after.DestinationSnapshot);
        await AssertInitialSnapshotAsync(fixture, first).ConfigureAwait(true);
    }

    [Fact]
    public async Task MalformedOccupiedPageListCannotPublishAZeroFilledCopy()
    {
        var reads = new ConcurrentQueue<SourceRead>();
        var control = new SourceReadControl { MalformedPageList = true };
        var application = CreateApplication(reads, control);
        await using var disposal = application.ConfigureAwait(true);
        var fixture = await CreateFixtureAsync(application, reads, control).ConfigureAwait(true);
        await CreateInitialSparseSourceAsync(fixture.Source).ConfigureAwait(true);
        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() => CopyLatestSnapshotAsync(fixture))
            .ConfigureAwait(true);
        Assert.Equal("CannotVerifyCopySource", rejected.ErrorCode);
        Assert.False((await fixture.Target.ExistsAsync().ConfigureAwait(true)).Value);
        Assert.Empty(reads);
        control.MalformedPageList = false;
        var snapshot = await CopyLatestSnapshotAsync(fixture).ConfigureAwait(true);
        await AssertInitialSnapshotAsync(fixture, snapshot).ConfigureAwait(true);
    }

    [Fact]
    public async Task SourceBlobQueryParametersCannotSkipOccupiedPages()
    {
        var reads = new ConcurrentQueue<SourceRead>();
        var control = new SourceReadControl();
        var application = CreateApplication(reads, control);
        await using var disposal = application.ConfigureAwait(true);
        var fixture = await CreateFixtureAsync(application, reads, control).ConfigureAwait(true);
        await CreateInitialSparseSourceAsync(fixture.Source).ConfigureAwait(true);
        await fixture.Source.ResizeAsync(8192).ConfigureAwait(true);
        using var payload = new MemoryStream(Enumerable.Repeat((byte)0xB6, 512).ToArray(), writable: false);
        await fixture.Source.UploadPagesAsync(payload, 4096).ConfigureAwait(true);
        var snapshot = await CopyLatestSnapshotAsync(fixture, "marker=1&maxresults=1").ConfigureAwait(true);
        var expected = new byte[8192];
        Array.Fill(expected, (byte)0xA5, 0, 512);
        Array.Fill(expected, (byte)0xB6, 4096, 512);
        Assert.Equal(expected, (await fixture.Target.WithSnapshot(snapshot).DownloadContentAsync()
            .ConfigureAwait(true)).Value.Content.ToArray());
        AssertTransferredBytes(fixture, 1024);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(1)]
    public async Task InvalidSecondRangeBodyKeepsPriorCopyAndPermitsReclamationAndRetry(int bodyLengthDelta)
    {
        var reads = new ConcurrentQueue<SourceRead>();
        var control = new SourceReadControl();
        var application = CreateApplication(reads, control);
        await using var disposal = application.ConfigureAwait(true);
        var fixture = await CreateFixtureAsync(application, reads, control).ConfigureAwait(true);
        var first = await CopyInitialSparseSnapshotAsync(fixture).ConfigureAwait(true);
        var before = (await fixture.Target.GetPropertiesAsync().ConfigureAwait(true)).Value;
        var payload = Enumerable.Repeat((byte)0xC7, 10 * 1024 * 1024).ToArray();
        await fixture.Source.ResizeAsync(12 * 1024 * 1024).ConfigureAwait(true);
        await UploadLargePagesAsync(fixture.Source, payload).ConfigureAwait(true);
        reads.Clear();
        control.RangeBodyLengthDelta = bodyLengthDelta;
        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() => CopyLatestSnapshotAsync(fixture))
            .ConfigureAwait(true);
        Assert.Equal("CannotVerifyCopySource", rejected.ErrorCode);
        Assert.Equal(500, rejected.Status);
        Assert.Equal(2, reads.Count);
        var after = (await fixture.Target.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(before.CopyId, after.CopyId);
        Assert.Equal(before.ETag, after.ETag);
        Assert.Equal(first, after.DestinationSnapshot);
        Assert.True(await application.Services.GetRequiredService<BlobService>()
            .CollectGarbageAsync(CancellationToken.None).ConfigureAwait(true) > 0);
        await AssertInitialSnapshotAsync(fixture, first).ConfigureAwait(true);
        control.RangeBodyLengthDelta = 0;
        reads.Clear();
        var retry = await CopyLatestSnapshotAsync(fixture).ConfigureAwait(true);
        var expected = new byte[12 * 1024 * 1024];
        payload.CopyTo(expected, 0);
        Assert.Equal(expected, (await fixture.Target.WithSnapshot(retry).DownloadContentAsync()
            .ConfigureAwait(true)).Value.Content.ToArray());
        AssertTransferredBytes(fixture, payload.Length);
        await AssertInitialSnapshotAsync(fixture, first).ConfigureAwait(true);
    }

    [Theory]
    [InlineData("2016-05-31", "2016-05-31")]
    [InlineData("2016-05-31", "2017-11-09")]
    [InlineData("2017-11-09", "2016-05-31")]
    public async Task CreationTimeHeaderAvailabilityDoesNotRestrictIncrementalCopyVersions(
        string firstVersion, string laterVersion)
    {
        var reads = new ConcurrentQueue<SourceRead>();
        var control = new SourceReadControl();
        var application = CreateApplication(reads, control);
        await using var disposal = application.ConfigureAwait(true);
        var fixture = await CreateFixtureAsync(application, reads, control).ConfigureAwait(true);
        using var transport = new HttpClient(application.Server.CreateHandler());
        await CreateInitialSparseSourceAsync(fixture.Source).ConfigureAwait(true);
        var first = await CopyLatestSnapshotWithVersionAsync(transport, fixture, firstVersion).ConfigureAwait(true);
        using var payload = new MemoryStream(Enumerable.Repeat((byte)0xB6, 512).ToArray(), writable: false);
        await fixture.Source.UploadPagesAsync(payload, 512).ConfigureAwait(true);
        var second = await CopyLatestSnapshotWithVersionAsync(transport, fixture, laterVersion).ConfigureAwait(true);
        Assert.Equal([new SourceHead(firstVersion, string.CompareOrdinal(firstVersion, "2017-11-09") >= 0),
            new SourceHead(laterVersion, string.CompareOrdinal(laterVersion, "2017-11-09") >= 0)], control.Heads.ToArray());
        var expected = new byte[4096];
        Array.Fill(expected, (byte)0xA5, 0, 512);
        Assert.Equal(expected, (await fixture.Target.WithSnapshot(first).DownloadContentAsync()
            .ConfigureAwait(true)).Value.Content.ToArray());
        Array.Fill(expected, (byte)0xB6, 512, 512);
        Assert.Equal(expected, (await fixture.Target.WithSnapshot(second).DownloadContentAsync()
            .ConfigureAwait(true)).Value.Content.ToArray());
        var before = (await fixture.Target.GetPropertiesAsync().ConfigureAwait(true)).Value;
        await fixture.Source.CreateAsync(4096).ConfigureAwait(true);
        using var rejected = await StartSnapshotCopyWithVersionAsync(transport, fixture, laterVersion).ConfigureAwait(true);
        Assert.Equal(HttpStatusCode.Conflict, rejected.StatusCode);
        var error = XDocument.Parse(await rejected.Content.ReadAsStringAsync().ConfigureAwait(true));
        Assert.Equal("IncrementalCopyBlobMismatch", error.Root!.Element("Code")!.Value);
        var after = (await fixture.Target.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(before.CopyId, after.CopyId);
        Assert.Equal(before.ETag, after.ETag);
        Assert.Equal(second, after.DestinationSnapshot);
        Assert.Equal(expected, (await fixture.Target.WithSnapshot(second).DownloadContentAsync()
            .ConfigureAwait(true)).Value.Content.ToArray());
    }

    [Fact]
    public async Task MissingCreationTimeIsStillRejectedForModernSourceRequests()
    {
        var reads = new ConcurrentQueue<SourceRead>();
        var control = new SourceReadControl { HideCreationTime = true };
        var application = CreateApplication(reads, control);
        await using var disposal = application.ConfigureAwait(true);
        var fixture = await CreateFixtureAsync(application, reads, control).ConfigureAwait(true);
        await CreateInitialSparseSourceAsync(fixture.Source).ConfigureAwait(true);
        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() => CopyLatestSnapshotAsync(fixture))
            .ConfigureAwait(true);
        Assert.Equal("CannotVerifyCopySource", rejected.ErrorCode);
        Assert.Equal(500, rejected.Status);
        Assert.False((await fixture.Target.ExistsAsync().ConfigureAwait(true)).Value);
        Assert.Empty(reads);
    }

    [Fact]
    public async Task SparseCopiesReadOnlyOccupiedAndChangedPagesAcrossResizeAndEmptyDelta()
    {
        var reads = new ConcurrentQueue<SourceRead>();
        var control = new SourceReadControl();
        var application = CreateApplication(reads, control);
        await using var disposal = application.ConfigureAwait(true);
        var fixture = await CreateFixtureAsync(application, reads, control).ConfigureAwait(true);
        var first = await CopyInitialSparseSnapshotAsync(fixture).ConfigureAwait(true);
        var second = await CopyGrowthAndClearAsync(fixture, first).ConfigureAwait(true);
        await CopyEmptyDeltaAndShrinkAsync(fixture, first, second).ConfigureAwait(true);
    }

    [Fact]
    public async Task OccupiedRangesAreTransferredInAtMostFourMiBReads()
    {
        var reads = new ConcurrentQueue<SourceRead>();
        var control = new SourceReadControl();
        var application = CreateApplication(reads, control);
        await using var disposal = application.ConfigureAwait(true);
        var fixture = await CreateFixtureAsync(application, reads, control).ConfigureAwait(true);
        var payload = Enumerable.Repeat((byte)0xC7, 10 * 1024 * 1024).ToArray();
        await fixture.Source.CreateAsync(12 * 1024 * 1024).ConfigureAwait(true);
        await UploadLargePagesAsync(fixture.Source, payload).ConfigureAwait(true);
        var snapshot = await CopyLatestSnapshotAsync(fixture).ConfigureAwait(true);
        AssertTransferredBytes(fixture, payload.Length);
        Assert.Equal(3, fixture.Reads.Count);
        Assert.All(fixture.Reads, read => Assert.InRange(read.ContentLength, 1, 4 * 1024 * 1024));
        var expected = new byte[12 * 1024 * 1024];
        payload.CopyTo(expected, 0);
        Assert.Equal(expected, (await fixture.Target.WithSnapshot(snapshot).DownloadContentAsync()
            .ConfigureAwait(true)).Value.Content.ToArray());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InconsistentSourceRangeDoesNotPublishAnIncrementalCopy(bool wrongUnit)
    {
        var reads = new ConcurrentQueue<SourceRead>();
        var control = new SourceReadControl { CorruptRangeUnits = wrongUnit };
        var application = CreateApplication(reads, control);
        await using var disposal = application.ConfigureAwait(true);
        var fixture = await CreateFixtureAsync(application, reads, control).ConfigureAwait(true);
        var first = await CopyInitialSparseSnapshotAsync(fixture).ConfigureAwait(true);
        var before = (await fixture.Target.GetPropertiesAsync().ConfigureAwait(true)).Value;
        using var payload = new MemoryStream(Enumerable.Repeat((byte)0xD8, 512).ToArray(), writable: false);
        await fixture.Source.UploadPagesAsync(payload, 0).ConfigureAwait(true);
        fixture.Control.CorruptRanges = true;
        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() => CopyLatestSnapshotAsync(fixture))
            .ConfigureAwait(true);
        Assert.Equal("CannotVerifyCopySource", rejected.ErrorCode);
        var after = (await fixture.Target.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(before.CopyId, after.CopyId);
        Assert.Equal(before.ETag, after.ETag);
        Assert.Equal(before.DestinationSnapshot, after.DestinationSnapshot);
        var expected = new byte[4096];
        Array.Fill(expected, (byte)0xA5, 0, 512);
        Assert.Equal(expected, (await fixture.Target.WithSnapshot(first).DownloadContentAsync()
            .ConfigureAwait(true)).Value.Content.ToArray());
    }

    private static SavaWebApplicationFactory CreateApplication(ConcurrentQueue<SourceRead> reads, SourceReadControl control)
    {
        SavaWebApplicationFactory? application = null;
        application = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal) { ["Sava:AsyncCopyCompletionDelay"] = "00:00:00" },
            () => new RecordingHandler(application?.Server.CreateHandler() ??
                throw new InvalidOperationException("Source is not initialized."), reads, control),
            disableMaintenance: true);
        return application;
    }

    private static async Task<CopyFixture> CreateFixtureAsync(
        SavaWebApplicationFactory application, ConcurrentQueue<SourceRead> reads, SourceReadControl control)
    {
        await application.InitializeAsync().ConfigureAwait(false);
        var sourceContainer = CreateClient(application, SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey).GetBlobContainerClient($"sparse-source-{Guid.NewGuid():N}");
        var targetContainer = CreateClient(application, SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey).GetBlobContainerClient($"sparse-target-{Guid.NewGuid():N}");
        await sourceContainer.CreateAsync().ConfigureAwait(false);
        await targetContainer.CreateAsync().ConfigureAwait(false);
        return new CopyFixture(sourceContainer.GetPageBlobClient("source.vhd"),
            targetContainer.GetPageBlobClient("backup.vhd"), reads, control);
    }

    private static async Task UploadLargePagesAsync(PageBlobClient source, byte[] payload)
    {
        for (var offset = 0; offset < payload.Length; offset += 4 * 1024 * 1024)
        {
            var length = Math.Min(4 * 1024 * 1024, payload.Length - offset);
            using var block = new MemoryStream(payload, offset, length, writable: false);
            await source.UploadPagesAsync(block, offset).ConfigureAwait(false);
        }
    }

    private static async Task<string> CopyInitialSparseSnapshotAsync(CopyFixture fixture)
    {
        await CreateInitialSparseSourceAsync(fixture.Source).ConfigureAwait(false);
        var destinationSnapshot = await CopyLatestSnapshotAsync(fixture).ConfigureAwait(false);
        var expected = new byte[4096];
        Array.Fill(expected, (byte)0xA5, 0, 512);
        Assert.Equal(expected, (await fixture.Target.WithSnapshot(destinationSnapshot).DownloadContentAsync()
            .ConfigureAwait(false)).Value.Content.ToArray());
        AssertTransferredBytes(fixture, 512);
        return destinationSnapshot;
    }

    private static async Task CreateInitialSparseSourceAsync(PageBlobClient source)
    {
        await source.CreateAsync(4096).ConfigureAwait(false);
        using var payload = new MemoryStream(Enumerable.Repeat((byte)0xA5, 512).ToArray(), writable: false);
        await source.UploadPagesAsync(payload, 0).ConfigureAwait(false);
    }

    private static async Task AssertInitialSnapshotAsync(CopyFixture fixture, string snapshot)
    {
        var expected = new byte[4096];
        Array.Fill(expected, (byte)0xA5, 0, 512);
        Assert.Equal(expected, (await fixture.Target.WithSnapshot(snapshot).DownloadContentAsync()
            .ConfigureAwait(false)).Value.Content.ToArray());
    }

    private static async Task<string> CopyGrowthAndClearAsync(CopyFixture fixture, string firstSnapshot)
    {
        fixture.Reads.Clear();
        await fixture.Source.ResizeAsync(8192).ConfigureAwait(false);
        await fixture.Source.ClearPagesAsync(new HttpRange(0, 512)).ConfigureAwait(false);
        using var payload = new MemoryStream(Enumerable.Repeat((byte)0xB6, 512).ToArray(), writable: false);
        await fixture.Source.UploadPagesAsync(payload, 4096).ConfigureAwait(false);
        var secondSnapshot = await CopyLatestSnapshotAsync(fixture).ConfigureAwait(false);
        var expected = new byte[8192];
        Array.Fill(expected, (byte)0xB6, 4096, 512);
        Assert.Equal(expected, (await fixture.Target.WithSnapshot(secondSnapshot).DownloadContentAsync()
            .ConfigureAwait(false)).Value.Content.ToArray());
        AssertTransferredBytes(fixture, 512);
        var original = new byte[4096];
        Array.Fill(original, (byte)0xA5, 0, 512);
        Assert.Equal(original, (await fixture.Target.WithSnapshot(firstSnapshot).DownloadContentAsync()
            .ConfigureAwait(false)).Value.Content.ToArray());
        return secondSnapshot;
    }

    private static async Task CopyEmptyDeltaAndShrinkAsync(CopyFixture fixture, string firstSnapshot, string secondSnapshot)
    {
        fixture.Reads.Clear();
        var unchanged = await CopyLatestSnapshotAsync(fixture).ConfigureAwait(false);
        Assert.NotEqual(secondSnapshot, unchanged, StringComparer.Ordinal);
        AssertTransferredBytes(fixture, 0);
        fixture.Reads.Clear();
        await fixture.Source.ResizeAsync(512).ConfigureAwait(false);
        var shrunk = await CopyLatestSnapshotAsync(fixture).ConfigureAwait(false);
        Assert.Equal(new byte[512], (await fixture.Target.WithSnapshot(shrunk).DownloadContentAsync()
            .ConfigureAwait(false)).Value.Content.ToArray());
        AssertTransferredBytes(fixture, 0);
        Assert.Equal(4096, (await fixture.Target.WithSnapshot(firstSnapshot).GetPropertiesAsync()
            .ConfigureAwait(false)).Value.ContentLength);
        var previous = (await fixture.Target.WithSnapshot(secondSnapshot).DownloadContentAsync()
            .ConfigureAwait(false)).Value.Content.ToArray();
        Assert.Equal(8192, previous.Length);
        Assert.Equal(Enumerable.Repeat((byte)0xB6, 512), previous.Skip(4096).Take(512));
    }

    private static async Task<string> CopyLatestSnapshotAsync(CopyFixture fixture, string? additionalSourceQuery = null)
    {
        var sourceSnapshot = (await fixture.Source.CreateSnapshotAsync().ConfigureAwait(false)).Value.Snapshot;
        fixture.Control.LastSourceSnapshot = sourceSnapshot;
        var sas = fixture.Source.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        if (!string.IsNullOrEmpty(additionalSourceQuery))
            sas = new Uri(sas.AbsoluteUri + '&' + additionalSourceQuery);
        var copy = await fixture.Target.StartCopyIncrementalAsync(sas, sourceSnapshot).ConfigureAwait(false);
        await copy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None).ConfigureAwait(false);
        return (await fixture.Target.GetPropertiesAsync().ConfigureAwait(false)).Value.DestinationSnapshot!;
    }

    private static async Task<string> CopyLatestSnapshotWithVersionAsync(
        HttpClient transport, CopyFixture fixture, string serviceVersion)
    {
        using var response = await StartSnapshotCopyWithVersionAsync(transport, fixture, serviceVersion).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var properties = (await fixture.Target.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(CopyStatus.Success, properties.CopyStatus);
        return properties.DestinationSnapshot!;
    }

    private static async Task<HttpResponseMessage> StartSnapshotCopyWithVersionAsync(
        HttpClient transport, CopyFixture fixture, string serviceVersion)
    {
        var snapshot = (await fixture.Source.CreateSnapshotAsync().ConfigureAwait(false)).Value.Snapshot;
        var source = fixture.Source.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        var target = fixture.Target.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5));
        var uri = new UriBuilder(target) { Query = target.Query.TrimStart('?') + "&comp=incrementalcopy" }.Uri;
        using var request = new HttpRequestMessage(HttpMethod.Put, uri) { Content = new ByteArrayContent([]) };
        request.Headers.TryAddWithoutValidation("x-ms-version", serviceVersion);
        request.Headers.TryAddWithoutValidation("x-ms-copy-source", source.AbsoluteUri + "&snapshot=" + Uri.EscapeDataString(snapshot));
        return await transport.SendAsync(request).ConfigureAwait(false);
    }

    private static void AssertTransferredBytes(CopyFixture fixture, long expected)
    {
        var dataReads = fixture.Reads.ToArray();
        Assert.All(dataReads, read => Assert.False(string.IsNullOrEmpty(read.Range)));
        Assert.Equal(expected, dataReads.Sum(read => read.ContentLength));
    }

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory application, string account, string key) =>
        new(new Uri($"http://{account}.localhost"), new StorageSharedKeyCredential(account, key),
            new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03)
            {
                Transport = new HttpClientTransport(application.Server.CreateHandler()),
                Retry = { MaxRetries = 0 }
            });

    private sealed record SourceRead(string? Range, long ContentLength);

    private sealed record SourceHead(string ServiceVersion, bool HasCreationTime);

    private sealed record CopyFixture(PageBlobClient Source, PageBlobClient Target,
        ConcurrentQueue<SourceRead> Reads, SourceReadControl Control);

    private sealed class SourceReadControl
    {
        public bool CorruptRanges { get; set; }

        public bool CorruptRangeUnits { get; init; }

        public bool HideCreationTime { get; init; }

        public bool MalformedPageList { get; set; }

        public bool MalformedPageDiff { get; set; }

        public string? LastSourceSnapshot { get; set; }

        public int RangeBodyLengthDelta { get; set; }

        public ConcurrentQueue<SourceHead> Heads { get; } = new();
    }

    private sealed class RecordingHandler(
        HttpMessageHandler innerHandler, ConcurrentQueue<SourceRead> reads, SourceReadControl control)
        : DelegatingHandler(innerHandler)
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var response = await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
            if (request.Method == HttpMethod.Head && response.IsSuccessStatusCode)
            {
                if (control.HideCreationTime)
                    response.Headers.Remove("x-ms-creation-time");
                control.Heads.Enqueue(new SourceHead(request.Headers.GetValues("x-ms-version").Single(),
                    response.Headers.Contains("x-ms-creation-time")));
            }
            if (request.Method == HttpMethod.Get &&
                !request.RequestUri!.Query.Contains("comp=pagelist", StringComparison.OrdinalIgnoreCase) &&
                response.IsSuccessStatusCode)
            {
                reads.Enqueue(new SourceRead(request.Headers.Range?.ToString(), response.Content.Headers.ContentLength ?? -1));
                if (control.CorruptRanges && response.Content.Headers.ContentRange is { Length: { } total })
                {
                    var length = response.Content.Headers.ContentLength!.Value;
                    response.Content.Headers.ContentRange = control.CorruptRangeUnits
                        ? new ContentRangeHeaderValue(0, length - 1, total) { Unit = "items" }
                        : new ContentRangeHeaderValue(512, 512 + length - 1, total);
                }
                if (control.RangeBodyLengthDelta != 0 && request.Headers.Range?.Ranges.FirstOrDefault()?.From >= 4 * 1024 * 1024)
                    await ChangeBodyLengthAsync(response, control.RangeBodyLengthDelta, cancellationToken).ConfigureAwait(false);
            }
            else if (request.Method == HttpMethod.Get && response.IsSuccessStatusCode &&
                     (control.MalformedPageList ||
                      control.MalformedPageDiff && request.RequestUri!.Query.Contains("prevsnapshot=", StringComparison.OrdinalIgnoreCase)))
            {
                response.Content.Dispose();
                response.Content = new StringContent("<NotPageList />", Encoding.UTF8, "application/xml");
            }
            return response;
        }

        private static async Task ChangeBodyLengthAsync(
            HttpResponseMessage response, int difference, CancellationToken cancellationToken)
        {
            var previous = response.Content;
            var body = await previous.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
            var changed = new byte[body.Length + difference];
            body.AsSpan(0, Math.Min(body.Length, changed.Length)).CopyTo(changed);
            var replacement = new ByteArrayContent(changed);
            foreach (var header in previous.Headers)
                replacement.Headers.TryAddWithoutValidation(header.Key, header.Value);
            replacement.Headers.ContentLength = previous.Headers.ContentLength;
            response.Content = replacement;
            previous.Dispose();
        }
    }
}
