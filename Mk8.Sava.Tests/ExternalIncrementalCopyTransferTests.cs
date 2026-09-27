using System.Collections.Concurrent;
using System.Net.Http.Headers;
using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;

namespace Mk8.Sava.Tests;

public sealed class ExternalIncrementalCopyTransferTests
{
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

    [Fact]
    public async Task InconsistentSourceRangeDoesNotPublishAnIncrementalCopy()
    {
        var reads = new ConcurrentQueue<SourceRead>();
        var control = new SourceReadControl();
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
                throw new InvalidOperationException("Source is not initialized."), reads, control));
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
        await fixture.Source.CreateAsync(4096).ConfigureAwait(false);
        using var payload = new MemoryStream(Enumerable.Repeat((byte)0xA5, 512).ToArray(), writable: false);
        await fixture.Source.UploadPagesAsync(payload, 0).ConfigureAwait(false);
        var destinationSnapshot = await CopyLatestSnapshotAsync(fixture).ConfigureAwait(false);
        var expected = new byte[4096];
        Array.Fill(expected, (byte)0xA5, 0, 512);
        Assert.Equal(expected, (await fixture.Target.WithSnapshot(destinationSnapshot).DownloadContentAsync()
            .ConfigureAwait(false)).Value.Content.ToArray());
        AssertTransferredBytes(fixture, 512);
        return destinationSnapshot;
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

    private static async Task<string> CopyLatestSnapshotAsync(CopyFixture fixture)
    {
        var sourceSnapshot = (await fixture.Source.CreateSnapshotAsync().ConfigureAwait(false)).Value.Snapshot;
        var sas = fixture.Source.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        var copy = await fixture.Target.StartCopyIncrementalAsync(sas, sourceSnapshot).ConfigureAwait(false);
        await copy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None).ConfigureAwait(false);
        return (await fixture.Target.GetPropertiesAsync().ConfigureAwait(false)).Value.DestinationSnapshot!;
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

    private sealed record CopyFixture(PageBlobClient Source, PageBlobClient Target,
        ConcurrentQueue<SourceRead> Reads, SourceReadControl Control);

    private sealed class SourceReadControl
    {
        public bool CorruptRanges { get; set; }
    }

    private sealed class RecordingHandler(
        HttpMessageHandler innerHandler, ConcurrentQueue<SourceRead> reads, SourceReadControl control)
        : DelegatingHandler(innerHandler)
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var response = await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
            if (request.Method == HttpMethod.Get &&
                !request.RequestUri!.Query.Contains("comp=pagelist", StringComparison.OrdinalIgnoreCase) &&
                response.IsSuccessStatusCode)
            {
                reads.Enqueue(new SourceRead(request.Headers.Range?.ToString(), response.Content.Headers.ContentLength ?? -1));
                if (control.CorruptRanges && response.Content.Headers.ContentRange is { Length: { } total })
                {
                    var length = response.Content.Headers.ContentLength!.Value;
                    response.Content.Headers.ContentRange = new ContentRangeHeaderValue(512, 512 + length - 1, total);
                }
            }
            return response;
        }
    }
}
