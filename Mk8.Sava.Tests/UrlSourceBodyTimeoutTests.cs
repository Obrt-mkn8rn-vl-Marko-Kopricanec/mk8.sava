using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Configuration;
using Mk8.Sava.Hosting;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class UrlSourceBodyTimeoutTests
{
    // An intercepted reserved fixture, never a DNS/network or deployment endpoint.
    private static readonly Uri SourceUri = new("https://body-source.example.test/blob");

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StalledBodyCannotReplaceContentAndReleasesAdmission(bool checksum)
    {
        using var body = new BodyProbe("partial"u8.ToArray(), stallAtEnd: true);
        var factory = CreateFactory(body, "content");
        await using var lifetime = factory.ConfigureAwait(false);
        using var transport = new HttpClientTransport(factory.Server.CreateHandler());
        var client = new BlobServiceClient(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });
        var container = client.GetBlobContainerClient("body-deadline");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlockBlobClient("preserved.bin");
        await container.GetBlobClient(blob.Name).UploadAsync(BinaryData.FromString("original bytes")).ConfigureAwait(true);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value.ETag;
        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var failure = await Assert.ThrowsAsync<RequestFailedException>(() => blob.SyncUploadFromUriAsync(SourceUri,
            new BlobSyncUploadFromUriOptions
            {
                DestinationConditions = new BlobRequestConditions { IfMatch = before },
                ContentHash = checksum ? AzureProtocolChecksum.Md5("expected complete bytes"u8) : null,
            }, guard.Token)).ConfigureAwait(true);

        Assert.Equal("CannotVerifyCopySource", failure.ErrorCode);
        Assert.False(guard.IsCancellationRequested);
        AssertRetired(body);
        Assert.Equal(before, (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value.ETag);
        Assert.Equal("original bytes", (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        await container.GetBlobClient("after.bin").UploadAsync(BinaryData.FromString("after timeout")).ConfigureAwait(true);
        Assert.Empty(Directory.EnumerateFiles(factory.Services.GetRequiredService<GatewayStagingPaths>().Staging, "url-source-*.tmp"));
    }

    [Theory]
    [InlineData("tags")]
    [InlineData("blocks")]
    [InlineData("pages")]
    [InlineData("error")]
    public async Task MetadataShapeAndErrorBodiesCannotWaitIndefinitely(string phase)
    {
        using var body = new BodyProbe("<"u8.ToArray(), stallAtEnd: true);
        var factory = CreateFactory(body, phase);
        await using var lifetime = factory.ConfigureAwait(false);
        var transfers = factory.Services.GetRequiredService<UrlTransferClient>();
        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var consumed = false;
        var failure = await Assert.ThrowsAsync<AzureStorageException>(async () =>
        {
            if (string.Equals(phase, "pages", StringComparison.Ordinal))
                _ = await transfers.ReadPageCopySourceAsync(CreateRequest(), SourceUri.AbsoluteUri, guard.Token).ConfigureAwait(false);
            else
                _ = await transfers.ReadAsync(CreateRequest(), SourceUri.AbsoluteUri, null, false, false, 512, false,
                    _ => { consumed = true; return Task.FromResult(0); }, guard.Token,
                    copySourceTags: string.Equals(phase, "tags", StringComparison.Ordinal),
                    preserveSourceShape: string.Equals(phase, "blocks", StringComparison.Ordinal)).ConfigureAwait(false);
        }).ConfigureAwait(true);

        Assert.Equal("CannotVerifyCopySource", failure.ErrorCode);
        Assert.False(consumed);
        Assert.False(guard.IsCancellationRequested);
        AssertRetired(body);
        if (string.Equals(phase, "error", StringComparison.Ordinal))
        {
            Assert.Equal("502", failure.ResponseHeaders["x-ms-copy-source-status-code"]);
            Assert.Equal("ServerBusy", failure.ResponseHeaders["x-ms-copy-source-error-code"]);
            Assert.False(failure.Details.ContainsKey("CopySourceErrorMessage"));
        }
    }

    [Fact]
    public async Task PinnedPageRangeUsesTheBodyDeadline()
    {
        using var body = new BodyProbe("part"u8.ToArray(), stallAtEnd: true);
        var factory = CreateFactory(body, "range");
        await using var lifetime = factory.ConfigureAwait(false);
        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var failure = await Assert.ThrowsAsync<AzureStorageException>(() =>
            factory.Services.GetRequiredService<UrlTransferClient>().ReadPinnedPageRangeAsync(
                CreateRequest(), SourceUri, "\"source-etag\"", 2048, new PageRange(512, 1023),
                async (source, token) => _ = await ReadAllAsync(source, token).ConfigureAwait(false),
                guard.Token)).ConfigureAwait(true);

        Assert.Equal("CannotVerifyCopySource", failure.ErrorCode);
        Assert.False(guard.IsCancellationRequested);
        AssertRetired(body);
    }

    [Fact]
    public async Task CallerCancellationIsNotRelabelledAsSourceTimeout()
    {
        using var body = new BodyProbe([], stallAtEnd: true);
        var factory = CreateFactory(body, "content", TimeSpan.FromSeconds(10));
        await using var lifetime = factory.ConfigureAwait(false);
        var transfers = factory.Services.GetRequiredService<UrlTransferClient>();
        using var caller = new CancellationTokenSource();
        var operation = transfers.ReadAsync(CreateRequest(), SourceUri.AbsoluteUri,
            null, false, false, 512, false, source => ReadAllAsync(source.Content, caller.Token), caller.Token);
        Exception? verification;
        Exception? completion;
        try
        {
            verification = await Record.ExceptionAsync(() =>
                body.Stalled.Task.WaitAsync(TimeSpan.FromSeconds(5), CancellationToken.None)).ConfigureAwait(true);
        }
        finally
        {
            await caller.CancelAsync().ConfigureAwait(true);
#pragma warning disable VSTHRD003 // This test owns operation, cancels its actual caller and joins it before the factory/response dependencies retire.
            completion = await Record.ExceptionAsync(() => operation).ConfigureAwait(true);
#pragma warning restore VSTHRD003
        }

        if (verification is not null)
            throw new AggregateException("Source-read admission and retained completion.", verification,
                completion ?? new InvalidOperationException("The source read completed without reaching the held body."));
        Assert.IsAssignableFrom<OperationCanceledException>(completion);
        Assert.True(caller.IsCancellationRequested);
        AssertRetired(body);
    }

    [Fact]
    public async Task ProgressCanContinueBeyondOneIdleBudget()
    {
        var expected = "progress"u8.ToArray();
        using var body = new BodyProbe(expected, stallAtEnd: false, TimeSpan.FromMilliseconds(80));
        var factory = CreateFactory(body, "content", TimeSpan.FromMilliseconds(500));
        await using var lifetime = factory.ConfigureAwait(false);
        using var caller = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await factory.Services.GetRequiredService<UrlTransferClient>().ReadAsync(CreateRequest(),
            SourceUri.AbsoluteUri, null, false, false, 512, false,
            source => ReadAllAsync(source.Content, caller.Token), caller.Token).ConfigureAwait(true);

        Assert.Equal(expected, result.Value);
        Assert.Equal(expected.Length + 1, body.ReadCount);
        Assert.False(body.CancellationObserved);
        Assert.True(body.Disposed);
        Assert.False(body.DisposedWhileReading);
    }

    [Theory]
    [InlineData(-1, false)]
    [InlineData(0, false)]
    [InlineData(99, false)]
    [InlineData(100, true)]
    [InlineData(30_000, true)]
    [InlineData(600_000, true)]
    [InlineData(600_001, false)]
    public void BodyDeadlineConfigurationIsFiniteAndBounded(int milliseconds, bool valid)
    {
        var options = new SavaOptions { UrlTransferReadIdleTimeout = TimeSpan.FromMilliseconds(milliseconds) };
        Assert.Equal(!valid, options.Validate(new ValidationContext(options)).Any(error =>
            error.MemberNames.Contains(nameof(SavaOptions.UrlTransferReadIdleTimeout), StringComparer.Ordinal)));
    }

    private static void AssertRetired(BodyProbe body)
    {
        Assert.True(body.CancellationObserved);
        Assert.Equal(0, body.Active);
        Assert.True(body.Disposed);
        Assert.False(body.DisposedWhileReading);
    }

    private static async Task<byte[]> ReadAllAsync(Stream source, CancellationToken cancellationToken)
    {
        var destination = new MemoryStream();
        await using var lifetime = destination.ConfigureAwait(false);
        await source.CopyToAsync(destination, cancellationToken).ConfigureAwait(false);
        return destination.ToArray();
    }

    private static SavaWebApplicationFactory CreateFactory(BodyProbe body, string phase, TimeSpan? timeout = null) => new(
        TimeProvider.System, new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:UrlTransferReadIdleTimeout"] = (timeout ?? TimeSpan.FromMilliseconds(100)).ToString("c", CultureInfo.InvariantCulture),
            ["Gateway:MaximumConcurrentRequests"] = "1",
            ["Gateway:MaximumQueuedRequests"] = "0",
        }, () => new BodySource(body, phase), disableMaintenance: true);

    private static HttpRequest CreateRequest()
    {
        var http = new DefaultHttpContext();
        StorageRequestContext.Set(http, new StorageRequestContext
        {
            Account = SavaWebApplicationFactory.AccountName, RequestId = "body-deadline",
            ResourceKind = StorageResourceKind.Blob, CanonicalResourcePath = "/account/container/blob",
            ServiceVersion = "2026-06-06", Authorization = StorageAuthorization.Anonymous,
        });
        return http.Request;
    }

    private sealed class BodySource(BodyProbe body, string phase) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            Task.FromResult(CreateResponse(request));

        private HttpResponseMessage CreateResponse(HttpRequestMessage request)
        {
            ArgumentNullException.ThrowIfNull(request);
            var selected = phase switch
            {
                "tags" => request.RequestUri?.Query.Contains("comp=tags", StringComparison.Ordinal) == true,
                "blocks" => request.RequestUri?.Query.Contains("comp=blocklist", StringComparison.Ordinal) == true,
                "pages" => request.RequestUri?.Query.Contains("comp=pagelist", StringComparison.Ordinal) == true,
                _ => true,
            };
            var status = phase switch
            {
                "error" => HttpStatusCode.BadGateway,
                "range" => HttpStatusCode.PartialContent,
                _ => HttpStatusCode.OK,
            };
            var response = new HttpResponseMessage(status);
            try
            {
                response.Content = selected ? new StreamContent(body) : new ByteArrayContent("source bytes"u8.ToArray());
                response.Content.Headers.ContentLength = string.Equals(phase, "range", StringComparison.Ordinal) ||
                    string.Equals(phase, "pages", StringComparison.Ordinal) ? 512 : selected ? body.DeclaredLength : 12;
                response.Headers.ETag = new EntityTagHeaderValue("\"source-etag\"");
                response.Headers.TryAddWithoutValidation("x-ms-blob-type", string.Equals(phase, "pages", StringComparison.Ordinal) ? "PageBlob" : "BlockBlob");
                response.Headers.TryAddWithoutValidation("x-ms-creation-time", DateTimeOffset.UnixEpoch.ToString("O", CultureInfo.InvariantCulture));
                if (status == HttpStatusCode.PartialContent)
                    response.Content.Headers.ContentRange = new ContentRangeHeaderValue(512, 1023, 2048);
                if (status == HttpStatusCode.BadGateway)
                    response.Headers.TryAddWithoutValidation("x-ms-error-code", "ServerBusy");
                return response;
            }
            catch
            {
                response.Dispose();
                throw;
            }
        }
    }

    private sealed class BodyProbe(byte[] bytes, bool stallAtEnd, TimeSpan delay = default) : Stream
    {
        private int _position;
        internal long DeclaredLength => stallAtEnd ? 64 : bytes.Length;
        internal int Active { get; private set; }
        internal int ReadCount { get; private set; }
        internal TaskCompletionSource Stalled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal bool CancellationObserved { get; private set; }
        internal bool Disposed { get; private set; }
        internal bool DisposedWhileReading { get; private set; }
        public override bool CanRead => !Disposed;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            ObjectDisposedException.ThrowIf(Disposed, this);
            Active++;
            ReadCount++;
            try
            {
                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                if (_position < bytes.Length)
                {
                    var count = Math.Min(buffer.Length, delay > TimeSpan.Zero ? 1 : bytes.Length - _position);
                    bytes.AsMemory(_position, count).CopyTo(buffer);
                    _position += count;
                    return count;
                }
                if (stallAtEnd)
                {
                    Stalled.TrySetResult();
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                }
                return 0;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                CancellationObserved = true;
                throw;
            }
            finally
            {
                Active--;
            }
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override void Flush() => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            DisposedWhileReading |= Active != 0;
            Disposed = true;
            base.Dispose(disposing);
        }
    }
}
