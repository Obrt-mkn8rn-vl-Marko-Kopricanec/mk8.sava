using System.ComponentModel.DataAnnotations;
using System.Net;
using System.Net.Http.Headers;
using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Configuration;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class UrlSourceHeaderTimeoutTests
{
    // All outbound HTTP is intercepted in memory; this reserved name is never resolved.
    private static readonly Uri SourceUri = new("https://header-source.example.test/blob");

    [Fact]
    public async Task SilentContentHeadersRefuseWithoutMutationAndReleaseGatewayAdmission()
    {
        using var source = new HeaderSource("content");
        var factory = CreateFactory(source);
        await using var lifetime = factory.ConfigureAwait(false);
        using var transport = new HttpClientTransport(factory.Server.CreateHandler());
        var owner = new BlobServiceClient(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });
        var container = owner.GetBlobContainerClient("source-header-timeout");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("preserved.bin");
        await blob.UploadAsync(BinaryData.FromString("original bytes")).ConfigureAwait(true);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value.ETag;
        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var error = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlockBlobClient(blob.Name).SyncUploadFromUriAsync(SourceUri, overwrite: true,
                cancellationToken: guard.Token)).ConfigureAwait(true);

        Assert.Equal("CannotVerifyCopySource", error.ErrorCode);
        Assert.Equal(500, error.Status);
        Assert.False(guard.IsCancellationRequested);
        Assert.True(source.CancellationObserved);
        Assert.Equal(0, source.Active);
        Assert.Equal(before, (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value.ETag);
        Assert.Equal("original bytes", (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        // The single Gateway permit is available to a later independent mutation.
        await container.GetBlobClient("after.bin").UploadAsync(BinaryData.FromString("after timeout")).ConfigureAwait(true);
    }

    [Theory]
    [InlineData("tags")]
    [InlineData("blocks")]
    [InlineData("head")]
    public async Task MetadataAndShapeHeaderWaitsUseTheSameDeadline(string phase)
    {
        using var source = new HeaderSource(phase);
        var factory = CreateFactory(source);
        await using var lifetime = factory.ConfigureAwait(false);
        var transfers = factory.Services.GetRequiredService<UrlTransferClient>();
        var request = CreateRequest();
        var consumed = false;
        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var error = await Assert.ThrowsAsync<AzureStorageException>(async () =>
        {
            if (string.Equals(phase, "head", StringComparison.Ordinal))
            {
                _ = await transfers.ReadPageCopySourceAsync(request, SourceUri.AbsoluteUri, guard.Token).ConfigureAwait(false);
            }
            else
            {
                _ = await transfers.ReadAsync(request, SourceUri.AbsoluteUri, sourceRange: null,
                    allowSourceCustomerProvidedKey: false, allowFileRequestIntent: false, maximumBytes: 512,
                    sourceLengthConflict: false, _ => { consumed = true; return Task.FromResult(0); }, guard.Token,
                    copySourceTags: string.Equals(phase, "tags", StringComparison.Ordinal),
                    preserveSourceShape: string.Equals(phase, "blocks", StringComparison.Ordinal)).ConfigureAwait(false);
            }
        }).ConfigureAwait(true);

        Assert.Equal("CannotVerifyCopySource", error.ErrorCode);
        Assert.False(consumed);
        Assert.False(guard.IsCancellationRequested);
        Assert.True(source.CancellationObserved);
        Assert.Equal(0, source.Active);
    }

    [Fact]
    public async Task CompletedHeadersDoNotApplyTheirDeadlineToLaterBodyConsumption()
    {
        using var source = new HeaderSource("none");
        var factory = CreateFactory(source);
        await using var lifetime = factory.ConfigureAwait(false);
        var transfers = factory.Services.GetRequiredService<UrlTransferClient>();
        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await transfers.ReadAsync(CreateRequest(), SourceUri.AbsoluteUri, sourceRange: null,
            allowSourceCustomerProvidedKey: false, allowFileRequestIntent: false, maximumBytes: 512,
            sourceLengthConflict: false, async response =>
            {
                await Task.Delay(TimeSpan.FromMilliseconds(300), guard.Token).ConfigureAwait(false);
                var output = new MemoryStream();
                await using var owned = output.ConfigureAwait(false);
                await response.Content.CopyToAsync(output, guard.Token).ConfigureAwait(false);
                return output.ToArray();
            }, guard.Token).ConfigureAwait(true);

        Assert.Equal("source bytes"u8.ToArray(), result.Value);
        Assert.False(source.CancellationObserved);
        Assert.Equal(0, source.Active);
    }

    [Theory]
    [InlineData(-1, false)]
    [InlineData(0, false)]
    [InlineData(99, false)]
    [InlineData(100, true)]
    [InlineData(30_000, true)]
    [InlineData(600_000, true)]
    [InlineData(600_001, false)]
    public void HeaderDeadlineConfigurationIsFiniteAndBounded(int milliseconds, bool valid)
    {
        var options = new SavaOptions { UrlTransferResponseHeaderTimeout = TimeSpan.FromMilliseconds(milliseconds) };
        var errors = options.Validate(new ValidationContext(options));
        Assert.Equal(!valid, errors.Any(error => error.MemberNames.Contains(
            nameof(SavaOptions.UrlTransferResponseHeaderTimeout), StringComparer.Ordinal)));
    }

    private static SavaWebApplicationFactory CreateFactory(HeaderSource source) => new(
        TimeProvider.System, new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:UrlTransferResponseHeaderTimeout"] = "00:00:00.100",
            ["Gateway:MaximumConcurrentRequests"] = "1",
            ["Gateway:MaximumQueuedRequests"] = "0",
        }, () => source, disableMaintenance: true);

    private static HttpRequest CreateRequest()
    {
        var http = new DefaultHttpContext();
        StorageRequestContext.Set(http, new StorageRequestContext
        {
            Account = SavaWebApplicationFactory.AccountName, RequestId = "header-deadline",
            ResourceKind = StorageResourceKind.Blob, CanonicalResourcePath = "/account/container/blob",
            ServiceVersion = "2023-11-03", Authorization = StorageAuthorization.Anonymous,
        });
        return http.Request;
    }

    private sealed class HeaderSource(string phase) : HttpMessageHandler
    {
        internal bool CancellationObserved { get; private set; }
        internal int Active { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(request);
            Active++;
            try
            {
                var silent = phase switch
                {
                    "content" => true,
                    "head" => request.Method == HttpMethod.Head,
                    "tags" => request.RequestUri?.Query.Contains("comp=tags", StringComparison.Ordinal) == true,
                    "blocks" => request.RequestUri?.Query.Contains("comp=blocklist", StringComparison.Ordinal) == true,
                    _ => false,
                };
                if (silent)
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                return CreateResponse();
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

        private static HttpResponseMessage CreateResponse()
        {
            var response = new HttpResponseMessage(HttpStatusCode.OK);
            try
            {
                response.Content = new ByteArrayContent("source bytes"u8.ToArray());
                response.Headers.ETag = new EntityTagHeaderValue("\"source-etag\"");
                response.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
                return response;
            }
            catch
            {
                response.Dispose();
                throw;
            }
        }
    }
}
