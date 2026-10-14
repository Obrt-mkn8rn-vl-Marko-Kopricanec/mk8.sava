using System.Net;
using System.Net.Http.Headers;
using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace Mk8.Sava.Tests;

public sealed class UrlSourceRangeAdmissionTests
{
    // The source hostname is a finite intercepted fixture; no DNS or network is used.
    private static readonly Uri SourceUri = new("https://source.example.test/content");
    private static readonly byte[] SourceBytes = [.. Enumerable.Range(0, 2048)
        .Select(index => (byte)((index / 512) + 1))];
    private static readonly byte[] OriginalBytes = [.. Enumerable.Repeat((byte)0xA9, 512)];

    [Theory]
    [InlineData("block", "shifted")]
    [InlineData("block", "shifted-checksum")]
    [InlineData("block", "missing")]
    [InlineData("block", "unit")]
    [InlineData("block", "length")]
    [InlineData("block", "total")]
    [InlineData("append", "shifted")]
    [InlineData("append", "shifted-checksum")]
    [InlineData("append", "missing")]
    [InlineData("append", "unit")]
    [InlineData("append", "length")]
    [InlineData("append", "total")]
    [InlineData("page", "shifted")]
    [InlineData("page", "shifted-checksum")]
    [InlineData("page", "missing")]
    [InlineData("page", "unit")]
    [InlineData("page", "length")]
    [InlineData("page", "total")]
    public async Task InvalidSourceCoordinatesCannotStageOrMutateDestination(string operation, string alteration)
    {
        var application = CreateApplication(alteration);
        await using var lifetime = application.ConfigureAwait(false);
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        var container = CreateClient(transport).GetBlobContainerClient("range-target");
        await container.CreateAsync().ConfigureAwait(true);
        var destination = await SeedDestinationAsync(container, operation).ConfigureAwait(true);
        var before = (await destination.GetPropertiesAsync().ConfigureAwait(true)).Value.ETag;
        var failure = await Assert.ThrowsAsync<RequestFailedException>(() =>
            TransferAsync(container, operation, checksum: string.Equals(alteration, "shifted-checksum", StringComparison.Ordinal),
                wrongSourceBytes: alteration.StartsWith("shifted", StringComparison.Ordinal)))
            .ConfigureAwait(true);

        Assert.Equal(500, failure.Status);
        Assert.Equal("CannotVerifyCopySource", failure.ErrorCode);
        Assert.Equal(before, (await destination.GetPropertiesAsync().ConfigureAwait(true)).Value.ETag);
        Assert.Equal(OriginalBytes, (await destination.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());
        if (string.Equals(operation, "block", StringComparison.Ordinal))
        {
            Assert.Empty((await container.GetBlockBlobClient("target").GetBlockListAsync(BlockListTypes.Uncommitted)
                .ConfigureAwait(true)).Value.UncommittedBlocks);
        }
    }

    [Theory]
    [InlineData("block", false, false)]
    [InlineData("block", true, false)]
    [InlineData("append", false, false)]
    [InlineData("append", true, false)]
    [InlineData("page", false, false)]
    [InlineData("page", true, false)]
    [InlineData("block", false, true)]
    [InlineData("append", false, true)]
    [InlineData("page", false, true)]
    public async Task ExactSourceCoordinatesPublishOnlyRequestedBytes(string operation, bool checksum, bool unknownTotal)
    {
        var application = CreateApplication(unknownTotal ? "unknown-total" : "none");
        await using var lifetime = application.ConfigureAwait(false);
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        var container = CreateClient(transport).GetBlobContainerClient("range-target");
        await container.CreateAsync().ConfigureAwait(true);
        var destination = await SeedDestinationAsync(container, operation).ConfigureAwait(true);

        await TransferAsync(container, operation, checksum).ConfigureAwait(true);
        if (string.Equals(operation, "block", StringComparison.Ordinal))
            await container.GetBlockBlobClient("target").CommitBlockListAsync([BlockId]).ConfigureAwait(true);
        var requested = SourceBytes.AsSpan(512, 512).ToArray();
        byte[] expected = string.Equals(operation, "append", StringComparison.Ordinal)
            ? [.. OriginalBytes, .. requested]
            : requested;
        Assert.Equal(expected, (await destination.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());
    }

    private static string BlockId => Convert.ToBase64String("source-range-block"u8);

    private static SavaWebApplicationFactory CreateApplication(string alteration) =>
        new(TimeProvider.System, new Dictionary<string, string?>(StringComparer.Ordinal),
            () => new SourceHandler(alteration), disableMaintenance: true);

    private static BlobServiceClient CreateClient(HttpClientTransport transport)
    {
        var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03) { Transport = transport };
        options.Retry.MaxRetries = 0;
        return new BlobServiceClient(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey), options);
    }

    private static async Task<BlobClient> SeedDestinationAsync(BlobContainerClient container, string operation)
    {
        switch (operation)
        {
            case "block":
                await container.GetBlobClient("target").UploadAsync(BinaryData.FromBytes(OriginalBytes)).ConfigureAwait(false);
                break;
            case "append":
            {
                var append = container.GetAppendBlobClient("target");
                await append.CreateAsync().ConfigureAwait(false);
                var source = new MemoryStream(OriginalBytes, writable: false);
                await using var sourceLifetime = source.ConfigureAwait(false);
                await append.AppendBlockAsync(source).ConfigureAwait(false);
                break;
            }
            case "page":
            {
                var page = container.GetPageBlobClient("target");
                await page.CreateAsync(512).ConfigureAwait(false);
                var source = new MemoryStream(OriginalBytes, writable: false);
                await using var sourceLifetime = source.ConfigureAwait(false);
                await page.UploadPagesAsync(source, 0).ConfigureAwait(false);
                break;
            }
            default:
                throw new ArgumentException("Unknown fixture operation.", nameof(operation));
        }
        return container.GetBlobClient("target");
    }

    private static async Task TransferAsync(BlobContainerClient container, string operation, bool checksum, bool wrongSourceBytes = false)
    {
        var digest = checksum ? AzureProtocolChecksum.Md5(SourceBytes.AsSpan(wrongSourceBytes ? 0 : 512, 512)) : null;
        var range = new HttpRange(512, 512);
        switch (operation)
        {
            case "block":
                await container.GetBlockBlobClient("target").StageBlockFromUriAsync(SourceUri, BlockId,
                    new StageBlockFromUriOptions { SourceRange = range, SourceContentHash = digest }).ConfigureAwait(false);
                break;
            case "append":
                await container.GetAppendBlobClient("target").AppendBlockFromUriAsync(SourceUri,
                    new AppendBlobAppendBlockFromUriOptions { SourceRange = range, SourceContentHash = digest }).ConfigureAwait(false);
                break;
            case "page":
                await container.GetPageBlobClient("target").UploadPagesFromUriAsync(SourceUri, range, new HttpRange(0, 512),
                    new PageBlobUploadPagesFromUriOptions { SourceContentHash = digest }).ConfigureAwait(false);
                break;
            default:
                throw new ArgumentException("Unknown fixture operation.", nameof(operation));
        }
    }

    private sealed class SourceHandler(string alteration) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Assert.Equal(SourceUri, request.RequestUri);
            Assert.Equal(HttpMethod.Get, request.Method);
            Assert.Equal("bytes=512-1023", request.Headers.Range?.ToString());
            var content = new ByteArrayContent(SourceBytes.AsSpan(
                alteration.StartsWith("shifted", StringComparison.Ordinal) ? 0 : 512, 512).ToArray());
            content.Headers.ContentRange = alteration switch
            {
                "shifted" or "shifted-checksum" => new ContentRangeHeaderValue(0, 511, 2048),
                "missing" => null,
                "unit" => new ContentRangeHeaderValue(512, 1023, 2048) { Unit = "items" },
                "length" => new ContentRangeHeaderValue(512, 1022, 2048),
                "total" => null,
                "unknown-total" => new ContentRangeHeaderValue(512, 1023),
                _ => new ContentRangeHeaderValue(512, 1023, 2048),
            };
            if (string.Equals(alteration, "total", StringComparison.Ordinal))
                content.Headers.TryAddWithoutValidation("Content-Range", "bytes 512-1023/1023");
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.PartialContent) { Content = content });
        }
    }
}
