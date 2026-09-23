using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace Mk8.Sava.Tests;

public sealed class AzureOperationResponseShapeTests(SavaWebApplicationFactory factory)
    : IClassFixture<SavaWebApplicationFactory>
{
    private static readonly string[] ReadOnlyBlobHeaders =
    [
        "Accept-Ranges",
        "Content-Type",
        "Content-Encoding",
        "Content-Language",
        "Content-Disposition",
        "Cache-Control",
        "x-ms-creation-time",
        "x-ms-blob-type",
        "x-ms-server-encrypted",
        "x-ms-access-tier",
        "x-ms-access-tier-inferred",
        "x-ms-archive-status",
        "x-ms-rehydrate-priority",
        "x-ms-expiry-time",
        "x-ms-lease-status",
        "x-ms-lease-state",
        "x-ms-lease-duration",
        "x-ms-meta-marker",
        "x-ms-tag-count",
        "x-ms-snapshot",
        "x-ms-blob-sequence-number",
        "x-ms-blob-committed-block-count",
        "x-ms-blob-sealed",
        "x-ms-copy-source",
        "x-ms-copy-progress",
        "x-ms-copy-completion-time",
        "x-ms-copy-status-description",
        "x-ms-incremental-copy",
        "x-ms-copy-destination-snapshot",
        "x-ms-immutability-policy-until-date",
        "x-ms-immutability-policy-mode",
        "x-ms-legal-hold"
    ];

    [Fact]
    public async Task BlobOperationsExposeOnlyTheirAzureResponseHeaders()
    {
        var service = CreateClient();
        var container = service.GetBlobContainerClient($"operation-response-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var block = await AssertBlockResponsesAsync(container).ConfigureAwait(true);
        await AssertCopyResponseAsync(container, block).ConfigureAwait(true);
        await AssertAppendResponsesAsync(container).ConfigureAwait(true);
        await AssertPageResponsesAsync(container).ConfigureAwait(true);
    }

    private static async Task<BlockBlobClient> AssertBlockResponsesAsync(BlobContainerClient container)
    {
        var block = container.GetBlockBlobClient("block.bin");
        var upload = await block.UploadAsync(
            BinaryData.FromString("initial block payload").ToStream(),
            new BlobUploadOptions
            {
                Metadata = MarkerMetadata(),
                Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["class"] = "write" },
                HttpHeaders = new BlobHttpHeaders
                {
                    ContentType = "application/x-operation-response",
                    CacheControl = "private"
                }
            }).ConfigureAwait(false);
        AssertEntityResponse(upload.GetRawResponse());
        AssertRequestServerEncrypted(upload.GetRawResponse());
        AssertNoReadOnlyHeaders(upload.GetRawResponse());
        AssertNoCopyOperationHeaders(upload.GetRawResponse());

        var setBlockProperties = await block.SetHttpHeadersAsync(
            new BlobHttpHeaders { ContentType = "application/x-updated-response" }).ConfigureAwait(false);
        AssertEntityResponse(setBlockProperties.GetRawResponse());
        AssertNoReadOnlyHeaders(setBlockProperties.GetRawResponse());

        var blockId = Convert.ToBase64String("response-block-0001"u8);
        var staged = await block.StageBlockAsync(blockId,
            BinaryData.FromString("committed block payload").ToStream()).ConfigureAwait(false);
        Assert.Equal(201, staged.GetRawResponse().Status);
        AssertRequestServerEncrypted(staged.GetRawResponse());
        AssertNoReadOnlyHeaders(staged.GetRawResponse());
        Assert.False(staged.GetRawResponse().Headers.TryGetValue("ETag", out _));

        var committed = await block.CommitBlockListAsync(
            [blockId],
            new CommitBlockListOptions { Metadata = MarkerMetadata() }).ConfigureAwait(false);
        AssertEntityResponse(committed.GetRawResponse());
        AssertRequestServerEncrypted(committed.GetRawResponse());
        AssertNoReadOnlyHeaders(committed.GetRawResponse());
        AssertNoCopyOperationHeaders(committed.GetRawResponse());
        return block;
    }

    private static async Task AssertCopyResponseAsync(BlobContainerClient container, BlockBlobClient block)
    {
        var asynchronousCopy = container.GetBlobClient("asynchronous-copy.bin");
        var copyOperation = await asynchronousCopy.StartCopyFromUriAsync(
            block.Uri,
            new BlobCopyFromUriOptions { Metadata = MarkerMetadata() }).ConfigureAwait(false);
        Assert.Equal(202, copyOperation.GetRawResponse().Status);
        AssertEntityResponse(copyOperation.GetRawResponse());
        AssertNoReadOnlyHeaders(copyOperation.GetRawResponse());
        Assert.True(copyOperation.GetRawResponse().Headers.TryGetValue("x-ms-copy-id", out _));
        Assert.Equal("pending", Header(copyOperation.GetRawResponse(), "x-ms-copy-status"));
    }

    private static async Task AssertAppendResponsesAsync(BlobContainerClient container)
    {
        var append = container.GetAppendBlobClient("append.bin");
        var appendCreated = await append.CreateAsync(new AppendBlobCreateOptions { Metadata = MarkerMetadata() })
            .ConfigureAwait(false);
        AssertEntityResponse(appendCreated.GetRawResponse());
        AssertRequestServerEncrypted(appendCreated.GetRawResponse());
        AssertNoReadOnlyHeaders(appendCreated.GetRawResponse());

        var appended = await append.AppendBlockAsync(BinaryData.FromString("append payload").ToStream())
            .ConfigureAwait(false);
        AssertEntityResponse(appended.GetRawResponse());
        AssertRequestServerEncrypted(appended.GetRawResponse());
        Assert.Equal("0", Header(appended.GetRawResponse(), "x-ms-blob-append-offset"));
        Assert.Equal("1", Header(appended.GetRawResponse(), "x-ms-blob-committed-block-count"));
        AssertNoReadOnlyHeaders(appended.GetRawResponse(), "x-ms-blob-committed-block-count");

        var sealedAppend = await append.SealAsync().ConfigureAwait(false);
        AssertEntityResponse(sealedAppend.GetRawResponse());
        Assert.Equal("true", Header(sealedAppend.GetRawResponse(), "x-ms-blob-sealed"));
        AssertNoReadOnlyHeaders(sealedAppend.GetRawResponse(), "x-ms-blob-sealed");
    }

    private static async Task AssertPageResponsesAsync(BlobContainerClient container)
    {
        var page = container.GetPageBlobClient("page.bin");
        var pageCreated = await page.CreateAsync(
            1024,
            new PageBlobCreateOptions { Metadata = MarkerMetadata(), SequenceNumber = 7 }).ConfigureAwait(false);
        AssertEntityResponse(pageCreated.GetRawResponse());
        AssertRequestServerEncrypted(pageCreated.GetRawResponse());
        AssertNoReadOnlyHeaders(pageCreated.GetRawResponse());

        using var pageBytes = new MemoryStream(new byte[512]);
        var pageWritten = await page.UploadPagesAsync(pageBytes, 0).ConfigureAwait(false);
        AssertEntityResponse(pageWritten.GetRawResponse());
        AssertRequestServerEncrypted(pageWritten.GetRawResponse());
        Assert.Equal("7", Header(pageWritten.GetRawResponse(), "x-ms-blob-sequence-number"));
        AssertNoReadOnlyHeaders(pageWritten.GetRawResponse(), "x-ms-blob-sequence-number");

        var sequenceUpdated = await page.UpdateSequenceNumberAsync(SequenceNumberAction.Update, 9)
            .ConfigureAwait(false);
        AssertEntityResponse(sequenceUpdated.GetRawResponse());
        Assert.Equal("9", Header(sequenceUpdated.GetRawResponse(), "x-ms-blob-sequence-number"));
        AssertNoReadOnlyHeaders(sequenceUpdated.GetRawResponse(), "x-ms-blob-sequence-number");

        var ranges = await page.GetPageRangesAsync().ConfigureAwait(false);
        AssertEntityResponse(ranges.GetRawResponse());
        Assert.Equal("1024", Header(ranges.GetRawResponse(), "x-ms-blob-content-length"));
        AssertNoReadOnlyHeaders(ranges.GetRawResponse(), "Content-Type");
    }

    private static Dictionary<string, string> MarkerMetadata() => new(StringComparer.Ordinal) { ["marker"] = "must-not-leak" };

    private static void AssertEntityResponse(Response response)
    {
        Assert.True(response.Headers.TryGetValue("ETag", out _));
        Assert.True(response.Headers.TryGetValue("Last-Modified", out _));
    }

    private static void AssertRequestServerEncrypted(Response response) =>
        Assert.Equal("true", Header(response, "x-ms-request-server-encrypted"));

    private static void AssertNoCopyOperationHeaders(Response response)
    {
        Assert.False(response.Headers.TryGetValue("x-ms-copy-id", out _));
        Assert.False(response.Headers.TryGetValue("x-ms-copy-status", out _));
    }

    private static void AssertNoReadOnlyHeaders(Response response, params string[] allowed)
    {
        foreach (var header in ReadOnlyBlobHeaders.Except(allowed, StringComparer.OrdinalIgnoreCase))
            Assert.False(response.Headers.TryGetValue(header, out _), $"Unexpected response header: {header}");
    }

    private static string Header(Response response, string name)
    {
        Assert.True(response.Headers.TryGetValue(name, out var value), $"Missing response header: {name}");
        return value;
    }

    private BlobServiceClient CreateClient()
    {
        var endpoint = new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost");
        return new BlobServiceClient(
            endpoint,
            new StorageSharedKeyCredential(
                SavaWebApplicationFactory.AccountName,
                SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions
            {
                Transport = new HttpClientTransport(factory.Server.CreateHandler()),
                Retry = { MaxRetries = 0 }
            });
    }
}
