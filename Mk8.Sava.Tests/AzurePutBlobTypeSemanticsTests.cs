using System.Net;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class AzurePutBlobTypeSemanticsTests(SavaWebApplicationFactory factory)
    : IClassFixture<SavaWebApplicationFactory>
{
    [Fact]
    public async Task PutBlobCannotChangeAnExistingBlobType()
    {
        var service = CreateClient();
        var container = service.GetBlobContainerClient($"put-type-{Guid.NewGuid():N}");
        await container.CreateAsync();

        var block = container.GetBlockBlobClient("block.bin");
        var blockPayload = "original block"u8.ToArray();
        await block.UploadAsync(new BinaryData(blockPayload).ToStream());

        var append = container.GetAppendBlobClient("append.bin");
        var appendPayload = "original append"u8.ToArray();
        await append.CreateAsync();
        await append.AppendBlockAsync(new BinaryData(appendPayload).ToStream());

        var page = container.GetPageBlobClient("page.bin");
        var pagePayload = new byte[512];
        "original page"u8.CopyTo(pagePayload);
        await page.CreateAsync(pagePayload.Length);
        await page.UploadPagesAsync(new MemoryStream(pagePayload), 0);

        using var transport = new HttpClient(factory.Server.CreateHandler());
        await AssertTypeChangeRejectedAsync(transport, block, "AppendBlob", []);
        await AssertTypeChangeRejectedAsync(transport, append, "PageBlob", []);
        await AssertTypeChangeRejectedAsync(transport, page, "BlockBlob", "replacement"u8.ToArray());

        Assert.Equal(BlobType.Block, (await block.GetPropertiesAsync()).Value.BlobType);
        Assert.Equal(blockPayload, (await block.DownloadContentAsync()).Value.Content.ToArray());
        Assert.Equal(BlobType.Append, (await append.GetPropertiesAsync()).Value.BlobType);
        Assert.Equal(appendPayload, (await append.DownloadContentAsync()).Value.Content.ToArray());
        Assert.Equal(BlobType.Page, (await page.GetPropertiesAsync()).Value.BlobType);
        Assert.Equal(pagePayload, (await page.DownloadContentAsync()).Value.Content.ToArray());
    }

    [Fact]
    public async Task PutBlobRejectsPageBlobOnlyHeadersForBlockAndAppendBlobs()
    {
        var service = CreateClient();
        var container = service.GetBlobContainerClient($"put-page-headers-{Guid.NewGuid():N}");
        await container.CreateAsync();
        using var transport = new HttpClient(factory.Server.CreateHandler());

        var cases = new[]
        {
            (Type: "BlockBlob", Header: "x-ms-blob-content-length", Value: "512"),
            (Type: "AppendBlob", Header: "x-ms-blob-content-length", Value: "512"),
            (Type: "BlockBlob", Header: "x-ms-blob-sequence-number", Value: "7"),
            (Type: "AppendBlob", Header: "x-ms-blob-sequence-number", Value: "7")
        };

        foreach (var (type, header, value) in cases)
        {
            var blob = container.GetBlobClient($"{type}-{header}-{Guid.NewGuid():N}.bin");
            using var request = CreatePutRequest(blob, type, []);
            request.Headers.TryAddWithoutValidation(header, value);
            using var response = await transport.SendAsync(request);

            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", ResponseHeader(response, "x-ms-error-code"));
            Assert.False((await blob.ExistsAsync()).Value);
        }
    }

    [Fact]
    public async Task MetadataPublicationCannotChangeAnActiveBlobType()
    {
        var service = CreateClient();
        var container = service.GetBlobContainerClient($"metadata-type-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlockBlobClient("immutable-type.bin");
        await blob.UploadAsync(BinaryData.FromString("original").ToStream());

        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var current = await metadata.GetBlobAsync(
                          SavaWebApplicationFactory.AccountName,
                          container.Name,
                          blob.Name,
                          versionId: null,
                          snapshot: null,
                          includeDeleted: false,
                          CancellationToken.None)
                      ?? throw new InvalidOperationException("The test blob was not persisted.");

        await Assert.ThrowsAsync<StorageBlobTypeMismatchException>(() => metadata.PublishBlobAsync(
            current with { Kind = BlobKind.AppendBlob },
            current.GenerationId,
            current.Revision,
            hierarchicalNamespace: false,
            CancellationToken.None));

        var persisted = await metadata.GetBlobAsync(
            current.Account,
            current.Container,
            current.Name,
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None);
        Assert.NotNull(persisted);
        Assert.Equal(BlobKind.BlockBlob, persisted.Kind);
        Assert.Equal(current.Revision, persisted.Revision);
    }

    private static async Task AssertTypeChangeRejectedAsync(
        HttpClient transport,
        BlobBaseClient blob,
        string requestedType,
        byte[] content)
    {
        using var request = CreatePutRequest(blob, requestedType, content);
        if (requestedType == "PageBlob")
            request.Headers.TryAddWithoutValidation("x-ms-blob-content-length", "512");
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        Assert.Equal("InvalidBlobType", ResponseHeader(response, "x-ms-error-code"));
    }

    private static HttpRequestMessage CreatePutRequest(BlobBaseClient blob, string type, byte[] content)
    {
        var request = new HttpRequestMessage(
            HttpMethod.Put,
            blob.GenerateSasUri(
                BlobSasPermissions.Create | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent(content)
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        request.Headers.TryAddWithoutValidation("x-ms-blob-type", type);
        return request;
    }

    private static string ResponseHeader(HttpResponseMessage response, string name) =>
        response.Headers.TryGetValues(name, out var values)
            ? values.Single()
            : throw new InvalidOperationException($"Response header {name} was not returned.");

    private BlobServiceClient CreateClient()
    {
        var transportClient = new HttpClient(factory.Server.CreateHandler())
        {
            BaseAddress = new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost")
        };
        return new BlobServiceClient(
            transportClient.BaseAddress,
            new StorageSharedKeyCredential(
                SavaWebApplicationFactory.AccountName,
                SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions
            {
                Transport = new HttpClientTransport(transportClient),
                Retry = { MaxRetries = 0 }
            });
    }
}
