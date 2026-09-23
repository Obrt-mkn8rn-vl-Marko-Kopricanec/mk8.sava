using System.Net;
using System.Security.Cryptography;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class AzureTransactionalChecksumResponseTests(SavaWebApplicationFactory factory)
    : IClassFixture<SavaWebApplicationFactory>
{
    [Fact]
    public async Task TransactionalChecksumResponsesAreComputedAndVersionExact()
    {
        var service = CreateClient();
        var container = service.GetBlobContainerClient($"checksum-response-{Guid.NewGuid():N}");
        await container.CreateAsync();
        using var transport = new HttpClient(factory.Server.CreateHandler());

        var putContent = "put blob response checksum"u8.ToArray();
        using (var response = await PutBlobAsync(
                   transport,
                   container.GetBlockBlobClient("put.bin"),
                   putContent,
                   "2023-11-03"))
        {
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            AssertChecksums(response, putContent, expectMd5: true, expectCrc64: true);
        }

        var blockContent = "put block response checksum"u8.ToArray();
        using (var response = await PutBlockAsync(
                   transport,
                   container.GetBlockBlobClient("modern-no-md5.bin"),
                   blockContent,
                   "2023-11-03",
                   sendMd5: false))
        {
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            AssertChecksums(response, blockContent, expectMd5: false, expectCrc64: true);
        }

        using (var response = await PutBlockAsync(
                   transport,
                   container.GetBlockBlobClient("modern-md5.bin"),
                   blockContent,
                   "2023-11-03",
                   sendMd5: true))
        {
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            AssertChecksums(response, blockContent, expectMd5: true, expectCrc64: false);
        }

        using (var response = await PutBlockAsync(
                   transport,
                   container.GetBlockBlobClient("latest-md5.bin"),
                   blockContent,
                   "2026-12-06",
                   sendMd5: true))
        {
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            AssertChecksums(response, blockContent, expectMd5: true, expectCrc64: true);
        }

        using (var response = await PutBlockAsync(
                   transport,
                   container.GetBlockBlobClient("legacy.bin"),
                   blockContent,
                   "2018-11-09",
                   sendMd5: false))
        {
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            AssertChecksums(response, blockContent, expectMd5: true, expectCrc64: false);
        }
    }

    private static async Task<HttpResponseMessage> PutBlobAsync(
        HttpClient transport,
        BlockBlobClient blob,
        byte[] content,
        string version)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Put,
            blob.GenerateSasUri(
                BlobSasPermissions.Create | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent(content)
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
        return await transport.SendAsync(request).ConfigureAwait(false);
    }

    private static async Task<HttpResponseMessage> PutBlockAsync(
        HttpClient transport,
        BlockBlobClient blob,
        byte[] content,
        string version,
        bool sendMd5)
    {
        var blockId = Convert.ToBase64String("checksum-block-0001"u8);
        var uri = AppendQuery(
            blob.GenerateSasUri(
                BlobSasPermissions.Create | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(5)),
            $"comp=block&blockid={Uri.EscapeDataString(blockId)}");
        using var request = new HttpRequestMessage(HttpMethod.Put, uri)
        {
            Content = new ByteArrayContent(content)
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        if (sendMd5)
            request.Content.Headers.ContentMD5 = AzureProtocolChecksum.Md5(content);
        return await transport.SendAsync(request).ConfigureAwait(false);
    }

    private static void AssertChecksums(
        HttpResponseMessage response,
        byte[] content,
        bool expectMd5,
        bool expectCrc64)
    {
        var expectedMd5 = Convert.ToBase64String(AzureProtocolChecksum.Md5(content));
        var actualMd5 = response.Content.Headers.ContentMD5 is { } md5
            ? Convert.ToBase64String(md5)
            : null;
        if (expectMd5)
            Assert.Equal(expectedMd5, actualMd5);
        else
            Assert.Null(actualMd5);

        var crc64 = new StorageCrc64();
        crc64.Append(content);
        var expectedCrc64 = Convert.ToBase64String(crc64.GetHash());
        var hasCrc64 = response.Headers.TryGetValues("x-ms-content-crc64", out var values);
        if (expectCrc64)
        {
            Assert.True(hasCrc64);
            Assert.Equal(expectedCrc64, values!.Single());
        }
        else
        {
            Assert.False(hasCrc64);
        }
    }

    private static Uri AppendQuery(Uri uri, string query)
    {
        var builder = new UriBuilder(uri)
        {
            Query = string.IsNullOrEmpty(uri.Query)
                ? query
                : uri.Query.TrimStart('?') + "&" + query
        };
        return builder.Uri;
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
