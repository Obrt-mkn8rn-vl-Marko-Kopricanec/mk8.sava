using System.Net;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class AzureRangeChecksumBoundaryTests(SavaWebApplicationFactory factory)
    : IClassFixture<SavaWebApplicationFactory>
{
    private const int MaximumTransactionalRange = 4 * 1024 * 1024;

    [Theory]
    [InlineData("x-ms-range-get-content-md5")]
    [InlineData("x-ms-range-get-content-crc64")]
    public async Task GetBlobTransactionalChecksumHonorsFourMiBBoundary(string checksumHeader)
    {
        var account = SavaWebApplicationFactory.AccountName;
        var service = new BlobServiceClient(
            new Uri($"http://{account}.localhost"),
            new StorageSharedKeyCredential(account, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions
            {
                Transport = new HttpClientTransport(factory.Server.CreateHandler()),
                Retry = { MaxRetries = 0 }
            });
        var container = service.GetBlobContainerClient($"checksum-boundary-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("payload.bin");
        var payload = new byte[MaximumTransactionalRange + 1];
        DeterministicTestBytes.Fill(0xA40B, payload);
        await blob.UploadAsync(BinaryData.FromBytes(payload)).ConfigureAwait(true);
        var readUri = blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        using var transport = new HttpClient(factory.Server.CreateHandler());

        await AssertMaximumRangeAsync(transport, readUri, checksumHeader, payload).ConfigureAwait(true);
        await AssertRejectedRangeAsync(transport, readUri, checksumHeader, null).ConfigureAwait(true);
        await AssertRejectedRangeAsync(
            transport, readUri, checksumHeader, $"bytes=0-{MaximumTransactionalRange}").ConfigureAwait(true);
    }

    private static async Task AssertMaximumRangeAsync(
        HttpClient transport, Uri uri, string checksumHeader, byte[] expected)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, uri);
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        request.Headers.TryAddWithoutValidation("Range", $"bytes={MaximumTransactionalRange}-{MaximumTransactionalRange}");
        request.Headers.TryAddWithoutValidation("x-ms-range", $"bytes=0-{MaximumTransactionalRange - 1}");
        request.Headers.TryAddWithoutValidation(checksumHeader, "true");
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.PartialContent, response.StatusCode);
        Assert.Equal($"bytes 0-{MaximumTransactionalRange - 1}/{expected.Length}",
            response.Content.Headers.ContentRange?.ToString());
        var returned = await response.Content.ReadAsByteArrayAsync().ConfigureAwait(false);
        Assert.Equal(expected.AsSpan(0, MaximumTransactionalRange).ToArray(), returned);
        if (string.Equals(checksumHeader, "x-ms-range-get-content-md5", StringComparison.Ordinal))
            Assert.Equal(AzureProtocolChecksum.Md5(returned), response.Content.Headers.ContentMD5);
        else
        {
            var crc64 = new StorageCrc64();
            crc64.Append(returned);
            Assert.Equal(Convert.ToBase64String(crc64.GetHash()),
                response.Headers.GetValues("x-ms-content-crc64").Single());
        }
    }

    private static async Task AssertRejectedRangeAsync(
        HttpClient transport, Uri uri, string checksumHeader, string? range)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, uri);
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        if (range is not null)
            request.Headers.TryAddWithoutValidation("Range", range);
        request.Headers.TryAddWithoutValidation(checksumHeader, "true");
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
    }
}
