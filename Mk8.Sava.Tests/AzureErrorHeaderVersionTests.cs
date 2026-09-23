using System.Net;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;

namespace Mk8.Sava.Tests;

public sealed class AzureErrorHeaderVersionTests(SavaWebApplicationFactory factory)
    : IClassFixture<SavaWebApplicationFactory>
{
    [Fact]
    public async Task FailedReadsExposeErrorCodeHeaderOnlyFromVersion20170729()
    {
        var account = SavaWebApplicationFactory.AccountName;
        var client = new BlobServiceClient(
            new Uri($"http://{account}.localhost"),
            new StorageSharedKeyCredential(account, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions
            {
                Transport = new HttpClientTransport(factory.Server.CreateHandler()),
                Retry = { MaxRetries = 0 }
            });
        var container = client.GetBlobContainerClient($"error-version-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("present.bin");
        await blob.UploadAsync(BinaryData.FromString("versioned response")).ConfigureAwait(true);
        var etag = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value.ETag.ToString();
        var expiry = DateTimeOffset.UtcNow.AddMinutes(5);
        var readUri = blob.GenerateSasUri(BlobSasPermissions.Read, expiry);
        var missingUri = container.GetBlobClient("missing.bin").GenerateSasUri(BlobSasPermissions.Read, expiry);
        using var transport = new HttpClient(factory.Server.CreateHandler());

        await AssertVersionAsync(transport, readUri, missingUri, etag, "2017-04-17", false).ConfigureAwait(true);
        await AssertVersionAsync(transport, readUri, missingUri, etag, "2017-07-29", true).ConfigureAwait(true);
        await AssertVersionAsync(transport, readUri, missingUri, etag, "2023-11-03", true).ConfigureAwait(true);
    }

    private static async Task AssertVersionAsync(
        HttpClient transport, Uri readUri, Uri missingUri, string etag, string version, bool expectHeader)
    {
        using var conditionalRequest = new HttpRequestMessage(HttpMethod.Get, readUri);
        conditionalRequest.Headers.TryAddWithoutValidation("x-ms-version", version);
        conditionalRequest.Headers.TryAddWithoutValidation("If-None-Match", etag);
        using var conditional = await transport.SendAsync(conditionalRequest).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.NotModified, conditional.StatusCode);
        AssertErrorHeader(conditional, expectHeader, "ConditionNotMet");
        Assert.Empty(await conditional.Content.ReadAsByteArrayAsync().ConfigureAwait(false));

        using var missingRequest = new HttpRequestMessage(HttpMethod.Get, missingUri);
        missingRequest.Headers.TryAddWithoutValidation("x-ms-version", version);
        using var missing = await transport.SendAsync(missingRequest).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);
        AssertErrorHeader(missing, expectHeader, "BlobNotFound");
    }

    private static void AssertErrorHeader(HttpResponseMessage response, bool expected, string code)
    {
        if (expected)
            Assert.Equal(code, response.Headers.GetValues("x-ms-error-code").Single());
        else
            Assert.False(response.Headers.Contains("x-ms-error-code"));
    }
}
