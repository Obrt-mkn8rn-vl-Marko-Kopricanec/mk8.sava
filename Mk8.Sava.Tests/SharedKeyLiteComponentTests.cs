using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;

namespace Mk8.Sava.Tests;

public sealed class SharedKeyLiteComponentTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LiteSignsComponentAndEntityMethodButNotTimeout(bool ordinaryDateOnly)
    {
        var application = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);
        await using var lifetime = application.ConfigureAwait(false);
        using var sdkTransport = new HttpClientTransport(application.Server.CreateHandler());
        var options = new BlobClientOptions { Transport = sdkTransport, Retry = { MaxRetries = 0 } };
        var owner = new BlobServiceClient(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey), options);
        var container = owner.GetBlobContainerClient("lite-component");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("present.bin");
        await blob.UploadAsync(BinaryData.FromString("component fixture")).ConfigureAwait(true);
        await blob.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["proof"] = "retained" })
            .ConfigureAwait(true);
        using var client = new HttpClient(application.Server.CreateHandler());
        using var request = new HttpRequestMessage(HttpMethod.Head, new Uri(blob.Uri + "?comp=metadata&timeout=7"));
        SignComponent(request, ordinaryDateOnly);
        using var response = await client.SendAsync(request).ConfigureAwait(true);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("retained", Assert.Single(response.Headers.GetValues("x-ms-meta-proof")));
        Assert.Empty(await response.Content.ReadAsByteArrayAsync().ConfigureAwait(true));

        await AssertReusedMacAsync(client, request, new Uri(blob.Uri + "?comp=metadata&timeout=9"), HttpStatusCode.OK)
            .ConfigureAwait(true);
        await AssertReusedMacAsync(client, request, new Uri(blob.Uri + "?comp=tags&timeout=7"), HttpStatusCode.Forbidden)
            .ConfigureAwait(true);
        Assert.Equal("component fixture", (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    private static async Task AssertReusedMacAsync(HttpClient client, HttpRequestMessage original, Uri uri, HttpStatusCode expected)
    {
        using var changed = new HttpRequestMessage(HttpMethod.Head, uri);
        foreach (var header in original.Headers)
            changed.Headers.TryAddWithoutValidation(header.Key, header.Value);
        using var response = await client.SendAsync(changed).ConfigureAwait(false);
        Assert.Equal(expected, response.StatusCode);
        if (expected == HttpStatusCode.Forbidden)
            Assert.Equal("AuthenticationFailed", Assert.Single(response.Headers.GetValues("x-ms-error-code")));
    }

    private static void SignComponent(HttpRequestMessage request, bool ordinaryDateOnly)
    {
        var uri = request.RequestUri ?? throw new InvalidOperationException("The finite signing fixture requires its URI.");
        var date = DateTimeOffset.UtcNow.ToString("R", CultureInfo.InvariantCulture);
        request.Headers.TryAddWithoutValidation(ordinaryDateOnly ? "Date" : "x-ms-date", date);
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        // Independent literal Blob Lite string; no production/test canonicalization helper.
        var text = "HEAD\n\n\n" + (ordinaryDateOnly ? date : string.Empty) + "\n" +
            (ordinaryDateOnly ? string.Empty : "x-ms-date:" + date + "\n") + "x-ms-version:2023-11-03\n/" +
            SavaWebApplicationFactory.AccountName + uri.AbsolutePath + "?comp=metadata";
        using var mac = new HMACSHA256(Convert.FromBase64String(SavaWebApplicationFactory.AccountKey));
        request.Headers.Authorization = new AuthenticationHeaderValue("SharedKeyLite",
            SavaWebApplicationFactory.AccountName + ":" + Convert.ToBase64String(mac.ComputeHash(Encoding.UTF8.GetBytes(text))));
    }
}
