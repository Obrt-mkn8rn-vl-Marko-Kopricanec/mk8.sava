using System.Net;
using System.Net.Http.Headers;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Mk8.Sava.Tests;

public sealed class WebsiteConditionalResponseTests
{
    [Theory]
    [InlineData(false, "etag", null)]
    [InlineData(true, "etag", null)]
    [InlineData(false, "date", null)]
    [InlineData(true, "date", null)]
    [InlineData(false, "miss", null)]
    [InlineData(true, "miss", null)]
    [InlineData(false, "none", null)]
    [InlineData(true, "none", null)]
    [InlineData(false, "etag", "2026-06-06")]
    [InlineData(true, "etag", "2026-06-06")]
    [InlineData(false, "date", "2026-06-06")]
    [InlineData(true, "date", "2026-06-06")]
    [InlineData(false, "miss", "2026-06-06")]
    [InlineData(true, "miss", "2026-06-06")]
    [InlineData(false, "none", "2026-06-06")]
    [InlineData(true, "none", "2026-06-06")]
    public async Task WebsiteConditionsPreserveTheSelectedValidatorsAndBodyRules(bool head, string condition, string? version)
    {
        var application = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);
        await using var applicationLifetime = application.ConfigureAwait(false);
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        var owner = new BlobServiceClient(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });
        var blob = await SeedWebsiteAsync(owner).ConfigureAwait(true);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value;
        using var web = new HttpClient(application.Server.CreateHandler());
        // This is an intercepted, finite website-addressing fixture, not a deployment hostname.
        var endpoint = $"http://{SavaWebApplicationFactory.AccountName}.z99.web.test/index.html";
        using var initial = await web.GetAsync(new Uri(endpoint)).ConfigureAwait(true);
        Assert.Equal(HttpStatusCode.OK, initial.StatusCode);
        Assert.Equal("website bytes", await initial.Content.ReadAsStringAsync().ConfigureAwait(true));
        var expected = new Representation(endpoint,
            Assert.IsType<EntityTagHeaderValue>(initial.Headers.ETag).ToString(),
            Assert.IsType<DateTimeOffset>(initial.Content.Headers.LastModified));
        await VerifyResponseAsync(web, expected, head, condition, version).ConfigureAwait(true);
        Assert.Equal(before.ETag, (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value.ETag);
        Assert.Equal("website bytes", (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    private static async Task<BlobClient> SeedWebsiteAsync(BlobServiceClient owner)
    {
        var properties = (await owner.GetPropertiesAsync().ConfigureAwait(false)).Value;
        properties.StaticWebsite.Enabled = true;
        properties.StaticWebsite.IndexDocument = "index.html";
        await owner.SetPropertiesAsync(properties).ConfigureAwait(false);
        var blob = owner.GetBlobContainerClient("$web").GetBlobClient("index.html");
        await blob.UploadAsync(BinaryData.FromString("website bytes"), new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders { ContentType = "text/html", CacheControl = "public,max-age=60" },
        }).ConfigureAwait(false);
        return blob;
    }

    private sealed record Representation(string Endpoint, string ETag, DateTimeOffset LastModified);

    private static async Task VerifyResponseAsync(HttpClient web, Representation expected,
        bool head, string condition, string? version)
    {
        using var request = new HttpRequestMessage(head ? HttpMethod.Head : HttpMethod.Get, expected.Endpoint);
        if (version is not null)
            Assert.True(request.Headers.TryAddWithoutValidation("x-ms-version", version));
        if (string.Equals(condition, "etag", StringComparison.Ordinal))
            request.Headers.IfNoneMatch.Add(EntityTagHeaderValue.Parse(expected.ETag));
        else if (string.Equals(condition, "date", StringComparison.Ordinal))
            request.Headers.IfModifiedSince = expected.LastModified;
        else if (string.Equals(condition, "miss", StringComparison.Ordinal))
            request.Headers.IfNoneMatch.Add(new EntityTagHeaderValue("\"another-representation\""));
        using var response = await web.SendAsync(request).ConfigureAwait(false);
        var notModified = condition is "etag" or "date";
        Assert.Equal(notModified ? HttpStatusCode.NotModified : HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(expected.ETag, response.Headers.ETag?.ToString());
        Assert.Equal(expected.LastModified, response.Content.Headers.LastModified);
        Assert.True(response.Headers.CacheControl?.Public);
        Assert.Equal(TimeSpan.FromSeconds(60), response.Headers.CacheControl?.MaxAge);
        Assert.Equal(head || notModified ? string.Empty : "website bytes",
            await response.Content.ReadAsStringAsync().ConfigureAwait(false));
        if (notModified)
        {
            Assert.Null(response.Content.Headers.ContentType);
            Assert.Null(response.Content.Headers.ContentRange);
            Assert.True(response.Content.Headers.ContentLength is null or 0);
        }
    }
}
