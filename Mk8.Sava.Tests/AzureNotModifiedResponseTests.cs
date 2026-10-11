using System.Net;
using System.Globalization;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mk8.Sava.Storage;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class AzureNotModifiedResponseTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData("2011-08-18", false, false)]
    [InlineData("2011-08-18", false, true)]
    [InlineData("2011-08-18", true, false)]
    [InlineData("2011-08-18", true, true)]
    [InlineData("2017-04-17", false, false)]
    [InlineData("2017-04-17", false, true)]
    [InlineData("2017-04-17", true, false)]
    [InlineData("2017-04-17", true, true)]
    [InlineData("2026-06-06", false, false)]
    [InlineData("2026-06-06", false, true)]
    [InlineData("2026-06-06", true, false)]
    [InlineData("2026-06-06", true, true)]
    public async Task ConditionalReadRetainsRepresentationValidators(
        string version, bool head, bool modifiedSince)
    {
        var factory = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);
        await using var disposal = factory.ConfigureAwait(true);
        using var sdkTransport = new HttpClientTransport(factory.Server.CreateHandler());
        var blob = await CreateBlobAsync(factory, sdkTransport).ConfigureAwait(true);
        using var transport = new HttpClient(factory.Server.CreateHandler());
        var uri = blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        using var baselineRequest = new HttpRequestMessage(HttpMethod.Get, uri);
        baselineRequest.Headers.Add("x-ms-version", version);
        using var baseline = await transport.SendAsync(baselineRequest).ConfigureAwait(true);
        Assert.Equal(HttpStatusCode.OK, baseline.StatusCode);
        var etag = Assert.Single(baseline.Headers.GetValues("ETag"));
        var lastModified = Assert.Single(baseline.Content.Headers.GetValues("Last-Modified"));
        var cacheControl = Assert.Single(baseline.Headers.GetValues("Cache-Control"));
        using var request = new HttpRequestMessage(head ? HttpMethod.Head : HttpMethod.Get, uri);
        request.Headers.Add("x-ms-version", version);
        request.Headers.TryAddWithoutValidation(modifiedSince ? "If-Modified-Since" : "If-None-Match",
            modifiedSince ? lastModified : etag);
        using var response = await transport.SendAsync(request).ConfigureAwait(true);
        var body = await response.Content.ReadAsByteArrayAsync().ConfigureAwait(true);
        ReportResponse(response, body);
        Assert.Equal(HttpStatusCode.NotModified, response.StatusCode);
        Assert.Empty(body);
        Assert.Equal(etag, Assert.Single(response.Headers.GetValues("ETag")));
        Assert.Equal(cacheControl, Assert.Single(response.Headers.GetValues("Cache-Control")));
        Assert.Equal(lastModified, Assert.Single(response.Content.Headers.GetValues("Last-Modified")));
        Assert.NotNull(response.Headers.Date);
        if (string.Equals(version, "2026-06-06", StringComparison.Ordinal))
            Assert.Equal("ConditionNotMet", Assert.Single(response.Headers.GetValues("x-ms-error-code")));
        else
            Assert.False(response.Headers.Contains("x-ms-error-code"));
        Assert.False(response.Content.Headers.Contains("Content-MD5"));
        Assert.False(response.Headers.Contains("x-ms-meta-private"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RejectedReadsDoNotExposeRepresentationHeaders(bool unauthorized)
    {
        var factory = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);
        await using var disposal = factory.ConfigureAwait(true);
        using var sdkTransport = new HttpClientTransport(factory.Server.CreateHandler());
        var blob = await CreateBlobAsync(factory, sdkTransport).ConfigureAwait(true);
        using var transport = new HttpClient(factory.Server.CreateHandler());
        var uri = blob.GenerateSasUri(unauthorized ? BlobSasPermissions.Write : BlobSasPermissions.Read,
            DateTimeOffset.UtcNow.AddMinutes(5));
        using var request = new HttpRequestMessage(HttpMethod.Get, uri);
        request.Headers.Add("x-ms-version", "2026-06-06");
        request.Headers.TryAddWithoutValidation("If-Match", "\"different-generation\"");
        using var response = await transport.SendAsync(request).ConfigureAwait(true);
        Assert.Equal(unauthorized ? HttpStatusCode.Forbidden : HttpStatusCode.PreconditionFailed,
            response.StatusCode);
        Assert.False(response.Headers.Contains("ETag"));
        Assert.False(response.Headers.Contains("Cache-Control"));
        Assert.False(response.Content.Headers.Contains("Last-Modified"));
        Assert.False(response.Headers.Contains("x-ms-meta-private"));
        Assert.Equal("validator body", (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConditionalReadRetainsCorsAndSignedCacheOverride(bool overrideCache)
    {
        var factory = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);
        await using var disposal = factory.ConfigureAwait(true);
        using var sdkTransport = new HttpClientTransport(factory.Server.CreateHandler());
        var blob = await CreateBlobAsync(factory, sdkTransport).ConfigureAwait(true);
        var service = CreateClient(sdkTransport);
        var properties = (await service.GetPropertiesAsync().ConfigureAwait(true)).Value;
        properties.Cors.Add(new BlobCorsRule
        {
            AllowedOrigins = "https://client.example",
            AllowedMethods = "GET,HEAD",
            AllowedHeaders = "*",
            ExposedHeaders = "ETag",
            MaxAgeInSeconds = 60,
        });
        await service.SetPropertiesAsync(properties).ConfigureAwait(true);
        var sas = new BlobSasBuilder
        {
            BlobContainerName = blob.BlobContainerName,
            BlobName = blob.Name,
            Resource = "b",
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(5),
            CacheControl = overrideCache ? "private, max-age=17" : null,
        };
        sas.SetPermissions(BlobSasPermissions.Read);
        var uri = blob.GenerateSasUri(sas);
        using var transport = new HttpClient(factory.Server.CreateHandler());
        using var baselineRequest = new HttpRequestMessage(HttpMethod.Get, uri);
        baselineRequest.Headers.Add("Origin", "https://client.example");
        using var baseline = await transport.SendAsync(baselineRequest).ConfigureAwait(true);
        Assert.Equal(HttpStatusCode.OK, baseline.StatusCode);
        var etag = Assert.Single(baseline.Headers.GetValues("ETag"));
        using var request = new HttpRequestMessage(HttpMethod.Get, uri);
        request.Headers.Add("Origin", "https://client.example");
        request.Headers.TryAddWithoutValidation("If-None-Match", etag);
        using var response = await transport.SendAsync(request).ConfigureAwait(true);
        var body = await response.Content.ReadAsByteArrayAsync().ConfigureAwait(true);
        ReportResponse(response, body);
        Assert.Equal(HttpStatusCode.NotModified, response.StatusCode);
        Assert.Empty(body);
        Assert.Equal(etag, Assert.Single(response.Headers.GetValues("ETag")));
        var cache = response.Headers.CacheControl;
        Assert.NotNull(cache);
        Assert.True(cache.Private);
        Assert.Equal(TimeSpan.FromSeconds(overrideCache ? 17 : 60), cache.MaxAge);
        Assert.Equal(baseline.Headers.CacheControl?.ToString(), cache.ToString());
        foreach (var header in new[] { "Vary", "Access-Control-Allow-Origin", "Access-Control-Expose-Headers" })
            Assert.Equal(baseline.Headers.GetValues(header), response.Headers.GetValues(header), StringComparer.Ordinal);
        Assert.False(response.Headers.Contains("x-ms-meta-private"));
    }

    private static BlobServiceClient CreateClient(HttpClientTransport transport)
    {
        const string account = SavaWebApplicationFactory.AccountName;
        return new BlobServiceClient(new Uri($"http://{account}.localhost"),
            new StorageSharedKeyCredential(account, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions
            {
                Transport = transport,
                Retry = { MaxRetries = 0 },
            });
    }

    private static async Task<BlobClient> CreateBlobAsync(
        SavaWebApplicationFactory factory, HttpClientTransport transport)
    {
        Assert.DoesNotContain(factory.Services.GetServices<IHostedService>(),
            service => service is StorageMaintenanceService);
        var client = CreateClient(transport);
        var container = client.GetBlobContainerClient($"not-modified-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("present.bin");
        await blob.UploadAsync(BinaryData.FromString("validator body"), new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders { CacheControl = "private, max-age=60" },
            Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["private"] = "not-a-304-header" },
        }).ConfigureAwait(false);
        return blob;
    }

    private void ReportResponse(HttpResponseMessage response, byte[] body) =>
        output.WriteLine("Status={0}; BodyLength={1}; ETag={2}; Cache-Control={3}; Last-Modified={4}; Vary={5}",
            ((int)response.StatusCode).ToString(CultureInfo.InvariantCulture),
            body.Length.ToString(CultureInfo.InvariantCulture),
            response.Headers.ETag?.ToString(), response.Headers.CacheControl?.ToString(),
            response.Content.Headers.LastModified?.ToString("R", CultureInfo.InvariantCulture),
            string.Join(',', response.Headers.Vary));
}
