using System.Net;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageAccountAddressingTests
{
    // Reserved synthetic authorities exercise account selection, not an
    // approved public hostname or a reverse-proxy host allowlist.
    private const string UnknownHost = "missing.blob.example";
    private const string Payload = "account-bound signed content";

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task PathStyleSasUsesPathFallbackUnlessHostSelectsAnotherConfiguredAccount(
        bool configuredHost, bool uppercaseHost)
    {
        var factory = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);
        await using var disposal = factory.ConfigureAwait(true);
        using var sdkTransport = new HttpClientTransport(factory.Server.CreateHandler());
        var blob = await CreateBlobAsync(factory, sdkTransport, pathStyle: true).ConfigureAwait(true);
        using var transport = new HttpClient(factory.Server.CreateHandler());
        using var request = new HttpRequestMessage(HttpMethod.Get,
            blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)));
        var host = configuredHost ? SavaWebApplicationFactory.SecondAccountName + ".localhost" : UnknownHost;
        request.Headers.Host = uppercaseHost ? host.ToUpperInvariant() : host;
        using var response = await transport.SendAsync(request).ConfigureAwait(true);
        Assert.Equal(configuredHost ? HttpStatusCode.Forbidden : HttpStatusCode.OK, response.StatusCode);
        if (configuredHost)
        {
            Assert.Equal("AuthenticationFailed", Assert.Single(response.Headers.GetValues("x-ms-error-code")));
            Assert.Null(response.Headers.ETag);
        }
        else
        {
            Assert.Equal(Payload, await response.Content.ReadAsStringAsync().ConfigureAwait(true));
        }
        Assert.Equal(Payload, (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task HostStyleSasCannotRecoverItsOriginalAccountFromAnUnknownHost(
        bool configuredHost, bool uppercaseHost)
    {
        var factory = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);
        await using var disposal = factory.ConfigureAwait(true);
        using var sdkTransport = new HttpClientTransport(factory.Server.CreateHandler());
        var blob = await CreateBlobAsync(factory, sdkTransport, pathStyle: false).ConfigureAwait(true);
        using var transport = new HttpClient(factory.Server.CreateHandler());
        using var request = new HttpRequestMessage(HttpMethod.Get,
            blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)));
        var host = configuredHost ? SavaWebApplicationFactory.AccountName + ".localhost" : UnknownHost;
        request.Headers.Host = uppercaseHost ? host.ToUpperInvariant() : host;
        using var response = await transport.SendAsync(request).ConfigureAwait(true);
        Assert.Equal(configuredHost ? HttpStatusCode.OK : HttpStatusCode.Forbidden, response.StatusCode);
        if (configuredHost)
        {
            Assert.Equal(Payload, await response.Content.ReadAsStringAsync().ConfigureAwait(true));
        }
        else
        {
            Assert.Equal("AuthenticationFailed", Assert.Single(response.Headers.GetValues("x-ms-error-code")));
            Assert.Null(response.Headers.ETag);
        }
        Assert.Equal(Payload, (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    private static async Task<BlobClient> CreateBlobAsync(
        SavaWebApplicationFactory factory, HttpClientTransport transport, bool pathStyle)
    {
        Assert.DoesNotContain(factory.Services.GetServices<IHostedService>(),
            service => service is StorageMaintenanceService);
        const string account = SavaWebApplicationFactory.AccountName;
        var endpoint = pathStyle ? new Uri($"http://127.0.0.1/{account}") : new Uri($"http://{account}.localhost");
        var service = new BlobServiceClient(endpoint,
            new StorageSharedKeyCredential(account, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions
            {
                Transport = transport,
                Retry = { MaxRetries = 0 },
            });
        var container = service.GetBlobContainerClient($"account-address-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("present.bin");
        await blob.UploadAsync(BinaryData.FromString(Payload)).ConfigureAwait(false);
        return blob;
    }
}
