using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class SharedKeyEscapedPathTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData("plus+.bin")]
    [InlineData("percent%2B.bin")]
    [InlineData("ampersand&.bin")]
    [InlineData("colon:.bin")]
    [InlineData("at@.bin")]
    [InlineData("escaped +%/present.bin")]
    public async Task SdkSharedKeyPreservesEscapedObjectIdentityAndRejectsWrongKey(string name)
    {
        var factory = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);
        await using var factoryDisposal = factory.ConfigureAwait(true);
        var application = factory.WithWebHostBuilder(builder => builder.ConfigureServices(services =>
            services.AddSingleton<IStartupFilter>(new RequestPathObservation(output))));
        await using var applicationDisposal = application.ConfigureAwait(true);
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        Assert.DoesNotContain(application.Services.GetServices<IHostedService>(),
            service => service is StorageMaintenanceService);
        var options = new BlobClientOptions
        {
            Transport = transport,
            Retry = { MaxRetries = 0 },
        };
        var endpoint = new Uri("http://127.0.0.1/" + SavaWebApplicationFactory.AccountName);
        var service = new BlobServiceClient(endpoint,
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            options);
        var container = service.GetBlobContainerClient($"escaped-key-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient(name);
        var payload = BinaryData.FromString("exact object: " + name);
        output.WriteLine("SDK path: {0}", blob.Uri.AbsolutePath);
        await blob.UploadAsync(payload).ConfigureAwait(true);
        Assert.Equal(payload.ToString(), (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());

        var denied = new BlobClient(blob.Uri,
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.SecondAccountKey),
            options);
        var failure = await Assert.ThrowsAsync<RequestFailedException>(() => denied.UploadAsync(BinaryData.FromString("replacement"), overwrite: true))
            .ConfigureAwait(true);
        Assert.Equal(403, failure.Status);
        Assert.Equal("AuthenticationFailed", failure.ErrorCode);
        Assert.Equal(payload.ToString(), (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        var names = new List<string>();
        await foreach (var item in container.GetBlobsAsync().ConfigureAwait(true))
            names.Add(item.Name);
        Assert.Equal(name, Assert.Single(names));
    }

    [Theory]
    [InlineData("plus+.bin", "plus%2B.bin")]
    [InlineData("percent%2B.bin", "percent%252B.bin")]
    public async Task SignatureForOneDecodedNameCannotReadALiteralPercentName(string signedName, string otherName)
    {
        var factory = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);
        await using var disposal = factory.ConfigureAwait(true);
        using var setupTransport = new HttpClientTransport(factory.Server.CreateHandler());
        Assert.DoesNotContain(factory.Services.GetServices<IHostedService>(),
            service => service is StorageMaintenanceService);
        var credential = new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);
        var service = new BlobServiceClient(new Uri("http://127.0.0.1/" + SavaWebApplicationFactory.AccountName),
            credential, new BlobClientOptions { Transport = setupTransport, Retry = { MaxRetries = 0 } });
        var container = service.GetBlobContainerClient($"escaped-replay-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var signedBlob = container.GetBlobClient(signedName);
        var otherBlob = container.GetBlobClient(otherName);
        foreach (var blob in new[] { signedBlob, otherBlob })
        {
            // SAS signs the decoded resource identity, so setup does not depend
            // on the SharedKey escaped-path fallback under test.
            var writable = new BlobClient(blob.GenerateSasUri(BlobSasPermissions.Create | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(5)), new BlobClientOptions { Transport = setupTransport, Retry = { MaxRetries = 0 } });
            await writable.UploadAsync(BinaryData.FromString("object: " + blob.Name)).ConfigureAwait(true);
        }
        using var substitutedPath = new SubstituteSignedPathHandler(factory.Server.CreateHandler(), otherBlob.Uri);
        using var redirectedHttp = new HttpClient(substitutedPath, disposeHandler: false);
        using var redirectedTransport = new HttpClientTransport(redirectedHttp);
        var signedClient = new BlobClient(signedBlob.Uri, credential,
            new BlobClientOptions { Transport = redirectedTransport, Retry = { MaxRetries = 0 } });
        var failure = await Assert.ThrowsAsync<RequestFailedException>(() => signedClient.DownloadContentAsync()).ConfigureAwait(true);
        Assert.Equal(403, failure.Status);
        Assert.Equal("AuthenticationFailed", failure.ErrorCode);
        foreach (var blob in new[] { signedBlob, otherBlob })
        {
            var readable = new BlobClient(blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)),
                new BlobClientOptions { Transport = setupTransport, Retry = { MaxRetries = 0 } });
            Assert.Equal("object: " + blob.Name, (await readable.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        }
    }

    private sealed class SubstituteSignedPathHandler(HttpMessageHandler inner, Uri destination) : DelegatingHandler(inner)
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(request);
            // The SDK has already signed the original URI. Only this controlled
            // TestServer transport substitutes the object path afterward.
            request.RequestUri = destination;
            return base.SendAsync(request, cancellationToken);
        }
    }

    private sealed class RequestPathObservation(ITestOutputHelper output) : IStartupFilter
    {
        public Action<IApplicationBuilder> Configure(Action<IApplicationBuilder> next) => application =>
        {
            application.Use(async (context, continuation) =>
            {
                output.WriteLine("RawTarget={0}; Path={1}; Candidates={2}",
                    context.Features.Get<IHttpRequestFeature>()?.RawTarget ?? "<absent>",
                    context.Request.Path.Value,
                    string.Join('|', StorageResourcePath.GetSignaturePathCandidates(context.Request)));
                await continuation(context).ConfigureAwait(false);
            });
            next(application);
        };
    }
}
