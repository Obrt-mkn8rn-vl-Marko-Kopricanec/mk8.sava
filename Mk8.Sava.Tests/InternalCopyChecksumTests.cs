using System.Net;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Mk8.Sava.Protocol;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class InternalCopyChecksumTests(ITestOutputHelper output)
{
    private static readonly byte[] Payload = "source bytes, checksum admission"u8.ToArray();

    [Theory]
    [InlineData(false, "none", null)]
    [InlineData(true, "none", null)]
    [InlineData(false, "md5", null)]
    [InlineData(true, "md5", null)]
    [InlineData(false, "crc64", null)]
    [InlineData(true, "crc64", null)]
    [InlineData(false, "wrong-md5", "Md5Mismatch")]
    [InlineData(true, "wrong-md5", "Md5Mismatch")]
    [InlineData(false, "wrong-crc64", "Crc64Mismatch")]
    [InlineData(true, "wrong-crc64", "Crc64Mismatch")]
    [InlineData(false, "bad-md5", "InvalidHeaderValue")]
    [InlineData(true, "bad-md5", "InvalidHeaderValue")]
    [InlineData(false, "bad-crc64", "InvalidHeaderValue")]
    [InlineData(true, "bad-crc64", "InvalidHeaderValue")]
    [InlineData(false, "empty-md5", "InvalidHeaderValue")]
    [InlineData(true, "empty-md5", "InvalidHeaderValue")]
    [InlineData(false, "empty-crc64", "InvalidHeaderValue")]
    [InlineData(true, "empty-crc64", "InvalidHeaderValue")]
    [InlineData(false, "both", "BothCrc64AndMd5Specified")]
    [InlineData(true, "both", "BothCrc64AndMd5Specified")]
    public async Task InternalSynchronousCopyChecksSourceDigestsBeforeDestinationPublication(
        bool existingDestination, string supplied, string? errorCode)
    {
        var application = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);
        await using var applicationLifetime = application.ConfigureAwait(false);
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        var owner = new BlobServiceClient(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });
        var container = owner.GetBlobContainerClient("internal-copy-checksum");
        await container.CreateAsync().ConfigureAwait(true);
        var source = container.GetBlobClient("source.bin");
        await source.UploadAsync(BinaryData.FromBytes(Payload)).ConfigureAwait(true);
        var destination = container.GetBlobClient("destination.bin");
        string? before = null;
        if (existingDestination)
        {
            await destination.UploadAsync(BinaryData.FromString("original destination")).ConfigureAwait(true);
            await destination.SetTagsAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["old"] = "retained" }).ConfigureAwait(true);
            before = (await destination.GetPropertiesAsync().ConfigureAwait(true)).Value.ETag.ToString();
        }
        using var handler = CreateIngressHandler(application, supplied, output);
        using var client = new HttpClient(handler, disposeHandler: false);
        using var request = CreateRequest(source, destination, supplied);
        using var response = await client.SendAsync(request).ConfigureAwait(true);
        Assert.Equal(errorCode is null ? HttpStatusCode.Accepted : HttpStatusCode.BadRequest, response.StatusCode);
        if (errorCode is not null)
        {
            Assert.Equal(errorCode, Assert.Single(response.Headers.GetValues("x-ms-error-code")));
            Assert.False(response.Headers.Contains("ETag"));
        }
        await VerifyDestinationAsync(destination, existingDestination, errorCode is not null, before).ConfigureAwait(true);
        Assert.Equal(Payload, (await source.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());
    }

    private static HttpMessageHandler CreateIngressHandler(SavaWebApplicationFactory application, string supplied,
        ITestOutputHelper output) =>
        application.Server.CreateHandler(context =>
        {
            var name = supplied switch
            {
                "empty-md5" => "x-ms-source-content-md5",
                "empty-crc64" => "x-ms-source-content-crc64",
                _ => null,
            };
            if (name is null)
                return;
            var present = context.Request.Headers.TryGetValue(name, out var values);
            output.WriteLine($"Empty checksum control before explicit TestServer ingress: Header={name}, Present={present}, Count={values.Count}.");
            // An empty HttpRequestMessage value need not survive its adapter.
            // Establish the actual server input this refusal control requires.
            context.Request.Headers[name] = string.Empty;
            Assert.True(context.Request.Headers.ContainsKey(name));
            Assert.Equal(1, context.Request.Headers[name].Count);
            Assert.Equal(string.Empty, context.Request.Headers[name][0]);
        });

    private static HttpRequestMessage CreateRequest(BlobClient source, BlobClient destination, string supplied)
    {
        var md5 = Convert.ToBase64String(AzureProtocolChecksum.Md5(Payload));
        var crc = new StorageCrc64();
        crc.Append(Payload);
        var crc64 = Convert.ToBase64String(crc.GetHash());
        var (md5Header, crcHeader) = supplied switch
        {
            "none" => ((string?)null, null),
            "md5" => (md5, null),
            "crc64" => (null, crc64),
            "wrong-md5" => (Convert.ToBase64String(new byte[16]), null),
            "wrong-crc64" => (null, Convert.ToBase64String(new byte[8])),
            "bad-md5" => ("not-base64", null),
            "bad-crc64" => (null, "not-base64"),
            "empty-md5" => (string.Empty, null),
            "empty-crc64" => (null, string.Empty),
            "both" => (md5, crc64),
            _ => throw new ArgumentOutOfRangeException(nameof(supplied)),
        };
        var sourceUri = source.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        var destinationUri = destination.GenerateSasUri(BlobSasPermissions.Create | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(5));
        return CreateOwnedRequest(sourceUri, destinationUri, md5Header, crcHeader);
    }

    private static HttpRequestMessage CreateOwnedRequest(Uri source, Uri destination, string? md5, string? crc64)
    {
        var transferred = false;
        var request = new HttpRequestMessage(HttpMethod.Put, destination);
        try
        {
            request.Content = new ByteArrayContent([]);
            request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            request.Headers.TryAddWithoutValidation("x-ms-requires-sync", "true");
            request.Headers.TryAddWithoutValidation("x-ms-copy-source", source.AbsoluteUri);
            if (md5 is not null)
                request.Headers.TryAddWithoutValidation("x-ms-source-content-md5", md5);
            if (crc64 is not null)
                request.Headers.TryAddWithoutValidation("x-ms-source-content-crc64", crc64);
            transferred = true;
            return request;
        }
        finally
        {
            if (!transferred)
                request.Dispose();
        }
    }

    private static async Task VerifyDestinationAsync(BlobClient destination, bool existed, bool refused, string? before)
    {
        if (!refused)
        {
            Assert.Equal(Payload, (await destination.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
            return;
        }
        if (!existed)
        {
            Assert.False((await destination.ExistsAsync().ConfigureAwait(false)).Value);
            return;
        }
        Assert.Equal(before, (await destination.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag.ToString());
        Assert.Equal("original destination", (await destination.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        Assert.Equal(new KeyValuePair<string, string>("old", "retained"),
            Assert.Single((await destination.GetTagsAsync().ConfigureAwait(false)).Value.Tags));
    }
}
