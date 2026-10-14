using System.Net;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class InternalCopyAdmissionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task VerifiedInternalCopyReleasesItsReadPermitBeforeCopyingToAnotherEncryptionDomain(bool crc64)
    {
        var application = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sava:MaximumConcurrentStorageReads"] = "1",
            }, disableMaintenance: true);
        await using var applicationLifetime = application.ConfigureAwait(false);
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        var owner = CreateOwner(transport, SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);
        var sourceContainer = owner.GetBlobContainerClient("checksum-source");
        var destinationContainer = owner.GetBlobContainerClient("checksum-destination");
        await sourceContainer.CreateAsync().ConfigureAwait(true);
        await destinationContainer.CreateAsync().ConfigureAwait(true);
        var source = sourceContainer.GetBlobClient("source.bin");
        var destination = destinationContainer.GetBlobClient("destination.bin");
        var payload = Enumerable.Range(0, 64 * 1024).Select(index => (byte)(index % 251)).ToArray();
        await source.UploadAsync(BinaryData.FromBytes(payload)).ConfigureAwait(true);
        using var client = new HttpClient(application.Server.CreateHandler());
        using var request = new HttpRequestMessage(HttpMethod.Put, destination.GenerateSasUri(
            BlobSasPermissions.Create | BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent([]),
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        request.Headers.TryAddWithoutValidation("x-ms-requires-sync", "true");
        request.Headers.TryAddWithoutValidation("x-ms-encryption-scope", "copy-scope");
        request.Headers.TryAddWithoutValidation("x-ms-copy-source",
            source.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)).AbsoluteUri);
        request.Headers.TryAddWithoutValidation(crc64 ? "x-ms-source-content-crc64" : "x-ms-source-content-md5",
            Convert.ToBase64String(Checksum(payload, crc64)));
        // Await the actual send under its own finite cancellation, not an outer
        // timeout that could abandon the send while the factory retires.
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        using var response = await client.SendAsync(request, cancellation.Token).ConfigureAwait(true);
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Equal(payload, (await destination.DownloadContentAsync(cancellation.Token).ConfigureAwait(true)).Value.Content.ToArray());
        Assert.Equal(payload, (await source.DownloadContentAsync(cancellation.Token).ConfigureAwait(true)).Value.Content.ToArray());
        Assert.Equal("copy-scope", (await destination.GetPropertiesAsync(cancellationToken: cancellation.Token).ConfigureAwait(true)).Value.EncryptionScope);
        Assert.Null((await source.GetPropertiesAsync(cancellationToken: cancellation.Token).ConfigureAwait(true)).Value.EncryptionScope);
    }

    private static BlobServiceClient CreateOwner(HttpClientTransport transport, string account, string key) =>
        new(new Uri($"http://127.0.0.1/{account}"), new StorageSharedKeyCredential(account, key),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });

    private static byte[] Checksum(byte[] payload, bool crc64)
    {
        if (!crc64)
            return AzureProtocolChecksum.Md5(payload);
        var checksum = new StorageCrc64();
        checksum.Append(payload);
        return checksum.GetHash();
    }
}
