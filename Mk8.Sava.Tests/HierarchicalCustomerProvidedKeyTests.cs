using System.Security.Cryptography;
using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Mk8.Sava.Tests;

public sealed class HierarchicalCustomerProvidedKeyTests
{
    [Fact]
    public async Task HnsRejectsCustomerProvidedKeysWhileFlatStorageStillAcceptsThem()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
        await using var applicationDisposal = application.ConfigureAwait(false);
        var customerKey = new CustomerProvidedKey(RandomNumberGenerator.GetBytes(32));

        var hns = CreateClient(application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey,
            null).GetBlobContainerClient($"hns-cpk-{Guid.NewGuid():N}");
        var hnsKeyed = CreateClient(application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey,
            customerKey).GetBlobContainerClient(hns.Name);
        await hns.CreateAsync();
        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() =>
            hnsKeyed.GetBlobClient("rejected.bin").UploadAsync(BinaryData.FromString("not stored")));
        Assert.Equal(409, rejected.Status);
        Assert.Equal("BlobOperationNotSupported", rejected.ErrorCode);
        Assert.False((await hns.GetBlobClient("rejected.bin").ExistsAsync()).Value);

        await hns.GetBlobClient("plain.bin").UploadAsync(BinaryData.FromString("plain"));
        var readFailure = await Assert.ThrowsAsync<RequestFailedException>(() =>
            hnsKeyed.GetBlobClient("plain.bin").DownloadContentAsync());
        Assert.Equal(409, readFailure.Status);
        Assert.Equal("BlobOperationNotSupported", readFailure.ErrorCode);
        Assert.Equal("plain", (await hns.GetBlobClient("plain.bin").DownloadContentAsync()).Value.Content.ToString());

        var hnsScoped = CreateClient(application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey,
            null,
            "hns-scope").GetBlobContainerClient(hns.Name).GetBlobClient("scoped.bin");
        await hnsScoped.UploadAsync(BinaryData.FromString("scope works"));
        Assert.Equal("hns-scope", (await hnsScoped.GetPropertiesAsync()).Value.EncryptionScope);
        Assert.Equal("scope works", (await hnsScoped.DownloadContentAsync()).Value.Content.ToString());

        var flatKeyed = CreateClient(application,
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey,
            customerKey).GetBlobContainerClient($"flat-cpk-{Guid.NewGuid():N}");
        await flatKeyed.CreateAsync();
        var flatBlob = flatKeyed.GetBlobClient("encrypted.bin");
        await flatBlob.UploadAsync(BinaryData.FromString("exact flat bytes"));
        Assert.Equal("exact flat bytes", (await flatBlob.DownloadContentAsync()).Value.Content.ToString());
    }

    private static BlobServiceClient CreateClient(
        SavaWebApplicationFactory application,
        string account,
        string key,
        CustomerProvidedKey? customerKey,
        string? encryptionScope = null) =>
        new(new Uri($"https://{account}.localhost"),
            new StorageSharedKeyCredential(account, key),
            new BlobClientOptions
            {
                Transport = new HttpClientTransport(application.Server.CreateHandler()),
                CustomerProvidedKey = customerKey,
                EncryptionScope = encryptionScope,
                Retry = { MaxRetries = 0 }
            });
}
