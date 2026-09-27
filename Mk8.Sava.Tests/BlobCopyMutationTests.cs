using System.Globalization;
using System.Net;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;

namespace Mk8.Sava.Tests;

public sealed class BlobCopyMutationTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AppendAndSealRetainCompletedCopyProperties(bool seal)
    {
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(application).GetBlobContainerClient($"copy-append-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var source = container.GetAppendBlobClient("source.bin");
        await source.CreateAsync().ConfigureAwait(true);
        using (var payload = BinaryData.FromString("copied payload").ToStream())
            await source.AppendBlockAsync(payload).ConfigureAwait(true);
        var destination = container.GetAppendBlobClient("destination.bin");
        await CompleteCopyAsync(source, destination).ConfigureAwait(true);
        var before = (await destination.GetPropertiesAsync().ConfigureAwait(true)).Value;

        if (seal)
            await destination.SealAsync().ConfigureAwait(true);
        else
        {
            using var suffix = BinaryData.FromString(" suffix").ToStream();
            await destination.AppendBlockAsync(suffix).ConfigureAwait(true);
        }
        var after = (await destination.GetPropertiesAsync().ConfigureAwait(true)).Value;
        AssertCopyPropertiesEqual(before, after);
        Assert.NotEqual(before.ETag, after.ETag);
        Assert.Equal(seal, after.IsSealed);
        Assert.Equal(before.BlobCommittedBlockCount + (seal ? 0 : 1), after.BlobCommittedBlockCount);
        Assert.Equal(seal ? "copied payload" : "copied payload suffix",
            (await destination.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        Assert.Equal("copied payload", (await source.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    [Fact]
    public async Task SettingAndClearingHnsExpiryRetainCompletedCopyProperties()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true"
        });
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(application).GetBlobContainerClient($"copy-expiry-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var source = container.GetBlobClient("source.bin");
        await source.UploadAsync(BinaryData.FromString("copied payload")).ConfigureAwait(true);
        var destination = container.GetBlobClient("destination.bin");
        await CompleteCopyAsync(source, destination).ConfigureAwait(true);
        var before = (await destination.GetPropertiesAsync().ConfigureAwait(true)).Value;
        using var transport = new HttpClient(application.Server.CreateHandler());
        var expiresOn = DateTimeOffset.UtcNow.AddHours(2);

        await SetExpiryAsync(transport, destination, expiresOn).ConfigureAwait(true);
        var expiring = (await destination.GetPropertiesAsync().ConfigureAwait(true)).Value;
        AssertCopyPropertiesEqual(before, expiring);
        Assert.NotEqual(before.ETag, expiring.ETag);
        Assert.Equal(expiresOn.ToUnixTimeSeconds(), expiring.ExpiresOn.ToUnixTimeSeconds());
        await SetExpiryAsync(transport, destination, expiresOn: null).ConfigureAwait(true);
        var cleared = (await destination.GetPropertiesAsync().ConfigureAwait(true)).Value;
        AssertCopyPropertiesEqual(before, cleared);
        Assert.Equal(default, cleared.ExpiresOn);
        Assert.NotEqual(expiring.ETag, cleared.ETag);
        Assert.Equal("copied payload", (await destination.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    [Theory]
    [InlineData("set-policy")]
    [InlineData("delete-policy")]
    [InlineData("set-hold")]
    [InlineData("clear-hold")]
    public async Task RetentionChangesRetainCompletedCopyProperties(string operation)
    {
        var containerName = $"copy-retention-{Guid.NewGuid():N}";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:VersioningEnabled"] = "true",
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:ImmutableStorageWithVersioningContainers:0"] = containerName
        });
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(application).GetBlobContainerClient(containerName);
        await container.CreateAsync().ConfigureAwait(true);
        var source = container.GetBlobClient("source.bin");
        await source.UploadAsync(BinaryData.FromString("copied payload")).ConfigureAwait(true);
        var destination = container.GetBlobClient("destination.bin");
        var policy = new BlobImmutabilityPolicy
        {
            ExpiresOn = DateTimeOffset.UtcNow.AddHours(2),
            PolicyMode = BlobImmutabilityPolicyMode.Unlocked
        };
        await CompleteCopyAsync(source, destination, new BlobCopyFromUriOptions
        {
            DestinationImmutabilityPolicy = string.Equals(operation, "delete-policy", StringComparison.Ordinal) ? policy : null,
            LegalHold = string.Equals(operation, "clear-hold", StringComparison.Ordinal) ? true : null
        }).ConfigureAwait(true);
        var before = (await destination.GetPropertiesAsync().ConfigureAwait(true)).Value;

        switch (operation)
        {
            case "set-policy":
                await destination.SetImmutabilityPolicyAsync(policy).ConfigureAwait(true);
                break;
            case "delete-policy":
                await destination.DeleteImmutabilityPolicyAsync().ConfigureAwait(true);
                break;
            case "set-hold":
                await destination.SetLegalHoldAsync(true).ConfigureAwait(true);
                break;
            case "clear-hold":
                await destination.SetLegalHoldAsync(false).ConfigureAwait(true);
                break;
            default:
                Assert.Fail($"Unknown retention mutation: {operation}.");
                break;
        }
        var after = (await destination.GetPropertiesAsync().ConfigureAwait(true)).Value;
        AssertCopyPropertiesEqual(before, after);
        Assert.Equal(before.ETag, after.ETag);
        Assert.Equal(before.LastModified, after.LastModified);
        Assert.Equal(before.VersionId, after.VersionId);
        Assert.Equal(string.Equals(operation, "set-hold", StringComparison.Ordinal), after.HasLegalHold);
        Assert.Equal(string.Equals(operation, "set-policy", StringComparison.Ordinal),
            after.ImmutabilityPolicy?.ExpiresOn.HasValue ?? false);
        Assert.Equal("copied payload", (await destination.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    private static async Task CompleteCopyAsync(
        BlobBaseClient source, BlobBaseClient destination, BlobCopyFromUriOptions? options = null)
    {
        var copy = await destination.StartCopyFromUriAsync(source.Uri, options).ConfigureAwait(false);
        await copy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None).ConfigureAwait(false);
        var properties = (await destination.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(CopyStatus.Success, properties.CopyStatus);
        Assert.Equal(copy.Id, properties.CopyId);
        Assert.False(string.IsNullOrEmpty(properties.CopyId));
    }

    private static async Task SetExpiryAsync(HttpClient transport, BlobClient destination, DateTimeOffset? expiresOn)
    {
        var uri = new UriBuilder(destination.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)));
        uri.Query += "&comp=expiry";
        using var request = new HttpRequestMessage(HttpMethod.Put, uri.Uri) { Content = new ByteArrayContent([]) };
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        request.Headers.TryAddWithoutValidation("x-ms-expiry-option", expiresOn.HasValue ? "Absolute" : "NeverExpire");
        if (expiresOn.HasValue)
            request.Headers.TryAddWithoutValidation("x-ms-expiry-time", expiresOn.Value.ToString("R", CultureInfo.InvariantCulture));
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    private static void AssertCopyPropertiesEqual(BlobProperties expected, BlobProperties actual)
    {
        Assert.Equal(expected.CopyId, actual.CopyId);
        Assert.Equal(expected.CopyStatus, actual.CopyStatus);
        Assert.Equal(expected.CopySource, actual.CopySource);
        Assert.Equal(expected.CopyProgress, actual.CopyProgress);
        Assert.Equal(expected.CopyCompletedOn, actual.CopyCompletedOn);
        Assert.Equal(expected.CopyStatusDescription, actual.CopyStatusDescription);
    }

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory application) =>
        new(new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03)
            {
                Transport = new HttpClientTransport(application.Server.CreateHandler()),
                Retry = { MaxRetries = 0 }
            });
}
