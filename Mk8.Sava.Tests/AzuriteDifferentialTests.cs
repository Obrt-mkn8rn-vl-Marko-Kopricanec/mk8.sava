using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Mk8.Sava.Tests;

public sealed class AzuriteDifferentialTests
{
    [AzuriteFact]
    [Trait("Category", "Azurite")]
    public async Task SupportedFlatBlobSdkOperationsMatchAzurite()
    {
        var connectionString = Environment.GetEnvironmentVariable(AzuriteFactAttribute.ConnectionStringVariable)
            ?? throw new InvalidOperationException("The Azurite connection string was removed after discovery.");
        var azurite = new BlobServiceClient(connectionString, CreateOptions());
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var local = CreateLocalClient(application);
        var name = $"mk8-azurite-{Guid.NewGuid():N}";
        var azuriteContainer = azurite.GetBlobContainerClient(name);
        var localContainer = local.GetBlobContainerClient(name);
        try
        {
            var expected = await ExerciseAsync(azuriteContainer).ConfigureAwait(false);
            var actual = await ExerciseAsync(localContainer).ConfigureAwait(false);
            Assert.Equal(expected, actual);
        }
        finally
        {
            await DeleteIfExistsAsync(localContainer).ConfigureAwait(false);
            await DeleteIfExistsAsync(azuriteContainer).ConfigureAwait(false);
        }
    }

    private static BlobServiceClient CreateLocalClient(SavaWebApplicationFactory application)
    {
        var account = SavaWebApplicationFactory.AccountName;
        var endpoint = new Uri($"http://{account}.localhost");
        var options = CreateOptions();
        options.Transport = new HttpClientTransport(application.Server.CreateHandler());
        return new BlobServiceClient(
            endpoint,
            new StorageSharedKeyCredential(account, SavaWebApplicationFactory.AccountKey),
            options);
    }

    private static BlobClientOptions CreateOptions() =>
        new(BlobClientOptions.ServiceVersion.V2023_11_03) { Retry = { MaxRetries = 0 } };

    private static async Task<FlatBlobObservation> ExerciseAsync(BlobContainerClient container)
    {
        var created = await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("nested/payload.bin");
        var bytes = new byte[8192];
        DeterministicTestBytes.Fill(0xA20B, bytes);
        var uploaded = await blob.UploadAsync(BinaryData.FromBytes(bytes), new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders { ContentType = "application/x-mk8-azurite" },
            Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["phase"] = "initial" }
        }).ConfigureAwait(false);
        var properties = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        var full = (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray();
        var range = await ReadRangeAsync(blob).ConfigureAwait(false);
        var condition = await ExerciseConditionAsync(blob).ConfigureAwait(false);
        var names = new List<string>();
        await foreach (var item in container.GetBlobsAsync().ConfigureAwait(false))
            names.Add(item.Name);
        await blob.DeleteAsync().ConfigureAwait(false);
        var missing = await Assert.ThrowsAsync<RequestFailedException>(
            () => blob.GetPropertiesAsync()).ConfigureAwait(false);
        return new FlatBlobObservation(
            created.GetRawResponse().Status,
            uploaded.GetRawResponse().Status,
            properties.ContentLength,
            properties.ContentType,
            properties.Metadata["phase"],
            Convert.ToBase64String(full),
            Convert.ToBase64String(range),
            condition.Status,
            condition.ErrorCode,
            missing.Status,
            missing.ErrorCode,
            string.Join(',', names));
    }

    private static async Task<byte[]> ReadRangeAsync(BlobClient blob)
    {
        var result = await blob.DownloadStreamingAsync(new BlobDownloadOptions
        {
            Range = new HttpRange(4093, 1025)
        }).ConfigureAwait(false);
        var stream = result.Value.Content;
        await using var streamDisposal = stream.ConfigureAwait(false);
        using var buffer = new MemoryStream();
        await stream.CopyToAsync(buffer).ConfigureAwait(false);
        return buffer.ToArray();
    }

    private static async Task<RequestFailedException> ExerciseConditionAsync(BlobClient blob)
    {
        var stale = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag;
        await blob.SetMetadataAsync(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["phase"] = "updated" })
            .ConfigureAwait(false);
        return await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.SetMetadataAsync(
                new Dictionary<string, string>(StringComparer.Ordinal) { ["phase"] = "rejected" },
                new BlobRequestConditions { IfMatch = stale })).ConfigureAwait(false);
    }

    private static async Task DeleteIfExistsAsync(BlobContainerClient container)
    {
        try
        {
            await container.DeleteIfExistsAsync().ConfigureAwait(false);
        }
        catch (RequestFailedException exception) when (exception.Status == 404)
        {
        }
    }

    private sealed record FlatBlobObservation(
        int CreateStatus,
        int UploadStatus,
        long Length,
        string ContentType,
        string Metadata,
        string FullBytes,
        string RangeBytes,
        int StaleConditionStatus,
        string? StaleConditionCode,
        int MissingStatus,
        string? MissingCode,
        string ListedNames);
}
