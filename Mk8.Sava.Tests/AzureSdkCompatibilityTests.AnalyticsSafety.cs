using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Microsoft.AspNetCore.WebUtilities;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed partial class AzureSdkCompatibilityTests
{
    [Fact]
    public async Task AnalyticsLogRedactsRequestAndReferrerSasSecrets()
    {
        var application = new SavaWebApplicationFactory();
        await using var applicationDisposal = application.ConfigureAwait(false);
        var service = CreateClient(application);
        var properties = (await service.GetPropertiesAsync()).Value;
        properties.Logging = new BlobAnalyticsLogging
        {
            Version = "2.0",
            Read = true,
            Write = true,
            Delete = false,
            RetentionPolicy = new BlobRetentionPolicy { Enabled = false }
        };
        await service.SetPropertiesAsync(properties);

        var container = service.GetBlobContainerClient($"analytics-safety-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("payload.bin");
        await blob.UploadAsync(BinaryData.FromString("content"));
        var sasUri = blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        var sasSignature = QueryHelpers.ParseQuery(sasUri.Query)["sig"].ToString();
        var marker = $"safe-{Guid.NewGuid():N}";
        using var request = new HttpRequestMessage(HttpMethod.Get, sasUri);
        request.Headers.TryAddWithoutValidation("x-ms-client-request-id", marker);
        request.Headers.TryAddWithoutValidation(
            "Referer", "https://user:password@example.test/landing?sig=referrer-secret&safe=keep");
        using var transport = new HttpClient(application.Server.CreateHandler());

        using var response = await transport.SendAsync(request);

        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
        var logs = service.GetBlobContainerClient(StorageAnalyticsService.LogsContainerName);
        var matchingRecords = new List<string>();
        await foreach (var log in logs.GetBlobsAsync().ConfigureAwait(true))
        {
            var content = (await logs.GetBlobClient(log.Name).DownloadContentAsync().ConfigureAwait(true))
                .Value.Content.ToString();
            if (content.Contains(marker, StringComparison.Ordinal))
                matchingRecords.Add(content);
        }

        var record = Assert.Single(matchingRecords);
        var fields = ParseAnalyticsLogFields(Assert.Single(record.Split('\n', StringSplitOptions.RemoveEmptyEntries)));
        Assert.Equal(38, fields.Count);
        Assert.Equal(marker, fields[29]);
        Assert.Contains("sig=XXXXX", fields[11], StringComparison.Ordinal);
        Assert.DoesNotContain(sasSignature, record, StringComparison.Ordinal);
        Assert.Contains("safe=keep", fields[28], StringComparison.Ordinal);
        Assert.DoesNotContain("password", record, StringComparison.Ordinal);
        Assert.DoesNotContain("referrer-secret", record, StringComparison.Ordinal);
    }
}
