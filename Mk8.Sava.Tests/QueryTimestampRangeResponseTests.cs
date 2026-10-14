using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace Mk8.Sava.Tests;

public sealed class QueryTimestampRangeResponseTests
{
    [Theory]
    [InlineData("TO_TIMESTAMP('9999-12-31T23:59:59Z') + 1")]
    [InlineData("1 + TO_TIMESTAMP('9999-12-31T23:59:59Z')")]
    [InlineData("TO_TIMESTAMP('0001-01-01T00:00:00Z') - 1")]
    public async Task ArithmeticRangeFailureUsesTheActualSdkQueryErrorChannel(string expression)
    {
        var application = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);
        await using var applicationLifetime = application.ConfigureAwait(false);
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        var owner = new BlobServiceClient(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });
        var container = owner.GetBlobContainerClient("query-time-range");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlockBlobClient("row.csv");
        var upload = BinaryData.FromString("1\n").ToStream();
        await using var uploadLifetime = upload.ConfigureAwait(false);
        await blob.UploadAsync(upload).ConfigureAwait(true);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value;
        var errors = new List<BlobQueryError>();
        var options = new BlobQueryOptions();
        options.ErrorHandler += errors.Add;
        var response = await blob.QueryAsync($"SELECT {expression} FROM BlobStorage;", options).ConfigureAwait(true);
        var content = response.Value.Content;
        await using var contentLifetime = content.ConfigureAwait(false);
        using var reader = new StreamReader(content, leaveOpen: true);
        Assert.Empty(await reader.ReadToEndAsync().ConfigureAwait(true));
        var failure = Assert.Single(errors);
        Assert.True(failure.IsFatal);
        Assert.Equal("InvalidType", failure.Name);
        Assert.Equal(before.ETag, (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value.ETag);
        Assert.Equal("1\n", (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }
}
