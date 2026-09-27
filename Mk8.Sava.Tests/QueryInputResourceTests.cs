using System.Globalization;
using System.Text;
using System.Text.Json;
using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

[Collection("Parquet resource bounds")]
public sealed class QueryInputResourceTests
{
    [Theory]
    [InlineData("csv", 50000)]
    [InlineData("array", 50000)]
    [InlineData("object", 50000)]
    public async Task ExcessiveInputFieldsFailEvenWhenTheResultIsOneSmallAggregate(string format, int count) =>
        await AssertQueryAsync(format, count, overCapacity: true).ConfigureAwait(true);

    [Theory]
    [InlineData("csv", 64)]
    [InlineData("array", 64)]
    [InlineData("object", 64)]
    public async Task InputFieldCountIsNotConfusedWithTheSelectProjectionLimit(string format, int count) =>
        await AssertQueryAsync(format, count, overCapacity: false).ConfigureAwait(true);

    [Theory]
    [InlineData("array")]
    [InlineData("object")]
    public void JsonFieldAdmissionDoesNotAllocateCountSizedCellOrNameArrays(string format)
    {
        using var document = JsonDocument.Parse(CreateRecord(format, 50000));
        var before = GC.GetAllocatedBytesForCurrentThread();
        var error = Assert.Throws<AzureStorageException>(() => BlobQueryResources.ValidateJsonRow(
            document.RootElement, BlobQueryResources.BufferedInputBytes, 8L * 1024 * 1024, CancellationToken.None));
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal("ServerBusy", error.ErrorCode);
        Assert.InRange(allocated, 0, 256 * 1024);
    }

    [Fact]
    public async Task CsvAdmissionIncludesTheRetainedHeaderBeforeReadingTheFirstResult()
    {
        var bytes = Encoding.UTF8.GetBytes(new string('h', 750 * 1024) + "\n" + new string('v', 400 * 1024) + "\n");
        using var input = new MemoryStream(bytes, writable: false);
        using var response = new MemoryStream();
        var format = new BlobQueryTextFormat(BlobQueryFormatKind.Delimited, ",", '"', "\n", '"', true, []);
        var request = new BlobQueryRequest("SELECT * FROM BlobStorage;", format, format with { HasHeaders = false });
        var error = await Assert.ThrowsAsync<AzureStorageException>(() => BlobQueryProtocol.ExecuteAsync(
            request, input, response, bytes.Length, CancellationToken.None,
            maximumInputMemoryBytes: 8L * 1024 * 1024)).ConfigureAwait(true);
        Assert.Equal("ServerBusy", error.ErrorCode);
        Assert.Equal(0, response.Length);
    }

    private static async Task AssertQueryAsync(string format, int count, bool overCapacity)
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaximumBlobQueryInputMemoryBytes"] = (8L * 1024 * 1024).ToString(CultureInfo.InvariantCulture)
        });
        await using var applicationDisposal = application.ConfigureAwait(false);
        var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03)
        {
            Transport = new HttpClientTransport(application.CreateClient())
        };
        options.Retry.MaxRetries = 0;
        var client = new BlobServiceClient(new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey), options);
        var container = client.GetBlobContainerClient("query-input-capacity");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlockBlobClient("row");
        var bytes = Encoding.UTF8.GetBytes(CreateRecord(format, count) + "\n");
        using var upload = new MemoryStream(bytes, writable: false);
        await blob.UploadAsync(upload).ConfigureAwait(false);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        var query = new BlobQueryOptions
        {
            InputTextConfiguration = string.Equals(format, "csv", StringComparison.Ordinal)
                ? new BlobQueryCsvTextOptions { RecordSeparator = "\n" }
                : new BlobQueryJsonTextOptions { RecordSeparator = "\n" }
        };
        if (overCapacity)
            await AssertRejectedAsync(blob, query).ConfigureAwait(false);
        else
        {
            var result = await blob.QueryAsync("SELECT * FROM BlobStorage;", query).ConfigureAwait(false);
            using var reader = new StreamReader(result.Value.Content);
            Assert.Equal(string.Join(',', Enumerable.Repeat("0", count)) + "\n", await reader.ReadToEndAsync().ConfigureAwait(false));
        }
        Assert.Equal(before.ETag, (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag);
        Assert.Equal(bytes, (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    private static async Task AssertRejectedAsync(BlockBlobClient blob, BlobQueryOptions query)
    {
        var failure = await Assert.ThrowsAsync<RequestFailedException>(async () =>
        {
            var result = await blob.QueryAsync("SELECT COUNT(*) FROM BlobStorage;", query).ConfigureAwait(false);
            var content = result.Value.Content;
            await using var disposal = content.ConfigureAwait(false);
            await content.CopyToAsync(Stream.Null).ConfigureAwait(false);
        }).ConfigureAwait(false);
        Assert.Equal(503, failure.Status);
        Assert.Equal("ServerBusy", failure.ErrorCode);
        var response = failure.GetRawResponse();
        Assert.NotNull(response);
        Assert.True(response.Headers.TryGetValue("Retry-After", out var retryAfter));
        Assert.Equal("1", retryAfter);
    }

    private static string CreateRecord(string format, int count) => format switch
    {
        "csv" => string.Join(',', Enumerable.Repeat("0", count)),
        "array" => $"[{string.Join(',', Enumerable.Repeat("0", count))}]",
        "object" => $"{{{string.Join(',', Enumerable.Range(0, count).Select(index => $"\"c{index}\":0"))}}}",
        _ => throw new ArgumentOutOfRangeException(nameof(format))
    };
}
