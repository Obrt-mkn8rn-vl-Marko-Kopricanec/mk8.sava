using System.Text.Json;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace Mk8.Sava.Tests;

public sealed class QueryCsvContractTests(SavaWebApplicationFactory application)
    : IClassFixture<SavaWebApplicationFactory>
{
    [Theory]
    [InlineData('\\', "a\\b,c", "\"a\\\\b,c\"\n")]
    [InlineData('\\', "a,b\\", "\"a,b\\\\\"\n")]
    [InlineData('!', "a!b,c", "\"a!!b,c\"\n")]
    [InlineData('"', "a\"b,c", "\"a\"\"b,c\"\n")]
    [InlineData('\\', "a\\b", "a\\b\n")]
    [InlineData('\\', "quotes \"and\\slashes\",yes", "\"quotes \\\"and\\\\slashes\\\",yes\"\n")]
    public async Task ConfiguredCsvEscapingPreservesExactValuesAcrossTheSdkRoundtrip(
        char escape, string value, string expectedCsv)
    {
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        var input = JsonSerializer.Serialize(new Dictionary<string, string>(StringComparer.Ordinal) { ["value"] = value }) + "\n";
        var blob = await CreateBlobAsync(transport, input).ConfigureAwait(true);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value;
        var errors = new List<BlobQueryError>();
        var csv = await ReadQueryAsync(blob, "SELECT value FROM BlobStorage;", new BlobQueryOptions
        {
            InputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "\n" },
            OutputTextConfiguration = Csv(escape),
        }, errors).ConfigureAwait(true);
        Assert.Equal(expectedCsv, csv);
        Assert.Empty(errors);

        var roundtrip = await CreateBlobAsync(transport, csv).ConfigureAwait(true);
        var decoded = await ReadQueryAsync(roundtrip, "SELECT * FROM BlobStorage;", new BlobQueryOptions
        {
            InputTextConfiguration = Csv(escape),
            OutputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "\n" },
        }, errors).ConfigureAwait(true);
        using var document = JsonDocument.Parse(decoded);
        Assert.Equal(value, document.RootElement.GetProperty("_1").GetString());
        Assert.Single(document.RootElement.EnumerateObject());
        Assert.Empty(errors);
        await AssertBlobUnchangedAsync(blob, input, before.ETag).ConfigureAwait(true);
    }

    [Theory]
    [InlineData(false, "", "")]
    [InlineData(false, "\"\"", "{\"_1\":\"\"}\n")]
    [InlineData(false, "\"\"\n", "{\"_1\":\"\"}\n")]
    [InlineData(false, "\"\"\n\"\"", "{\"_1\":\"\"}\n{\"_1\":\"\"}\n")]
    [InlineData(false, "one\n\"\"", "{\"_1\":\"one\"}\n{\"_1\":\"\"}\n")]
    [InlineData(false, "\"\",tail", "{\"_1\":\"\",\"_2\":\"tail\"}\n")]
    [InlineData(false, "left,\"\"", "{\"_1\":\"left\",\"_2\":\"\"}\n")]
    [InlineData(false, "\n", "{\"_1\":\"\"}\n")]
    [InlineData(false, "\"\"\r\n\"\"", "{\"_1\":\"\"}\n{\"_1\":\"\"}\n")]
    [InlineData(false, "\"\"\n\n", "{\"_1\":\"\"}\n{\"_1\":\"\"}\n")]
    [InlineData(true, "name\n\"\"", "{\"name\":\"\"}\n")]
    [InlineData(true, "name\n\"\"\n", "{\"name\":\"\"}\n")]
    public async Task FinalQuotedEmptyRecordsAreNotConfusedWithAbsentRecords(
        bool hasHeaders, string input, string expected)
    {
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        var blob = await CreateBlobAsync(transport, input).ConfigureAwait(true);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value;
        var errors = new List<BlobQueryError>();
        var output = await ReadQueryAsync(blob, "SELECT * FROM BlobStorage;", new BlobQueryOptions
        {
            InputTextConfiguration = Csv('\\', hasHeaders),
            OutputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "\n" },
        }, errors).ConfigureAwait(true);
        Assert.Equal(expected, output);
        Assert.Empty(errors);
        await AssertBlobUnchangedAsync(blob, input, before.ETag).ConfigureAwait(true);
    }

    [Theory]
    [InlineData("left,right\none", "\n", 11L)]
    [InlineData("left,right\none,two,three", "\n", 11L)]
    [InlineData("left,right\none,two\nbad", "one,two\n\n", 19L)]
    public async Task HeaderWidthMismatchUsesTheSdkFatalDataErrorChannel(
        string input, string expectedOutput, long expectedPosition)
    {
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        var blob = await CreateBlobAsync(transport, input).ConfigureAwait(true);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value;
        var errors = new List<BlobQueryError>();
        var output = await ReadQueryAsync(blob, "SELECT left, right FROM BlobStorage;", new BlobQueryOptions
        {
            InputTextConfiguration = Csv('\\', hasHeaders: true),
            OutputTextConfiguration = Csv('\\'),
        }, errors).ConfigureAwait(true);
        Assert.Equal(expectedOutput, output);
        var failure = Assert.Single(errors);
        Assert.True(failure.IsFatal);
        Assert.Equal("ParseError", failure.Name);
        Assert.Equal(expectedPosition, failure.Position);
        await AssertBlobUnchangedAsync(blob, input, before.ETag).ConfigureAwait(true);
    }

    [Theory]
    [InlineData(true, "left,right\none,two\n", "one,two\n")]
    [InlineData(true, "left,right\none,\n", "one,\n")]
    [InlineData(true, "left,right\n", "")]
    [InlineData(false, "one\ntwo,three\n", "one\ntwo,three\n")]
    public async Task ValidWidthAndHeaderlessOrdinalRowsRemainUsable(
        bool hasHeaders, string input, string expected)
    {
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        var blob = await CreateBlobAsync(transport, input).ConfigureAwait(true);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value;
        var errors = new List<BlobQueryError>();
        var output = await ReadQueryAsync(blob, "SELECT * FROM BlobStorage;", new BlobQueryOptions
        {
            InputTextConfiguration = Csv('\\', hasHeaders),
            OutputTextConfiguration = Csv('\\'),
        }, errors).ConfigureAwait(true);
        Assert.Equal(expected, output);
        Assert.Empty(errors);
        await AssertBlobUnchangedAsync(blob, input, before.ETag).ConfigureAwait(true);
    }

    private static async Task<BlockBlobClient> CreateBlobAsync(HttpClientTransport transport, string input)
    {
        var service = new BlobServiceClient(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });
        var container = service.GetBlobContainerClient($"query-csv-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlockBlobClient("rows");
        var upload = BinaryData.FromString(input).ToStream();
        await using var uploadLifetime = upload.ConfigureAwait(false);
        await blob.UploadAsync(upload).ConfigureAwait(false);
        return blob;
    }

    private static BlobQueryCsvTextOptions Csv(char escape, bool hasHeaders = false) => new()
    {
        ColumnSeparator = ",", QuotationCharacter = '"', EscapeCharacter = escape,
        RecordSeparator = "\n", HasHeaders = hasHeaders,
    };

    private static async Task<string> ReadQueryAsync(BlockBlobClient blob, string expression,
        BlobQueryOptions options, List<BlobQueryError> errors)
    {
        options.ErrorHandler += errors.Add;
        var response = await blob.QueryAsync(expression, options).ConfigureAwait(false);
        var content = response.Value.Content;
        await using var contentLifetime = content.ConfigureAwait(false);
        using var reader = new StreamReader(content, leaveOpen: true);
        return await reader.ReadToEndAsync().ConfigureAwait(false);
    }

    private static async Task AssertBlobUnchangedAsync(BlockBlobClient blob, string input, Azure.ETag expected)
    {
        Assert.Equal(expected, (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag);
        Assert.Equal(input, (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
    }
}
