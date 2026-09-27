using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace Mk8.Sava.Tests;

// Microsoft Azure SDK 12.32.0 recordings pin these observable outcomes;
// requests run against mk8.sava, not a live Azure account.
public sealed class RecordedQueryCompatibilityTests(SavaWebApplicationFactory application)
    : IClassFixture<SavaWebApplicationFactory>
{
    [Theory]
    [InlineData("100,hello,300,400\n150,250,350,450\n", 0)]
    [InlineData("150,250,350,450\n100,hello,300,400\n", 16)]
    [InlineData("é,250,350,450\n100,hello,300,400\n", 15)]
    [InlineData("😀,250,350,450\n100,hello,300,400\n", 17)]
    public async Task InvalidNumericComparisonReportsANonfatalErrorAndContinues(string csv, long position)
    {
        var blob = await UploadAsync(csv).ConfigureAwait(true);
        var errors = new List<BlobQueryError>();
        var options = new BlobQueryOptions();
        options.ErrorHandler += errors.Add;
        Assert.Equal("250\n", await QueryAsync(blob, "select _2 from BlobStorage where _2 > 100", options).ConfigureAwait(true));
        var error = Assert.Single(errors);
        Assert.False(error.IsFatal);
        Assert.Equal("InvalidTypeConversion", error.Name);
        Assert.Equal("Invalid type conversion.", error.Description);
        Assert.Equal(position, error.Position);
        Assert.Equal(csv, (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    [Fact]
    public async Task RecordedJsonConfigurationWithEmptySeparatorsDefaultsToNewline()
    {
        const string json = "{\"_1\":\"100\",\"_2\":\"200\",\"_3\":\"300\",\"_4\":\"400\"}\n" +
                            "{\"_1\":\"150\",\"_2\":\"250\",\"_3\":\"350\",\"_4\":\"450\"}\n" +
                            "{\"_1\":\"180\",\"_2\":\"280\",\"_3\":\"380\",\"_4\":\"480\"}\n";
        var blob = await UploadAsync(json).ConfigureAwait(true);
        var options = new BlobQueryOptions
        {
            InputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "" },
            OutputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "" }
        };
        Assert.Equal(json, await QueryAsync(blob, "select * from BlobStorage", options).ConfigureAwait(true));
    }

    [Fact]
    public async Task RecordedFatalJsonParseErrorEndsTheQueryWithItsErrorEvent()
    {
        var blob = await UploadAsync("100,200,300,400\n150,250,350,450\n").ConfigureAwait(true);
        var errors = new List<BlobQueryError>();
        var options = new BlobQueryOptions
        {
            InputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "" }
        };
        options.ErrorHandler += errors.Add;
        Assert.Equal("\n", await QueryAsync(blob, "select * from BlobStorage", options).ConfigureAwait(true));
        var error = Assert.Single(errors);
        Assert.True(error.IsFatal);
        Assert.Equal("ParseError", error.Name);
        Assert.Equal("Unexpected token ',' at [byte: 3]. Expecting tokens '{', or '['. ", error.Description);
        Assert.Equal(0, error.Position);
    }

    [Fact]
    public async Task OfficialSdkRoundTripsWhitespaceOnlyTagsWithoutMutatingContentOrEntityHeaders()
    {
        var blob = await UploadAsync("tagged-content").ConfigureAwait(true);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value;
        var tags = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [" "] = "  ",
            ["empty"] = "",
            [" padding "] = " value "
        };
        await blob.SetTagsAsync(tags).ConfigureAwait(true);
        var actual = (await blob.GetTagsAsync().ConfigureAwait(true)).Value.Tags;
        Assert.Equal(tags.Count, actual.Count);
        foreach (var pair in tags)
            Assert.Equal(pair.Value, actual[pair.Key]);
        var after = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(before.ETag, after.ETag);
        Assert.Equal(before.LastModified, after.LastModified);
        Assert.Equal("tagged-content", (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    private async Task<BlockBlobClient> UploadAsync(string content)
    {
        var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03)
        {
            Transport = new HttpClientTransport(application.CreateClient())
        };
        var service = new BlobServiceClient(new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey), options);
        var container = service.GetBlobContainerClient($"recorded-query-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlockBlobClient("fixture");
        using var upload = BinaryData.FromString(content).ToStream();
        await blob.UploadAsync(upload).ConfigureAwait(false);
        return blob;
    }

    private static async Task<string> QueryAsync(BlockBlobClient blob, string sql, BlobQueryOptions options)
    {
        var result = await blob.QueryAsync(sql, options).ConfigureAwait(false);
        var content = result.Value.Content;
        await using var disposal = content.ConfigureAwait(false);
        using var reader = new StreamReader(content, leaveOpen: true);
        return await reader.ReadToEndAsync().ConfigureAwait(false);
    }
}
