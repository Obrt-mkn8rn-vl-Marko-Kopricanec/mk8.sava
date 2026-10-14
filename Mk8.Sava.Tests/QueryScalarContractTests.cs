using System.Globalization;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class QueryScalarContractTests(SavaWebApplicationFactory application)
    : IClassFixture<SavaWebApplicationFactory>
{
    [Theory]
    [InlineData("9007199254740991", 9007199254740991L)]
    [InlineData("9007199254740992", 9007199254740992L)]
    [InlineData("9007199254740993", 9007199254740993L)]
    [InlineData("9223372036854775807", long.MaxValue)]
    [InlineData("-9223372036854775808", long.MinValue)]
    [InlineData("1.75", 1L)]
    [InlineData("-1.75", -1L)]
    [InlineData("1e3", 1000L)]
    public void IntegerCastPreservesExactTextAndTruncatesFractions(string text, long expected) =>
        Assert.Equal(expected, SelectInteger(new QueryCell(text)));

    [Theory]
    [InlineData("9007199254740993")]
    [InlineData("9223372036854775807")]
    [InlineData("-9223372036854775808")]
    public void IntegerCastDoesNotRoundDecimalThroughBinaryFloatingPoint(string text)
    {
        var number = decimal.Parse(text, CultureInfo.InvariantCulture);
        Assert.Equal(long.Parse(text, CultureInfo.InvariantCulture), SelectInteger(new QueryCell(number)));
    }

    [Theory]
    [InlineData("9223372036854775808")]
    [InlineData("-9223372036854775809")]
    [InlineData("1e100")]
    [InlineData("not-a-number")]
    public void InvalidIntegerCastUsesTheQueryErrorContract(string text)
    {
        var failure = Assert.Throws<BlobQueryDataException>(() => SelectInteger(new QueryCell(text)));
        Assert.Equal("InvalidType", failure.Name);
    }

    [Theory]
    [InlineData("NULL")]
    [InlineData("TRUE")]
    [InlineData("FALSE")]
    public void QuotedKeywordsResolveTheirActualField(string name)
    {
        var result = BlobQueryPlan.Parse($"SELECT \"{name}\" FROM BlobStorage;")
            .Select(new QueryRow([name], [new QueryCell(37L)]));
        Assert.NotNull(result);
        Assert.Equal(37L, Assert.IsType<long>(Assert.Single(result.Values).Value));
    }

    [Fact]
    public void UnquotedKeywordsRemainLiterals()
    {
        var result = BlobQueryPlan.Parse("SELECT NULL, TRUE, FALSE FROM BlobStorage;")
            .Select(new QueryRow([], []));
        Assert.NotNull(result);
        Assert.Null(result.Values[0].Value);
        Assert.True(Assert.IsType<bool>(result.Values[1].Value));
        Assert.False(Assert.IsType<bool>(result.Values[2].Value));
    }

    [Theory]
    [InlineData("TO_TIMESTAMP('9999-12-31T23:59:59Z') + 1")]
    [InlineData("1 + TO_TIMESTAMP('9999-12-31T23:59:59Z')")]
    [InlineData("TO_TIMESTAMP('0001-01-01T00:00:00Z') - 1")]
    public void TimestampArithmeticRangeErrorsAreNormalized(string expression)
    {
        var plan = BlobQueryPlan.Parse($"SELECT {expression} FROM BlobStorage;");
        var failure = Assert.Throws<BlobQueryDataException>(() => plan.Select(new QueryRow([], [])));
        Assert.Equal("InvalidType", failure.Name);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ActualSdkPreservesIntegerAndQuotedFieldValues(bool json)
    {
        const string text = "9007199254740993";
        var input = json ? "{\"NULL\":9007199254740993,\"TRUE\":9223372036854775807,\"FALSE\":-9223372036854775808}\n"
            : "NULL,TRUE,FALSE\n9007199254740993,9223372036854775807,-9223372036854775808\n";
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        var owner = new BlobServiceClient(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });
        var container = owner.GetBlobContainerClient($"query-scalars-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlockBlobClient("values");
        var upload = BinaryData.FromString(input).ToStream();
        await using var uploadLifetime = upload.ConfigureAwait(false);
        await blob.UploadAsync(upload).ConfigureAwait(true);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value;
        var errors = new List<BlobQueryError>();
        var options = new BlobQueryOptions
        {
            InputTextConfiguration = json ? new BlobQueryJsonTextOptions { RecordSeparator = "\n" }
                : new BlobQueryCsvTextOptions { HasHeaders = true, RecordSeparator = "\n" },
        };
        options.ErrorHandler += errors.Add;
        var response = await blob.QueryAsync("SELECT CAST(\"NULL\" AS INT), CAST(\"TRUE\" AS INT), CAST(\"FALSE\" AS INT) FROM BlobStorage;", options).ConfigureAwait(true);
        var content = response.Value.Content;
        await using var contentLifetime = content.ConfigureAwait(false);
        using var reader = new StreamReader(content, leaveOpen: true);
        Assert.Equal(text + ",9223372036854775807,-9223372036854775808\n", await reader.ReadToEndAsync().ConfigureAwait(true));
        Assert.Empty(errors);
        Assert.Equal(before.ETag, (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value.ETag);
        Assert.Equal(input, (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    private static long SelectInteger(QueryCell cell)
    {
        var result = BlobQueryPlan.Parse("SELECT CAST(_1 AS INT) FROM BlobStorage;")
            .Select(new QueryRow(["_1"], [cell]));
        Assert.NotNull(result);
        return Assert.IsType<long>(Assert.Single(result.Values).Value);
    }
}
