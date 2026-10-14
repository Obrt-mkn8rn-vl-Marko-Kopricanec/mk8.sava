using System.Globalization;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class QueryFloatingComparisonTests(SavaWebApplicationFactory application)
    : IClassFixture<SavaWebApplicationFactory>
{
    [Theory]
    [InlineData("float:1e100", ">", "float:1e99", true)]
    [InlineData("float:1e99", "<", "float:1e100", true)]
    [InlineData("float:1e100", "=", "float:1e100", true)]
    [InlineData("float:1.0000000000000002", ">", "float:1", true)]
    [InlineData("float:1.0000000000000002", "!=", "float:1", true)]
    [InlineData("float:5e-324", ">", "long:0", true)]
    [InlineData("float:-5e-324", "<", "long:0", true)]
    [InlineData("float:-0", "=", "decimal:0", true)]
    [InlineData("float:1.7976931348623157e308", ">", "decimal:79228162514264337593543950335", true)]
    [InlineData("float:-1.7976931348623157e308", "<", "decimal:-79228162514264337593543950335", true)]
    [InlineData("long:9007199254740993", ">", "float:9007199254740992", true)]
    [InlineData("long:9007199254740993", "=", "float:9007199254740992", false)]
    [InlineData("float:9223372036854775808", ">", "long:9223372036854775807", true)]
    [InlineData("float:-9223372036854775808", "=", "long:-9223372036854775808", true)]
    [InlineData("decimal:79228162514264337593543950335", ">", "long:9223372036854775807", true)]
    [InlineData("decimal:1.0000000000000000000000000001", ">", "long:1", true)]
    [InlineData("float:0.1", ">", "decimal:0.1", true)]
    [InlineData("float:0.5", "=", "decimal:0.5", true)]
    [InlineData("float:0.1", "=", "float:0.10000000000000001", true)]
    [InlineData("float:1e100", ">", "string:1e99", true)]
    public void FiniteNumericComparisonsRespectTheirRepresentedValues(
        string left, string operation, string right, bool expected)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var plan = BlobQueryPlan.Parse($"SELECT 1 FROM BlobStorage WHERE left {operation} right;");
        var selected = plan.Select(new QueryRow(["left", "right"], [Cell(left), Cell(right)]));
        Assert.Equal(expected, selected is not null);
        if (selected is not null)
            Assert.Equal(1L, Assert.IsType<long>(Assert.Single(selected.Values).Value));
    }

    [Theory]
    [InlineData("float:NaN", "float:1")]
    [InlineData("float:Infinity", "float:1")]
    [InlineData("float:1", "string:not-a-number")]
    [InlineData("float:1", "boolean:true")]
    public void InvalidNumericComparisonsKeepTheConversionErrorChannel(string left, string right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var plan = BlobQueryPlan.Parse("SELECT 1 FROM BlobStorage WHERE left = right;");
        var failure = Assert.Throws<BlobQueryDataException>(() =>
            plan.Select(new QueryRow(["left", "right"], [Cell(left), Cell(right)])));
        Assert.Equal("InvalidTypeConversion", failure.Name);
    }

    [Theory]
    [InlineData("1e100\n", "SELECT CAST(_1 AS FLOAT) FROM BlobStorage WHERE CAST(_1 AS FLOAT) > CAST('1e99' AS FLOAT);", "1E+100\n")]
    [InlineData("1e100\n", "SELECT 1 FROM BlobStorage WHERE CAST(_1 AS FLOAT) BETWEEN CAST('1e99' AS FLOAT) AND CAST('1e101' AS FLOAT);", "1\n")]
    [InlineData("1e100\n", "SELECT 1 FROM BlobStorage WHERE CAST(_1 AS FLOAT) IN (CAST('1e100' AS FLOAT), CAST('1e99' AS FLOAT));", "1\n")]
    [InlineData("1e100\n1e99\n", "SELECT MIN(CAST(_1 AS FLOAT)) FROM BlobStorage;", "1E+99\n")]
    [InlineData("1.0000000000000002\n", "SELECT 1 FROM BlobStorage WHERE CAST(_1 AS FLOAT) > CAST('1' AS FLOAT);", "1\n")]
    public async Task ActualSdkReturnsLargeAndCloseFiniteResultsWithoutSkippingRows(
        string input, string expression, string expected)
    {
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        var service = new BlobServiceClient(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });
        var container = service.GetBlobContainerClient($"query-float-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlockBlobClient("rows");
        var upload = BinaryData.FromString(input).ToStream();
        await using var uploadLifetime = upload.ConfigureAwait(false);
        await blob.UploadAsync(upload).ConfigureAwait(true);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value;
        var errors = new List<BlobQueryError>();
        var options = new BlobQueryOptions();
        options.ErrorHandler += errors.Add;
        var response = await blob.QueryAsync(expression, options).ConfigureAwait(true);
        var content = response.Value.Content;
        await using var contentLifetime = content.ConfigureAwait(false);
        using var reader = new StreamReader(content, leaveOpen: true);
        Assert.Equal(expected, await reader.ReadToEndAsync().ConfigureAwait(true));
        Assert.Empty(errors);
        Assert.Equal(before.ETag, (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value.ETag);
        Assert.Equal(input, (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    private static QueryCell Cell(string specification)
    {
        var parts = specification.Split(':', 2);
        return parts[0] switch
        {
            "float" => new QueryCell(double.Parse(parts[1], NumberStyles.Float, CultureInfo.InvariantCulture)),
            "decimal" => new QueryCell(decimal.Parse(parts[1], NumberStyles.Number, CultureInfo.InvariantCulture)),
            "long" => new QueryCell(long.Parse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture)),
            "string" => new QueryCell(parts[1]),
            "boolean" => new QueryCell(bool.Parse(parts[1])),
            _ => throw new ArgumentException("Unknown finite comparison control type.", nameof(specification)),
        };
    }
}
