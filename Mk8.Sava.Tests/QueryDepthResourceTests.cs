using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Blobs.Models;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

[Collection("Parquet resource bounds")]
public sealed class QueryDepthResourceTests(SavaWebApplicationFactory application) : IClassFixture<SavaWebApplicationFactory>
{
    [Theory]
    [InlineData("parentheses")]
    [InlineData("unary")]
    [InlineData("not")]
    [InlineData("cast")]
    [InlineData("function")]
    [InlineData("binary")]
    [InlineData("and")]
    [InlineData("or")]
    [InlineData("table")]
    public async Task ExcessiveDepthIsRetryableAndCannotTerminateTheService(string shape) =>
        await AssertDepthRejectionAsync(application, shape).ConfigureAwait(true);

    [Theory]
    [InlineData(127)]
    [InlineData(128)]
    public void EvaluationDepthBoundaryPreservesExactArithmetic(int operands)
    {
        var plan = BlobQueryPlan.Parse($"SELECT {string.Join('+', Enumerable.Repeat("1", operands))} FROM BlobStorage;");
        var result = plan.Select(new QueryRow([], []));
        Assert.NotNull(result);
        Assert.Equal((long)operands, Assert.IsType<long>(Assert.Single(result.Values).Value));
    }

    [Fact]
    public void EvaluationDepthJustBeyondBoundaryFailsWithoutEvaluatingTheTree()
    {
        var error = Assert.Throws<AzureStorageException>(() => BlobQueryPlan.Parse(
            $"SELECT {string.Join('+', Enumerable.Repeat("1", 129))} FROM BlobStorage;"));
        Assert.Equal("ServerBusy", error.ErrorCode);
    }

    [Fact]
    public void ParserDepthBoundaryAcceptsAnOrdinaryNestedExpression()
    {
        var plan = BlobQueryPlan.Parse($"SELECT {new string('(', 127)}1{new string(')', 127)} FROM BlobStorage;");
        var result = plan.Select(new QueryRow([], []));
        Assert.NotNull(result);
        Assert.Equal(1L, Assert.IsType<long>(Assert.Single(result.Values).Value));
    }

    private static async Task AssertDepthRejectionAsync(SavaWebApplicationFactory application, string shape)
    {
        var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03)
        {
            Transport = new HttpClientTransport(application.CreateClient())
        };
        options.Retry.MaxRetries = 0;
        var service = new BlobServiceClient(new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey), options);
        var container = service.GetBlobContainerClient($"query-depth-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlockBlobClient("row.csv");
        using var upload = new MemoryStream("1\n"u8.ToArray(), writable: false);
        await blob.UploadAsync(upload).ConfigureAwait(false);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        var error = await Assert.ThrowsAsync<RequestFailedException>(async () =>
        {
            var query = new BlobQueryOptions
            {
                InputTextConfiguration = string.Equals(shape, "table", StringComparison.Ordinal)
                    ? new BlobQueryJsonTextOptions { RecordSeparator = "\n" }
                    : new BlobQueryCsvTextOptions { RecordSeparator = "\n" }
            };
            var result = await blob.QueryAsync(CreateExpression(shape), query).ConfigureAwait(false);
            var content = result.Value.Content;
            await using var disposal = content.ConfigureAwait(false);
            await content.CopyToAsync(Stream.Null).ConfigureAwait(false);
        }).ConfigureAwait(false);
        Assert.Equal(503, error.Status);
        Assert.Equal("ServerBusy", error.ErrorCode);
        var response = error.GetRawResponse();
        Assert.NotNull(response);
        Assert.True(response.Headers.TryGetValue("Retry-After", out var retryAfter));
        Assert.Equal("1", retryAfter);
        Assert.Equal(before.ETag, (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag);
        Assert.Equal("1\n", (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
    }

    private static string CreateExpression(string shape)
    {
        const int count = 10000;
        return shape switch
        {
            "parentheses" => $"SELECT {new string('(', count)}1{new string(')', count)} FROM BlobStorage;",
            "unary" => $"SELECT {string.Concat(Enumerable.Repeat("- ", count))}1 FROM BlobStorage;",
            "not" => $"SELECT _1 FROM BlobStorage WHERE {string.Concat(Enumerable.Repeat("NOT ", count))}true;",
            "cast" => $"SELECT {string.Concat(Enumerable.Repeat("CAST(", count))}1{string.Concat(Enumerable.Repeat(" AS INT)", count))} FROM BlobStorage;",
            "function" => $"SELECT {string.Concat(Enumerable.Repeat("LOWER(", count))}'x'{new string(')', count)} FROM BlobStorage;",
            "binary" => $"SELECT {string.Join('+', Enumerable.Repeat("1", count))} FROM BlobStorage;",
            "and" => $"SELECT _1 FROM BlobStorage WHERE {string.Join(" AND ", Enumerable.Repeat("true", count))};",
            "or" => $"SELECT _1 FROM BlobStorage WHERE {string.Join(" OR ", Enumerable.Repeat("false", count))};",
            "table" => $"SELECT * FROM BlobStorage[*].{string.Join('.', Enumerable.Repeat("x", count))};",
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };
    }
}
