using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class QueryAstLegalityTests(SavaWebApplicationFactory application)
    : IClassFixture<SavaWebApplicationFactory>
{
    [Theory]
    [InlineData("SELECT COUNT(*) + 1 FROM BlobStorage;")]
    [InlineData("SELECT 1 + SUM(_1) FROM BlobStorage;")]
    [InlineData("SELECT -SUM(_1) FROM BlobStorage;")]
    [InlineData("SELECT CAST(COUNT(*) AS INT) FROM BlobStorage;")]
    [InlineData("SELECT COALESCE(COUNT(*), 0) FROM BlobStorage;")]
    [InlineData("SELECT SUM(COUNT(*)) FROM BlobStorage;")]
    [InlineData("SELECT SUM(sys.split(10485760)) FROM BlobStorage;")]
    [InlineData("SELECT sys.split(10485760) + 1 FROM BlobStorage;")]
    [InlineData("SELECT CAST(sys.split(10485760) AS INT) FROM BlobStorage;")]
    [InlineData("SELECT LOWER(CAST(sys.split(10485760) AS STRING)) FROM BlobStorage;")]
    [InlineData("SELECT _1 FROM BlobStorage WHERE COUNT(*) > 0;")]
    [InlineData("SELECT _1 FROM BlobStorage WHERE true AND COUNT(*) > 0;")]
    [InlineData("SELECT _1 FROM BlobStorage WHERE false OR sys.split(10485760) > 0;")]
    [InlineData("SELECT _1 FROM BlobStorage WHERE NOT COUNT(*);")]
    [InlineData("SELECT _1 FROM BlobStorage WHERE COUNT(*) IS NULL;")]
    [InlineData("SELECT _1 FROM BlobStorage WHERE COUNT(*) IS MISSING;")]
    [InlineData("SELECT _1 FROM BlobStorage WHERE 1 BETWEEN SUM(_1) AND 2;")]
    [InlineData("SELECT _1 FROM BlobStorage WHERE 1 IN (COUNT(*), 2);")]
    [InlineData("SELECT _1 FROM BlobStorage WHERE 1 IN (sys.split(10485760), 2);")]
    [InlineData("SELECT _1 FROM BlobStorage WHERE CAST(COUNT(*) AS BOOLEAN);")]
    [InlineData("SELECT COUNT(*), SUM(_1) FROM BlobStorage;")]
    [InlineData("SELECT sys.split(10485760) FROM BlobStorage WHERE true;")]
    [InlineData("SELECT COUNT(*) + 1 FROM BlobStorage LIMIT 0;")]
    [InlineData("SELECT _1 FROM BlobStorage WHERE false AND COUNT(*) > 0;")]
    public void UnsupportedNestedAndPredicateFormsAreRejectedDuringParsing(string expression)
    {
        var failure = Assert.Throws<AzureStorageException>(() => BlobQueryPlan.Parse(expression));
        Assert.Equal(400, failure.StatusCode);
        Assert.Equal("InvalidQueryParameterValue", failure.ErrorCode);
    }

    [Theory]
    [InlineData("SELECT COUNT(*) + 1 FROM BlobStorage;")]
    [InlineData("SELECT SUM(COUNT(*)) FROM BlobStorage;")]
    [InlineData("SELECT COALESCE(COUNT(*), 0) FROM BlobStorage;")]
    [InlineData("SELECT sys.split(10485760) + 1 FROM BlobStorage;")]
    [InlineData("SELECT _1 FROM BlobStorage WHERE COUNT(*) > 0;")]
    [InlineData("SELECT _1 FROM BlobStorage WHERE 1 IN (sys.split(10485760), 2);")]
    [InlineData("SELECT COUNT(*) + 1 FROM BlobStorage LIMIT 0;")]
    [InlineData("SELECT _1 FROM BlobStorage WHERE false AND COUNT(*) > 0;")]
    public async Task ActualSdkReceivesAnInvalidQueryBeforeAnyResultStream(string expression)
    {
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        var blob = await CreateBlobAsync(transport).ConfigureAwait(true);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value;
        var failure = await Assert.ThrowsAsync<RequestFailedException>(async () =>
        {
            var response = await blob.QueryAsync(expression).ConfigureAwait(false);
            var content = response.Value.Content;
            await using var contentLifetime = content.ConfigureAwait(false);
            await content.CopyToAsync(Stream.Null).ConfigureAwait(false);
        }).ConfigureAwait(true);
        Assert.Equal(400, failure.Status);
        Assert.Equal("InvalidQueryParameterValue", failure.ErrorCode);
        Assert.Equal(before.ETag, (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value.ETag);
        Assert.Equal("3\n", (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    [Theory]
    [InlineData("SELECT SUM(CAST(_1 AS INT) + 1) FROM BlobStorage WHERE _1 > 0;", 4L)]
    [InlineData("SELECT COUNT(COALESCE(_1, 0)) FROM BlobStorage;", 1L)]
    [InlineData("SELECT MAX(-_1) FROM BlobStorage;", -3L)]
    public void SupportedRootAggregatesRetainScalarOperands(string expression, long expected)
    {
        var plan = BlobQueryPlan.Parse(expression);
        Assert.True(plan.IsAggregate);
        Assert.False(plan.IsSplit);
        plan.Accumulate(new QueryRow(["_1"], [new QueryCell(3L)]));
        var result = plan.CompleteAggregate();
        Assert.NotNull(result);
        Assert.Equal(expected, Assert.IsType<long>(Assert.Single(result.Values).Value));
    }

    [Fact]
    public void SupportedRootSplitRetainsItsSizeAndAlias()
    {
        var plan = BlobQueryPlan.Parse("SELECT sys.split(10485760) AS bytes FROM BlobStorage;");
        Assert.True(plan.IsSplit);
        Assert.False(plan.IsAggregate);
        Assert.Equal(10485760L, plan.SplitSize);
        Assert.Equal("bytes", plan.SplitName);
    }

    [Theory]
    [InlineData("SELECT COUNT(*) + 1 FROM BlobStorage;")]
    [InlineData("SELECT _1 FROM BlobStorage WHERE false AND COUNT(*) > 0;")]
    public async Task InvalidTreesDoNotReadInputOrInitializeTheEnvelope(string expression)
    {
        var input = new ReadForbiddenInput();
        await using var inputLifetime = input.ConfigureAwait(false);
        var output = new MemoryStream();
        await using var outputLifetime = output.ConfigureAwait(false);
        var format = new BlobQueryTextFormat(BlobQueryFormatKind.Delimited, ",", '"', "\n", '\\', HasHeaders: false, []);
        var failure = await Assert.ThrowsAsync<AzureStorageException>(() => BlobQueryProtocol.ExecuteAsync(
            new BlobQueryRequest(expression, format, format), input, output, 2, CancellationToken.None)).ConfigureAwait(true);
        Assert.Equal(400, failure.StatusCode);
        Assert.Equal("InvalidQueryParameterValue", failure.ErrorCode);
        Assert.Equal(0, input.ReadCount);
        Assert.Equal(0, output.Length);
    }

    private static async Task<BlockBlobClient> CreateBlobAsync(HttpClientTransport transport)
    {
        var service = new BlobServiceClient(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });
        var container = service.GetBlobContainerClient($"query-ast-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlockBlobClient("rows");
        var upload = BinaryData.FromString("3\n").ToStream();
        await using var uploadLifetime = upload.ConfigureAwait(false);
        await blob.UploadAsync(upload).ConfigureAwait(false);
        return blob;
    }

    private sealed class ReadForbiddenInput() : MemoryStream("3\n"u8.ToArray(), writable: false)
    {
        public int ReadCount { get; private set; }
        private IOException UnexpectedRead()
        {
            ReadCount++;
            return new IOException("An invalid query attempted to read its input.");
        }
        public override int Read(byte[] buffer, int offset, int count) => throw UnexpectedRead();
        public override int Read(Span<byte> buffer) => throw UnexpectedRead();
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            Task.FromException<int>(UnexpectedRead());
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            ValueTask.FromException<int>(UnexpectedRead());
    }
}
