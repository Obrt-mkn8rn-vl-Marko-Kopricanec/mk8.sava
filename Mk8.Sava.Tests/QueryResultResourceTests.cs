using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Mk8.Sava.Protocol;
using System.Text.Json;

namespace Mk8.Sava.Tests;

[Collection("Parquet resource bounds")]
public sealed class QueryResultResourceTests
{
    [Theory]
    [InlineData("csv")]
    [InlineData("json")]
    [InlineData("arrow")]
    public async Task AmplifiedResultRowFailsBeforeResponseStreaming(string format) =>
        await AssertCapacityAsync(format).ConfigureAwait(true);

    [Theory]
    [InlineData("csv")]
    [InlineData("json")]
    [InlineData("arrow")]
    public async Task AdequateCapacityPreservesEveryAmplifiedCell(string format) =>
        await AssertSuccessfulQueryAsync(format).ConfigureAwait(true);

    [Fact]
    public async Task AvroFramingNeverCopiesAnEntireWideResultIntoOneBlock()
    {
        using var destination = new RecordingStream();
        using var writer = new BlobQueryAvroWriter(destination);
        var bytes = new byte[3 * 1024 * 1024 + 7];
        await writer.AppendDataAsync(bytes, CancellationToken.None).ConfigureAwait(true);
        await writer.CompleteAsync(bytes.Length, CancellationToken.None).ConfigureAwait(true);
        Assert.InRange(destination.MaximumWriteBytes, 64 * 1024, 64 * 1024 + 64);
        Assert.True(destination.TotalWriteBytes > bytes.Length);
        Assert.True(destination.WriteCount >= 49);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.AppendDataAsync(bytes, CancellationToken.None)).ConfigureAwait(true);
    }

    private static async Task AssertSuccessfulQueryAsync(string format)
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaximumBlobQueryResultMemoryBytes"] = (128L * 1024 * 1024).ToString(System.Globalization.CultureInfo.InvariantCulture)
        });
        await using var applicationDisposal = application.ConfigureAwait(false);
        var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03)
        {
            Transport = new HttpClientTransport(application.CreateClient())
        };
        var client = new BlobServiceClient(new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey), options);
        var container = client.GetBlobContainerClient("query-result-capacity");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlockBlobClient("row.csv");
        var cell = new string('x', 128 * 1024);
        using var upload = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(cell + "\n"), writable: false);
        await blob.UploadAsync(upload).ConfigureAwait(false);
        var expression = $"SELECT {string.Join(", ", Enumerable.Range(0, 32).Select(column => $"_1 AS c{column}"))} FROM BlobStorage;";
        var result = await blob.QueryAsync(expression, new BlobQueryOptions { OutputTextConfiguration = CreateFormat(format) }).ConfigureAwait(false);
        var content = result.Value.Content;
        await using var disposal = content.ConfigureAwait(false);
        await AssertCellsAsync(content, format, cell).ConfigureAwait(false);
    }

    private static async Task AssertCellsAsync(Stream content, string format, string cell)
    {
        if (string.Equals(format, "arrow", StringComparison.Ordinal))
        {
            using var reader = new ArrowStreamReader(content, leaveOpen: true);
            using var batch = await reader.ReadNextRecordBatchAsync().ConfigureAwait(false);
            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);
            for (var column = 0; column < 32; column++)
                Assert.Equal(cell, Assert.IsType<StringArray>(batch.Column(column)).GetString(0));
            Assert.Null(await reader.ReadNextRecordBatchAsync().ConfigureAwait(false));
            return;
        }
        using var text = new StreamReader(content, leaveOpen: true);
        var output = await text.ReadToEndAsync().ConfigureAwait(false);
        if (string.Equals(format, "csv", StringComparison.Ordinal))
        {
            Assert.Equal(string.Join(',', Enumerable.Repeat(cell, 32)) + "\n", output);
            return;
        }
        using var json = JsonDocument.Parse(output);
        Assert.Equal(32, json.RootElement.EnumerateObject().Count());
        for (var column = 0; column < 32; column++)
            Assert.Equal(cell, json.RootElement.GetProperty($"c{column}").GetString());
    }

    private static async Task AssertCapacityAsync(string format)
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaximumBlobQueryResultMemoryBytes"] = (8L * 1024 * 1024).ToString(System.Globalization.CultureInfo.InvariantCulture)
        });
        await using var applicationDisposal = application.ConfigureAwait(false);
        var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03)
        {
            Transport = new HttpClientTransport(application.CreateClient())
        };
        options.Retry.MaxRetries = 0;
        var client = new BlobServiceClient(new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey), options);
        var container = client.GetBlobContainerClient("query-result-capacity");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlockBlobClient("row.csv");
        var bytes = System.Text.Encoding.UTF8.GetBytes(new string('x', 128 * 1024) + "\n");
        using var upload = new MemoryStream(bytes, writable: false);
        await blob.UploadAsync(upload).ConfigureAwait(false);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        var expression = $"SELECT {string.Join(", ", Enumerable.Range(0, 32).Select(column => $"_1 AS c{column}"))} FROM BlobStorage;";
        var query = new BlobQueryOptions { OutputTextConfiguration = CreateFormat(format) };
        var failure = await Assert.ThrowsAsync<RequestFailedException>(async () =>
        {
            var result = await blob.QueryAsync(expression, query).ConfigureAwait(false);
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
        Assert.Equal(before.ETag, (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag);
        Assert.Equal(bytes, (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    private static BlobQueryTextOptions CreateFormat(string format)
    {
        if (string.Equals(format, "csv", StringComparison.Ordinal))
            return new BlobQueryCsvTextOptions { RecordSeparator = "\n" };
        if (string.Equals(format, "json", StringComparison.Ordinal))
            return new BlobQueryJsonTextOptions { RecordSeparator = "\n" };
        var arrow = new BlobQueryArrowOptions();
        for (var column = 0; column < 32; column++)
            arrow.Schema.Add(new BlobQueryArrowField { Name = $"c{column}", Type = BlobQueryArrowFieldType.String });
        return arrow;
    }

    private sealed class RecordingStream : MemoryStream
    {
        public int MaximumWriteBytes { get; private set; }
        public long TotalWriteBytes { get; private set; }
        public int WriteCount { get; private set; }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            MaximumWriteBytes = Math.Max(MaximumWriteBytes, buffer.Length);
            TotalWriteBytes += buffer.Length;
            WriteCount++;
            return ValueTask.CompletedTask;
        }
    }
}
