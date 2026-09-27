using System.Globalization;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace Mk8.Sava.Tests;

public sealed class QueryArrowBufferingTests(SavaWebApplicationFactory application) : IClassFixture<SavaWebApplicationFactory>
{
    [Theory]
    [InlineData(256, 1, 16 * 1024)]
    [InlineData(256, 4, 4 * 1024)]
    [InlineData(2, 1, 3 * 1024 * 1024)]
    public async Task WideResultsFlushBeforeTheRowCountLimitWithoutLosingCells(int rowCount, int columnCount, int cellCharacters) =>
        await AssertBatchingAsync(application, rowCount, columnCount, cellCharacters).ConfigureAwait(true);

    private static async Task AssertBatchingAsync(SavaWebApplicationFactory application, int rowCount, int columnCount, int cellCharacters)
    {
        var container = CreateClient(application).GetBlobContainerClient($"arrow-buffer-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlockBlobClient("rows.csv");
        using var input = new MemoryStream(CreateInput(rowCount, columnCount, cellCharacters), writable: false);
        await blob.UploadAsync(input).ConfigureAwait(false);
        var format = new BlobQueryArrowOptions();
        for (var column = 0; column < columnCount; column++)
            format.Schema.Add(new BlobQueryArrowField { Name = $"c{column}", Type = BlobQueryArrowFieldType.String });
        var expression = $"SELECT {string.Join(", ", Enumerable.Range(1, columnCount).Select(column => $"_{column}"))} FROM BlobStorage;";
        var result = await blob.QueryAsync(expression, new BlobQueryOptions { OutputTextConfiguration = format }).ConfigureAwait(false);
        var content = result.Value.Content;
        await using var contentDisposal = content.ConfigureAwait(false);
        using var reader = new ArrowStreamReader(content, leaveOpen: true);
        var emittedRows = 0;
        var emittedBatches = 0;
        // Four MiB of retained UTF-16 cell data, before object/Arrow overhead.
        // A single irreducible wide row remains valid and gets its own batch.
        var maximumRows = Math.Max(1, 4 * 1024 * 1024 / (2 * cellCharacters * columnCount));
        while (await reader.ReadNextRecordBatchAsync().ConfigureAwait(false) is { } batch)
        {
            using (batch)
            {
                Assert.InRange(batch.Length, 1, maximumRows);
                for (var column = 0; column < columnCount; column++)
                {
                    var values = Assert.IsType<StringArray>(batch.Column(column));
                    for (var row = 0; row < batch.Length; row++)
                        Assert.Equal(CreateCell(emittedRows + row, column, cellCharacters), values.GetString(row));
                }
                emittedRows += batch.Length;
                emittedBatches++;
            }
        }
        Assert.Equal(rowCount, emittedRows);
        Assert.True(emittedBatches > 1, "Wide records were retained until the row-count cap instead of being streamed in byte-weighted batches.");
    }

    private static byte[] CreateInput(int rowCount, int columnCount, int cellCharacters)
    {
        var rows = new StringBuilder();
        for (var row = 0; row < rowCount; row++)
        {
            for (var column = 0; column < columnCount; column++)
            {
                if (column > 0)
                    rows.Append(',');
                rows.Append(CreateCell(row, column, cellCharacters));
            }
            rows.Append('\n');
        }
        return Encoding.UTF8.GetBytes(rows.ToString());
    }

    private static string CreateCell(int row, int column, int characters)
    {
        var prefix = string.Create(CultureInfo.InvariantCulture, $"{row:D4}-{column:D2}:");
        return prefix + new string((char)('a' + column), characters - prefix.Length);
    }

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory application)
    {
        var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03)
        {
            Transport = new HttpClientTransport(application.CreateClient())
        };
        options.Retry.MaxRetries = 0;
        return new BlobServiceClient(new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey), options);
    }
}
