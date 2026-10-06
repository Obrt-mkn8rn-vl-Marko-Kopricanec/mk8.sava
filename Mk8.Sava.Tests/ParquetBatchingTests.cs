using System.Text.Json;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;
using Parquet;
using Parquet.Schema;

namespace Mk8.Sava.Tests;

[Collection("Parquet resource bounds")]
public sealed class ParquetBatchingTests(SavaWebApplicationFactory application) : IClassFixture<SavaWebApplicationFactory>
{
    [Fact]
    public async Task FirstNumericRowDoesNotAllocateWholeRowGroupColumns() =>
        await AssertFirstRowAllocationAsync().ConfigureAwait(true);

    [Fact]
    public async Task OfficialSdkPreservesScalarBinaryUuidDecimalAndDateValues() =>
        await AssertScalarValuesAsync(application).ConfigureAwait(true);

    [Fact]
    public async Task EmptyRowGroupRemainsAValidEmptyInput() =>
        await AssertEmptyGroupAsync(application).ConfigureAwait(true);

    [Fact]
    public async Task SynchronousParquetReadsHonorRequestCancellation() =>
        await AssertSynchronousCancellationAsync(application).ConfigureAwait(true);

    private static async Task AssertFirstRowAllocationAsync()
    {
        const int rows = 500_000;
        var fields = Enumerable.Range(0, 8).Select(column => new DataField<int>($"c{column}")).ToArray();
        using var input = new MemoryStream();
        var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), input, new ParquetOptions
        {
            CompressionMethod = CompressionMethod.None,
            DictionaryEncodingSampleSize = 2048
        }).ConfigureAwait(false);
        await using (writer.ConfigureAwait(false))
        {
            using var group = writer.CreateRowGroup();
            for (var column = 0; column < fields.Length; column++)
            {
                var values = Enumerable.Range(1, rows).Select(row => row + column).ToArray();
                await group.WriteAsync<int>(fields[column], values.AsMemory()).ConfigureAwait(false);
            }
            group.CompleteValidate();
        }
        input.Position = 0;
        var names = fields.Select(field => field.Name).ToArray();
        var reader = ParquetQueryBatchReader.ReadRowsAsync(input, fields, names, CancellationToken.None).GetAsyncEnumerator();
        await using var readerDisposal = reader.ConfigureAwait(false);
        var thread = Environment.CurrentManagedThreadId;
        var before = GC.GetAllocatedBytesForCurrentThread();
        Assert.True(await reader.MoveNextAsync().ConfigureAwait(false));
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        // The pinned C data-interface importer and MemoryStream complete this
        // first move synchronously; prove that before using thread-local counts.
        Assert.Equal(thread, Environment.CurrentManagedThreadId);
        // This writer puts each complete column into one page. The managed
        // native-I/O bridge may copy those encoded pages, but must not create
        // row-group-sized cell/boxed-value arrays (over 128 MiB here).
        Assert.InRange(allocated, 0, input.Length + 1024 * 1024);
        Assert.Equal(fields.Length, reader.Current.Values.Count);
        for (var column = 0; column < fields.Length; column++)
            Assert.Equal((long)column + 1, reader.Current.Values[column].Value);
    }

    private static async Task AssertScalarValuesAsync(SavaWebApplicationFactory application)
    {
        using var input = new MemoryStream();
        var unsigned = new DataField<ulong>("unsigned");
        var amount = new DecimalDataField("amount", precision: 20, scale: 4, isNullable: true);
        var binary = new DataField<byte[]>("binary");
        var identity = new DataField<Guid>("identity");
        var date = new DateTimeDataField("date", DateTimeFormat.Date);
        var maybe = new DataField<int?>("maybe");
        var ids = new[] { Guid.Parse("12345678-1234-5678-90ab-cdef12345678"), Guid.Parse("abcdefab-1234-5678-90ab-cdef12345678") };
        var writer = await ParquetWriter.CreateAsync(new ParquetSchema(unsigned, amount, binary, identity, date, maybe), input).ConfigureAwait(false);
        await using (writer.ConfigureAwait(false))
        {
            using var group = writer.CreateRowGroup();
            await group.WriteAsync<ulong>(unsigned, new ulong[] { ulong.MaxValue, 7 }.AsMemory()).ConfigureAwait(false);
            await group.WriteAsync<decimal>(amount, new decimal?[] { 12.3456M, null }.AsMemory()).ConfigureAwait(false);
            await group.WriteAsync(binary, new byte[]?[] { [0, 1, 2, 254, 255], null }).ConfigureAwait(false);
            await group.WriteAsync<Guid>(identity, ids.AsMemory()).ConfigureAwait(false);
            await group.WriteAsync<DateTime>(date, new DateTime[] { new(2026, 9, 20), new(2026, 9, 21) }.AsMemory()).ConfigureAwait(false);
            await group.WriteAsync<int>(maybe, new int?[] { null, -5 }.AsMemory()).ConfigureAwait(false);
            group.CompleteValidate();
        }
        input.Position = 0;
        var blob = await UploadAsync(application, input).ConfigureAwait(false);
        var lines = await QueryJsonAsync(blob,
            "SELECT unsigned AS unsigned, amount AS amount, binary AS binary, identity AS identity, date AS date, maybe AS maybe FROM BlobStorage;").ConfigureAwait(false);
        Assert.Equal(2, lines.Length);
        using var first = JsonDocument.Parse(lines[0]);
        Assert.Equal((decimal)ulong.MaxValue, first.RootElement.GetProperty("unsigned").GetDecimal());
        Assert.Equal(12.3456M, first.RootElement.GetProperty("amount").GetDecimal());
        Assert.Equal("AAEC/v8=", first.RootElement.GetProperty("binary").GetString());
        Assert.Equal(ids[0], first.RootElement.GetProperty("identity").GetGuid());
        Assert.Equal(new DateTimeOffset(2026, 9, 20, 0, 0, 0, TimeSpan.Zero), first.RootElement.GetProperty("date").GetDateTimeOffset());
        Assert.Equal(JsonValueKind.Null, first.RootElement.GetProperty("maybe").ValueKind);
        using var second = JsonDocument.Parse(lines[1]);
        Assert.Equal(7, second.RootElement.GetProperty("unsigned").GetInt64());
        Assert.Equal(JsonValueKind.Null, second.RootElement.GetProperty("amount").ValueKind);
        Assert.Equal(JsonValueKind.Null, second.RootElement.GetProperty("binary").ValueKind);
        Assert.Equal(ids[1], second.RootElement.GetProperty("identity").GetGuid());
        Assert.Equal(new DateTimeOffset(2026, 9, 21, 0, 0, 0, TimeSpan.Zero), second.RootElement.GetProperty("date").GetDateTimeOffset());
        Assert.Equal(-5, second.RootElement.GetProperty("maybe").GetInt64());
    }

    private static async Task AssertEmptyGroupAsync(SavaWebApplicationFactory application)
    {
        var field = new DataField<int>("id");
        using var input = new MemoryStream();
        var writer = await ParquetWriter.CreateAsync(new ParquetSchema(field), input).ConfigureAwait(false);
        await using (writer.ConfigureAwait(false))
        {
            using var group = writer.CreateRowGroup();
            await group.WriteAsync<int>(field, ReadOnlyMemory<int>.Empty).ConfigureAwait(false);
            group.CompleteValidate();
        }
        input.Position = 0;
        var blob = await UploadAsync(application, input).ConfigureAwait(false);
        Assert.Empty(await QueryJsonAsync(blob, "SELECT id AS id FROM BlobStorage;").ConfigureAwait(false));
        Assert.Equal(["{\"count\":0}"], await QueryJsonAsync(blob, "SELECT COUNT(*) AS count FROM BlobStorage;").ConfigureAwait(false));
    }

    private static async Task AssertSynchronousCancellationAsync(SavaWebApplicationFactory application)
    {
        using var input = new MemoryStream("not-read"u8.ToArray(), writable: false);
        var blob = await UploadAsync(application, input).ConfigureAwait(false);
        var service = application.Services.GetRequiredService<BlobService>();
        var record = await service.GetBlobAsync(SavaWebApplicationFactory.AccountName, blob.BlobContainerName,
            blob.Name, versionId: null, snapshot: null, includeDeleted: false, CancellationToken.None).ConfigureAwait(false);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync().ConfigureAwait(false);
        var stream = new BlobSeekableReadStream(
            (offset, length, destination, token) => service.WriteContentAsync(
                record, new BlobEncryption(null, null), offset, length, destination, token),
            record.Content.Length, cancellation.Token);
        await using var disposal = stream.ConfigureAwait(false);
        var bytes = new byte[4];
        Assert.ThrowsAny<OperationCanceledException>(() => stream.Read(bytes, 0, bytes.Length));
        Assert.ThrowsAny<OperationCanceledException>(() => stream.Read(bytes.AsSpan()));
        Assert.Equal(0, stream.Position);
    }

    private static async Task<BlockBlobClient> UploadAsync(SavaWebApplicationFactory application, Stream input)
    {
        var container = CreateClient(application).GetBlobContainerClient($"parquet-batch-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlockBlobClient("rows.parquet");
        await blob.UploadAsync(input).ConfigureAwait(false);
        return blob;
    }

    private static async Task<string[]> QueryJsonAsync(BlockBlobClient blob, string expression)
    {
        var errors = new List<BlobQueryError>();
        var options = new BlobQueryOptions
        {
            InputTextConfiguration = new BlobQueryParquetTextOptions(),
            OutputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "\n" }
        };
        options.ErrorHandler += errors.Add;
        var result = await blob.QueryAsync(expression, options).ConfigureAwait(false);
        var content = result.Value.Content;
        await using var contentDisposal = content.ConfigureAwait(false);
        using var reader = new StreamReader(content);
        var text = await reader.ReadToEndAsync().ConfigureAwait(false);
        Assert.Empty(errors);
        return text.Split('\n', StringSplitOptions.RemoveEmptyEntries);
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
