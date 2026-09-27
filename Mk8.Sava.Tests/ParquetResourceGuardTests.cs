using System.Buffers.Binary;
using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Mk8.Sava.Protocol;
using ParquetSharp;
using ParquetSharp.IO;

namespace Mk8.Sava.Tests;

[Collection("Parquet resource bounds")]
public sealed class ParquetResourceGuardTests(SavaWebApplicationFactory application) : IClassFixture<SavaWebApplicationFactory>
{
    [Theory]
    [InlineData(4, 12, 12, 29, 29, 4)]
    [InlineData(2, 12, 12, 29, 29, 4)]
    [InlineData(3, 13, 12, 30, 29, 4)]
    [InlineData(3, 12, 13, 29, 30, 4)]
    [InlineData(3, 12, 12, 29, 29, 34)]
    [InlineData(3, 12, 12, 29, 30, 4)]
    [InlineData(int.MaxValue, 12, 12, 33, 33, 4)]
    public async Task InvalidPageGeometryFailsBeforeAnyResultRows(
        int values, int compressed, int uncompressed, long totalCompressed, long totalUncompressed, long offset) =>
        await AssertInvalidGeometryAsync(application, values, compressed, uncompressed, totalCompressed, totalUncompressed, offset).ConfigureAwait(true);

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task HostileCompactFooterFailsWithoutCountSizedAllocations(int shape) =>
        await AssertHostileFooterAsync(shape).ConfigureAwait(true);

    [Theory]
    [InlineData(Compression.Uncompressed, ParquetDataPageVersion.V1, false)]
    [InlineData(Compression.Snappy, ParquetDataPageVersion.V2, false)]
    [InlineData(Compression.Gzip, ParquetDataPageVersion.V1, false)]
    [InlineData(Compression.Zstd, ParquetDataPageVersion.V2, false)]
    [InlineData(Compression.Uncompressed, ParquetDataPageVersion.V1, true)]
    [InlineData(Compression.Snappy, ParquetDataPageVersion.V2, true)]
    [InlineData(Compression.Gzip, ParquetDataPageVersion.V1, true)]
    [InlineData(Compression.Zstd, ParquetDataPageVersion.V2, true)]
    public async Task NativeWriterMultiplePagesRemainExactlyQueryable(Compression compression, ParquetDataPageVersion pageVersion,
        bool dictionary) =>
        await AssertMultiplePagesAsync(compression, pageVersion, dictionary).ConfigureAwait(true);

    [Fact]
    public async Task ImpossibleDictionaryDimensionsFailBeforeDecoding() =>
        await AssertDictionaryGeometryAsync().ConfigureAwait(true);

    [Fact]
    public async Task FlattenedNestedSchemaIsRejectedBeforeRecursiveReconstruction() =>
        await AssertNestedSchemaAsync().ConfigureAwait(true);

    [Fact]
    public async Task ValidInputOverCapacityReturnsRetryable503BeforeStreaming() =>
        await AssertCapacityAsync().ConfigureAwait(true);

    private static async Task AssertInvalidGeometryAsync(
        SavaWebApplicationFactory application, int values, int compressed, int uncompressed,
        long totalCompressed, long totalUncompressed, long offset)
    {
        var bytes = ParquetQueryTests.CreatePlainFixture(3, 3, values, compressed, uncompressed,
            totalCompressed, totalUncompressed, offset);
        using var input = new MemoryStream(bytes, writable: false);
        var prepared = await BlobQueryProtocol.PrepareParquetInputAsync(input, 256L * 1024 * 1024, CancellationToken.None).ConfigureAwait(false);
        Assert.NotNull(prepared.Error);
        Assert.Equal("InvalidParquetFile", prepared.Error.Name);
        Assert.Empty(prepared.Fields);
        Assert.Empty(prepared.Names);
        await ParquetQueryTests.AssertCorruptQueryAsync(application, bytes).ConfigureAwait(false);
    }

    private static async Task AssertHostileFooterAsync(int shape)
    {
        byte[] footer = shape switch
        {
            0 => [0x29, 0xFC, 0x80, 0xA8, 0xD6, 0xB9, 0x07, 0], // 2 billion schema structs.
            1 => [0x16, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0],
            2 => Enumerable.Repeat((byte)0x1C, 66).Concat(new byte[67]).ToArray(),
            3 => [0x68, 0x80, 0xA8, 0xD6, 0xB9, 0x07, 0], // 2 billion created_by bytes.
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };
        using var input = new MemoryStream();
        input.Write("PAR1"u8);
        input.Write(footer);
        var length = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(length, footer.Length);
        input.Write(length);
        input.Write("PAR1"u8);
        input.Position = 0;
        var thread = Environment.CurrentManagedThreadId;
        var before = GC.GetAllocatedBytesForCurrentThread();
        var prepared = await BlobQueryProtocol.PrepareParquetInputAsync(input, 256L * 1024 * 1024, CancellationToken.None).ConfigureAwait(false);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(thread, Environment.CurrentManagedThreadId);
        Assert.InRange(allocated, 0, 256 * 1024);
        Assert.NotNull(prepared.Error);
        Assert.Equal("InvalidParquetFile", prepared.Error.Name);
    }

    private static async Task AssertMultiplePagesAsync(Compression compression, ParquetDataPageVersion pageVersion, bool dictionary)
    {
        var bytes = CreateNativeFixture(5000, 1, compression, pageVersion, dictionary);
        using var input = new MemoryStream(bytes, writable: false);
        var prepared = await BlobQueryProtocol.PrepareParquetInputAsync(input, 256L * 1024 * 1024, CancellationToken.None).ConfigureAwait(false);
        Assert.Null(prepared.Error);
        Assert.Single(prepared.Fields);
        var rows = 0;
        await foreach (var row in ParquetQueryBatchReader.ReadRowsAsync(
                           input, prepared.Fields, prepared.Names, CancellationToken.None, prepared.MetadataBytes).ConfigureAwait(false))
        {
            Assert.Equal((long)rows + 1, Assert.Single(row.Values).Value);
            rows++;
        }
        Assert.Equal(5000, rows);
    }

    private static async Task AssertDictionaryGeometryAsync()
    {
        var bytes = CreateNativeFixture(5000, 1, Compression.Uncompressed, ParquetDataPageVersion.V1, dictionary: true);
        IncreaseDictionaryCount(bytes);
        using var input = new MemoryStream(bytes, writable: false);
        var prepared = await BlobQueryProtocol.PrepareParquetInputAsync(input, 256L * 1024 * 1024, CancellationToken.None).ConfigureAwait(false);
        Assert.NotNull(prepared.Error);
        Assert.Equal("InvalidParquetFile", prepared.Error.Name);
    }

    private static void IncreaseDictionaryCount(byte[] bytes)
    {
        var reader = new ParquetCompactReader(bytes.AsSpan(4), CancellationToken.None);
        var last = 0;
        while (reader.ReadField(ref last, out var field, out var type))
        {
            if (field != 7)
            {
                reader.Skip(type);
                continue;
            }
            Assert.Equal(12, type);
            last = 0;
            Assert.True(reader.ReadField(ref last, out field, out type));
            Assert.Equal(1, field);
            Assert.Equal(5, type);
            var position = reader.Position + 4;
            Assert.Equal(5000, reader.ReadInteger(type));
            Assert.Equal(position + 2, reader.Position + 4);
            // Same two-byte varint width; no offset or payload size changes.
            bytes[position] = 0xFE;
            bytes[position + 1] = 0x7F; // zigzag 8,191 entries, over 20,000 data bytes.
            return;
        }
        Assert.Fail("The independent writer did not emit a dictionary page.");
    }

    private static async Task AssertNestedSchemaAsync()
    {
        using var footer = new MemoryStream();
        footer.Write([0x29, 0xFC, 66]); // 66 SchemaElement structs, encoded as one flat list.
        for (var element = 0; element < 65; element++)
            footer.Write([0x48, 1, (byte)'x', 0x15, 2, 0]); // name and one child.
        footer.Write([0x15, 2, 0x38, 1, (byte)'x', 0, 0]); // final primitive and footer stop.
        using var input = new MemoryStream();
        input.Write("PAR1"u8);
        input.Write(footer.GetBuffer().AsSpan(0, checked((int)footer.Length)));
        var length = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(length, checked((int)footer.Length));
        input.Write(length);
        input.Write("PAR1"u8);
        input.Position = 0;
        var prepared = await BlobQueryProtocol.PrepareParquetInputAsync(input, 256L * 1024 * 1024, CancellationToken.None).ConfigureAwait(false);
        Assert.NotNull(prepared.Error);
        Assert.Equal("UnsupportedParquetType", prepared.Error.Name);
    }

    private static async Task AssertCapacityAsync()
    {
        var bytes = CreateNativeFixture(20, 8, Compression.Snappy, ParquetDataPageVersion.V2);
        using var low = new MemoryStream(bytes, writable: false);
        var exception = await Assert.ThrowsAsync<AzureStorageException>(() =>
            BlobQueryProtocol.PrepareParquetInputAsync(low, 8L * 1024 * 1024, CancellationToken.None)).ConfigureAwait(false);
        Assert.Equal(503, exception.StatusCode);
        Assert.Equal("ServerBusy", exception.ErrorCode);
        using var adequate = new MemoryStream(bytes, writable: false);
        Assert.Null((await BlobQueryProtocol.PrepareParquetInputAsync(
            adequate, 32L * 1024 * 1024, CancellationToken.None).ConfigureAwait(false)).Error);

        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaximumParquetQueryMemoryBytes"] = (8L * 1024 * 1024).ToString(System.Globalization.CultureInfo.InvariantCulture)
        });
        await using var applicationDisposal = application.ConfigureAwait(false);
        var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03)
        {
            Transport = new HttpClientTransport(application.CreateClient())
        };
        options.Retry.MaxRetries = 0;
        var client = new BlobServiceClient(new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey), options);
        var container = client.GetBlobContainerClient("parquet-capacity");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlockBlobClient("rows.parquet");
        using var upload = new MemoryStream(bytes, writable: false);
        await blob.UploadAsync(upload).ConfigureAwait(false);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        var queryOptions = new BlobQueryOptions { InputTextConfiguration = new BlobQueryParquetTextOptions() };
        var failure = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.QueryAsync("SELECT * FROM BlobStorage;", queryOptions)).ConfigureAwait(false);
        Assert.Equal(503, failure.Status);
        Assert.Equal("ServerBusy", failure.ErrorCode);
        var response = failure.GetRawResponse();
        Assert.NotNull(response);
        Assert.True(response.Headers.TryGetValue("Retry-After", out var retryAfter));
        Assert.Equal("1", retryAfter);
        Assert.Equal(before.ETag, (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag);
        Assert.Equal(bytes, (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        var empty = await blob.QueryAsync("SELECT * FROM BlobStorage LIMIT 0;", queryOptions).ConfigureAwait(false);
        var content = empty.Value.Content;
        await using var contentDisposal = content.ConfigureAwait(false);
        using var text = new StreamReader(content);
        Assert.Empty(await text.ReadToEndAsync().ConfigureAwait(false));
    }

    private static byte[] CreateNativeFixture(int rows, int columns, Compression compression, ParquetDataPageVersion pageVersion,
        bool dictionary = false)
    {
        using var input = new MemoryStream();
        using var output = new ManagedOutputStream(input, leaveOpen: true);
        using var builder = new WriterPropertiesBuilder();
        if (dictionary)
            builder.EnableDictionary();
        else
            builder.DisableDictionary();
        using var properties = builder.Compression(compression)
            .DataPagesize(1024).WriteBatchSize(128).DataPageVersion(pageVersion).Build();
        var schema = Enumerable.Range(0, columns).Select(column => new Column<int>($"c{column}")).ToArray();
        using (var writer = new ParquetFileWriter(output, schema, properties))
        {
            using var group = writer.AppendRowGroup();
            for (var column = 0; column < columns; column++)
            {
                using var values = group.NextColumn().LogicalWriter<int>();
                values.WriteBatch(Enumerable.Range(1, rows).ToArray());
            }
            writer.Close();
        }
        return input.ToArray();
    }
}
