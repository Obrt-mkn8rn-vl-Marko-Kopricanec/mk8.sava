using System.Buffers.Binary;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Parquet;

namespace Mk8.Sava.Tests;

public sealed class ParquetQueryTests(SavaWebApplicationFactory application) : IClassFixture<SavaWebApplicationFactory>
{
    [Fact]
    public async Task MinimalPlainFixtureHasThreeIndependentlyDecodedValues() =>
        await AssertFixtureAsync().ConfigureAwait(true);

    [Fact]
    public async Task ValidPlainInputReturnsExactlyItsDeclaredRows() =>
        await AssertQueryAsync(application, 3, 3, corrupt: false).ConfigureAwait(true);

    [Theory]
    [InlineData(4, 4)]
    [InlineData(2, 2)]
    [InlineData(4, 3)]
    [InlineData(-1, -1)]
    [InlineData(1_000_000, 1_000_000)]
    public async Task InconsistentDimensionsNeverInventOrTruncateRows(long fileRows, long groupRows) =>
        await AssertQueryAsync(application, fileRows, groupRows, corrupt: true).ConfigureAwait(true);

    private static async Task AssertFixtureAsync()
    {
        using var input = new MemoryStream(CreatePlainFixture(3, 3), writable: false);
        var reader = await ParquetReader.CreateAsync(input, leaveStreamOpen: true).ConfigureAwait(false);
        await using var disposal = reader.ConfigureAwait(false);
        Assert.NotNull(reader.Metadata);
        Assert.Equal(3, reader.Metadata.NumRows);
        Assert.Equal(1, reader.RowGroupCount);
        var field = Assert.Single(reader.Schema.GetDataFields());
        Assert.Equal("id", field.Name);
        Assert.False(field.IsNullable);
        using var group = reader.OpenRowGroupReader(0);
        Assert.Equal(3, group.RowCount);
        var metadata = group.GetMetadata(field);
        Assert.NotNull(metadata);
        Assert.NotNull(metadata.MetaData);
        Assert.Equal(3, metadata.MetaData.NumValues);
        var values = new int[3];
        await group.ReadAsync(field, values.AsMemory()).ConfigureAwait(false);
        Assert.Equal([1, 2, 3], values);
    }

    internal static Task AssertCorruptQueryAsync(SavaWebApplicationFactory application, byte[] bytes) =>
        AssertQueryAsync(application, 3, 3, corrupt: true, bytes);

    private static async Task AssertQueryAsync(SavaWebApplicationFactory application, long fileRows, long groupRows, bool corrupt,
        byte[]? fixture = null)
    {
        var container = CreateClient(application).GetBlobContainerClient($"parquet-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlockBlobClient("rows.parquet");
        var bytes = fixture ?? CreatePlainFixture(fileRows, groupRows);
        using var input = new MemoryStream(bytes, writable: false);
        await blob.UploadAsync(input).ConfigureAwait(false);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        var errors = new List<BlobQueryError>();
        var options = new BlobQueryOptions
        {
            InputTextConfiguration = new BlobQueryParquetTextOptions(),
            OutputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "\n" }
        };
        options.ErrorHandler += errors.Add;
        var result = await blob.QueryAsync("SELECT id AS id FROM BlobStorage;", options).ConfigureAwait(false);
        var content = result.Value.Content;
        await using var contentDisposal = content.ConfigureAwait(false);
        using var output = new StreamReader(content);
        var rows = await output.ReadToEndAsync().ConfigureAwait(false);
        if (corrupt)
        {
            var error = Assert.Single(errors);
            Assert.True(error.IsFatal);
            Assert.Equal("InvalidParquetFile", error.Name);
            Assert.Empty(rows);
        }
        else
        {
            Assert.Empty(errors);
            Assert.Equal("{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n", rows);
        }
        Assert.Equal(before.ETag, (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag);
        Assert.Equal(bytes, (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    internal static byte[] CreatePlainFixture(long fileRows, long groupRows,
        int pageValues = 3, int pageCompressedBytes = 12, int pageUncompressedBytes = 12,
        long columnCompressedBytes = 29, long columnUncompressedBytes = 29, long dataPageOffset = 4)
    {
        // Apache Parquet's compact-Thrift schema: one required INT32 column,
        // one uncompressed PLAIN data page containing 1, 2, 3. Only the file
        // and row-group row counts vary; the column and page always declare 3.
        using var input = new MemoryStream();
        input.Write("PAR1"u8);
        input.Write([0x15, 0, 0x15]);
        WriteCompactInteger(input, pageUncompressedBytes);
        input.WriteByte(0x15);
        WriteCompactInteger(input, pageCompressedBytes);
        input.Write([0x2C, 0x15]);
        WriteCompactInteger(input, pageValues);
        input.Write([0x15, 0, 0x15, 6, 0x15, 6, 0, 0]);
        Span<byte> integer = stackalloc byte[sizeof(int)];
        for (var value = 1; value <= 3; value++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(integer, value);
            input.Write(integer);
        }
        using var footer = new MemoryStream();
        footer.Write([0x15, 2, 0x19, 0x2C, 0x48, 6]);
        footer.Write("schema"u8);
        footer.Write([0x15, 2, 0, 0x15, 2, 0x25, 0, 0x18, 2]);
        footer.Write("id"u8);
        footer.Write([0, 0x16]);
        WriteCompactInteger(footer, fileRows);
        footer.Write([0x19, 0x1C, 0x19, 0x1C, 0x26, 8, 0x1C, 0x15, 2, 0x19, 0x25, 0, 6, 0x19, 0x18, 2]);
        footer.Write("id"u8);
        footer.Write([0x15, 0, 0x16, 6, 0x16]);
        WriteCompactInteger(footer, columnUncompressedBytes);
        footer.WriteByte(0x16);
        WriteCompactInteger(footer, columnCompressedBytes);
        footer.WriteByte(0x26);
        WriteCompactInteger(footer, dataPageOffset);
        footer.Write([0, 0, 0x16]);
        WriteCompactInteger(footer, columnUncompressedBytes);
        footer.WriteByte(0x16);
        WriteCompactInteger(footer, groupRows);
        footer.Write([0, 0]);
        input.Write(footer.GetBuffer().AsSpan(0, checked((int)footer.Length)));
        BinaryPrimitives.WriteInt32LittleEndian(integer, checked((int)footer.Length));
        input.Write(integer);
        input.Write("PAR1"u8);
        return input.ToArray();
    }

    private static void WriteCompactInteger(Stream output, long value)
    {
        var encoded = unchecked((ulong)(value << 1) ^ (ulong)(value >> 63));
        while (encoded >= 128)
        {
            output.WriteByte((byte)((encoded & 127) | 128));
            encoded >>= 7;
        }
        output.WriteByte((byte)encoded);
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
