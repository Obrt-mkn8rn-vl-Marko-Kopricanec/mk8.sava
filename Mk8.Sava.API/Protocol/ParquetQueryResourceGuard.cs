using System.Buffers.Binary;
using Parquet;
using Parquet.Meta;

namespace Mk8.Sava.Protocol;

internal sealed class ParquetQueryResourceGuard(long memoryBytes)
{
    // Account for managed/native metadata objects, strings, references and the
    // simultaneous footer copies. This is an admission estimate, not RSS.
    private const int MetadataExpansionFactor = 256;
    public int MaximumMetadataBytes { get; } = checked((int)(memoryBytes / MetadataExpansionFactor));
    private long _footerStart;
    private long _footerMemory;

    public async Task ValidateFooterAsync(Stream input, CancellationToken cancellationToken)
    {
        if (!input.CanSeek || input.Length < 12)
            throw Invalid();
        var marker = new byte[8];
        input.Position = 0;
        await input.ReadExactlyAsync(marker.AsMemory(0, 4), cancellationToken).ConfigureAwait(false);
        if (!marker.AsSpan(0, 4).SequenceEqual("PAR1"u8))
            throw Invalid();
        input.Position = input.Length - 8;
        await input.ReadExactlyAsync(marker, cancellationToken).ConfigureAwait(false);
        if (!marker.AsSpan(4, 4).SequenceEqual("PAR1"u8))
            throw Invalid();
        var length = BinaryPrimitives.ReadUInt32LittleEndian(marker);
        if (length == 0 || length > input.Length - 12)
            throw Invalid();
        if (length > MaximumMetadataBytes)
            throw CapacityExceeded();
        _footerStart = input.Length - 8 - length;
        _footerMemory = (long)length * MetadataExpansionFactor;
        if (_footerMemory + 4 * 1024 * 1024 > memoryBytes)
            throw CapacityExceeded();
        var bytes = new byte[checked((int)length)];
        input.Position = _footerStart;
        await input.ReadExactlyAsync(bytes, cancellationToken).ConfigureAwait(false);
        ValidateCompactFooter(bytes, cancellationToken);
        input.Position = 0;
    }

    private void ValidateCompactFooter(ReadOnlySpan<byte> bytes, CancellationToken cancellationToken)
    {
        var compact = new ParquetCompactReader(bytes, cancellationToken);
        compact.SkipStruct();
        if (compact.Position != bytes.Length)
            throw Invalid();
        // Schema nesting is flattened in Thrift. A generic wire-depth check
        // cannot protect a decoder that recursively rebuilds that schema tree.
        var schema = new ParquetCompactReader(bytes, cancellationToken);
        var last = 0;
        var found = false;
        while (schema.ReadField(ref last, out var field, out var type))
        {
            if (field == 2)
            {
                if (found || type != 9)
                    throw Invalid();
                found = true;
                ValidateFlatSchema(ref schema);
            }
            else
                schema.Skip(type);
        }
        if (!found)
            throw Invalid();
    }

    private void ValidateFlatSchema(ref ParquetCompactReader reader)
    {
        var (count, type) = reader.ReadListHeader();
        if (count < 2 || type != 12)
            throw Invalid();
        for (var entry = 0; entry < count; entry++)
        {
            var last = 0;
            var physical = -1;
            var children = 0;
            var width = 0;
            while (reader.ReadField(ref last, out var field, out var fieldType))
            {
                if (field is 1 or 2 or 5)
                {
                    if (fieldType != 5)
                        throw Invalid();
                    var number = checked((int)reader.ReadInteger(fieldType));
                    switch (field)
                    {
                        case 1: physical = number; break;
                        case 2: width = number; break;
                        case 5: children = number; break;
                    }
                }
                else
                    reader.Skip(fieldType);
            }
            if (entry == 0)
            {
                if (physical != -1 || children < 1 || children > count - 1)
                    throw Invalid();
                if (children != count - 1)
                    throw UnsupportedSchema();
            }
            else
            {
                if (children > 0 || physical == -1)
                    throw UnsupportedSchema();
                if (children < 0 || physical is < 0 or > 7 || (physical == 7 && width <= 0))
                    throw Invalid();
                if (physical == 7 && width > memoryBytes / 8)
                    throw CapacityExceeded();
            }
        }
    }

    private static BlobQueryDataException UnsupportedSchema() => new(
        "UnsupportedParquetType", "Nested and repeated Parquet fields are not supported by Query Blob Contents.", 0);

    public async Task ValidatePagesAsync(ParquetReader reader, Stream input, CancellationToken cancellationToken)
    {
        var metadata = reader.Metadata ?? throw Invalid();
        for (var groupIndex = 0; groupIndex < metadata.RowGroups.Count; groupIndex++)
        {
            var group = metadata.RowGroups[groupIndex];
            long estimated = _footerMemory + 4 * 1024 * 1024;
            for (var columnIndex = 0; columnIndex < group.Columns.Count; columnIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                estimated = checked(estimated + await ValidateColumnAsync(
                    group.Columns[columnIndex], input, cancellationToken).ConfigureAwait(false));
            }
            if (estimated > memoryBytes)
                throw CapacityExceeded();
        }
        input.Position = 0;
    }

    private async Task<long> ValidateColumnAsync(ColumnChunk column, Stream input, CancellationToken cancellationToken)
    {
        var metadata = column.MetaData ?? throw Invalid();
        if (!string.IsNullOrEmpty(column.FilePath))
            throw new BlobQueryDataException("UnsupportedParquetType", "Parquet query columns must be contained in the queried blob.", 0);
        var start = metadata.DataPageOffset;
        if (metadata.DictionaryPageOffset is { } dictionary)
            start = Math.Min(start, dictionary);
        if (metadata.IndexPageOffset is { } index)
            start = Math.Min(start, index);
        if (start < 4 || start > _footerStart || metadata.TotalCompressedSize < 0 ||
            metadata.TotalCompressedSize > _footerStart - start || metadata.TotalUncompressedSize < 0)
        {
            throw Invalid();
        }
        var end = start + metadata.TotalCompressedSize;
        long values = 0;
        long uncompressed = 0;
        long largestPage = 0;
        long dictionaryMemory = 0;
        var sawDictionary = false;
        var sawData = false;
        while (start < end)
        {
            var page = await ReadPageAsync(input, start, end, cancellationToken).ConfigureAwait(false);
            if (page.CompressedBytes > end - start - page.HeaderBytes ||
                ((int)metadata.Codec == 0 || (page.Type == 3 && !page.IsCompressed)) &&
                page.CompressedBytes != page.UncompressedBytes)
            {
                throw Invalid();
            }
            if (page.Type == 2)
            {
                if (sawDictionary || sawData || metadata.DictionaryPageOffset != start)
                    throw Invalid();
                sawDictionary = true;
                ValidateDictionaryDimensions(metadata, page);
                dictionaryMemory = 32L * page.Values + 2L * page.UncompressedBytes;
            }
            else if (page.Type is 0 or 3)
            {
                if ((!sawData && metadata.DataPageOffset != start) || page.Values > metadata.NumValues - values)
                    throw Invalid();
                sawData = true;
                values += page.Values;
            }
            uncompressed = checked(uncompressed + page.HeaderBytes + page.UncompressedBytes);
            // Some delta/dictionary decoders keep page-sized value/length
            // indexes, even when the encoded payload is tiny.
            largestPage = Math.Max(largestPage, 8L * page.UncompressedBytes + 2L * page.CompressedBytes +
                (page.Type is 0 or 3 ? 32L * page.Values : 0));
            start += page.HeaderBytes + (long)page.CompressedBytes;
        }
        if (values != metadata.NumValues || uncompressed != metadata.TotalUncompressedSize ||
            (metadata.DictionaryPageOffset is not null && !sawDictionary))
        {
            throw Invalid();
        }
        // Buffered native input, encoded-page copies, decoded page data,
        // dictionary entries and a bounded numeric Arrow batch coexist.
        return checked(1024 * 1024 + 64 * 1024 + largestPage + dictionaryMemory);
    }

    private static void ValidateDictionaryDimensions(ColumnMetaData metadata, ParquetPageGeometry page)
    {
        var minimumBytes = (int)metadata.Type switch
        {
            0 => 0, // BOOLEAN is bit-packed.
            1 or 4 or 6 => 4,
            2 or 5 => 8,
            3 => 12,
            7 => 1,
            _ => throw Invalid()
        };
        if (minimumBytes == 0 ? page.Values > 8L * page.UncompressedBytes
            : (long)page.Values * minimumBytes > page.UncompressedBytes)
        {
            throw Invalid();
        }
    }

    private async Task<ParquetPageGeometry> ReadPageAsync(
        Stream input, long start, long end, CancellationToken cancellationToken)
    {
        var limit = checked((int)Math.Min(MaximumMetadataBytes, end - start));
        var size = Math.Min(128, limit);
        while (true)
        {
            var bytes = new byte[size];
            input.Position = start;
            await input.ReadExactlyAsync(bytes, cancellationToken).ConfigureAwait(false);
            try
            {
                return ParquetPageGeometry.Read(bytes, cancellationToken);
            }
            catch (EndOfStreamException)
            {
                if (size == limit)
                {
                    if (limit < end - start)
                        throw CapacityExceeded();
                    throw Invalid();
                }
                size = Math.Min(checked(size * 2), limit);
            }
        }
    }

    private static BlobQueryDataException Invalid() => new("InvalidParquetFile", "The Parquet file geometry is invalid.", 0);

    private static AzureStorageException CapacityExceeded() => new(
        503, "ServerBusy", "The Parquet query exceeds this deployment's configured decoding capacity.",
        responseHeaders: new Dictionary<string, string>(StringComparer.Ordinal) { ["Retry-After"] = "1" });
}
