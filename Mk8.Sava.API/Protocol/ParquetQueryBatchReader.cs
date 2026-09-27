using System.Globalization;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Ipc;
using Parquet.Schema;
using ParquetSharp.IO;

namespace Mk8.Sava.Protocol;

internal static class ParquetQueryBatchReader
{
    private const int NumericBatchRows = 1024;

    public static async IAsyncEnumerable<QueryRow> ReadRowsAsync(
        Stream input,
        DataField[] fields,
        string[] names,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var properties = ParquetSharp.ReaderProperties.GetDefaultReaderProperties();
        properties.EnableBufferedStream();
        properties.BufferSize = 1024 * 1024;
        properties.EnablePageChecksumVerification();
        using var arrowProperties = ParquetSharp.Arrow.ArrowReaderProperties.GetDefault();
        arrowProperties.PreBuffer = false;
        arrowProperties.UseThreads = false;
        // Variable-width values can each occupy a complete page. Do not multiply
        // that footprint by 1,024 merely because the row count would fit a batch.
        arrowProperties.BatchSize = fields.Any(field =>
            field.ClrType == typeof(ReadOnlyMemory<char>) || field.ClrType == typeof(ReadOnlyMemory<byte>))
            ? 1 : NumericBatchRows;
        using var file = new ManagedRandomAccessFile(input, leaveOpen: true);
        using var reader = CreateReader(file, properties, arrowProperties, cancellationToken);
        using var batches = reader.GetRecordBatchReader();
        long emitted = 0;
        while (await ReadBatchAsync(batches, cancellationToken).ConfigureAwait(false) is { } batch)
        {
            using (batch)
            {
                if (batch.ColumnCount != fields.Length || batch.Length < 0 || batch.Length > arrowProperties.BatchSize)
                    throw InvalidInput();
                for (var row = 0; row < batch.Length; row++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var cells = new QueryCell[fields.Length];
                    for (var column = 0; column < fields.Length; column++)
                        cells[column] = ReadCell(batch.Column(column), fields[column], row);
                    emitted++;
                    yield return new QueryRow(names, cells);
                }
            }
        }
        using var parquet = reader.ParquetReader;
        using var metadata = parquet.FileMetaData;
        if (emitted != metadata.NumRows)
            throw InvalidInput();
    }

    private static ParquetSharp.Arrow.FileReader CreateReader(
        ManagedRandomAccessFile file,
        ParquetSharp.ReaderProperties properties,
        ParquetSharp.Arrow.ArrowReaderProperties arrowProperties,
        CancellationToken cancellationToken)
    {
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new ParquetSharp.Arrow.FileReader(file, properties, arrowProperties);
        }
        catch (ParquetSharp.ParquetException)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw InvalidInput();
        }
    }

    private static async ValueTask<RecordBatch?> ReadBatchAsync(IArrowArrayStream batches, CancellationToken cancellationToken)
    {
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await batches.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            // The C data-interface bridge transports native errors as managed
            // exceptions; cancellation must not be mislabeled as corrupt data.
            cancellationToken.ThrowIfCancellationRequested();
            throw InvalidInput();
        }
    }

    private static QueryCell ReadCell(IArrowArray values, DataField field, int row)
    {
        if (values.IsNull(row))
            return new QueryCell((object?)null);
        try
        {
            if (values is UInt64Array unsigned)
            {
                var number = unsigned.GetValue(row)!.Value;
                return number <= long.MaxValue ? new QueryCell((long)number) : new QueryCell((decimal)number);
            }
            object? value = values switch
            {
                BooleanArray data => data.GetValue(row),
                Int8Array data => (long)data.GetValue(row)!.Value,
                UInt8Array data => (long)data.GetValue(row)!.Value,
                Int16Array data => (long)data.GetValue(row)!.Value,
                UInt16Array data => (long)data.GetValue(row)!.Value,
                Int32Array data => (long)data.GetValue(row)!.Value,
                UInt32Array data => (long)data.GetValue(row)!.Value,
                Int64Array data => data.GetValue(row),
                FloatArray data => (double)data.GetValue(row)!.Value,
                DoubleArray data => data.GetValue(row),
                StringArray data => data.GetString(row),
                LargeStringArray data => data.GetString(row),
                StringViewArray data => data.GetString(row),
                BinaryArray data => NormalizeBinary(data.GetBytes(row), field),
                LargeBinaryArray data => NormalizeBinary(data.GetBytes(row), field),
                BinaryViewArray data => NormalizeBinary(data.GetBytes(row), field),
                TimestampArray data => data.GetTimestamp(row),
                Date32Array data => data.GetDateTimeOffset(row),
                Date64Array data => data.GetDateTimeOffset(row),
                Decimal128Array data => NormalizeDecimal(data.GetString(row)),
                Decimal256Array data => NormalizeDecimal(data.GetString(row)),
                FixedSizeBinaryArray data => NormalizeBinary(data.GetBytes(row), field),
                _ => throw new BlobQueryDataException("UnsupportedParquetType", "A Parquet field uses an unsupported data type.", 0)
            };
            return new QueryCell(value);
        }
        catch (Exception exception) when (exception is ArgumentException or OverflowException or FormatException)
        {
            throw InvalidInput();
        }
    }

    private static string NormalizeBinary(ReadOnlySpan<byte> bytes, DataField field) => field.ClrType == typeof(Guid)
        ? new Guid(bytes, bigEndian: true).ToString("D")
        : Convert.ToBase64String(bytes);

    private static decimal NormalizeDecimal(string? text)
    {
        if (text is null || !decimal.TryParse(text, NumberStyles.Number, CultureInfo.InvariantCulture, out var value) ||
            !CanonicalDecimal(text.AsSpan()).SequenceEqual(CanonicalDecimal(value.ToString(CultureInfo.InvariantCulture).AsSpan())))
        {
            throw InvalidInput();
        }
        return value;
    }

    private static ReadOnlySpan<char> CanonicalDecimal(ReadOnlySpan<char> value) => value.Contains('.')
        ? value.TrimEnd('0').TrimEnd('.') : value;

    private static BlobQueryDataException InvalidInput() => new("InvalidParquetFile", "The Parquet query input is invalid or corrupt.", 0);
}
