using System.Buffers;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Xml;
using System.Xml.Linq;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Parquet;
using Parquet.Data;

namespace Mk8.Sava.Protocol;

internal static class BlobQueryProtocol
{
    private const int MaximumExpressionBytes = 256 * 1024;
    private const int MaximumRecordCharacters = 16 * 1024 * 1024;
    private const int ArrowRecordBatchSize = 1024;
    private static readonly byte[] Utf8Preamble = [0xEF, 0xBB, 0xBF];

    public static async Task<BlobQueryRequest> ReadRequestAsync(
        Stream body,
        CancellationToken cancellationToken)
    {
        try
        {
            using var reader = ProtocolParsing.CreateXmlReader(body);
            var document = await XDocument.LoadAsync(reader, LoadOptions.None, cancellationToken).ConfigureAwait(false);
            var root = document.Root;
            if (!string.Equals(root?.Name.LocalName, "QueryRequest", StringComparison.Ordinal))
                throw InvalidXml("The QueryRequest root element is required.");

            var queryType = ChildValue(root, "QueryType");
            if (!string.Equals(queryType, "SQL", StringComparison.OrdinalIgnoreCase))
                throw InvalidXml("Only the SQL query type is supported.");

            var expression = ChildValue(root, "Expression");
            if (string.IsNullOrWhiteSpace(expression) || Encoding.UTF8.GetByteCount(expression) > MaximumExpressionBytes)
                throw InvalidXml("The query expression is missing or exceeds the 256 KiB limit.");

            var input = ReadFormat(Child(root, "InputSerialization"), input: true);
            var output = ReadFormat(Child(root, "OutputSerialization"), input: false);
            var plan = BlobQueryPlan.Parse(expression);
            if (plan.HasJsonTablePath && input.Kind != BlobQueryFormatKind.Json)
            {
                throw new AzureStorageException(
                    StatusCodes.Status400BadRequest,
                    "InvalidQueryParameterValue",
                    "Nested BlobStorage table paths require JSON query input.");
            }
            if (plan.IsSplit && input.Kind != BlobQueryFormatKind.Delimited)
            {
                throw new AzureStorageException(
                    StatusCodes.Status400BadRequest,
                    "InvalidQueryParameterValue",
                    "Sys.Split requires delimited query input.");
            }
            return new BlobQueryRequest(expression, input, output);
        }
        catch (AzureStorageException)
        {
            throw;
        }
        catch (XmlException exception)
        {
            throw InvalidXml(exception.Message);
        }
    }

    public static async Task ExecuteAsync(
        BlobQueryRequest request,
        Stream input,
        Stream response,
        long totalBytes,
        CancellationToken cancellationToken)
    {
        var plan = BlobQueryPlan.Parse(request.Expression);
        using var avro = new BlobQueryAvroWriter(response);
        await avro.InitializeAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            var selections = ExecutePlanAsync(input, request.Input, plan, cancellationToken)
                .GetAsyncEnumerator(cancellationToken);
            await using (selections.ConfigureAwait(false))
            {
                if (request.Output.Kind == BlobQueryFormatKind.Arrow)
                {
                    await WriteArrowResultsAsync(
                        selections,
                        request.Output.ArrowSchema,
                        avro,
                        cancellationToken).ConfigureAwait(false);
                    await avro.CompleteAsync(totalBytes, cancellationToken).ConfigureAwait(false);
                    return;
                }

                var wroteHeader = false;
                while (await selections.MoveNextAsync().ConfigureAwait(false))
                {
                    var selected = selections.Current;

                    if (!wroteHeader && request.Output.Kind == BlobQueryFormatKind.Delimited && request.Output.HasHeaders)
                    {
                        await avro.AppendDataAsync(
                            EncodeDelimited(selected.Names.Select(name => new QueryCell(name)).ToArray(), request.Output),
                            cancellationToken).ConfigureAwait(false);
                        wroteHeader = true;
                    }

                    var encoded = request.Output.Kind switch
                    {
                        BlobQueryFormatKind.Delimited => EncodeDelimited(selected.Values, request.Output),
                        BlobQueryFormatKind.Json => EncodeJson(selected, request.Output),
                        _ => throw new InvalidOperationException("Unknown query output format.")
                    };
                    await avro.AppendDataAsync(encoded, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (BlobQueryDataException exception)
        {
            await avro.WriteErrorAsync(true, exception.Name, exception.Message, exception.Position, cancellationToken).ConfigureAwait(false);
        }

        await avro.CompleteAsync(totalBytes, cancellationToken).ConfigureAwait(false);
    }

    private static async IAsyncEnumerable<QuerySelection> ExecutePlanAsync(
        Stream input,
        BlobQueryTextFormat format,
        BlobQueryPlan plan,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (plan.IsSplit)
        {
            await foreach (var selection in ReadSplitSelectionsAsync(
                               input,
                               format,
                               plan.SplitSize,
                               plan.SplitName,
                               cancellationToken).ConfigureAwait(false))
            {
                yield return selection;
            }
            yield break;
        }

        var rows = ReadRowsAsync(input, format, plan, cancellationToken)
            .GetAsyncEnumerator(cancellationToken);
        await using (rows.ConfigureAwait(false))
        {
            await foreach (var selection in SelectRowsAsync(rows, plan, cancellationToken).ConfigureAwait(false))
                yield return selection;
        }
    }

    private static async IAsyncEnumerable<QuerySelection> SelectRowsAsync(
        IAsyncEnumerator<QueryRow> rows,
        BlobQueryPlan plan,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (plan.IsAggregate)
        {
            if (plan.LimitReached)
                yield break;
            while (await rows.MoveNextAsync().ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                plan.Accumulate(rows.Current);
            }
            if (plan.CompleteAggregate() is { } aggregate)
                yield return aggregate;
            yield break;
        }

        while (!plan.LimitReached && await rows.MoveNextAsync().ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (plan.Select(rows.Current) is { } selected)
                yield return selected;
        }
    }

    private static BlobQueryTextFormat ReadFormat(XElement? serialization, bool input)
    {
        var format = serialization is null ? null : Child(serialization, "Format");
        var type = format is null ? "delimited" : ChildValue(format, "Type")?.ToRequiredLowerInvariant();
        if (string.IsNullOrEmpty(type))
            throw InvalidXml("A query serialization format type is required.");

        if (type is "delimited" or "csv")
        {
            var configuration = format is null ? null : Child(format, "DelimitedTextConfiguration");
            var column = ChildValue(configuration, "ColumnSeparator") ?? ",";
            var record = ChildValue(configuration, "RecordSeparator") ?? "\n";
            var quote = ParseCharacter(ChildValue(configuration, "FieldQuote"), '"', "FieldQuote");
            var escape = ParseCharacter(ChildValue(configuration, "EscapeChar"), '\\', "EscapeChar");
            var headers = ParseBoolean(ChildValue(configuration, "HasHeaders"), false, "HasHeaders");
            ValidateSeparator(column, "ColumnSeparator");
            ValidateSeparator(record, "RecordSeparator");
            if (string.Equals(column, record, StringComparison.Ordinal))
                throw InvalidXml("ColumnSeparator and RecordSeparator must differ.");
            return new BlobQueryTextFormat(
                BlobQueryFormatKind.Delimited,
                column,
                quote,
                record,
                escape,
                headers,
                []);
        }

        if (string.Equals(type, "json", StringComparison.Ordinal))
        {
            var configuration = format is null ? null : Child(format, "JsonTextConfiguration");
            var record = ChildValue(configuration, "RecordSeparator") ?? "\n";
            ValidateSeparator(record, "RecordSeparator");
            return new BlobQueryTextFormat(BlobQueryFormatKind.Json, ",", '"', record, '\\', false, []);
        }

        if (!input && string.Equals(type, "arrow", StringComparison.Ordinal))
        {
            var configuration = Child(format, "ArrowConfiguration")
                                ?? throw InvalidXml("ArrowConfiguration is required for Arrow output.");
            var schema = Child(configuration, "Schema")
                         ?? throw InvalidXml("An Arrow output schema is required.");
            var fields = schema.Elements()
                .Where(element => string.Equals(element.Name.LocalName, "Field", StringComparison.Ordinal))
                .Select((field, index) => ReadArrowField(field, index))
                .ToArray();
            if (fields.Length is < 1 or > 256)
                throw InvalidXml("An Arrow output schema must contain between one and 256 fields.");
            return new BlobQueryTextFormat(BlobQueryFormatKind.Arrow, ",", '"', "\n", '\\', false, fields);
        }

        if (input && string.Equals(type, "parquet", StringComparison.Ordinal))
            return new BlobQueryTextFormat(BlobQueryFormatKind.Parquet, ",", '"', "\n", '\\', false, []);

        var direction = input ? "input" : "output";
        throw new AzureStorageException(
            StatusCodes.Status400BadRequest,
            "BlobQueryError",
            $"The {direction} query format '{type}' is not supported.");
    }

    private static QueryArrowColumn ReadArrowField(XElement field, int index)
    {
        var type = ChildValue(field, "Type")?.ToRequiredLowerInvariant();
        var kind = type switch
        {
            "int64" => BlobQueryArrowFieldKind.Int64,
            "bool" => BlobQueryArrowFieldKind.Bool,
            "timestamp[ms]" => BlobQueryArrowFieldKind.Timestamp,
            "string" => BlobQueryArrowFieldKind.String,
            "double" => BlobQueryArrowFieldKind.Double,
            "decimal" => BlobQueryArrowFieldKind.Decimal,
            _ => throw InvalidXml($"Arrow field {index + 1} has an unsupported type '{type}'.")
        };
        var name = ChildValue(field, "Name");
        if (string.IsNullOrWhiteSpace(name))
            name = $"_{index + 1}";

        var precision = ParseArrowInteger(field, "Precision", kind == BlobQueryArrowFieldKind.Decimal ? null : 0);
        var scale = ParseArrowInteger(field, "Scale", kind == BlobQueryArrowFieldKind.Decimal ? null : 0);
        if (kind == BlobQueryArrowFieldKind.Decimal &&
            (precision is < 1 or > 38 || scale < 0 || scale > precision))
        {
            throw InvalidXml("Arrow decimal fields require precision from 1 through 38 and scale from 0 through precision.");
        }
        if (kind != BlobQueryArrowFieldKind.Decimal && (precision != 0 || scale != 0))
            throw InvalidXml("Arrow Precision and Scale are valid only for decimal fields.");

        return new QueryArrowColumn(kind, name, precision, scale);
    }

    private static int ParseArrowInteger(XElement field, string name, int? fallback)
    {
        var value = ChildValue(field, name);
        if (string.IsNullOrEmpty(value))
        {
            if (fallback.HasValue)
                return fallback.Value;
            throw InvalidXml($"Arrow decimal field {name} is required.");
        }
        if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed))
            throw InvalidXml($"Arrow field {name} must be an integer.");
        return parsed;
    }

    private static async Task WriteArrowResultsAsync(
        IAsyncEnumerator<QuerySelection> rows,
        IReadOnlyList<QueryArrowColumn> fields,
        BlobQueryAvroWriter avro,
        CancellationToken cancellationToken)
    {
        var schema = new Schema(
            fields.Select(CreateArrowField),
            new Dictionary<string, string>(StringComparer.Ordinal));
        using var dataStream = new BlobQueryAvroDataStream(avro, cancellationToken);
        using var writer = new ArrowStreamWriter(dataStream, schema, leaveOpen: true);
        await writer.WriteStartAsync(cancellationToken).ConfigureAwait(false);

        var batch = new List<QuerySelection>(ArrowRecordBatchSize);
        while (await rows.MoveNextAsync().ConfigureAwait(false))
        {
            var selected = rows.Current;
            if (selected.Values.Count != fields.Count)
            {
                throw new BlobQueryDataException(
                    "InvalidArrowSchema",
                    $"The query selected {selected.Values.Count} columns but the Arrow schema defines {fields.Count} fields.",
                    0);
            }

            batch.Add(selected);
            if (batch.Count < ArrowRecordBatchSize)
                continue;
            await WriteArrowBatchAsync(writer, schema, fields, batch, cancellationToken).ConfigureAwait(false);
            batch.Clear();
        }

        if (batch.Count > 0)
            await WriteArrowBatchAsync(writer, schema, fields, batch, cancellationToken).ConfigureAwait(false);
        await writer.WriteEndAsync(cancellationToken).ConfigureAwait(false);
    }

    private static Field CreateArrowField(QueryArrowColumn field)
    {
        IArrowType type = field.Kind switch
        {
            BlobQueryArrowFieldKind.Int64 => Int64Type.Default,
            BlobQueryArrowFieldKind.Bool => BooleanType.Default,
            BlobQueryArrowFieldKind.Timestamp => new TimestampType(TimeUnit.Millisecond, (string?)null),
            BlobQueryArrowFieldKind.String => StringType.Default,
            BlobQueryArrowFieldKind.Double => DoubleType.Default,
            BlobQueryArrowFieldKind.Decimal => new Decimal128Type(field.Precision, field.Scale),
            _ => throw new InvalidOperationException("Unknown Arrow field type.")
        };
        return new Field(field.Name, type, nullable: true);
    }

    private static async Task WriteArrowBatchAsync(
        ArrowStreamWriter writer,
        Schema schema,
        IReadOnlyList<QueryArrowColumn> fields,
        List<QuerySelection> rows,
        CancellationToken cancellationToken)
    {
        var arrays = fields
            .Select((field, index) => BuildArrowArray(field, rows, index))
            .ToArray();
        using var batch = new RecordBatch(schema, arrays, rows.Count);
        await writer.WriteRecordBatchAsync(batch, cancellationToken).ConfigureAwait(false);
    }

    private static IArrowArray BuildArrowArray(
        QueryArrowColumn field,
        List<QuerySelection> rows,
        int column)
    {
        try
        {
            return field.Kind switch
            {
                BlobQueryArrowFieldKind.Int64 => BuildInt64ArrowArray(field, rows, column),
                BlobQueryArrowFieldKind.Bool => BuildBoolArrowArray(field, rows, column),
                BlobQueryArrowFieldKind.Timestamp => BuildTimestampArrowArray(field, rows, column),
                BlobQueryArrowFieldKind.String => BuildStringArrowArray(rows, column),
                BlobQueryArrowFieldKind.Double => BuildDoubleArrowArray(field, rows, column),
                BlobQueryArrowFieldKind.Decimal => BuildDecimalArrowArray(field, rows, column),
                _ => throw new InvalidOperationException("Unknown Arrow field type.")
            };
        }
        catch (BlobQueryDataException)
        {
            throw;
        }
        catch (Exception exception) when (exception is ArgumentException or FormatException or OverflowException)
        {
            throw new BlobQueryDataException(
                "InvalidArrowType",
                $"A value could not be represented by Arrow field '{field.Name}' as {ArrowTypeName(field.Kind)}.",
                0);
        }
    }

    private static Int64Array BuildInt64ArrowArray(QueryArrowColumn field, List<QuerySelection> rows, int column)
    {
        var builder = new Int64Array.Builder().Reserve(rows.Count);
        foreach (ref readonly var row in CollectionsMarshal.AsSpan(rows))
        {
            var cell = row.Values[column];
            if (cell.IsNullLike)
                builder.AppendNull();
            else if (cell.Value is long integer)
                builder.Append(integer);
            else if (long.TryParse(cell.ToText(), NumberStyles.Integer, CultureInfo.InvariantCulture, out integer))
                builder.Append(integer);
            else
                throw InvalidArrowValue(field, cell);
        }
        return builder.Build();
    }

    private static BooleanArray BuildBoolArrowArray(QueryArrowColumn field, List<QuerySelection> rows, int column)
    {
        var builder = new BooleanArray.Builder().Reserve(rows.Count);
        foreach (ref readonly var row in CollectionsMarshal.AsSpan(rows))
        {
            var cell = row.Values[column];
            if (cell.IsNullLike)
                builder.AppendNull();
            else if (cell.Value is bool boolean)
                builder.Append(boolean);
            else if (bool.TryParse(cell.ToText(), out boolean))
                builder.Append(boolean);
            else
                throw InvalidArrowValue(field, cell);
        }
        return builder.Build();
    }

    private static TimestampArray BuildTimestampArrowArray(QueryArrowColumn field, List<QuerySelection> rows, int column)
    {
        var type = new TimestampType(TimeUnit.Millisecond, (string?)null);
        var builder = new TimestampArray.Builder(type).Reserve(rows.Count);
        foreach (ref readonly var row in CollectionsMarshal.AsSpan(rows))
        {
            var cell = row.Values[column];
            if (cell.IsNullLike)
                builder.AppendNull();
            else if (cell.Value is DateTimeOffset timestamp)
                builder.Append(timestamp);
            else if (DateTimeOffset.TryParse(
                         cell.ToText(),
                         CultureInfo.InvariantCulture,
                         DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                         out timestamp))
            {
                builder.Append(timestamp);
            }
            else
                throw InvalidArrowValue(field, cell);
        }
        return builder.Build();
    }

    private static StringArray BuildStringArrowArray(List<QuerySelection> rows, int column)
    {
        var builder = new StringArray.Builder().Reserve(rows.Count);
        foreach (ref readonly var row in CollectionsMarshal.AsSpan(rows))
        {
            var cell = row.Values[column];
            if (cell.IsNullLike)
                builder.AppendNull();
            else
                builder.Append(cell.ToText());
        }
        return builder.Build();
    }

    private static DoubleArray BuildDoubleArrowArray(QueryArrowColumn field, List<QuerySelection> rows, int column)
    {
        var builder = new DoubleArray.Builder().Reserve(rows.Count);
        foreach (ref readonly var row in CollectionsMarshal.AsSpan(rows))
        {
            var cell = row.Values[column];
            if (cell.IsNullLike)
                builder.AppendNull();
            else if (cell.Value is double floating)
                builder.Append(floating);
            else if (double.TryParse(cell.ToText(), NumberStyles.Float, CultureInfo.InvariantCulture, out floating))
                builder.Append(floating);
            else
                throw InvalidArrowValue(field, cell);
        }
        return builder.Build();
    }

    private static Decimal128Array BuildDecimalArrowArray(QueryArrowColumn field, List<QuerySelection> rows, int column)
    {
        var builder = new Decimal128Array.Builder(new Decimal128Type(field.Precision, field.Scale));
        builder.Reserve(rows.Count);
        foreach (ref readonly var row in CollectionsMarshal.AsSpan(rows))
        {
            var cell = row.Values[column];
            if (cell.IsNullLike)
                builder.AppendNull();
            else
                builder.Append(cell.ToText());
        }
        return builder.Build();
    }

    private static BlobQueryDataException InvalidArrowValue(QueryArrowColumn field, QueryCell cell) => new(
        "InvalidArrowType",
        $"The value '{cell.ToText()}' could not be represented by Arrow field '{field.Name}' as {ArrowTypeName(field.Kind)}.",
        0);

    private static string ArrowTypeName(BlobQueryArrowFieldKind kind) => kind switch
    {
        BlobQueryArrowFieldKind.Int64 => "int64",
        BlobQueryArrowFieldKind.Bool => "bool",
        BlobQueryArrowFieldKind.Timestamp => "timestamp[ms]",
        BlobQueryArrowFieldKind.String => "string",
        BlobQueryArrowFieldKind.Double => "double",
        BlobQueryArrowFieldKind.Decimal => "decimal",
        _ => throw new InvalidOperationException("Unknown Arrow field type.")
    };

    private static async IAsyncEnumerable<QueryRow> ReadParquetRowsAsync(
        Stream input,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (!input.CanSeek)
        {
            throw new BlobQueryDataException(
                "InvalidParquetFile",
                "The Parquet query input is not seekable.",
                0);
        }

        ParquetReader reader;
        try
        {
            reader = await ParquetReader.CreateAsync(
                input,
                leaveStreamOpen: true,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            throw InvalidParquetFile();
        }

        await using (reader.ConfigureAwait(false))
        {
            var fields = reader.Schema.GetDataFields();
            if (fields.Length == 0)
                throw InvalidParquetFile("The Parquet schema does not contain any data fields.");
            if (fields.Any(field => field.Path.Length != 1 || field.MaxRepetitionLevel != 0 || field.IsArray))
            {
                throw new BlobQueryDataException(
                    "UnsupportedParquetType",
                    "Nested and repeated Parquet fields are not supported by Query Blob Contents.",
                    0);
            }

            var names = fields.Select(field => field.Name).ToArray();
            for (var groupIndex = 0; groupIndex < reader.RowGroupCount; groupIndex++)
            {
                await foreach (var row in ReadParquetGroupAsync(reader, fields, names, groupIndex, cancellationToken)
                                   .ConfigureAwait(false))
                {
                    yield return row;
                }
            }
        }
    }

    private static async IAsyncEnumerable<QueryRow> ReadParquetGroupAsync(
        ParquetReader reader,
        Parquet.Schema.DataField[] fields,
        string[] names,
        int groupIndex,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var group = reader.OpenRowGroupReader(groupIndex);
        if (group.RowCount > int.MaxValue)
            throw InvalidParquetFile("A Parquet row group contains too many rows.");
        var rowCount = checked((int)group.RowCount);
        var columns = new object?[fields.Length][];
        for (var column = 0; column < fields.Length; column++)
        {
            columns[column] = await ReadParquetColumnAsync(
                group,
                fields[column],
                rowCount,
                cancellationToken).ConfigureAwait(false);
        }

        for (var row = 0; row < rowCount; row++)
        {
            var cells = new QueryCell[fields.Length];
            for (var column = 0; column < fields.Length; column++)
                cells[column] = new QueryCell(columns[column][row]);
            yield return new QueryRow(names, cells);
        }
    }

    private static async Task<object?[]> ReadParquetColumnAsync(
        ParquetRowGroupReader group,
        Parquet.Schema.DataField field,
        int rowCount,
        CancellationToken cancellationToken)
    {
        try
        {
            return await ReadParquetColumnCoreAsync(group, field, rowCount, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (BlobQueryDataException)
        {
            throw;
        }
        catch (Exception)
        {
            throw InvalidParquetFile();
        }
    }

    private static async Task<object?[]> ReadParquetColumnCoreAsync(
        ParquetRowGroupReader group,
        Parquet.Schema.DataField field,
        int rowCount,
        CancellationToken cancellationToken)
    {
        var type = field.ClrType;
        if (type == typeof(ReadOnlyMemory<char>))
        {
            var values = new string?[rowCount];
            await group.ReadAsync(field, values.AsMemory(), cancellationToken: cancellationToken).ConfigureAwait(false);
            return values;
        }
        if (type == typeof(ReadOnlyMemory<byte>))
        {
            var values = new byte[]?[rowCount];
            await group.ReadAsync(field, values.AsMemory(), cancellationToken: cancellationToken).ConfigureAwait(false);
            return values.Select(value => value is null ? null : Convert.ToBase64String(value)).ToArray();
        }
        var integral = await ReadParquetIntegralColumnAsync(group, field, rowCount, cancellationToken).ConfigureAwait(false);
        if (integral is not null)
            return integral;
        if (type == typeof(ulong))
            return await ReadParquetValueColumnAsync<ulong>(group, field, rowCount, cancellationToken).ConfigureAwait(false);
        if (type == typeof(float))
            return await ReadParquetValueColumnAsync<float>(group, field, rowCount, cancellationToken).ConfigureAwait(false);
        if (type == typeof(double))
            return await ReadParquetValueColumnAsync<double>(group, field, rowCount, cancellationToken).ConfigureAwait(false);
        if (type == typeof(decimal))
            return await ReadParquetValueColumnAsync<decimal>(group, field, rowCount, cancellationToken).ConfigureAwait(false);
        if (type == typeof(BigDecimal))
            return await ReadParquetValueColumnAsync<BigDecimal>(group, field, rowCount, cancellationToken).ConfigureAwait(false);
        if (type == typeof(System.Numerics.BigInteger))
            return await ReadParquetValueColumnAsync<BigInteger>(group, field, rowCount, cancellationToken).ConfigureAwait(false);
        if (type == typeof(DateTime))
            return await ReadParquetValueColumnAsync<DateTime>(group, field, rowCount, cancellationToken).ConfigureAwait(false);
        if (type == typeof(DateOnly))
            return await ReadParquetValueColumnAsync<DateOnly>(group, field, rowCount, cancellationToken).ConfigureAwait(false);
        if (type == typeof(Guid))
            return await ReadParquetValueColumnAsync<Guid>(group, field, rowCount, cancellationToken).ConfigureAwait(false);

        throw new BlobQueryDataException(
            "UnsupportedParquetType",
            $"Parquet field '{field.Name}' uses an unsupported data type.",
            0);
    }

    private static async Task<object?[]?> ReadParquetIntegralColumnAsync(
        ParquetRowGroupReader group,
        Parquet.Schema.DataField field,
        int rowCount,
        CancellationToken cancellationToken)
    {
        var type = field.ClrType;
        if (type == typeof(bool))
            return await ReadParquetValueColumnAsync<bool>(group, field, rowCount, cancellationToken).ConfigureAwait(false);
        if (type == typeof(byte))
            return await ReadParquetValueColumnAsync<byte>(group, field, rowCount, cancellationToken).ConfigureAwait(false);
        if (type == typeof(sbyte))
            return await ReadParquetValueColumnAsync<sbyte>(group, field, rowCount, cancellationToken).ConfigureAwait(false);
        if (type == typeof(short))
            return await ReadParquetValueColumnAsync<short>(group, field, rowCount, cancellationToken).ConfigureAwait(false);
        if (type == typeof(ushort))
            return await ReadParquetValueColumnAsync<ushort>(group, field, rowCount, cancellationToken).ConfigureAwait(false);
        if (type == typeof(int))
            return await ReadParquetValueColumnAsync<int>(group, field, rowCount, cancellationToken).ConfigureAwait(false);
        if (type == typeof(uint))
            return await ReadParquetValueColumnAsync<uint>(group, field, rowCount, cancellationToken).ConfigureAwait(false);
        if (type == typeof(long))
            return await ReadParquetValueColumnAsync<long>(group, field, rowCount, cancellationToken).ConfigureAwait(false);
        return null;
    }

    private static async Task<object?[]> ReadParquetValueColumnAsync<T>(
        ParquetRowGroupReader group,
        Parquet.Schema.DataField field,
        int rowCount,
        CancellationToken cancellationToken)
        where T : struct
    {
        var result = new object?[rowCount];
        if (field.IsNullable)
        {
            var values = new T?[rowCount];
            await group.ReadAsync(
                field,
                values.AsMemory(),
                cancellationToken: cancellationToken).ConfigureAwait(false);
            for (var index = 0; index < values.Length; index++)
                result[index] = values[index] is { } value ? NormalizeParquetValue(value) : null;
        }
        else
        {
            var values = new T[rowCount];
            await group.ReadAsync(
                field,
                values.AsMemory(),
                cancellationToken: cancellationToken).ConfigureAwait(false);
            for (var index = 0; index < values.Length; index++)
                result[index] = NormalizeParquetValue(values[index]);
        }
        return result;
    }

    private static object NormalizeParquetValue<T>(T value) where T : struct => value switch
    {
        byte number => (long)number,
        sbyte number => (long)number,
        short number => (long)number,
        ushort number => (long)number,
        int number => (long)number,
        uint number => (long)number,
        ulong number when number <= long.MaxValue => (long)number,
        ulong number => (decimal)number,
        float number => (double)number,
        DateTime dateTime => dateTime.Kind == DateTimeKind.Unspecified
            ? new DateTimeOffset(dateTime, TimeSpan.Zero)
            : new DateTimeOffset(dateTime).ToUniversalTime(),
        DateOnly date => date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        Guid guid => guid.ToString("D"),
        BigDecimal number => number.ToString(),
        System.Numerics.BigInteger number => number.ToString(CultureInfo.InvariantCulture),
        _ => value
    };

    private static BlobQueryDataException InvalidParquetFile(
        string message = "The Parquet query input is invalid or corrupt.") => new(
        "InvalidParquetFile",
        message,
        0);

    private static async IAsyncEnumerable<QuerySelection> ReadSplitSelectionsAsync(
        Stream input,
        BlobQueryTextFormat format,
        long targetBytes,
        string outputName,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var recordSeparator = Encoding.UTF8.GetBytes(format.RecordSeparator);
        var columnSeparator = Encoding.UTF8.GetBytes(format.ColumnSeparator);
        var quote = Encoding.UTF8.GetBytes(format.Quote.ToString());
        var escape = Encoding.UTF8.GetBytes(format.Escape.ToString());
        var doubledQuoteEscaping = format.Quote == format.Escape;
        using var reader = new BlobQueryByteReader(input);
        var recordBytes = await ReadSplitPreambleAsync(reader, cancellationToken).ConfigureAwait(false);
        var batchBytes = 0L;
        var inQuotes = false;
        var atFieldStart = true;

        while (await reader.HasDataAsync(cancellationToken).ConfigureAwait(false))
        {
            if (inQuotes)
            {
                (recordBytes, inQuotes) = await ConsumeQuotedSplitInputAsync(
                    reader,
                    quote,
                    escape,
                    doubledQuoteEscaping,
                    recordBytes,
                    cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (atFieldStart && await reader.TryConsumeAsync(quote, cancellationToken).ConfigureAwait(false))
            {
                recordBytes = checked(recordBytes + quote.Length);
                atFieldStart = false;
                inQuotes = true;
                continue;
            }
            if (await reader.TryConsumeAsync(recordSeparator, cancellationToken).ConfigureAwait(false))
            {
                recordBytes = checked(recordBytes + recordSeparator.Length);
                batchBytes = checked(batchBytes + recordBytes);
                recordBytes = 0;
                atFieldStart = true;
                if (batchBytes >= targetBytes)
                {
                    yield return new QuerySelection([outputName], [new QueryCell(batchBytes)]);
                    batchBytes = 0;
                }
                continue;
            }
            (recordBytes, atFieldStart) = await ConsumeSplitFieldByteAsync(
                reader,
                columnSeparator,
                recordBytes,
                cancellationToken).ConfigureAwait(false);
        }

        if (inQuotes)
            throw new BlobQueryDataException("UnclosedQuote", "A delimited query input field contains an unclosed quote.", 0);
        batchBytes = checked(batchBytes + recordBytes);
        if (batchBytes > 0)
            yield return new QuerySelection([outputName], [new QueryCell(batchBytes)]);
    }

    private static async Task<long> ReadSplitPreambleAsync(
        BlobQueryByteReader reader,
        CancellationToken cancellationToken) =>
        await reader.TryConsumeAsync(Utf8Preamble, cancellationToken).ConfigureAwait(false)
            ? Utf8Preamble.Length
            : 0;

    private static async Task<(long RecordBytes, bool InQuotes)> ConsumeQuotedSplitInputAsync(
        BlobQueryByteReader reader,
        byte[] quote,
        byte[] escape,
        bool doubledQuoteEscaping,
        long recordBytes,
        CancellationToken cancellationToken)
    {
        if (doubledQuoteEscaping && await reader.TryConsumeAsync(quote, cancellationToken).ConfigureAwait(false))
        {
            recordBytes = checked(recordBytes + quote.Length);
            if (await reader.TryConsumeAsync(quote, cancellationToken).ConfigureAwait(false))
                return (checked(recordBytes + quote.Length), true);
            return (recordBytes, false);
        }
        if (!doubledQuoteEscaping && await reader.TryConsumeAsync(escape, cancellationToken).ConfigureAwait(false))
        {
            recordBytes = checked(recordBytes + escape.Length);
            recordBytes = checked(recordBytes + await reader.ConsumeUtf8ScalarAsync(cancellationToken).ConfigureAwait(false));
            return (recordBytes, true);
        }
        if (!doubledQuoteEscaping && await reader.TryConsumeAsync(quote, cancellationToken).ConfigureAwait(false))
            return (checked(recordBytes + quote.Length), false);
        _ = await reader.ReadByteAsync(cancellationToken).ConfigureAwait(false);
        return (checked(recordBytes + 1), true);
    }

    private static async Task<(long RecordBytes, bool AtFieldStart)> ConsumeSplitFieldByteAsync(
        BlobQueryByteReader reader,
        byte[] columnSeparator,
        long recordBytes,
        CancellationToken cancellationToken)
    {
        if (await reader.TryConsumeAsync(columnSeparator, cancellationToken).ConfigureAwait(false))
            return (checked(recordBytes + columnSeparator.Length), true);
        _ = await reader.ReadByteAsync(cancellationToken).ConfigureAwait(false);
        return (checked(recordBytes + 1), false);
    }

    private static async IAsyncEnumerable<QueryRow> ReadRowsAsync(
        Stream input,
        BlobQueryTextFormat format,
        BlobQueryPlan plan,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (format.Kind == BlobQueryFormatKind.Parquet)
        {
            await foreach (var row in ReadParquetRowsAsync(input, cancellationToken).ConfigureAwait(false))
                yield return row;
            yield break;
        }

        using var reader = new StreamReader(
            input,
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true),
            detectEncodingFromByteOrderMarks: true,
            bufferSize: 64 * 1024,
            leaveOpen: true);

        if (format.Kind == BlobQueryFormatKind.Delimited)
        {
            await foreach (var row in ReadDelimitedQueryRowsAsync(reader, format, cancellationToken).ConfigureAwait(false))
                yield return row;
            yield break;
        }

        await foreach (var row in ReadJsonQueryRowsAsync(reader, format, plan, cancellationToken).ConfigureAwait(false))
            yield return row;
    }

    private static async IAsyncEnumerable<QueryRow> ReadDelimitedQueryRowsAsync(
        TextReader reader,
        BlobQueryTextFormat format,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        string[]? headers = null;
        var rowNumber = 0L;
        await foreach (var fields in ReadDelimitedRowsAsync(reader, format, cancellationToken).ConfigureAwait(false))
        {
            rowNumber++;
            if (headers is null && format.HasHeaders)
            {
                headers = fields.ToArray();
                EnsureUniqueHeaders(headers, rowNumber);
                continue;
            }

            var names = headers is null
                ? Enumerable.Range(1, fields.Count).Select(index => $"_{index}").ToArray()
                : headers;
            var cells = fields.Select(value => new QueryCell(value)).ToArray();
            yield return new QueryRow(names, cells);
        }
    }

    private static async IAsyncEnumerable<QueryRow> ReadJsonQueryRowsAsync(
        TextReader reader,
        BlobQueryTextFormat format,
        BlobQueryPlan plan,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var position = 0L;
        await foreach (var record in ReadRawRecordsAsync(reader, format.RecordSeparator, cancellationToken).ConfigureAwait(false))
        {
            if (string.IsNullOrWhiteSpace(record))
            {
                position += record.Length + format.RecordSeparator.Length;
                continue;
            }

            JsonDocument document;
            try
            {
                document = JsonDocument.Parse(record);
            }
            catch (JsonException exception)
            {
                throw new BlobQueryDataException(
                    "InvalidJson",
                    exception.Message,
                    position + (exception.BytePositionInLine ?? 0));
            }

            using (document)
            {
                var root = document.RootElement;
                if (!plan.HasJsonTablePath &&
                    root.ValueKind is not (JsonValueKind.Object or JsonValueKind.Array))
                {
                    throw new BlobQueryDataException(
                        "InvalidJsonType",
                        "Each JSON query input record must be an object or array.",
                        position);
                }
                foreach (var row in plan.ExpandJsonRows(root))
                    yield return row;
            }

            position += record.Length + format.RecordSeparator.Length;
        }
    }

    private static async IAsyncEnumerable<IReadOnlyList<string>> ReadDelimitedRowsAsync(
        TextReader reader,
        BlobQueryTextFormat format,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<char>.Shared.Rent(64 * 1024);
        var state = new DelimitedReaderState(format);

        try
        {
            while (true)
            {
                var read = await reader.ReadAsync(buffer.AsMemory(), cancellationToken).ConfigureAwait(false);
                if (read == 0)
                    break;

                // The async iterator must index only the freshly read prefix; span enumeration cannot cross yield return.
#pragma warning disable HLQ013
                for (var index = 0; index < read; index++)
                {
                    var completed = state.Consume(buffer[index]);
                    if (completed is not null)
                        yield return completed;
                }
#pragma warning restore HLQ013
            }

            var final = state.Complete();
            if (final is not null)
                yield return final;
        }
        finally
        {
            ArrayPool<char>.Shared.Return(buffer, clearArray: true);
        }
    }

    private sealed class DelimitedReaderState(BlobQueryTextFormat format)
    {
        private readonly StringBuilder _field = new();
        private readonly List<string> _fields = [];
        private bool _inQuotes;
        private bool _quotePending;
        private bool _escapePending;
        private int _recordCharacters;

        public string[]? Consume(char character)
        {
            _recordCharacters++;
            if (_recordCharacters > MaximumRecordCharacters)
                throw new BlobQueryDataException("RecordTooLarge", "A query input record exceeds 16 MiB.", 0);

            if (_escapePending)
            {
                _field.Append(character);
                _escapePending = false;
                return null;
            }

            if (_quotePending)
            {
                if (character == format.Quote)
                {
                    _field.Append(character);
                    _quotePending = false;
                    return null;
                }
                _inQuotes = false;
                _quotePending = false;
            }

            return _inQuotes ? ConsumeQuoted(character) : ConsumeUnquoted(character);
        }

        private string[]? ConsumeQuoted(char character)
        {
            if (character == format.Quote)
                _quotePending = true;
            else if (format.Escape != format.Quote && character == format.Escape)
                _escapePending = true;
            else
                _field.Append(character);
            return null;
        }

        private string[]? ConsumeUnquoted(char character)
        {
            if (character == format.Quote && _field.Length == 0)
            {
                _inQuotes = true;
                return null;
            }

            _field.Append(character);
            if (EndsWith(_field, format.RecordSeparator))
            {
                _field.Length -= format.RecordSeparator.Length;
                if (string.Equals(format.RecordSeparator, "\n", StringComparison.Ordinal) && _field.Length > 0 && _field[^1] == '\r')
                    _field.Length--;
                _fields.Add(_field.ToString());
                _field.Clear();
                var completed = _fields.ToArray();
                _fields.Clear();
                _recordCharacters = 0;
                return completed;
            }
            if (EndsWith(_field, format.ColumnSeparator))
            {
                _field.Length -= format.ColumnSeparator.Length;
                _fields.Add(_field.ToString());
                _field.Clear();
            }
            return null;
        }

        public string[]? Complete()
        {
            if (_inQuotes && !_quotePending)
                throw new BlobQueryDataException("UnclosedQuote", "A delimited query input field contains an unclosed quote.", 0);
            if (_escapePending)
                _field.Append(format.Escape);
            if (_field.Length == 0 && _fields.Count == 0)
                return null;
            _fields.Add(_field.ToString());
            return _fields.ToArray();
        }
    }

    private static async IAsyncEnumerable<string> ReadRawRecordsAsync(
        TextReader reader,
        string separator,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<char>.Shared.Rent(64 * 1024);
        var record = new StringBuilder();
        try
        {
            while (true)
            {
                var read = await reader.ReadAsync(buffer.AsMemory(), cancellationToken).ConfigureAwait(false);
                if (read == 0)
                    break;
                // The async iterator must index only the freshly read prefix; span enumeration cannot cross yield return.
#pragma warning disable HLQ013
                for (var index = 0; index < read; index++)
                {
                    record.Append(buffer[index]);
                    if (record.Length > MaximumRecordCharacters)
                        throw new BlobQueryDataException("RecordTooLarge", "A query input record exceeds 16 MiB.", 0);
                    if (!EndsWith(record, separator))
                        continue;
                    record.Length -= separator.Length;
                    if (string.Equals(separator, "\n", StringComparison.Ordinal) && record.Length > 0 && record[^1] == '\r')
                        record.Length--;
                    yield return record.ToString();
                    record.Clear();
                }
#pragma warning restore HLQ013
            }
            if (record.Length > 0)
                yield return record.ToString();
        }
        finally
        {
            ArrayPool<char>.Shared.Return(buffer, clearArray: true);
        }
    }

    private static byte[] EncodeDelimited(IReadOnlyList<QueryCell> cells, BlobQueryTextFormat format)
    {
        var builder = new StringBuilder();
        for (var index = 0; index < cells.Count; index++)
        {
            if (index > 0)
                builder.Append(format.ColumnSeparator);
            var value = cells[index].ToText();
            var quote = value.Contains(format.ColumnSeparator, StringComparison.Ordinal) ||
                        value.Contains(format.RecordSeparator, StringComparison.Ordinal) ||
                        value.Contains(format.Quote, StringComparison.Ordinal) ||
                        value.Contains('\r', StringComparison.Ordinal) ||
                        value.Contains('\n', StringComparison.Ordinal);
            if (!quote)
            {
                builder.Append(value);
                continue;
            }
            builder.Append(format.Quote);
            foreach (var character in value)
            {
                if (character == format.Quote)
                {
                    if (format.Escape == format.Quote)
                        builder.Append(format.Quote);
                    else
                        builder.Append(format.Escape);
                }
                builder.Append(character);
            }
            builder.Append(format.Quote);
        }
        builder.Append(format.RecordSeparator);
        return Encoding.UTF8.GetBytes(builder.ToString());
    }

    private static byte[] EncodeJson(QuerySelection selection, BlobQueryTextFormat format)
    {
        using var buffer = new MemoryStream();
        using (var json = new Utf8JsonWriter(buffer))
        {
            json.WriteStartObject();
            for (var index = 0; index < selection.Values.Count; index++)
                selection.Values[index].WriteJson(json, selection.Names[index]);
            json.WriteEndObject();
        }
        buffer.Write(Encoding.UTF8.GetBytes(format.RecordSeparator));
        return buffer.ToArray();
    }

    private static bool EndsWith(StringBuilder builder, string value)
    {
        if (builder.Length < value.Length)
            return false;
        var offset = builder.Length - value.Length;
        for (var index = 0; index < value.Length; index++)
        {
            if (builder[offset + index] != value[index])
                return false;
        }
        return true;
    }

    private static void EnsureUniqueHeaders(IReadOnlyList<string> headers, long row)
    {
        var unique = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (headers.Any(string.IsNullOrWhiteSpace) || headers.Any(header => !unique.Add(header)))
            throw new BlobQueryDataException("InvalidHeaders", "Delimited input headers must be nonempty and unique.", row);
    }

    private static char ParseCharacter(string? value, char fallback, string name)
    {
        if (value is null)
            return fallback;
        if (value.Length != 1)
            throw InvalidXml($"{name} must contain exactly one character.");
        return value[0];
    }

    private static bool ParseBoolean(string? value, bool fallback, string name) => value?.ToRequiredLowerInvariant() switch
    {
        null or "" => fallback,
        "true" => true,
        "false" => false,
        _ => throw InvalidXml($"{name} must be true or false.")
    };

    private static void ValidateSeparator(string value, string name)
    {
        if (value.Length is < 1 or > 16)
            throw InvalidXml($"{name} must contain between one and sixteen characters.");
    }

    private static XElement? Child(XElement? parent, string name) =>
        parent?.Elements().FirstOrDefault(element => string.Equals(element.Name.LocalName, name, StringComparison.Ordinal));

    private static string? ChildValue(XElement? parent, string name) => Child(parent, name)?.Value;

    private static AzureStorageException InvalidXml(string detail) => new(
        StatusCodes.Status400BadRequest,
        "InvalidXmlDocument",
        $"The query request XML is invalid. {detail}");

    private sealed class BlobQueryByteReader(Stream input) : IDisposable
    {
        private readonly byte[] _buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        private int _start;
        private int _end;
        private bool _endOfStream;

        public ValueTask<bool> HasDataAsync(CancellationToken cancellationToken) =>
            EnsureAsync(1, cancellationToken);

        public ValueTask<bool> TryConsumeAsync(byte[] value, CancellationToken cancellationToken)
        {
            if (_end - _start >= value.Length)
                return ValueTask.FromResult(TryConsumeBuffered(value));
            return TryConsumeSlowAsync(value, cancellationToken);
        }

        public ValueTask<int> ReadByteAsync(CancellationToken cancellationToken)
        {
            if (_start < _end)
                return ValueTask.FromResult((int)_buffer[_start++]);
            return ReadByteSlowAsync(cancellationToken);
        }

        public async ValueTask<int> ConsumeUtf8ScalarAsync(CancellationToken cancellationToken)
        {
            var first = await ReadByteAsync(cancellationToken).ConfigureAwait(false);
            if (first < 0)
                return 0;
            var length = first switch
            {
                < 0x80 => 1,
                >= 0xC2 and <= 0xDF => 2,
                >= 0xE0 and <= 0xEF => 3,
                >= 0xF0 and <= 0xF4 => 4,
                _ => 1
            };
            var consumed = 1;
            while (consumed < length && await ReadByteAsync(cancellationToken).ConfigureAwait(false) >= 0)
                consumed++;
            return consumed;
        }

        public void Dispose()
        {
            ArrayPool<byte>.Shared.Return(_buffer, clearArray: true);
        }

        private bool TryConsumeBuffered(byte[] value)
        {
            if (!_buffer.AsSpan(_start, value.Length).SequenceEqual(value))
                return false;
            _start += value.Length;
            return true;
        }

        private async ValueTask<bool> TryConsumeSlowAsync(
            byte[] value,
            CancellationToken cancellationToken) =>
            await EnsureAsync(value.Length, cancellationToken).ConfigureAwait(false) && TryConsumeBuffered(value);

        private async ValueTask<int> ReadByteSlowAsync(CancellationToken cancellationToken) =>
            await EnsureAsync(1, cancellationToken).ConfigureAwait(false) ? _buffer[_start++] : -1;

        private ValueTask<bool> EnsureAsync(int count, CancellationToken cancellationToken)
        {
            if (_end - _start >= count)
                return ValueTask.FromResult(true);
            return FillAsync(count, cancellationToken);
        }

        private async ValueTask<bool> FillAsync(int count, CancellationToken cancellationToken)
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThan(count, _buffer.Length);
            if (_start > 0)
            {
                _buffer.AsSpan(_start, _end - _start).CopyTo(_buffer);
                _end -= _start;
                _start = 0;
            }
            while (_end < count && !_endOfStream)
            {
                var read = await input.ReadAsync(_buffer.AsMemory(_end), cancellationToken).ConfigureAwait(false);
                if (read == 0)
                {
                    _endOfStream = true;
                    break;
                }
                _end += read;
            }
            return _end >= count;
        }
    }
}
