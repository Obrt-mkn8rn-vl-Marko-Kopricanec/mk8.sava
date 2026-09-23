using System.Buffers;
using System.Globalization;
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

internal enum BlobQueryFormatKind
{
    Delimited,
    Json,
    Arrow,
    Parquet
}

internal sealed record BlobQueryTextFormat(
    BlobQueryFormatKind Kind,
    string ColumnSeparator,
    char Quote,
    string RecordSeparator,
    char Escape,
    bool HasHeaders,
    IReadOnlyList<QueryArrowColumn> ArrowSchema);

internal enum BlobQueryArrowFieldKind
{
    Int64,
    Bool,
    Timestamp,
    String,
    Double,
    Decimal
}

internal sealed record QueryArrowColumn(
    BlobQueryArrowFieldKind Kind,
    string Name,
    int Precision,
    int Scale);

internal sealed record BlobQueryRequest(
    string Expression,
    BlobQueryTextFormat Input,
    BlobQueryTextFormat Output);

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
        var avro = new BlobQueryAvroWriter(response);
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
        var type = format is null ? "delimited" : ChildValue(format, "Type")?.ToLowerInvariant();
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
        var type = ChildValue(field, "Type")?.ToLowerInvariant();
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
        IReadOnlyList<QuerySelection> rows,
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
        IReadOnlyList<QuerySelection> rows,
        int column)
    {
        try
        {
            switch (field.Kind)
            {
                case BlobQueryArrowFieldKind.Int64:
                    {
                        var builder = new Int64Array.Builder().Reserve(rows.Count);
                        foreach (var row in rows)
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
                case BlobQueryArrowFieldKind.Bool:
                    {
                        var builder = new BooleanArray.Builder().Reserve(rows.Count);
                        foreach (var row in rows)
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
                case BlobQueryArrowFieldKind.Timestamp:
                    {
                        var type = new TimestampType(TimeUnit.Millisecond, (string?)null);
                        var builder = new TimestampArray.Builder(type).Reserve(rows.Count);
                        foreach (var row in rows)
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
                case BlobQueryArrowFieldKind.String:
                    {
                        var builder = new StringArray.Builder().Reserve(rows.Count);
                        foreach (var row in rows)
                        {
                            var cell = row.Values[column];
                            if (cell.IsNullLike)
                                builder.AppendNull();
                            else
                                builder.Append(cell.ToText());
                        }
                        return builder.Build();
                    }
                case BlobQueryArrowFieldKind.Double:
                    {
                        var builder = new DoubleArray.Builder().Reserve(rows.Count);
                        foreach (var row in rows)
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
                case BlobQueryArrowFieldKind.Decimal:
                    {
                        var builder = new Decimal128Array.Builder(
                            new Decimal128Type(field.Precision, field.Scale));
                        builder.Reserve(rows.Count);
                        foreach (var row in rows)
                        {
                            var cell = row.Values[column];
                            if (cell.IsNullLike)
                                builder.AppendNull();
                            else
                                builder.Append(cell.ToText());
                        }
                        return builder.Build();
                    }
                default:
                    throw new InvalidOperationException("Unknown Arrow field type.");
            }
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
        var recordBytes = 0L;
        var batchBytes = 0L;
        var inQuotes = false;
        var atFieldStart = true;

        if (await reader.TryConsumeAsync(Utf8Preamble, cancellationToken).ConfigureAwait(false))
            recordBytes += Utf8Preamble.Length;

        while (await reader.HasDataAsync(cancellationToken).ConfigureAwait(false))
        {
            if (inQuotes)
            {
                if (doubledQuoteEscaping && await reader.TryConsumeAsync(quote, cancellationToken).ConfigureAwait(false))
                {
                    recordBytes = checked(recordBytes + quote.Length);
                    if (await reader.TryConsumeAsync(quote, cancellationToken).ConfigureAwait(false))
                    {
                        recordBytes = checked(recordBytes + quote.Length);
                        continue;
                    }
                    inQuotes = false;
                    continue;
                }
                if (!doubledQuoteEscaping && await reader.TryConsumeAsync(escape, cancellationToken).ConfigureAwait(false))
                {
                    recordBytes = checked(recordBytes + escape.Length);
                    recordBytes = checked(recordBytes + await reader.ConsumeUtf8ScalarAsync(cancellationToken).ConfigureAwait(false));
                    continue;
                }
                if (!doubledQuoteEscaping && await reader.TryConsumeAsync(quote, cancellationToken).ConfigureAwait(false))
                {
                    recordBytes = checked(recordBytes + quote.Length);
                    inQuotes = false;
                    continue;
                }
                _ = await reader.ReadByteAsync(cancellationToken).ConfigureAwait(false);
                recordBytes = checked(recordBytes + 1);
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
            if (await reader.TryConsumeAsync(columnSeparator, cancellationToken).ConfigureAwait(false))
            {
                recordBytes = checked(recordBytes + columnSeparator.Length);
                atFieldStart = true;
                continue;
            }

            _ = await reader.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            recordBytes = checked(recordBytes + 1);
            atFieldStart = false;
        }

        if (inQuotes)
            throw new BlobQueryDataException("UnclosedQuote", "A delimited query input field contains an unclosed quote.", 0);
        batchBytes = checked(batchBytes + recordBytes);
        if (batchBytes > 0)
            yield return new QuerySelection([outputName], [new QueryCell(batchBytes)]);
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
            yield break;
        }

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
        var field = new StringBuilder();
        var fields = new List<string>();
        var inQuotes = false;
        var quotePending = false;
        var escapePending = false;
        var recordCharacters = 0;

        try
        {
            while (true)
            {
                var read = await reader.ReadAsync(buffer.AsMemory(), cancellationToken).ConfigureAwait(false);
                if (read == 0)
                    break;

                for (var index = 0; index < read; index++)
                {
                    var character = buffer[index];
                    recordCharacters++;
                    if (recordCharacters > MaximumRecordCharacters)
                        throw new BlobQueryDataException("RecordTooLarge", "A query input record exceeds 16 MiB.", 0);

                    if (escapePending)
                    {
                        field.Append(character);
                        escapePending = false;
                        continue;
                    }

                    if (quotePending)
                    {
                        if (character == format.Quote)
                        {
                            field.Append(character);
                            quotePending = false;
                            continue;
                        }
                        inQuotes = false;
                        quotePending = false;
                    }

                    if (inQuotes)
                    {
                        if (character == format.Quote)
                        {
                            quotePending = true;
                            continue;
                        }
                        if (format.Escape != format.Quote && character == format.Escape)
                        {
                            escapePending = true;
                            continue;
                        }
                        field.Append(character);
                        continue;
                    }

                    if (character == format.Quote && field.Length == 0)
                    {
                        inQuotes = true;
                        continue;
                    }

                    field.Append(character);
                    if (EndsWith(field, format.RecordSeparator))
                    {
                        field.Length -= format.RecordSeparator.Length;
                        if (string.Equals(format.RecordSeparator, "\n", StringComparison.Ordinal) && field.Length > 0 && field[^1] == '\r')
                            field.Length--;
                        fields.Add(field.ToString());
                        field.Clear();
                        yield return fields.ToArray();
                        fields.Clear();
                        recordCharacters = 0;
                    }
                    else if (EndsWith(field, format.ColumnSeparator))
                    {
                        field.Length -= format.ColumnSeparator.Length;
                        fields.Add(field.ToString());
                        field.Clear();
                    }
                }
            }

            if (inQuotes && !quotePending)
                throw new BlobQueryDataException("UnclosedQuote", "A delimited query input field contains an unclosed quote.", 0);
            if (escapePending)
                field.Append(format.Escape);
            if (field.Length > 0 || fields.Count > 0)
            {
                fields.Add(field.ToString());
                yield return fields.ToArray();
            }
        }
        finally
        {
            ArrayPool<char>.Shared.Return(buffer, clearArray: true);
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

    private static bool ParseBoolean(string? value, bool fallback, string name) => value?.ToLowerInvariant() switch
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
            if (count > _buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(count));
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

internal sealed class BlobQueryAvroDataStream(
    BlobQueryAvroWriter writer,
    CancellationToken requestCancellationToken) : Stream
{
    private long _position;

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => _position;
    public override long Position
    {
        get => _position;
        set => throw new NotSupportedException();
    }

    public override void Flush()
    {
    }

    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public override void Write(byte[] buffer, int offset, int count) =>
        Write(buffer.AsSpan(offset, count));

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        writer.AppendDataAsync(buffer.ToArray(), requestCancellationToken).GetAwaiter().GetResult();
        _position += buffer.Length;
    }

    public override async Task WriteAsync(
        byte[] buffer,
        int offset,
        int count,
        CancellationToken cancellationToken)
    {
        await WriteAsync(buffer.AsMemory(offset, count), cancellationToken).ConfigureAwait(false);
    }

    public override async ValueTask WriteAsync(
        ReadOnlyMemory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        if (!cancellationToken.CanBeCanceled || cancellationToken == requestCancellationToken)
        {
            await writer.AppendDataAsync(buffer, requestCancellationToken).ConfigureAwait(false);
            _position += buffer.Length;
            return;
        }

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            requestCancellationToken,
            cancellationToken);
        await writer.AppendDataAsync(buffer, linked.Token).ConfigureAwait(false);
        _position += buffer.Length;
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
}

internal sealed class BlobQueryAvroWriter(Stream destination)
{
    private const int DataBlockSize = 64 * 1024;
    private const string Schema = "[" +
        "{\"type\":\"record\",\"name\":\"com.microsoft.azure.storage.queryBlobContents.resultData\",\"fields\":[{\"name\":\"data\",\"type\":\"bytes\"}]}," +
        "{\"type\":\"record\",\"name\":\"com.microsoft.azure.storage.queryBlobContents.error\",\"fields\":[{\"name\":\"fatal\",\"type\":\"boolean\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"description\",\"type\":\"string\"},{\"name\":\"position\",\"type\":\"long\"}]}," +
        "{\"type\":\"record\",\"name\":\"com.microsoft.azure.storage.queryBlobContents.progress\",\"fields\":[{\"name\":\"bytesScanned\",\"type\":\"long\"},{\"name\":\"totalBytes\",\"type\":\"long\"}]}," +
        "{\"type\":\"record\",\"name\":\"com.microsoft.azure.storage.queryBlobContents.end\",\"fields\":[{\"name\":\"totalBytes\",\"type\":\"long\"}]}]";

    private readonly byte[] _syncMarker = RandomNumberGenerator.GetBytes(16);
    private readonly MemoryStream _data = new(DataBlockSize);
    private bool _initialized;
    private bool _completed;

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        if (_initialized)
            throw new InvalidOperationException("The Avro writer is already initialized.");
        _initialized = true;

        using var header = new MemoryStream();
        header.Write("Obj\x01"u8);
        WriteLong(header, 2);
        WriteString(header, "avro.schema");
        WriteBytes(header, Encoding.UTF8.GetBytes(Schema));
        WriteString(header, "avro.codec");
        WriteBytes(header, "null"u8);
        WriteLong(header, 0);
        header.Write(_syncMarker);
        await destination.WriteAsync(header.GetBuffer().AsMemory(0, checked((int)header.Length)), cancellationToken).ConfigureAwait(false);
    }

    public async Task AppendDataAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        EnsureWritable();
        if (_data.Length > 0 && _data.Length + data.Length > DataBlockSize)
            await FlushDataAsync(cancellationToken).ConfigureAwait(false);
        if (data.Length >= DataBlockSize)
        {
            await WriteRecordAsync(0, payload => WriteBytes(payload, data.Span), cancellationToken).ConfigureAwait(false);
            return;
        }
        await _data.WriteAsync(data, cancellationToken).ConfigureAwait(false);
    }

    public async Task WriteErrorAsync(
        bool fatal,
        string name,
        string description,
        long position,
        CancellationToken cancellationToken)
    {
        EnsureWritable();
        await FlushDataAsync(cancellationToken).ConfigureAwait(false);
        await WriteRecordAsync(1, payload =>
        {
            payload.WriteByte(fatal ? (byte)1 : (byte)0);
            WriteString(payload, name);
            WriteString(payload, description);
            WriteLong(payload, position);
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task CompleteAsync(long totalBytes, CancellationToken cancellationToken)
    {
        EnsureWritable();
        await FlushDataAsync(cancellationToken).ConfigureAwait(false);
        await WriteRecordAsync(2, payload =>
        {
            WriteLong(payload, totalBytes);
            WriteLong(payload, totalBytes);
        }, cancellationToken).ConfigureAwait(false);
        await WriteRecordAsync(3, payload => WriteLong(payload, totalBytes), cancellationToken).ConfigureAwait(false);
        await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
        _completed = true;
    }

    private async Task FlushDataAsync(CancellationToken cancellationToken)
    {
        if (_data.Length == 0)
            return;
        var length = checked((int)_data.Length);
        await WriteRecordAsync(0, payload => WriteBytes(payload, _data.GetBuffer().AsSpan(0, length)), cancellationToken).ConfigureAwait(false);
        _data.SetLength(0);
    }

    private async Task WriteRecordAsync(
        long branch,
        Action<MemoryStream> encode,
        CancellationToken cancellationToken)
    {
        using var payload = new MemoryStream();
        WriteLong(payload, branch);
        encode(payload);

        using var block = new MemoryStream();
        WriteLong(block, 1);
        WriteLong(block, payload.Length);
        payload.Position = 0;
        payload.CopyTo(block);
        block.Write(_syncMarker);
        await destination.WriteAsync(block.GetBuffer().AsMemory(0, checked((int)block.Length)), cancellationToken).ConfigureAwait(false);
    }

    private void EnsureWritable()
    {
        if (!_initialized || _completed)
            throw new InvalidOperationException("The Avro writer is not writable.");
    }

    private static void WriteBytes(Stream stream, ReadOnlySpan<byte> value)
    {
        WriteLong(stream, value.Length);
        stream.Write(value);
    }

    private static void WriteString(Stream stream, string value) => WriteBytes(stream, Encoding.UTF8.GetBytes(value));

    private static void WriteLong(Stream stream, long value)
    {
        var encoded = unchecked((ulong)((value << 1) ^ (value >> 63)));
        while ((encoded & ~0x7FUL) != 0)
        {
            stream.WriteByte((byte)((encoded & 0x7F) | 0x80));
            encoded >>= 7;
        }
        stream.WriteByte((byte)encoded);
    }
}

internal sealed class QueryMissing
{
    public static QueryMissing Value { get; } = new();

    private QueryMissing()
    {
    }
}

internal sealed record QueryCell(object? Value)
{
    public bool IsMissing => Value is QueryMissing;
    public bool IsNullLike => Value is null or QueryMissing;

    public static QueryCell FromJson(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined => new QueryCell((object?)null),
        JsonValueKind.String => new QueryCell(element.GetString()),
        JsonValueKind.True => new QueryCell(true),
        JsonValueKind.False => new QueryCell(false),
        JsonValueKind.Number when element.TryGetInt64(out var integer) => new QueryCell(integer),
        JsonValueKind.Number when element.TryGetDecimal(out var number) => new QueryCell(number),
        JsonValueKind.Object or JsonValueKind.Array => new QueryCell(element.Clone()),
        _ => new QueryCell(element.GetRawText())
    };

    public string ToText() => Value switch
    {
        null => string.Empty,
        bool boolean => boolean ? "true" : "false",
        DateTimeOffset timestamp => timestamp.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
        DateTime timestamp => timestamp.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
        JsonElement element => element.GetRawText(),
        QueryMissing => string.Empty,
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture) ?? string.Empty,
        _ => Value.ToString() ?? string.Empty
    };

    public void WriteJson(Utf8JsonWriter writer, string name)
    {
        switch (Value)
        {
            case null:
                writer.WriteNull(name);
                break;
            case bool boolean:
                writer.WriteBoolean(name, boolean);
                break;
            case long integer:
                writer.WriteNumber(name, integer);
                break;
            case decimal number:
                writer.WriteNumber(name, number);
                break;
            case double number:
                writer.WriteNumber(name, number);
                break;
            case DateTimeOffset timestamp:
                writer.WriteString(name, timestamp.ToUniversalTime());
                break;
            case DateTime timestamp:
                writer.WriteString(name, timestamp.ToUniversalTime());
                break;
            case JsonElement element:
                writer.WritePropertyName(name);
                element.WriteTo(writer);
                break;
            case QueryMissing:
                writer.WriteNull(name);
                break;
            default:
                writer.WriteString(name, ToText());
                break;
        }
    }
}

internal sealed record QueryRow(
    IReadOnlyList<string> Names,
    IReadOnlyList<QueryCell> Values,
    bool PreserveMissing = false)
{
    public QueryCell Resolve(string name, bool caseSensitive = false)
    {
        if (name.Length > 1 && name[0] == '_' &&
            int.TryParse(name.AsSpan(1), NumberStyles.None, CultureInfo.InvariantCulture, out var ordinal) &&
            ordinal >= 1 && ordinal <= Values.Count)
        {
            return Values[ordinal - 1];
        }

        for (var index = 0; index < Names.Count; index++)
        {
            if (string.Equals(
                    Names[index],
                    name,
                    caseSensitive ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase))
                return Values[index];
        }
        return new QueryCell(PreserveMissing ? QueryMissing.Value : null);
    }
}

internal sealed record QuerySelection(IReadOnlyList<string> Names, IReadOnlyList<QueryCell> Values);

internal sealed class BlobQueryPlan
{
    private readonly IReadOnlyList<QueryProjection> _projections;
    private readonly QueryPredicate? _predicate;
    private readonly long? _limit;
    private readonly QueryAggregateState? _aggregate;
    private readonly IReadOnlyList<QueryTableSegment> _tablePath;
    private readonly long? _splitSize;
    private readonly string? _splitName;
    private long _selectedRows;

    private BlobQueryPlan(
        IReadOnlyList<QueryProjection> projections,
        QueryPredicate? predicate,
        long? limit,
        QueryAggregateState? aggregate,
        IReadOnlyList<QueryTableSegment> tablePath,
        long? splitSize,
        string? splitName)
    {
        _projections = projections;
        _predicate = predicate;
        _limit = limit;
        _aggregate = aggregate;
        _tablePath = tablePath;
        _splitSize = splitSize;
        _splitName = splitName;
    }

    public static BlobQueryPlan Parse(string expression) => new QueryParser(expression).Parse();

    public bool LimitReached => _limit.HasValue && _selectedRows >= _limit.Value;
    public bool IsAggregate => _aggregate is not null;
    public bool HasJsonTablePath => _tablePath.Count > 0;
    public bool IsSplit => _splitSize.HasValue;
    public long SplitSize => _splitSize ?? throw new InvalidOperationException("The query is not a split query.");
    public string SplitName => _splitName ?? throw new InvalidOperationException("The query is not a split query.");

    public IEnumerable<QueryRow> ExpandJsonRows(JsonElement root)
    {
        IEnumerable<JsonElement> nodes = [root];
        foreach (var segment in _tablePath)
            nodes = ExpandJsonSegment(nodes, segment);
        foreach (var node in nodes)
            yield return CreateJsonRow(node);
    }

    public QuerySelection? Select(QueryRow row)
    {
        if (LimitReached)
            return null;
        if (_predicate is not null && !_predicate.Evaluate(row))
            return null;
        _selectedRows++;
        if (_projections is [{ Star: true }])
            return new QuerySelection(row.Names, row.Values);
        return new QuerySelection(
            _projections.Select(projection => projection.Name).ToArray(),
            _projections.Select(projection => projection.Expression!.Evaluate(row)).ToArray());
    }

    public void Accumulate(QueryRow row)
    {
        if (_aggregate is null)
            throw new InvalidOperationException("The query is not an aggregate query.");
        if (_predicate is null || _predicate.Evaluate(row))
            _aggregate.Accumulate(row);
    }

    public QuerySelection? CompleteAggregate()
    {
        if (_aggregate is null)
            throw new InvalidOperationException("The query is not an aggregate query.");
        if (_limit == 0)
            return null;
        _selectedRows = 1;
        return new QuerySelection([_aggregate.Name], [_aggregate.Complete()]);
    }

    private sealed record QueryProjection(bool Star, QueryExpression? Expression, string Name);

    private sealed record QueryTableSegment(
        string? Name,
        int? Index,
        bool Wildcard,
        bool CaseSensitive);

    private abstract record QueryExpression
    {
        public abstract QueryCell Evaluate(QueryRow row);
    }

    private sealed record QueryFieldSegment(string? Name, int? Index, bool CaseSensitive);

    private sealed record QueryOperand(
        IReadOnlyList<QueryFieldSegment>? Path,
        QueryCell? Literal) : QueryExpression
    {
        public override QueryCell Evaluate(QueryRow row)
        {
            if (Path is null)
                return Literal!;
            var first = Path[0];
            var value = row.Resolve(first.Name!, first.CaseSensitive);
            for (var index = 1; index < Path.Count; index++)
            {
                if (value.IsNullLike)
                    return value;
                if (value.Value is not JsonElement element)
                    return new QueryCell(QueryMissing.Value);
                var segment = Path[index];
                if (segment.Index is { } arrayIndex)
                {
                    if (element.ValueKind != JsonValueKind.Array ||
                        arrayIndex < 0 ||
                        arrayIndex >= element.GetArrayLength())
                    {
                        return new QueryCell(QueryMissing.Value);
                    }
                    value = QueryCell.FromJson(element[arrayIndex]);
                    continue;
                }
                if (element.ValueKind != JsonValueKind.Object ||
                    !TryGetJsonProperty(element, segment.Name!, segment.CaseSensitive, out var property))
                {
                    return new QueryCell(QueryMissing.Value);
                }
                value = QueryCell.FromJson(property);
            }
            return value;
        }
    }

    private sealed record QueryBinaryExpression(
        QueryExpression Left,
        QueryExpression Right,
        string Operator) : QueryExpression
    {
        public override QueryCell Evaluate(QueryRow row) =>
            EvaluateBinary(Left.Evaluate(row), Right.Evaluate(row), Operator);
    }

    private sealed record QueryUnaryExpression(
        QueryExpression Operand,
        bool Negate) : QueryExpression
    {
        public override QueryCell Evaluate(QueryRow row)
        {
            var value = Operand.Evaluate(row);
            if (value.IsNullLike || !Negate)
                return value;
            try
            {
                if (value.Value is long integer)
                    return new QueryCell(checked(-integer));
                if (TryDouble(value, out var number))
                    return new QueryCell(-number);
                throw InvalidType("A unary numeric operator requires a numeric value.");
            }
            catch (OverflowException)
            {
                throw InvalidType("The unary numeric result is outside the supported range.");
            }
        }
    }

    private sealed record QueryCastExpression(
        QueryExpression Operand,
        QueryValueType Type) : QueryExpression
    {
        public override QueryCell Evaluate(QueryRow row) => Cast(Operand.Evaluate(row), Type);
    }

    private sealed record QueryFunctionExpression(
        string Name,
        IReadOnlyList<QueryExpression> Arguments) : QueryExpression
    {
        public override QueryCell Evaluate(QueryRow row) => EvaluateFunction(Name, Arguments, row);
    }

    private sealed record QueryAggregateExpression(
        QueryAggregateKind Kind,
        QueryExpression? Operand,
        bool CountStar) : QueryExpression
    {
        public override QueryCell Evaluate(QueryRow row) =>
            throw new InvalidOperationException("Aggregate expressions are evaluated across rows.");
    }

    private sealed record QuerySplitExpression(long Size) : QueryExpression
    {
        public override QueryCell Evaluate(QueryRow row) =>
            throw new InvalidOperationException("Sys.Split is evaluated over raw input records.");
    }

    private abstract record QueryPredicate
    {
        public abstract bool Evaluate(QueryRow row);
    }

    private sealed record LogicalPredicate(QueryPredicate Left, QueryPredicate Right, bool And) : QueryPredicate
    {
        public override bool Evaluate(QueryRow row) => And
            ? Left.Evaluate(row) && Right.Evaluate(row)
            : Left.Evaluate(row) || Right.Evaluate(row);
    }

    private sealed record NotPredicate(QueryPredicate Inner) : QueryPredicate
    {
        public override bool Evaluate(QueryRow row) => !Inner.Evaluate(row);
    }

    private sealed record NullPredicate(QueryExpression Operand, bool Negated) : QueryPredicate
    {
        public override bool Evaluate(QueryRow row) => (Operand.Evaluate(row).Value is null) != Negated;
    }

    private sealed record MissingPredicate(QueryExpression Operand, bool Negated) : QueryPredicate
    {
        public override bool Evaluate(QueryRow row) => Operand.Evaluate(row).IsMissing != Negated;
    }

    private sealed record TruthPredicate(QueryExpression Operand) : QueryPredicate
    {
        public override bool Evaluate(QueryRow row) => Operand.Evaluate(row).Value switch
        {
            null => false,
            QueryMissing => false,
            bool boolean => boolean,
            string text => !string.IsNullOrEmpty(text),
            long integer => integer != 0,
            decimal number => number != 0,
            double number => number != 0,
            _ => true
        };
    }

    private sealed record ComparisonPredicate(
        QueryExpression Left,
        QueryExpression Right,
        string Operator) : QueryPredicate
    {
        public override bool Evaluate(QueryRow row)
        {
            var left = Left.Evaluate(row);
            var right = Right.Evaluate(row);
            if (left.IsNullLike || right.IsNullLike)
                return false;

            var comparison = CompareValues(left, right);
            return Operator switch
            {
                "=" or "==" => comparison == 0,
                "!=" or "<>" => comparison != 0,
                "<" => comparison < 0,
                "<=" => comparison <= 0,
                ">" => comparison > 0,
                ">=" => comparison >= 0,
                _ => false
            };
        }
    }

    private sealed record BetweenPredicate(
        QueryExpression Value,
        QueryExpression Lower,
        QueryExpression Upper,
        bool Negated) : QueryPredicate
    {
        public override bool Evaluate(QueryRow row)
        {
            var value = Value.Evaluate(row);
            var lower = Lower.Evaluate(row);
            var upper = Upper.Evaluate(row);
            if (value.IsNullLike || lower.IsNullLike || upper.IsNullLike)
                return false;
            var matches = CompareValues(value, lower) >= 0 && CompareValues(value, upper) <= 0;
            return matches != Negated;
        }
    }

    private sealed record InPredicate(
        QueryExpression Value,
        IReadOnlyList<QueryExpression> Candidates,
        bool Negated) : QueryPredicate
    {
        public override bool Evaluate(QueryRow row)
        {
            var value = Value.Evaluate(row);
            if (value.IsNullLike)
                return false;
            var matches = Candidates
                .Select(candidate => candidate.Evaluate(row))
                .Any(candidate => !candidate.IsNullLike && CompareValues(value, candidate) == 0);
            return matches != Negated;
        }
    }

    private enum QueryValueType
    {
        Int,
        Float,
        String,
        Timestamp,
        Boolean
    }

    private enum QueryAggregateKind
    {
        Count,
        Average,
        Minimum,
        Maximum,
        Sum
    }

    private sealed class QueryAggregateState(
        string name,
        QueryAggregateKind kind,
        QueryExpression? operand,
        bool countStar)
    {
        private long _count;
        private decimal _decimalSum;
        private double _floatingSum;
        private bool _usesFloatingPoint;
        private QueryCell? _extreme;

        public string Name { get; } = name;

        public void Accumulate(QueryRow row)
        {
            if (kind == QueryAggregateKind.Count && countStar)
            {
                _count = checked(_count + 1);
                return;
            }

            var value = operand!.Evaluate(row);
            if (value.IsNullLike)
                return;

            switch (kind)
            {
                case QueryAggregateKind.Count:
                    _count = checked(_count + 1);
                    break;
                case QueryAggregateKind.Average:
                case QueryAggregateKind.Sum:
                    AccumulateNumber(value);
                    _count = checked(_count + 1);
                    break;
                case QueryAggregateKind.Minimum:
                    if (_extreme is null || CompareValues(value, _extreme) < 0)
                        _extreme = value;
                    _count = checked(_count + 1);
                    break;
                case QueryAggregateKind.Maximum:
                    if (_extreme is null || CompareValues(value, _extreme) > 0)
                        _extreme = value;
                    _count = checked(_count + 1);
                    break;
                default:
                    throw new InvalidOperationException("Unknown aggregate type.");
            }
        }

        public QueryCell Complete() => kind switch
        {
            QueryAggregateKind.Count => new QueryCell(_count),
            QueryAggregateKind.Average when _count == 0 => new QueryCell(null),
            QueryAggregateKind.Average => new QueryCell(
                (_usesFloatingPoint ? _floatingSum : (double)_decimalSum) / _count),
            QueryAggregateKind.Sum when _count == 0 => new QueryCell(null),
            QueryAggregateKind.Sum when _usesFloatingPoint => new QueryCell(_floatingSum),
            QueryAggregateKind.Sum when _decimalSum == decimal.Truncate(_decimalSum) &&
                                        _decimalSum is >= long.MinValue and <= long.MaxValue =>
                new QueryCell((long)_decimalSum),
            QueryAggregateKind.Sum => new QueryCell(_decimalSum),
            QueryAggregateKind.Minimum or QueryAggregateKind.Maximum => _extreme ?? new QueryCell(null),
            _ => throw new InvalidOperationException("Unknown aggregate type.")
        };

        private void AccumulateNumber(QueryCell value)
        {
            try
            {
                if (value.Value is double)
                {
                    if (!TryDouble(value, out var floating))
                        throw InvalidType($"The aggregate value '{value.ToText()}' is not numeric.");
                    if (!_usesFloatingPoint)
                    {
                        _floatingSum = (double)_decimalSum;
                        _usesFloatingPoint = true;
                    }
                    _floatingSum += floating;
                    if (!double.IsFinite(_floatingSum))
                        throw InvalidType("The aggregate result is outside the supported numeric range.");
                    return;
                }

                if (_usesFloatingPoint)
                {
                    if (!TryDouble(value, out var floating))
                        throw InvalidType($"The aggregate value '{value.ToText()}' is not numeric.");
                    _floatingSum += floating;
                    if (!double.IsFinite(_floatingSum))
                        throw InvalidType("The aggregate result is outside the supported numeric range.");
                    return;
                }

                if (!TryDecimal(value, out var exact))
                    throw InvalidType($"The aggregate value '{value.ToText()}' is not numeric.");
                _decimalSum = checked(_decimalSum + exact);
            }
            catch (OverflowException)
            {
                throw InvalidType("The aggregate result is outside the supported numeric range.");
            }
        }
    }

    private static QueryCell EvaluateBinary(QueryCell left, QueryCell right, string operation)
    {
        if (left.IsNullLike || right.IsNullLike)
            return new QueryCell(null);

        try
        {
            if (string.Equals(operation, "+", StringComparison.Ordinal) && IsTimestamp(left) && TryTimestamp(left, out var leftTimestamp) && TryDouble(right, out var rightDays))
                return new QueryCell(leftTimestamp.AddDays(rightDays));
            if (string.Equals(operation, "+", StringComparison.Ordinal) && TryDouble(left, out var leftDays) && IsTimestamp(right) && TryTimestamp(right, out var rightTimestamp))
                return new QueryCell(rightTimestamp.AddDays(leftDays));
            if (string.Equals(operation, "-", StringComparison.Ordinal) && IsTimestamp(left) && TryTimestamp(left, out leftTimestamp) && TryDouble(right, out rightDays))
                return new QueryCell(leftTimestamp.AddDays(-rightDays));

            if (left.Value is long leftInteger && right.Value is long rightInteger && !string.Equals(operation, "/", StringComparison.Ordinal))
            {
                return operation switch
                {
                    "+" => new QueryCell(checked(leftInteger + rightInteger)),
                    "-" => new QueryCell(checked(leftInteger - rightInteger)),
                    "*" => new QueryCell(checked(leftInteger * rightInteger)),
                    "%" when rightInteger != 0 => new QueryCell(leftInteger % rightInteger),
                    "%" => throw InvalidType("The remainder divisor cannot be zero."),
                    _ => throw InvalidType($"The arithmetic operator '{operation}' is not supported.")
                };
            }

            if (!TryDouble(left, out var leftNumber) || !TryDouble(right, out var rightNumber))
                throw InvalidType($"The arithmetic operator '{operation}' requires numeric values.");
            var result = operation switch
            {
                "+" => leftNumber + rightNumber,
                "-" => leftNumber - rightNumber,
                "*" => leftNumber * rightNumber,
                "/" when rightNumber != 0 => leftNumber / rightNumber,
                "/" => throw InvalidType("The divisor cannot be zero."),
                "%" when rightNumber != 0 => leftNumber % rightNumber,
                "%" => throw InvalidType("The remainder divisor cannot be zero."),
                _ => throw InvalidType($"The arithmetic operator '{operation}' is not supported.")
            };
            if (!double.IsFinite(result))
                throw InvalidType("The arithmetic result is outside the supported numeric range.");
            return new QueryCell(result);
        }
        catch (OverflowException)
        {
            throw InvalidType("The arithmetic result is outside the supported numeric range.");
        }
    }

    private static QueryCell Cast(QueryCell value, QueryValueType type)
    {
        if (value.IsNullLike)
            return value;

        try
        {
            return type switch
            {
                QueryValueType.Int when value.Value is long integer => new QueryCell(integer),
                QueryValueType.Int when value.Value is bool boolean => new QueryCell(boolean ? 1L : 0L),
                QueryValueType.Int when TryDouble(value, out var number) => new QueryCell(checked((long)Math.Truncate(number))),
                QueryValueType.Float when TryDouble(value, out var number) => new QueryCell(number),
                QueryValueType.String => new QueryCell(value.ToText()),
                QueryValueType.Timestamp when TryTimestamp(value, out var timestamp) => new QueryCell(timestamp),
                QueryValueType.Boolean when value.Value is bool boolean => new QueryCell(boolean),
                QueryValueType.Boolean when bool.TryParse(value.ToText(), out var boolean) => new QueryCell(boolean),
                QueryValueType.Boolean when TryDouble(value, out var number) => new QueryCell(number != 0),
                _ => throw InvalidType($"The value '{value.ToText()}' cannot be cast to {type.ToString().ToUpperInvariant()}.")
            };
        }
        catch (OverflowException)
        {
            throw InvalidType($"The value '{value.ToText()}' is outside the range of {type.ToString().ToUpperInvariant()}.");
        }
    }

    private static QueryCell EvaluateFunction(
        string name,
        IReadOnlyList<QueryExpression> arguments,
        QueryRow row)
    {
        if (string.Equals(name, "COALESCE", StringComparison.Ordinal))
        {
            foreach (var argument in arguments)
            {
                var candidate = argument.Evaluate(row);
                if (!candidate.IsNullLike)
                    return candidate;
            }
            return new QueryCell(null);
        }

        if (string.Equals(name, "UTCNOW", StringComparison.Ordinal))
            return new QueryCell(DateTimeOffset.UtcNow);

        var first = arguments[0].Evaluate(row);
        if (string.Equals(name, "NULLIF", StringComparison.Ordinal))
        {
            var second = arguments[1].Evaluate(row);
            return !first.IsNullLike && !second.IsNullLike && CompareValues(first, second) == 0
                ? new QueryCell(null)
                : first;
        }
        if (first.IsNullLike)
            return first;

        return name switch
        {
            "CHAR_LENGTH" or "CHARACTER_LENGTH" =>
                new QueryCell((long)first.ToText().EnumerateRunes().Count()),
            "LOWER" => new QueryCell(first.ToText().ToLowerInvariant()),
            "UPPER" => new QueryCell(first.ToText().ToUpperInvariant()),
            "SUBSTRING" => EvaluateSubstring(first, arguments, row),
            "DATE_ADD" => EvaluateDateAdd(first, arguments, row),
            "DATE_DIFF" => EvaluateDateDiff(first, arguments, row),
            "EXTRACT" => EvaluateExtract(first, arguments, row),
            "TO_STRING" => EvaluateTimestampString(first, arguments, row),
            "TO_TIMESTAMP" => Cast(first, QueryValueType.Timestamp),
            "TRIM" => EvaluateTrim(first, arguments, row),
            _ => throw InvalidType($"The function '{name}' is not supported.")
        };
    }

    private static QueryCell EvaluateSubstring(
        QueryCell value,
        IReadOnlyList<QueryExpression> arguments,
        QueryRow row)
    {
        var characters = value.ToText().EnumerateRunes().ToArray();
        var start = ToNonNegativeInt(arguments[1].Evaluate(row), "SUBSTRING start");
        if (start >= characters.Length)
            return new QueryCell(string.Empty);
        var length = arguments.Count == 2
            ? characters.Length - start
            : Math.Min(
                ToNonNegativeInt(arguments[2].Evaluate(row), "SUBSTRING length"),
                characters.Length - start);
        var result = new StringBuilder(length);
        for (var index = start; index < start + length; index++)
            result.Append(characters[index]);
        return new QueryCell(result.ToString());
    }

    private static QueryCell EvaluateDateAdd(
        QueryCell part,
        IReadOnlyList<QueryExpression> arguments,
        QueryRow row)
    {
        var quantity = Cast(arguments[1].Evaluate(row), QueryValueType.Int);
        var timestamp = Cast(arguments[2].Evaluate(row), QueryValueType.Timestamp);
        if (quantity.IsNullLike || timestamp.IsNullLike)
            return new QueryCell(null);
        var amount = (long)quantity.Value!;
        var value = (DateTimeOffset)timestamp.Value!;
        try
        {
            value = NormalizeDatePart(part.ToText()) switch
            {
                "year" => value.AddYears(checked((int)amount)),
                "month" => value.AddMonths(checked((int)amount)),
                "day" => value.AddDays(amount),
                "hour" => value.AddHours(amount),
                "minute" => value.AddMinutes(amount),
                "second" => value.AddSeconds(amount),
                _ => throw InvalidDatePart(part)
            };
            return new QueryCell(value);
        }
        catch (ArgumentOutOfRangeException)
        {
            throw InvalidType("The DATE_ADD result is outside the supported timestamp range.");
        }
        catch (OverflowException)
        {
            throw InvalidType("The DATE_ADD quantity is outside the supported range.");
        }
    }

    private static QueryCell EvaluateDateDiff(
        QueryCell part,
        IReadOnlyList<QueryExpression> arguments,
        QueryRow row)
    {
        var start = Cast(arguments[1].Evaluate(row), QueryValueType.Timestamp);
        var end = Cast(arguments[2].Evaluate(row), QueryValueType.Timestamp);
        if (start.IsNullLike || end.IsNullLike)
            return new QueryCell(null);
        var startTimestamp = ((DateTimeOffset)start.Value!).ToUniversalTime();
        var endTimestamp = ((DateTimeOffset)end.Value!).ToUniversalTime();
        var difference = NormalizeDatePart(part.ToText()) switch
        {
            "year" => endTimestamp.Year - startTimestamp.Year,
            "month" => checked((endTimestamp.Year - startTimestamp.Year) * 12L + endTimestamp.Month - startTimestamp.Month),
            "day" => BoundaryDifference(startTimestamp, endTimestamp, TimeSpan.TicksPerDay),
            "hour" => BoundaryDifference(startTimestamp, endTimestamp, TimeSpan.TicksPerHour),
            "minute" => BoundaryDifference(startTimestamp, endTimestamp, TimeSpan.TicksPerMinute),
            "second" => BoundaryDifference(startTimestamp, endTimestamp, TimeSpan.TicksPerSecond),
            _ => throw InvalidDatePart(part)
        };
        return new QueryCell(difference);
    }

    private static QueryCell EvaluateExtract(
        QueryCell part,
        IReadOnlyList<QueryExpression> arguments,
        QueryRow row)
    {
        var timestamp = Cast(arguments[1].Evaluate(row), QueryValueType.Timestamp);
        if (timestamp.IsNullLike)
            return new QueryCell(null);
        var value = (DateTimeOffset)timestamp.Value!;
        var extracted = NormalizeDatePart(part.ToText()) switch
        {
            "year" => value.Year,
            "month" => value.Month,
            "day" => value.Day,
            "hour" => value.Hour,
            "minute" => value.Minute,
            "second" => value.Second,
            "timezone_hour" => value.Offset.Hours,
            "timezone_minute" => value.Offset.Minutes,
            _ => throw InvalidDatePart(part)
        };
        return new QueryCell((long)extracted);
    }

    private static QueryCell EvaluateTimestampString(
        QueryCell timestamp,
        IReadOnlyList<QueryExpression> arguments,
        QueryRow row)
    {
        var converted = Cast(timestamp, QueryValueType.Timestamp);
        var format = arguments[1].Evaluate(row);
        if (converted.IsNullLike || format.IsNullLike)
            return new QueryCell(null);
        return new QueryCell(FormatTimestamp((DateTimeOffset)converted.Value!, format.ToText()));
    }

    private static QueryCell EvaluateTrim(
        QueryCell first,
        IReadOnlyList<QueryExpression> arguments,
        QueryRow row)
    {
        if (arguments.Count == 1)
            return new QueryCell(first.ToText().Trim());

        var characters = arguments[1].Evaluate(row);
        var value = arguments[2].Evaluate(row);
        if (characters.IsNullLike || value.IsNullLike)
            return new QueryCell(null);
        var trimCharacters = characters.ToText().ToCharArray();
        var text = value.ToText();
        var trimmed = first.ToText().ToUpperInvariant() switch
        {
            "BOTH" => text.Trim(trimCharacters),
            "LEADING" => text.TrimStart(trimCharacters),
            "TRAILING" => text.TrimEnd(trimCharacters),
            _ => throw InvalidType($"The TRIM mode '{first.ToText()}' is invalid.")
        };
        return new QueryCell(trimmed);
    }

    private static long BoundaryDifference(
        DateTimeOffset start,
        DateTimeOffset end,
        long unitTicks) =>
        end.UtcTicks / unitTicks - start.UtcTicks / unitTicks;

    private static string NormalizeDatePart(string part)
    {
        var normalized = part.Trim().ToLowerInvariant();
        return normalized.EndsWith('s') ? normalized[..^1] : normalized;
    }

    private static BlobQueryDataException InvalidDatePart(QueryCell part) =>
        InvalidType($"The date part '{part.ToText()}' is not supported.");

    private static string FormatTimestamp(DateTimeOffset timestamp, string format)
    {
        var result = new StringBuilder(format.Length + 8);
        for (var index = 0; index < format.Length;)
        {
            var character = format[index];
            if (character == '\'')
            {
                index++;
                while (index < format.Length && format[index] != '\'')
                    result.Append(format[index++]);
                if (index >= format.Length)
                    throw InvalidType("The TO_STRING format contains an unterminated quoted literal.");
                index++;
                continue;
            }

            var count = 1;
            while (index + count < format.Length && format[index + count] == character)
                count++;
            var token = format.Substring(index, count);
            result.Append(character switch
            {
                'y' when count == 2 => timestamp.ToString("yy", CultureInfo.InvariantCulture),
                'y' when count is 1 or 4 => timestamp.ToString("yyyy", CultureInfo.InvariantCulture),
                'M' when count == 1 => timestamp.Month.ToString(CultureInfo.InvariantCulture),
                'M' when count == 2 => timestamp.ToString("MM", CultureInfo.InvariantCulture),
                'M' when count == 3 => timestamp.ToString("MMM", CultureInfo.InvariantCulture),
                'M' when count == 4 => timestamp.ToString("MMMM", CultureInfo.InvariantCulture),
                'd' when count == 1 => timestamp.Day.ToString(CultureInfo.InvariantCulture),
                'd' when count == 2 => timestamp.ToString("dd", CultureInfo.InvariantCulture),
                'a' when count == 1 => timestamp.ToString("tt", CultureInfo.InvariantCulture),
                'h' when count == 1 => (timestamp.Hour % 12 is 0 ? 12 : timestamp.Hour % 12).ToString(CultureInfo.InvariantCulture),
                'h' when count == 2 => timestamp.ToString("hh", CultureInfo.InvariantCulture),
                'H' when count == 1 => timestamp.Hour.ToString(CultureInfo.InvariantCulture),
                'H' when count == 2 => timestamp.ToString("HH", CultureInfo.InvariantCulture),
                'm' when count == 1 => timestamp.Minute.ToString(CultureInfo.InvariantCulture),
                'm' when count == 2 => timestamp.ToString("mm", CultureInfo.InvariantCulture),
                's' when count == 1 => timestamp.Second.ToString(CultureInfo.InvariantCulture),
                's' when count == 2 => timestamp.ToString("ss", CultureInfo.InvariantCulture),
                'S' when count is >= 1 and <= 3 => timestamp.Millisecond
                    .ToString("D3", CultureInfo.InvariantCulture)[..count],
                'X' when count is >= 1 and <= 5 => FormatOffset(timestamp.Offset, count, useZulu: true),
                'x' when count is >= 1 and <= 5 => FormatOffset(timestamp.Offset, count, useZulu: false),
                _ when char.IsLetter(character) =>
                    throw InvalidType($"The TO_STRING format token '{token}' is not supported."),
                _ => token
            });
            index += count;
        }
        return result.ToString();
    }

    private static string FormatOffset(TimeSpan offset, int count, bool useZulu)
    {
        if (offset == TimeSpan.Zero && useZulu)
            return "Z";
        var sign = offset < TimeSpan.Zero ? "-" : "+";
        offset = offset.Duration();
        return count switch
        {
            1 => $"{sign}{offset.Hours:00}",
            2 or 4 => $"{sign}{offset.Hours:00}{offset.Minutes:00}",
            3 or 5 => $"{sign}{offset.Hours:00}:{offset.Minutes:00}",
            _ => throw InvalidType("The TO_STRING offset format is invalid.")
        };
    }

    private static int ToNonNegativeInt(QueryCell value, string description)
    {
        var converted = Cast(value, QueryValueType.Int);
        if (converted.Value is not long integer || integer is < 0 or > int.MaxValue)
            throw InvalidType($"{description} must be a non-negative integer.");
        return (int)integer;
    }

    private static int CompareValues(QueryCell left, QueryCell right)
    {
        if (TryDecimal(left, out var leftDecimal) && TryDecimal(right, out var rightDecimal))
            return leftDecimal.CompareTo(rightDecimal);
        if (TryTimestamp(left, out var leftTimestamp) && TryTimestamp(right, out var rightTimestamp))
            return leftTimestamp.CompareTo(rightTimestamp);
        if (left.Value is bool leftBoolean && right.Value is bool rightBoolean)
            return leftBoolean.CompareTo(rightBoolean);
        return string.Compare(left.ToText(), right.ToText(), StringComparison.Ordinal);
    }

    private static bool TryGetJsonProperty(
        JsonElement element,
        string name,
        bool caseSensitive,
        out JsonElement value)
    {
        if (caseSensitive)
            return element.TryGetProperty(name, out value);
        foreach (var property in element.EnumerateObject())
        {
            if (!string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase))
                continue;
            value = property.Value;
            return true;
        }
        value = default;
        return false;
    }

    private static IEnumerable<JsonElement> ExpandJsonSegment(
        IEnumerable<JsonElement> nodes,
        QueryTableSegment segment)
    {
        foreach (var node in nodes)
        {
            if (segment.Wildcard)
            {
                if (node.ValueKind != JsonValueKind.Array)
                    continue;
                foreach (var item in node.EnumerateArray())
                    yield return item;
                continue;
            }
            if (segment.Index is { } index)
            {
                if (node.ValueKind == JsonValueKind.Array && index >= 0 && index < node.GetArrayLength())
                    yield return node[index];
                continue;
            }
            if (node.ValueKind == JsonValueKind.Object &&
                TryGetJsonProperty(node, segment.Name!, segment.CaseSensitive, out var property))
            {
                yield return property;
            }
        }
    }

    private static QueryRow CreateJsonRow(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            var properties = element.EnumerateObject().ToArray();
            return new QueryRow(
                properties.Select(property => property.Name).ToArray(),
                properties.Select(property => QueryCell.FromJson(property.Value)).ToArray(),
                PreserveMissing: true);
        }
        if (element.ValueKind == JsonValueKind.Array)
        {
            var values = element.EnumerateArray().Select(QueryCell.FromJson).ToArray();
            return new QueryRow(
                Enumerable.Range(1, values.Length).Select(index => $"_{index}").ToArray(),
                values,
                PreserveMissing: true);
        }
        return new QueryRow(["_1"], [QueryCell.FromJson(element)], PreserveMissing: true);
    }

    private static bool TryDecimal(QueryCell value, out decimal number)
    {
        switch (value.Value)
        {
            case long integer:
                number = integer;
                return true;
            case decimal exact:
                number = exact;
                return true;
            case double floating when double.IsFinite(floating):
                try
                {
                    number = (decimal)floating;
                    return true;
                }
                catch (OverflowException)
                {
                    break;
                }
            case string text when decimal.TryParse(
                text,
                NumberStyles.Number | NumberStyles.AllowExponent,
                CultureInfo.InvariantCulture,
                out number):
                return true;
        }
        number = 0;
        return false;
    }

    private static bool TryDouble(QueryCell value, out double number)
    {
        switch (value.Value)
        {
            case long integer:
                number = integer;
                return true;
            case decimal exact:
                number = (double)exact;
                return double.IsFinite(number);
            case double floating when double.IsFinite(floating):
                number = floating;
                return true;
            default:
                return double.TryParse(
                           value.ToText(),
                           NumberStyles.Float,
                           CultureInfo.InvariantCulture,
                           out number) &&
                       double.IsFinite(number);
        }
    }

    private static bool TryTimestamp(QueryCell value, out DateTimeOffset timestamp)
    {
        switch (value.Value)
        {
            case DateTimeOffset offset:
                timestamp = offset;
                return true;
            case DateTime dateTime:
                timestamp = dateTime.Kind == DateTimeKind.Unspecified
                    ? new DateTimeOffset(dateTime, TimeSpan.Zero)
                    : new DateTimeOffset(dateTime);
                return true;
            default:
                return DateTimeOffset.TryParse(
                    value.ToText(),
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.AssumeUniversal,
                    out timestamp);
        }
    }

    private static bool IsTimestamp(QueryCell value) => value.Value is DateTimeOffset or DateTime;

    private static BlobQueryDataException InvalidType(string message) => new(
        "InvalidType",
        message,
        0);

    private enum QueryTokenKind
    {
        Identifier,
        String,
        Number,
        Symbol,
        End
    }

    private readonly record struct QueryToken(
        QueryTokenKind Kind,
        string Text,
        int Position,
        bool Quoted = false);

    private sealed class QueryParser
    {
        private readonly IReadOnlyList<QueryToken> _tokens;
        private readonly string? _sourceAlias;
        private int _position;

        public QueryParser(string expression)
        {
            _tokens = Tokenize(expression);
            _sourceAlias = FindSourceAlias(_tokens);
        }

        public BlobQueryPlan Parse()
        {
            ExpectKeyword("SELECT");
            var projections = ParseProjections();
            ExpectKeyword("FROM");
            var source = Expect(QueryTokenKind.Identifier, "BlobStorage");
            if (!string.Equals(source.Text, "BlobStorage", StringComparison.OrdinalIgnoreCase))
                throw InvalidQuery(source.Position, "Queries must read from BlobStorage.");
            var tablePath = ParseTablePath(source.Position);

            if (MatchKeyword("AS"))
                _ = Expect(QueryTokenKind.Identifier, "alias");
            else if (Current.Kind == QueryTokenKind.Identifier && !IsClauseKeyword(Current))
                _position++;

            QueryPredicate? predicate = null;
            if (MatchKeyword("WHERE"))
                predicate = ParseOr();
            long? limit = null;
            if (MatchKeyword("LIMIT"))
            {
                var token = Expect(QueryTokenKind.Number, "LIMIT value");
                if (!long.TryParse(token.Text, NumberStyles.None, CultureInfo.InvariantCulture, out var parsedLimit))
                    throw InvalidQuery(token.Position, "LIMIT must be a non-negative 64-bit integer.");
                limit = parsedLimit;
            }
            _ = MatchSymbol(";");
            if (Current.Kind != QueryTokenKind.End)
                throw InvalidQuery(Current.Position, $"Unexpected token '{Current.Text}'.");

            QueryAggregateState? aggregate = null;
            if (projections.Any(projection => projection.Expression is QueryAggregateExpression))
            {
                if (projections.Count != 1 ||
                    projections[0] is not { Star: false, Expression: QueryAggregateExpression expression } projection)
                {
                    throw InvalidQuery(
                        Current.Position,
                        "An aggregate query must select exactly one aggregate expression.");
                }
                aggregate = new QueryAggregateState(
                    projection.Name,
                    expression.Kind,
                    expression.Operand,
                    expression.CountStar);
                projections = [];
            }

            long? splitSize = null;
            string? splitName = null;
            if (projections.Any(projection => projection.Expression is QuerySplitExpression))
            {
                if (projections.Count != 1 ||
                    projections[0] is not { Star: false, Expression: QuerySplitExpression split } projection ||
                    predicate is not null || limit.HasValue || tablePath.Count > 0)
                {
                    throw InvalidQuery(
                        Current.Position,
                        "Sys.Split must be the only projection and cannot use WHERE, LIMIT, or a table path.");
                }
                splitSize = split.Size;
                splitName = projection.Name;
                projections = [];
            }
            return new BlobQueryPlan(
                projections,
                predicate,
                limit,
                aggregate,
                tablePath,
                splitSize,
                splitName);
        }

        private IReadOnlyList<QueryTableSegment> ParseTablePath(int sourcePosition)
        {
            var rootWildcard = false;
            if (MatchSymbol("["))
            {
                if (!MatchSymbol("*"))
                    throw InvalidQuery(Current.Position, "BlobStorage accepts only [*] at the root.");
                ExpectSymbol("]");
                rootWildcard = true;
            }

            var path = new List<QueryTableSegment>();
            while (MatchSymbol("."))
            {
                var property = Expect(QueryTokenKind.Identifier, "JSON table path field");
                path.Add(new QueryTableSegment(property.Text, null, false, property.Quoted));
                while (MatchSymbol("["))
                {
                    if (MatchSymbol("*"))
                    {
                        path.Add(new QueryTableSegment(null, null, true, false));
                    }
                    else
                    {
                        var indexToken = Expect(QueryTokenKind.Number, "JSON table path index");
                        if (!int.TryParse(indexToken.Text, NumberStyles.None, CultureInfo.InvariantCulture, out var index))
                            throw InvalidQuery(indexToken.Position, "A JSON table path index must be a non-negative integer.");
                        path.Add(new QueryTableSegment(null, index, false, false));
                    }
                    ExpectSymbol("]");
                }
            }

            if (path.Count > 0 && !rootWildcard)
            {
                throw InvalidQuery(
                    sourcePosition,
                    "A nested JSON table path must begin with BlobStorage[*].");
            }
            return path;
        }

        private IReadOnlyList<QueryProjection> ParseProjections()
        {
            var projections = new List<QueryProjection>();
            do
            {
                if (MatchSymbol("*"))
                {
                    projections.Add(new QueryProjection(true, null, "*"));
                    continue;
                }
                var expression = ParseExpression();
                var defaultName = $"_{projections.Count + 1}";
                var name = MatchKeyword("AS")
                    ? Expect(QueryTokenKind.Identifier, "alias").Text
                    : Current.Kind == QueryTokenKind.Identifier && !IsClauseKeyword(Current)
                        ? _tokens[_position++].Text
                        : defaultName;
                projections.Add(new QueryProjection(false, expression, name));
            }
            while (MatchSymbol(","));

            if (projections.Count == 0 || projections.Count > 49 ||
                projections.Any(projection => projection.Star) && projections.Count != 1)
            {
                throw InvalidQuery(Current.Position, "The SELECT projection is invalid.");
            }
            return projections;
        }

        private QueryPredicate ParseOr()
        {
            var left = ParseAnd();
            while (MatchKeyword("OR"))
                left = new LogicalPredicate(left, ParseAnd(), And: false);
            return left;
        }

        private QueryPredicate ParseAnd()
        {
            var left = ParseNot();
            while (MatchKeyword("AND"))
                left = new LogicalPredicate(left, ParseNot(), And: true);
            return left;
        }

        private QueryPredicate ParseNot()
        {
            if (MatchKeyword("NOT"))
                return new NotPredicate(ParseNot());
            if (MatchSymbol("("))
            {
                var nested = ParseOr();
                ExpectSymbol(")");
                return nested;
            }

            var left = ParseExpression();
            if (MatchKeyword("IS"))
            {
                var negated = MatchKeyword("NOT");
                if (MatchKeyword("NULL"))
                    return new NullPredicate(left, negated);
                if (MatchKeyword("MISSING"))
                    return new MissingPredicate(left, negated);
                throw InvalidQuery(Current.Position, "Expected NULL or MISSING after IS.");
            }
            var negatedSet = MatchKeyword("NOT");
            if (MatchKeyword("BETWEEN"))
            {
                var lower = ParseExpression();
                ExpectKeyword("AND");
                var upper = ParseExpression();
                return new BetweenPredicate(left, lower, upper, negatedSet);
            }
            if (MatchKeyword("IN"))
            {
                ExpectSymbol("(");
                var candidates = new List<QueryExpression>();
                do
                {
                    candidates.Add(ParseExpression());
                }
                while (MatchSymbol(","));
                ExpectSymbol(")");
                if (candidates.Count == 0)
                    throw InvalidQuery(Current.Position, "IN requires at least one value.");
                return new InPredicate(left, candidates, negatedSet);
            }
            if (negatedSet)
                throw InvalidQuery(Current.Position, "Expected BETWEEN or IN after NOT.");
            if (Current.Kind == QueryTokenKind.Symbol && Current.Text is "=" or "==" or "!=" or "<>" or "<" or "<=" or ">" or ">=")
            {
                var operation = _tokens[_position++].Text;
                return new ComparisonPredicate(left, ParseExpression(), operation);
            }
            return new TruthPredicate(left);
        }

        private QueryExpression ParseExpression() => ParseAdditive();

        private QueryExpression ParseAdditive()
        {
            var expression = ParseMultiplicative();
            while (Current.Kind == QueryTokenKind.Symbol && Current.Text is "+" or "-")
            {
                var operation = _tokens[_position++].Text;
                expression = new QueryBinaryExpression(expression, ParseMultiplicative(), operation);
            }
            return expression;
        }

        private QueryExpression ParseMultiplicative()
        {
            var expression = ParseUnary();
            while (Current.Kind == QueryTokenKind.Symbol && Current.Text is "*" or "/" or "%")
            {
                var operation = _tokens[_position++].Text;
                expression = new QueryBinaryExpression(expression, ParseUnary(), operation);
            }
            return expression;
        }

        private QueryExpression ParseUnary()
        {
            if (MatchSymbol("+"))
                return new QueryUnaryExpression(ParseUnary(), Negate: false);
            if (MatchSymbol("-"))
                return new QueryUnaryExpression(ParseUnary(), Negate: true);
            return ParsePrimary();
        }

        private QueryExpression ParsePrimary()
        {
            var token = Current;
            _position++;
            switch (token.Kind)
            {
                case QueryTokenKind.Identifier:
                    if (string.Equals(token.Text, "NULL", StringComparison.OrdinalIgnoreCase))
                        return new QueryOperand(null, new QueryCell(null));
                    if (string.Equals(token.Text, "TRUE", StringComparison.OrdinalIgnoreCase))
                        return new QueryOperand(null, new QueryCell(true));
                    if (string.Equals(token.Text, "FALSE", StringComparison.OrdinalIgnoreCase))
                        return new QueryOperand(null, new QueryCell(false));
                    if (!token.Quoted &&
                        string.Equals(token.Text, "sys", StringComparison.OrdinalIgnoreCase) &&
                        IsSysSplitStart())
                    {
                        _position += 3;
                        var sizeToken = Expect(QueryTokenKind.Number, "Sys.Split size");
                        if (!long.TryParse(sizeToken.Text, NumberStyles.None, CultureInfo.InvariantCulture, out var size) ||
                            size < 10L * 1024 * 1024)
                        {
                            throw InvalidQuery(
                                sizeToken.Position,
                                "The Sys.Split size must be an integer of at least 10485760 bytes.");
                        }
                        ExpectSymbol(")");
                        return new QuerySplitExpression(size);
                    }
                    if (!token.Quoted &&
                        string.Equals(token.Text, "CAST", StringComparison.OrdinalIgnoreCase) &&
                        MatchSymbol("("))
                    {
                        var operand = ParseExpression();
                        ExpectKeyword("AS");
                        var type = ParseValueType(Expect(QueryTokenKind.Identifier, "CAST type"));
                        ExpectSymbol(")");
                        return new QueryCastExpression(operand, type);
                    }
                    if (!token.Quoted && MatchSymbol("("))
                    {
                        var function = token.Text.ToUpperInvariant();
                        if (function is "COUNT" or "AVG" or "MIN" or "MAX" or "SUM")
                            return ParseAggregateExpression(token, function);
                        var arguments = function switch
                        {
                            "DATE_ADD" or "DATE_DIFF" => ParseDateFunctionArguments(),
                            "EXTRACT" => ParseExtractArguments(),
                            "TRIM" => ParseTrimArguments(),
                            _ => ParseFunctionArguments()
                        };
                        ValidateFunction(token, arguments.Count);
                        return new QueryFunctionExpression(function, arguments);
                    }
                    var path = new List<QueryFieldSegment>
                    {
                        new(token.Text, null, token.Quoted)
                    };
                    while (true)
                    {
                        if (MatchSymbol("."))
                        {
                            var fieldToken = Expect(QueryTokenKind.Identifier, "field name");
                            path.Add(new QueryFieldSegment(fieldToken.Text, null, fieldToken.Quoted));
                            continue;
                        }
                        if (MatchSymbol("["))
                        {
                            var indexToken = Expect(QueryTokenKind.Number, "array index");
                            if (!int.TryParse(indexToken.Text, NumberStyles.None, CultureInfo.InvariantCulture, out var arrayIndex))
                                throw InvalidQuery(indexToken.Position, "A JSON array index must be a non-negative integer.");
                            ExpectSymbol("]");
                            path.Add(new QueryFieldSegment(null, arrayIndex, false));
                            continue;
                        }
                        break;
                    }
                    if (path.Count > 1 &&
                        (string.Equals(path[0].Name, "BlobStorage", StringComparison.OrdinalIgnoreCase) ||
                         _sourceAlias is not null &&
                         string.Equals(path[0].Name, _sourceAlias, StringComparison.OrdinalIgnoreCase)))
                    {
                        path.RemoveAt(0);
                    }
                    return new QueryOperand(path, null);
                case QueryTokenKind.String:
                    return new QueryOperand(null, new QueryCell(token.Text));
                case QueryTokenKind.Number:
                    if (long.TryParse(token.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var integer))
                        return new QueryOperand(null, new QueryCell(integer));
                    if (double.TryParse(token.Text, NumberStyles.Float, CultureInfo.InvariantCulture, out var number) && double.IsFinite(number))
                        return new QueryOperand(null, new QueryCell(number));
                    throw InvalidQuery(token.Position, $"The numeric literal '{token.Text}' is invalid.");
                case QueryTokenKind.Symbol when string.Equals(token.Text, "(", StringComparison.Ordinal):
                    var nested = ParseExpression();
                    ExpectSymbol(")");
                    return nested;
                default:
                    throw InvalidQuery(token.Position, $"Expected an expression, found '{token.Text}'.");
            }
        }

        private QueryExpression ParseAggregateExpression(QueryToken token, string function)
        {
            var countStar = string.Equals(function, "COUNT", StringComparison.Ordinal) && MatchSymbol("*");
            QueryExpression? operand = null;
            if (!countStar)
                operand = ParseExpression();
            if (MatchSymbol(","))
                throw InvalidQuery(token.Position, $"The aggregate '{function}' accepts one argument.");
            ExpectSymbol(")");
            var kind = function switch
            {
                "COUNT" => QueryAggregateKind.Count,
                "AVG" => QueryAggregateKind.Average,
                "MIN" => QueryAggregateKind.Minimum,
                "MAX" => QueryAggregateKind.Maximum,
                "SUM" => QueryAggregateKind.Sum,
                _ => throw new InvalidOperationException("Unknown aggregate function.")
            };
            return new QueryAggregateExpression(kind, operand, countStar);
        }

        private List<QueryExpression> ParseFunctionArguments()
        {
            var arguments = new List<QueryExpression>();
            if (!MatchSymbol(")"))
            {
                do
                {
                    arguments.Add(ParseExpression());
                }
                while (MatchSymbol(","));
                ExpectSymbol(")");
            }
            return arguments;
        }

        private List<QueryExpression> ParseDateFunctionArguments()
        {
            var part = ExpectDatePart();
            ExpectSymbol(",");
            var first = ParseExpression();
            ExpectSymbol(",");
            var second = ParseExpression();
            ExpectSymbol(")");
            return [Literal(part), first, second];
        }

        private List<QueryExpression> ParseExtractArguments()
        {
            var part = ExpectDatePart();
            ExpectKeyword("FROM");
            var timestamp = ParseExpression();
            ExpectSymbol(")");
            return [Literal(part), timestamp];
        }

        private List<QueryExpression> ParseTrimArguments()
        {
            if (MatchSymbol(")"))
                throw InvalidQuery(Current.Position, "TRIM requires a value.");

            var mode = "BOTH";
            if (Current.Kind == QueryTokenKind.Identifier &&
                Current.Text.ToUpperInvariant() is "BOTH" or "LEADING" or "TRAILING")
            {
                mode = Current.Text.ToUpperInvariant();
                _position++;
                QueryExpression characters = Literal(" ");
                if (!MatchKeyword("FROM"))
                {
                    characters = ParseExpression();
                    ExpectKeyword("FROM");
                }
                var value = ParseExpression();
                ExpectSymbol(")");
                return [Literal(mode), characters, value];
            }

            var first = ParseExpression();
            if (MatchKeyword("FROM"))
            {
                var value = ParseExpression();
                ExpectSymbol(")");
                return [Literal(mode), first, value];
            }

            ExpectSymbol(")");
            return [first];
        }

        private string ExpectDatePart()
        {
            var token = Current;
            if (token.Kind is not (QueryTokenKind.Identifier or QueryTokenKind.String))
                throw InvalidQuery(token.Position, "Expected a date part.");
            _position++;
            return token.Text;
        }

        private static QueryExpression Literal(object? value) =>
            new QueryOperand(null, new QueryCell(value));

        private QueryToken Current => _tokens[_position];

        private bool IsSysSplitStart() =>
            _position + 2 < _tokens.Count &&
            _tokens[_position].Kind == QueryTokenKind.Symbol &&
string.Equals(_tokens[_position].Text, ".", StringComparison.Ordinal) &&
            IsKeyword(_tokens[_position + 1], "split") &&
            _tokens[_position + 2].Kind == QueryTokenKind.Symbol &&
string.Equals(_tokens[_position + 2].Text, "(", StringComparison.Ordinal);

        private bool MatchKeyword(string value)
        {
            if (!IsKeyword(Current, value))
                return false;
            _position++;
            return true;
        }

        private void ExpectKeyword(string value)
        {
            if (!MatchKeyword(value))
                throw InvalidQuery(Current.Position, $"Expected {value}.");
        }

        private bool MatchSymbol(string value)
        {
            if (Current.Kind != QueryTokenKind.Symbol || !string.Equals(Current.Text, value, StringComparison.Ordinal))
                return false;
            _position++;
            return true;
        }

        private void ExpectSymbol(string value)
        {
            if (!MatchSymbol(value))
                throw InvalidQuery(Current.Position, $"Expected '{value}'.");
        }

        private QueryToken Expect(QueryTokenKind kind, string description)
        {
            var token = Current;
            if (token.Kind != kind)
                throw InvalidQuery(token.Position, $"Expected {description}.");
            _position++;
            return token;
        }

        private static QueryValueType ParseValueType(QueryToken token) => token.Text.ToUpperInvariant() switch
        {
            "INT" => QueryValueType.Int,
            "FLOAT" => QueryValueType.Float,
            "STRING" => QueryValueType.String,
            "TIMESTAMP" => QueryValueType.Timestamp,
            "BOOLEAN" => QueryValueType.Boolean,
            _ => throw InvalidQuery(token.Position, $"The CAST type '{token.Text}' is not supported.")
        };

        private static void ValidateFunction(QueryToken token, int argumentCount)
        {
            var expected = token.Text.ToUpperInvariant() switch
            {
                "CHAR_LENGTH" or "CHARACTER_LENGTH" or "LOWER" or "UPPER" => (Minimum: 1, Maximum: 1),
                "SUBSTRING" => (Minimum: 2, Maximum: 3),
                "NULLIF" => (Minimum: 2, Maximum: 2),
                "COALESCE" => (Minimum: 1, Maximum: int.MaxValue),
                "UTCNOW" => (Minimum: 0, Maximum: 0),
                "DATE_ADD" or "DATE_DIFF" => (Minimum: 3, Maximum: 3),
                "EXTRACT" or "TO_STRING" => (Minimum: 2, Maximum: 2),
                "TO_TIMESTAMP" => (Minimum: 1, Maximum: 1),
                "TRIM" => (Minimum: 1, Maximum: 3),
                _ => throw InvalidQuery(token.Position, $"The function '{token.Text}' is not supported.")
            };
            if (argumentCount < expected.Minimum || argumentCount > expected.Maximum)
            {
                throw InvalidQuery(
                    token.Position,
                    $"The function '{token.Text}' received an invalid number of arguments.");
            }
        }

        private static bool IsKeyword(QueryToken token, string value) =>
            token.Kind == QueryTokenKind.Identifier && !token.Quoted &&
            string.Equals(token.Text, value, StringComparison.OrdinalIgnoreCase);

        private static bool IsClauseKeyword(QueryToken token) =>
            IsKeyword(token, "FROM") || IsKeyword(token, "WHERE") || IsKeyword(token, "LIMIT");

        private static string? FindSourceAlias(IReadOnlyList<QueryToken> tokens)
        {
            var depth = 0;
            for (var index = 0; index < tokens.Count; index++)
            {
                if (tokens[index].Kind == QueryTokenKind.Symbol)
                {
                    if (string.Equals(tokens[index].Text, "(", StringComparison.Ordinal))
                        depth++;
                    else if (string.Equals(tokens[index].Text, ")", StringComparison.Ordinal))
                        depth--;
                    continue;
                }
                if (depth != 0 || !IsKeyword(tokens[index], "FROM"))
                    continue;

                index++;
                if (index >= tokens.Count ||
                    tokens[index].Kind != QueryTokenKind.Identifier ||
                    !string.Equals(tokens[index].Text, "BlobStorage", StringComparison.OrdinalIgnoreCase))
                {
                    return null;
                }
                index++;
                if (index < tokens.Count && tokens[index].Kind == QueryTokenKind.Symbol && string.Equals(tokens[index].Text, "[", StringComparison.Ordinal))
                {
                    index += 3;
                }
                while (index + 1 < tokens.Count &&
                       tokens[index].Kind == QueryTokenKind.Symbol && string.Equals(tokens[index].Text, ".", StringComparison.Ordinal))
                {
                    index += 2;
                    while (index < tokens.Count &&
                           tokens[index].Kind == QueryTokenKind.Symbol && string.Equals(tokens[index].Text, "[", StringComparison.Ordinal))
                    {
                        index += 3;
                    }
                }
                if (index < tokens.Count && IsKeyword(tokens[index], "AS"))
                    index++;
                if (index < tokens.Count &&
                    tokens[index].Kind == QueryTokenKind.Identifier &&
                    !IsClauseKeyword(tokens[index]))
                {
                    return tokens[index].Text;
                }
                return null;
            }
            return null;
        }

        private static IReadOnlyList<QueryToken> Tokenize(string expression)
        {
            var tokens = new List<QueryToken>();
            for (var index = 0; index < expression.Length;)
            {
                if (char.IsWhiteSpace(expression[index]))
                {
                    index++;
                    continue;
                }

                var start = index;
                var character = expression[index];
                if (character == '\'' || character == '"')
                {
                    var delimiter = character;
                    index++;
                    var value = new StringBuilder();
                    var closed = false;
                    while (index < expression.Length)
                    {
                        character = expression[index++];
                        if (character != delimiter)
                        {
                            value.Append(character);
                            continue;
                        }
                        if (index < expression.Length && expression[index] == delimiter)
                        {
                            value.Append(delimiter);
                            index++;
                            continue;
                        }
                        closed = true;
                        break;
                    }
                    if (!closed)
                        throw InvalidQuery(start, "A quoted value is not terminated.");
                    tokens.Add(new QueryToken(
                        delimiter == '\'' ? QueryTokenKind.String : QueryTokenKind.Identifier,
                        value.ToString(),
                        start,
                        Quoted: delimiter == '"'));
                    continue;
                }

                if (char.IsLetter(character) || character == '_')
                {
                    index++;
                    while (index < expression.Length && (char.IsLetterOrDigit(expression[index]) || expression[index] is '_' or '$'))
                        index++;
                    tokens.Add(new QueryToken(QueryTokenKind.Identifier, expression[start..index], start));
                    continue;
                }

                if (char.IsDigit(character))
                {
                    index++;
                    while (index < expression.Length && char.IsDigit(expression[index]))
                        index++;
                    if (index < expression.Length && expression[index] == '.')
                    {
                        index++;
                        while (index < expression.Length && char.IsDigit(expression[index]))
                            index++;
                    }
                    if (index < expression.Length && expression[index] is 'e' or 'E')
                    {
                        index++;
                        if (index < expression.Length && expression[index] is '+' or '-')
                            index++;
                        var exponentStart = index;
                        while (index < expression.Length && char.IsDigit(expression[index]))
                            index++;
                        if (index == exponentStart)
                            throw InvalidQuery(start, "A numeric exponent requires at least one digit.");
                    }
                    tokens.Add(new QueryToken(QueryTokenKind.Number, expression[start..index], start));
                    continue;
                }

                if (index + 1 < expression.Length && expression.Substring(index, 2) is "<=" or ">=" or "!=" or "<>" or "==")
                {
                    tokens.Add(new QueryToken(QueryTokenKind.Symbol, expression.Substring(index, 2), start));
                    index += 2;
                    continue;
                }
                if (character is '*' or '/' or '%' or '+' or '-' or ',' or '.' or '(' or ')' or '[' or ']' or ';' or '=' or '<' or '>')
                {
                    tokens.Add(new QueryToken(QueryTokenKind.Symbol, character.ToString(), start));
                    index++;
                    continue;
                }
                throw InvalidQuery(start, $"Unexpected character '{character}'.");
            }
            tokens.Add(new QueryToken(QueryTokenKind.End, string.Empty, expression.Length));
            return tokens;
        }

        private static AzureStorageException InvalidQuery(int position, string detail) => new(
            StatusCodes.Status400BadRequest,
            "InvalidQueryParameterValue",
            $"The SQL query is invalid at position {position}. {detail}");
    }
}

internal sealed class BlobQueryDataException(string name, string message, long position) : Exception(message)
{
    public string Name { get; } = name;
    public long Position { get; } = position;
}
