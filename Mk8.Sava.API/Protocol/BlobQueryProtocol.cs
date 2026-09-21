using System.Buffers;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Xml;
using System.Xml.Linq;

namespace Mk8.Sava.Protocol;

internal enum BlobQueryFormatKind
{
    Delimited,
    Json
}

internal sealed record BlobQueryTextFormat(
    BlobQueryFormatKind Kind,
    string ColumnSeparator,
    char Quote,
    string RecordSeparator,
    char Escape,
    bool HasHeaders);

internal sealed record BlobQueryRequest(
    string Expression,
    BlobQueryTextFormat Input,
    BlobQueryTextFormat Output);

internal static class BlobQueryProtocol
{
    private const int MaximumExpressionBytes = 256 * 1024;
    private const int MaximumRecordCharacters = 16 * 1024 * 1024;

    public static async Task<BlobQueryRequest> ReadRequestAsync(
        Stream body,
        CancellationToken cancellationToken)
    {
        try
        {
            using var reader = ProtocolParsing.CreateXmlReader(body);
            var document = await XDocument.LoadAsync(reader, LoadOptions.None, cancellationToken);
            var root = document.Root;
            if (root?.Name.LocalName != "QueryRequest")
                throw InvalidXml("The QueryRequest root element is required.");

            var queryType = ChildValue(root, "QueryType");
            if (!string.Equals(queryType, "SQL", StringComparison.OrdinalIgnoreCase))
                throw InvalidXml("Only the SQL query type is supported.");

            var expression = ChildValue(root, "Expression");
            if (string.IsNullOrWhiteSpace(expression) || Encoding.UTF8.GetByteCount(expression) > MaximumExpressionBytes)
                throw InvalidXml("The query expression is missing or exceeds the 256 KiB limit.");

            var input = ReadFormat(Child(root, "InputSerialization"), input: true);
            var output = ReadFormat(Child(root, "OutputSerialization"), input: false);
            _ = BlobQueryPlan.Parse(expression);
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
        await avro.InitializeAsync(cancellationToken);

        try
        {
            await using var enumerator = ReadRowsAsync(input, request.Input, cancellationToken)
                .GetAsyncEnumerator(cancellationToken);
            var wroteHeader = false;
            while (await enumerator.MoveNextAsync())
            {
                var selected = plan.Select(enumerator.Current);
                if (selected is null)
                    continue;

                if (!wroteHeader && request.Output.Kind == BlobQueryFormatKind.Delimited && request.Output.HasHeaders)
                {
                    await avro.AppendDataAsync(
                        EncodeDelimited(selected.Names.Select(name => new QueryCell(name)).ToArray(), request.Output),
                        cancellationToken);
                    wroteHeader = true;
                }

                var encoded = request.Output.Kind switch
                {
                    BlobQueryFormatKind.Delimited => EncodeDelimited(selected.Values, request.Output),
                    BlobQueryFormatKind.Json => EncodeJson(selected, request.Output),
                    _ => throw new InvalidOperationException("Unknown query output format.")
                };
                await avro.AppendDataAsync(encoded, cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (BlobQueryDataException exception)
        {
            await avro.WriteErrorAsync(true, exception.Name, exception.Message, exception.Position, cancellationToken);
        }

        await avro.CompleteAsync(totalBytes, cancellationToken);
    }

    private static BlobQueryTextFormat ReadFormat(XElement? serialization, bool input)
    {
        var format = serialization is null ? null : Child(serialization, "Format");
        var type = format is null ? "delimited" : ChildValue(format, "Type")?.ToLowerInvariant();
        if (string.IsNullOrEmpty(type))
            throw InvalidXml("A query serialization format type is required.");

        if (type == "delimited")
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
            return new BlobQueryTextFormat(BlobQueryFormatKind.Delimited, column, quote, record, escape, headers);
        }

        if (type == "json")
        {
            var configuration = format is null ? null : Child(format, "JsonTextConfiguration");
            var record = ChildValue(configuration, "RecordSeparator") ?? "\n";
            ValidateSeparator(record, "RecordSeparator");
            return new BlobQueryTextFormat(BlobQueryFormatKind.Json, ",", '"', record, '\\', false);
        }

        var direction = input ? "input" : "output";
        throw new AzureStorageException(
            StatusCodes.Status400BadRequest,
            "BlobQueryError",
            $"The {direction} query format '{type}' is not supported.");
    }

    private static async IAsyncEnumerable<QueryRow> ReadRowsAsync(
        Stream input,
        BlobQueryTextFormat format,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
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
            await foreach (var fields in ReadDelimitedRowsAsync(reader, format, cancellationToken))
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
        await foreach (var record in ReadRawRecordsAsync(reader, format.RecordSeparator, cancellationToken))
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
                if (root.ValueKind == JsonValueKind.Object)
                {
                    var properties = root.EnumerateObject().ToArray();
                    yield return new QueryRow(
                        properties.Select(property => property.Name).ToArray(),
                        properties.Select(property => QueryCell.FromJson(property.Value)).ToArray());
                }
                else if (root.ValueKind == JsonValueKind.Array)
                {
                    var values = root.EnumerateArray().Select(QueryCell.FromJson).ToArray();
                    yield return new QueryRow(
                        Enumerable.Range(1, values.Length).Select(index => $"_{index}").ToArray(),
                        values);
                }
                else
                {
                    throw new BlobQueryDataException(
                        "InvalidJsonType",
                        "Each JSON query input record must be an object or array.",
                        position);
                }
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
                var read = await reader.ReadAsync(buffer.AsMemory(), cancellationToken);
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
                        if (format.RecordSeparator == "\n" && field.Length > 0 && field[^1] == '\r')
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
                var read = await reader.ReadAsync(buffer.AsMemory(), cancellationToken);
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
                    if (separator == "\n" && record.Length > 0 && record[^1] == '\r')
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
                        value.Contains('\r') || value.Contains('\n');
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
        parent?.Elements().FirstOrDefault(element => element.Name.LocalName == name);

    private static string? ChildValue(XElement? parent, string name) => Child(parent, name)?.Value;

    private static AzureStorageException InvalidXml(string detail) => new(
        StatusCodes.Status400BadRequest,
        "InvalidXmlDocument",
        $"The query request XML is invalid. {detail}");
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
        await destination.WriteAsync(header.GetBuffer().AsMemory(0, checked((int)header.Length)), cancellationToken);
    }

    public async Task AppendDataAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        EnsureWritable();
        if (_data.Length > 0 && _data.Length + data.Length > DataBlockSize)
            await FlushDataAsync(cancellationToken);
        if (data.Length >= DataBlockSize)
        {
            await WriteRecordAsync(0, payload => WriteBytes(payload, data.Span), cancellationToken);
            return;
        }
        await _data.WriteAsync(data, cancellationToken);
    }

    public async Task WriteErrorAsync(
        bool fatal,
        string name,
        string description,
        long position,
        CancellationToken cancellationToken)
    {
        EnsureWritable();
        await FlushDataAsync(cancellationToken);
        await WriteRecordAsync(1, payload =>
        {
            payload.WriteByte(fatal ? (byte)1 : (byte)0);
            WriteString(payload, name);
            WriteString(payload, description);
            WriteLong(payload, position);
        }, cancellationToken);
    }

    public async Task CompleteAsync(long totalBytes, CancellationToken cancellationToken)
    {
        EnsureWritable();
        await FlushDataAsync(cancellationToken);
        await WriteRecordAsync(2, payload =>
        {
            WriteLong(payload, totalBytes);
            WriteLong(payload, totalBytes);
        }, cancellationToken);
        await WriteRecordAsync(3, payload => WriteLong(payload, totalBytes), cancellationToken);
        await destination.FlushAsync(cancellationToken);
        _completed = true;
    }

    private async Task FlushDataAsync(CancellationToken cancellationToken)
    {
        if (_data.Length == 0)
            return;
        var length = checked((int)_data.Length);
        await WriteRecordAsync(0, payload => WriteBytes(payload, _data.GetBuffer().AsSpan(0, length)), cancellationToken);
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
        await destination.WriteAsync(block.GetBuffer().AsMemory(0, checked((int)block.Length)), cancellationToken);
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

internal sealed record QueryCell(object? Value)
{
    public static QueryCell FromJson(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined => new QueryCell((object?)null),
        JsonValueKind.String => new QueryCell(element.GetString()),
        JsonValueKind.True => new QueryCell(true),
        JsonValueKind.False => new QueryCell(false),
        JsonValueKind.Number when element.TryGetInt64(out var integer) => new QueryCell(integer),
        JsonValueKind.Number when element.TryGetDecimal(out var number) => new QueryCell(number),
        _ => new QueryCell(element.GetRawText())
    };

    public string ToText() => Value switch
    {
        null => string.Empty,
        bool boolean => boolean ? "true" : "false",
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
            default:
                writer.WriteString(name, ToText());
                break;
        }
    }
}

internal sealed record QueryRow(IReadOnlyList<string> Names, IReadOnlyList<QueryCell> Values)
{
    public QueryCell Resolve(string name)
    {
        if (name.Length > 1 && name[0] == '_' &&
            int.TryParse(name.AsSpan(1), NumberStyles.None, CultureInfo.InvariantCulture, out var ordinal) &&
            ordinal >= 1 && ordinal <= Values.Count)
        {
            return Values[ordinal - 1];
        }

        for (var index = 0; index < Names.Count; index++)
        {
            if (string.Equals(Names[index], name, StringComparison.OrdinalIgnoreCase))
                return Values[index];
        }
        return new QueryCell(null);
    }
}

internal sealed record QuerySelection(IReadOnlyList<string> Names, IReadOnlyList<QueryCell> Values);

internal sealed class BlobQueryPlan
{
    private readonly IReadOnlyList<QueryProjection> _projections;
    private readonly QueryPredicate? _predicate;

    private BlobQueryPlan(IReadOnlyList<QueryProjection> projections, QueryPredicate? predicate)
    {
        _projections = projections;
        _predicate = predicate;
    }

    public static BlobQueryPlan Parse(string expression) => new QueryParser(expression).Parse();

    public QuerySelection? Select(QueryRow row)
    {
        if (_predicate is not null && !_predicate.Evaluate(row))
            return null;
        if (_projections is [{ Star: true }])
            return new QuerySelection(row.Names, row.Values);
        return new QuerySelection(
            _projections.Select(projection => projection.Name).ToArray(),
            _projections.Select(projection => projection.Operand!.Resolve(row)).ToArray());
    }

    private sealed record QueryProjection(bool Star, QueryOperand? Operand, string Name);

    private sealed record QueryOperand(string? Field, QueryCell? Literal)
    {
        public QueryCell Resolve(QueryRow row) => Field is null ? Literal! : row.Resolve(Field);
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

    private sealed record NullPredicate(QueryOperand Operand, bool Negated) : QueryPredicate
    {
        public override bool Evaluate(QueryRow row) => (Operand.Resolve(row).Value is null) != Negated;
    }

    private sealed record TruthPredicate(QueryOperand Operand) : QueryPredicate
    {
        public override bool Evaluate(QueryRow row) => Operand.Resolve(row).Value switch
        {
            null => false,
            bool boolean => boolean,
            string text => !string.IsNullOrEmpty(text),
            long integer => integer != 0,
            decimal number => number != 0,
            _ => true
        };
    }

    private sealed record ComparisonPredicate(QueryOperand Left, QueryOperand Right, string Operator) : QueryPredicate
    {
        public override bool Evaluate(QueryRow row)
        {
            var left = Left.Resolve(row);
            var right = Right.Resolve(row);
            if (left.Value is null || right.Value is null)
                return Operator is "!=" or "<>" && left.Value is not null != (right.Value is not null);

            var comparison = Compare(left, right);
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

        private static int Compare(QueryCell left, QueryCell right)
        {
            if (decimal.TryParse(left.ToText(), NumberStyles.Number | NumberStyles.AllowExponent, CultureInfo.InvariantCulture, out var leftNumber) &&
                decimal.TryParse(right.ToText(), NumberStyles.Number | NumberStyles.AllowExponent, CultureInfo.InvariantCulture, out var rightNumber))
            {
                return leftNumber.CompareTo(rightNumber);
            }
            return string.Compare(left.ToText(), right.ToText(), StringComparison.Ordinal);
        }
    }

    private enum QueryTokenKind
    {
        Identifier,
        String,
        Number,
        Symbol,
        End
    }

    private readonly record struct QueryToken(QueryTokenKind Kind, string Text, int Position);

    private sealed class QueryParser
    {
        private readonly IReadOnlyList<QueryToken> _tokens;
        private int _position;

        public QueryParser(string expression)
        {
            _tokens = Tokenize(expression);
        }

        public BlobQueryPlan Parse()
        {
            ExpectKeyword("SELECT");
            var projections = ParseProjections();
            ExpectKeyword("FROM");
            var source = Expect(QueryTokenKind.Identifier, "BlobStorage");
            if (!string.Equals(source.Text, "BlobStorage", StringComparison.OrdinalIgnoreCase))
                throw InvalidQuery(source.Position, "Queries must read from BlobStorage.");

            if (MatchKeyword("AS"))
                _ = Expect(QueryTokenKind.Identifier, "alias");
            else if (Current.Kind == QueryTokenKind.Identifier && !IsKeyword(Current, "WHERE"))
                _position++;

            QueryPredicate? predicate = null;
            if (MatchKeyword("WHERE"))
                predicate = ParseOr();
            _ = MatchSymbol(";");
            if (Current.Kind != QueryTokenKind.End)
                throw InvalidQuery(Current.Position, $"Unexpected token '{Current.Text}'.");
            return new BlobQueryPlan(projections, predicate);
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
                var operand = ParseOperand();
                var defaultName = $"_{projections.Count + 1}";
                var name = MatchKeyword("AS")
                    ? Expect(QueryTokenKind.Identifier, "alias").Text
                    : Current.Kind == QueryTokenKind.Identifier && !IsKeyword(Current, "FROM")
                        ? _tokens[_position++].Text
                        : defaultName;
                projections.Add(new QueryProjection(false, operand, name));
            }
            while (MatchSymbol(","));

            if (projections.Count == 0 || projections.Count > 256 ||
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

            var left = ParseOperand();
            if (MatchKeyword("IS"))
            {
                var negated = MatchKeyword("NOT");
                ExpectKeyword("NULL");
                return new NullPredicate(left, negated);
            }
            if (Current.Kind == QueryTokenKind.Symbol && Current.Text is "=" or "==" or "!=" or "<>" or "<" or "<=" or ">" or ">=")
            {
                var operation = _tokens[_position++].Text;
                return new ComparisonPredicate(left, ParseOperand(), operation);
            }
            return new TruthPredicate(left);
        }

        private QueryOperand ParseOperand()
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
                    var field = token.Text;
                    if (MatchSymbol("."))
                        field = Expect(QueryTokenKind.Identifier, "field name").Text;
                    return new QueryOperand(field, null);
                case QueryTokenKind.String:
                    return new QueryOperand(null, new QueryCell(token.Text));
                case QueryTokenKind.Number:
                    if (long.TryParse(token.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var integer))
                        return new QueryOperand(null, new QueryCell(integer));
                    if (decimal.TryParse(token.Text, NumberStyles.Number | NumberStyles.AllowExponent, CultureInfo.InvariantCulture, out var number))
                        return new QueryOperand(null, new QueryCell(number));
                    throw InvalidQuery(token.Position, $"The numeric literal '{token.Text}' is invalid.");
                default:
                    throw InvalidQuery(token.Position, $"Expected an expression, found '{token.Text}'.");
            }
        }

        private QueryToken Current => _tokens[_position];

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
            if (Current.Kind != QueryTokenKind.Symbol || Current.Text != value)
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

        private static bool IsKeyword(QueryToken token, string value) =>
            token.Kind == QueryTokenKind.Identifier && string.Equals(token.Text, value, StringComparison.OrdinalIgnoreCase);

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
                        start));
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

                if (char.IsDigit(character) || character is '+' or '-' && index + 1 < expression.Length && char.IsDigit(expression[index + 1]))
                {
                    index++;
                    while (index < expression.Length && (char.IsDigit(expression[index]) || expression[index] is '.' or 'e' or 'E' or '+' or '-'))
                        index++;
                    tokens.Add(new QueryToken(QueryTokenKind.Number, expression[start..index], start));
                    continue;
                }

                if (index + 1 < expression.Length && expression.Substring(index, 2) is "<=" or ">=" or "!=" or "<>" or "==")
                {
                    tokens.Add(new QueryToken(QueryTokenKind.Symbol, expression.Substring(index, 2), start));
                    index += 2;
                    continue;
                }
                if (character is '*' or ',' or '.' or '(' or ')' or ';' or '=' or '<' or '>')
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
