using System.Globalization;
using System.Text;
using System.Text.Json;

namespace Mk8.Sava.Protocol;

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
            "LOWER" => new QueryCell(first.ToText().ToRequiredLowerInvariant()),
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
        var normalized = part.Trim().ToRequiredLowerInvariant();
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
        private readonly List<QueryToken> _tokens;
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

        private List<QueryTableSegment> ParseTablePath(int sourcePosition)
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

        private List<QueryProjection> ParseProjections()
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

        private QueryAggregateExpression ParseAggregateExpression(QueryToken token, string function)
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

        private static QueryOperand Literal(object? value) =>
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

        private static List<QueryToken> Tokenize(string expression)
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
