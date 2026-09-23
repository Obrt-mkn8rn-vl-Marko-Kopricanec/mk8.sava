namespace Mk8.Sava.Protocol;

internal static class BlobTagCondition
{
    private const int MaximumExpressionLength = 4096;
    private const int MaximumLogicalOperations = 10;
    private const int MaximumParenthesisDepth = 32;

    public static bool Evaluate(
        string expression,
        IReadOnlyDictionary<string, string> tags,
        string headerName)
    {
        if (string.IsNullOrWhiteSpace(expression) || expression.Length > MaximumExpressionLength)
            throw AzureStorageException.InvalidHeader(headerName, expression);

        var parser = new Parser(expression, headerName);
        var result = parser.ReadExpression(tags, 0);
        parser.SkipWhitespace();
        if (!parser.AtEnd)
            throw AzureStorageException.InvalidHeader(headerName, expression);
        return result;
    }

    private enum Comparison
    {
        Equal,
        NotEqual,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual
    }

    private sealed class Parser(string text, string headerName)
    {
        private int _logicalOperations;
        private int _offset;

        public bool AtEnd => _offset == text.Length;

        public bool ReadExpression(IReadOnlyDictionary<string, string> tags, int depth) =>
            ReadOr(tags, depth);

        public void SkipWhitespace()
        {
            while (_offset < text.Length && char.IsWhiteSpace(text[_offset]))
                _offset++;
        }

        private bool ReadOr(IReadOnlyDictionary<string, string> tags, int depth)
        {
            var result = ReadAnd(tags, depth);
            while (TryReadKeyword("OR"))
            {
                CountLogicalOperation();
                var right = ReadAnd(tags, depth);
                result = result || right;
            }
            return result;
        }

        private bool ReadAnd(IReadOnlyDictionary<string, string> tags, int depth)
        {
            var result = ReadPrimary(tags, depth);
            while (TryReadKeyword("AND"))
            {
                CountLogicalOperation();
                var right = ReadPrimary(tags, depth);
                result = result && right;
            }
            return result;
        }

        private bool ReadPrimary(IReadOnlyDictionary<string, string> tags, int depth)
        {
            SkipWhitespace();
            if (TryRead("("))
            {
                if (depth >= MaximumParenthesisDepth)
                    throw InvalidExpression();
                var result = ReadExpression(tags, depth + 1);
                SkipWhitespace();
                if (!TryRead(")"))
                    throw InvalidExpression();
                return result;
            }

            return ReadComparison(tags);
        }

        private bool ReadComparison(IReadOnlyDictionary<string, string> tags)
        {
            SkipWhitespace();
            var key = ReadTagIdentifier();
            SkipWhitespace();
            var comparison = ReadComparisonOperator();
            SkipWhitespace();
            var expected = ReadQuoted('\'');
            if (!ProtocolParsing.IsValidTagComponent(key, allowEmpty: false) ||
                !ProtocolParsing.IsValidTagComponent(expected, allowEmpty: true))
            {
                throw InvalidExpression();
            }

            if (!tags.TryGetValue(key, out var actual))
                return false;
            var order = string.CompareOrdinal(actual, expected);
            return comparison switch
            {
                Comparison.Equal => order == 0,
                Comparison.NotEqual => order != 0,
                Comparison.GreaterThan => order > 0,
                Comparison.GreaterThanOrEqual => order >= 0,
                Comparison.LessThan => order < 0,
                Comparison.LessThanOrEqual => order <= 0,
                _ => throw InvalidExpression()
            };
        }

        private string ReadTagIdentifier()
        {
            if (_offset < text.Length && text[_offset] == '"')
                return ReadQuoted('"');
            var start = _offset;
            if (_offset >= text.Length ||
                !(char.IsAsciiLetter(text[_offset]) || text[_offset] == '_'))
            {
                throw InvalidExpression();
            }
            _offset++;
            while (_offset < text.Length &&
                   (char.IsAsciiLetterOrDigit(text[_offset]) || text[_offset] == '_'))
            {
                _offset++;
            }
            return text[start.._offset];
        }

        private string ReadQuoted(char quote)
        {
            if (_offset >= text.Length || text[_offset] != quote)
                throw InvalidExpression();
            var start = ++_offset;
            while (_offset < text.Length && text[_offset] != quote)
                _offset++;
            if (_offset >= text.Length)
                throw InvalidExpression();
            var value = text[start.._offset];
            _offset++;
            return value;
        }

        private Comparison ReadComparisonOperator()
        {
            if (TryRead(">="))
                return Comparison.GreaterThanOrEqual;
            if (TryRead("<="))
                return Comparison.LessThanOrEqual;
            if (TryRead("<>"))
                return Comparison.NotEqual;
            if (TryRead("="))
                return Comparison.Equal;
            if (TryRead(">"))
                return Comparison.GreaterThan;
            if (TryRead("<"))
                return Comparison.LessThan;
            throw InvalidExpression();
        }

        private bool TryReadKeyword(string keyword)
        {
            var originalOffset = _offset;
            SkipWhitespace();
            if (_offset + keyword.Length > text.Length ||
                !text.AsSpan(_offset, keyword.Length).Equals(keyword, StringComparison.OrdinalIgnoreCase))
            {
                _offset = originalOffset;
                return false;
            }

            var end = _offset + keyword.Length;
            if (end < text.Length &&
                (char.IsAsciiLetterOrDigit(text[end]) || text[end] == '_'))
            {
                _offset = originalOffset;
                return false;
            }
            _offset = end;
            return true;
        }

        private bool TryRead(string value)
        {
            if (_offset + value.Length > text.Length ||
                !text.AsSpan(_offset, value.Length).SequenceEqual(value))
            {
                return false;
            }
            _offset += value.Length;
            return true;
        }

        private void CountLogicalOperation()
        {
            _logicalOperations++;
            if (_logicalOperations > MaximumLogicalOperations)
                throw InvalidExpression();
        }

        private AzureStorageException InvalidExpression() =>
            AzureStorageException.InvalidHeader(headerName, text);
    }
}
