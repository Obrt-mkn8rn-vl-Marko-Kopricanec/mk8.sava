using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.WebUtilities;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

internal static class BlobTagQuery
{
    private const string MarkerPrefix = "mk8t1.";
    private const int MaximumExpressionLength = 4096;
    private const int MaximumPredicates = 16;

    public static BlobTagFilter ParseFindExpression(string expression)
    {
        if (string.IsNullOrWhiteSpace(expression) || expression.Length > MaximumExpressionLength)
            throw AzureStorageException.InvalidQuery("where");

        var parser = new Parser(expression);
        var predicates = new List<BlobTagPredicate>();
        string? container = null;
        while (true)
        {
            parser.SkipWhitespace();
            var isContainer = parser.TryReadContainerIdentifier();
            var key = isContainer ? string.Empty : parser.ReadTagIdentifier();
            parser.SkipWhitespace();
            var comparison = parser.ReadComparison();
            parser.SkipWhitespace();
            var value = parser.ReadQuoted('\'');

            if (isContainer)
            {
                if (comparison != BlobTagComparison.Equal ||
                    container is not null ||
                    string.IsNullOrEmpty(value))
                {
                    throw AzureStorageException.InvalidQuery("where");
                }
                container = value;
            }
            else
            {
                if (!ProtocolParsing.IsValidTagComponent(key, allowEmpty: false) ||
                    !ProtocolParsing.IsValidTagComponent(value, allowEmpty: true) ||
                    predicates.Count >= MaximumPredicates)
                {
                    throw AzureStorageException.InvalidQuery("where");
                }
                predicates.Add(new BlobTagPredicate(key, comparison, value));
            }

            parser.SkipWhitespace();
            if (parser.AtEnd)
                break;
            parser.ReadAnd();
        }

        if (predicates.Count == 0)
            throw AzureStorageException.InvalidQuery("where");
        foreach (var group in predicates.GroupBy(predicate => predicate.Key, StringComparer.Ordinal))
        {
            if (group.Count(predicate => predicate.Comparison is
                    BlobTagComparison.GreaterThan or BlobTagComparison.GreaterThanOrEqual) > 1 ||
                group.Count(predicate => predicate.Comparison is
                    BlobTagComparison.LessThan or BlobTagComparison.LessThanOrEqual) > 1)
            {
                throw AzureStorageException.InvalidQuery("where");
            }
        }
        return new BlobTagFilter(container, predicates);
    }

    public static BlobTagCursor? DecodeMarker(
        StorageRequestContext request,
        string expression,
        string marker)
    {
        if (string.IsNullOrEmpty(marker))
            return null;
        if (!marker.StartsWith(MarkerPrefix, StringComparison.Ordinal) || marker.Length > 8192)
            throw AzureStorageException.InvalidQuery("marker");
        var separator = marker.LastIndexOf('.');
        var scope = CreateScope(request, expression);
        if (separator <= MarkerPrefix.Length ||
            !string.Equals(marker[(separator + 1)..], scope, StringComparison.Ordinal))
        {
            throw AzureStorageException.InvalidQuery("marker");
        }

        try
        {
            var cursor = JsonSerializer.Deserialize<BlobTagCursor>(
                WebEncoders.Base64UrlDecode(marker[MarkerPrefix.Length..separator]));
            if (cursor is null ||
                string.IsNullOrEmpty(cursor.Container) ||
                string.IsNullOrEmpty(cursor.Name) ||
                string.IsNullOrEmpty(cursor.GenerationId))
            {
                throw AzureStorageException.InvalidQuery("marker");
            }
            return cursor;
        }
        catch (Exception exception) when (exception is FormatException or JsonException)
        {
            throw AzureStorageException.InvalidQuery("marker");
        }
    }

    public static string EncodeMarker(
        StorageRequestContext request,
        string expression,
        BlobTagCursor cursor) =>
        $"{MarkerPrefix}{WebEncoders.Base64UrlEncode(JsonSerializer.SerializeToUtf8Bytes(cursor))}." +
        CreateScope(request, expression);

    private static string CreateScope(StorageRequestContext request, string expression)
    {
        var value = string.Join(
            '\n',
            request.Account,
            request.Container ?? string.Empty,
            request.ServiceVersion,
            expression);
        return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(value)).AsSpan(0, 12));
    }

    private sealed class Parser(string text)
    {
        private int _offset;

        public bool AtEnd => _offset == text.Length;

        public void SkipWhitespace()
        {
            while (_offset < text.Length && char.IsWhiteSpace(text[_offset]))
                _offset++;
        }

        public bool TryReadContainerIdentifier()
        {
            const string identifier = "@container";
            if (_offset + identifier.Length > text.Length ||
                !text.AsSpan(_offset, identifier.Length).Equals(
                    identifier,
                    StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
            _offset += identifier.Length;
            return true;
        }

        public string ReadQuoted(char quote)
        {
            if (_offset >= text.Length || text[_offset] != quote)
                throw AzureStorageException.InvalidQuery("where");
            var start = ++_offset;
            while (_offset < text.Length && text[_offset] != quote)
                _offset++;
            if (_offset >= text.Length)
                throw AzureStorageException.InvalidQuery("where");
            var value = text[start.._offset];
            _offset++;
            return value;
        }

        public string ReadTagIdentifier()
        {
            if (_offset < text.Length && text[_offset] == '"')
                return ReadQuoted('"');
            var start = _offset;
            if (_offset >= text.Length ||
                !(char.IsAsciiLetter(text[_offset]) || text[_offset] == '_'))
            {
                throw AzureStorageException.InvalidQuery("where");
            }
            _offset++;
            while (_offset < text.Length &&
                   (char.IsAsciiLetterOrDigit(text[_offset]) || text[_offset] == '_'))
            {
                _offset++;
            }
            return text[start.._offset];
        }

        public BlobTagComparison ReadComparison()
        {
            if (TryRead(">="))
                return BlobTagComparison.GreaterThanOrEqual;
            if (TryRead("<="))
                return BlobTagComparison.LessThanOrEqual;
            if (TryRead("="))
                return BlobTagComparison.Equal;
            if (TryRead(">"))
                return BlobTagComparison.GreaterThan;
            if (TryRead("<"))
                return BlobTagComparison.LessThan;
            throw AzureStorageException.InvalidQuery("where");
        }

        public void ReadAnd()
        {
            const string conjunction = "AND";
            if (_offset + conjunction.Length > text.Length ||
                !text.AsSpan(_offset, conjunction.Length).Equals(
                    conjunction,
                    StringComparison.OrdinalIgnoreCase))
            {
                throw AzureStorageException.InvalidQuery("where");
            }
            _offset += conjunction.Length;
            if (_offset < text.Length && !char.IsWhiteSpace(text[_offset]))
                throw AzureStorageException.InvalidQuery("where");
            SkipWhitespace();
            if (AtEnd)
                throw AzureStorageException.InvalidQuery("where");
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
    }
}
