using System.Globalization;

namespace Mk8.Sava.Protocol;

internal static class BlobQueryTimestamp
{
    private static readonly string[] Formats =
    [
        "yyyy", "yyyy'T'", "yyyy-MM", "yyyy-MM'T'", "yyyy-MM-dd", "yyyy-MM-dd'T'",
        "yyyy-MM-dd'T'HH:mmK", "yyyy-MM-dd'T'HH:mm:ssK", "yyyy-MM-dd'T'HH:mm:ss.FFFFFFFK"
    ];

    public static bool TryParse(string text, out DateTimeOffset timestamp)
    {
        var fraction = text.IndexOf('.', StringComparison.Ordinal);
        if (fraction >= 0 && (fraction + 1 == text.Length || !char.IsAsciiDigit(text[fraction + 1])))
        {
            timestamp = default;
            return false;
        }
        return DateTimeOffset.TryParseExact(text, Formats, CultureInfo.InvariantCulture,
            DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.AssumeUniversal, out timestamp);
    }
}
