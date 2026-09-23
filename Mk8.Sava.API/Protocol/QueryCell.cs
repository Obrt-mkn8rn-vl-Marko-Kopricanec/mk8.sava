using System.Globalization;
using System.Text.Json;

namespace Mk8.Sava.Protocol;

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
