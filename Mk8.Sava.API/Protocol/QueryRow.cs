using System.Globalization;

namespace Mk8.Sava.Protocol;

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
