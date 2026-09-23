namespace Mk8.Sava.Protocol;

internal static class RequiredLowercase
{
    public static string ToRequiredLowerInvariant(this string value)
    {
        ArgumentNullException.ThrowIfNull(value);
#pragma warning disable CA1308 // Azure wire canonicalization and Blob Query LOWER require lowercase output.
        return value.ToLowerInvariant();
#pragma warning restore CA1308
    }
}
