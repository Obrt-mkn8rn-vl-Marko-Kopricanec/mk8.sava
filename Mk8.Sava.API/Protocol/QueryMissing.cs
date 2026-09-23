namespace Mk8.Sava.Protocol;

internal sealed class QueryMissing
{
    public static QueryMissing Value { get; } = new();

    private QueryMissing()
    {
    }
}
