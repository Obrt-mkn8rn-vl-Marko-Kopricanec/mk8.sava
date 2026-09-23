namespace Mk8.Sava.Storage;

internal sealed record KeysetPage<T>(IReadOnlyList<T> Items, bool HasMore);
