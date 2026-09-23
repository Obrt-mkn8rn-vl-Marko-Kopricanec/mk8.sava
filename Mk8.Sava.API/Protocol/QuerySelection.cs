namespace Mk8.Sava.Protocol;

internal sealed record QuerySelection(IReadOnlyList<string> Names, IReadOnlyList<QueryCell> Values);
