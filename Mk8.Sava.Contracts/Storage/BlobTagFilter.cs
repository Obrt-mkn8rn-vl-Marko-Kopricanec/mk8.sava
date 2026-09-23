namespace Mk8.Sava.Storage;

internal sealed record BlobTagFilter(
    string? Container,
    IReadOnlyList<BlobTagPredicate> Predicates);
