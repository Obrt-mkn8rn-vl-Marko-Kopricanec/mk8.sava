namespace Mk8.Sava.Storage;

public sealed record BlobTagFilter(
    string? Container,
    IReadOnlyList<BlobTagPredicate> Predicates);
