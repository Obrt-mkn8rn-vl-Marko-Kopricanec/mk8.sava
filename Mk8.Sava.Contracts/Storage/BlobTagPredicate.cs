namespace Mk8.Sava.Storage;

public sealed record BlobTagPredicate(string Key, BlobTagComparison Comparison, string Value);
