namespace Mk8.Sava.Storage;

internal sealed record BlobTagPredicate(string Key, BlobTagComparison Comparison, string Value);
