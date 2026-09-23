namespace Mk8.Sava.Storage;

internal sealed record HierarchicalAclManifest
{
    public required int SchemaVersion { get; init; }
    public required List<HierarchicalAclManifestEntry> Entries { get; init; }
}

internal sealed record HierarchicalAclManifestEntry
{
    public required string Account { get; init; }
    public required string Container { get; init; }
    public required string Path { get; init; }
    public required string AccessAcl { get; init; }
}
