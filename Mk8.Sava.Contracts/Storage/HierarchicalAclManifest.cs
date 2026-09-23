namespace Mk8.Sava.Storage;

internal sealed record HierarchicalAclManifest
{
    public required int SchemaVersion { get; init; }
    public required List<HierarchicalAclManifestEntry> Entries { get; init; }
}
