namespace Mk8.Sava.Storage;

internal sealed record MetadataDatabaseInspection(
    int SchemaVersion,
    StorageMetadataInventory Inventory,
    IReadOnlyDictionary<string, bool> AccountNamespaceModes);
