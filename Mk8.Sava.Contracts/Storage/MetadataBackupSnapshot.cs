namespace Mk8.Sava.Storage;

internal sealed class MetadataBackupSnapshot(
    StorageMetadataInventory inventory,
    IDisposable contentPins) : IDisposable
{
    public StorageMetadataInventory Inventory { get; } = inventory;

    public void Dispose() => contentPins.Dispose();
}
