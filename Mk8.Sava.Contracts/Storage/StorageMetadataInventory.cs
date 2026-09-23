namespace Mk8.Sava.Storage;

public sealed record StorageMetadataInventory(
    IReadOnlySet<string> ReachableChunkIds,
    long LogicalBlobBytes,
    long LogicalStagedBlockBytes,
    int BlobRecordCount,
    int StagedBlockCount);
