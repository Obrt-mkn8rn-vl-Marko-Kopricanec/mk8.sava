namespace Mk8.Sava.Storage;

public sealed record StorageInventorySummary(
    long LogicalBlobBytes,
    long LogicalStagedBlockBytes,
    int BlobRecordCount,
    int StagedBlockCount,
    int ReachableChunkCount);
