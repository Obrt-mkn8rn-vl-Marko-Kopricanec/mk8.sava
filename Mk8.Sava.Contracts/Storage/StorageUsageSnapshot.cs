namespace Mk8.Sava.Storage;

public sealed record StorageUsageSnapshot(
    long LogicalBlobBytes,
    long LogicalStagedBlockBytes,
    long PhysicalChunkBytes,
    long StagingBytes,
    long MetadataBytes,
    int BlobRecordCount,
    int StagedBlockCount,
    int UniqueChunkCount,
    int ReachableChunkCount,
    long? AllocatedRootBytes = null,
    long PhysicalScanUnixSeconds = 0)
{
    public static StorageUsageSnapshot Empty { get; } = new(0, 0, 0, 0, 0, 0, 0, 0, 0);
}
