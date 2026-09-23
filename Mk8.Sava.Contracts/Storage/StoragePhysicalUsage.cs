namespace Mk8.Sava.Storage;

public sealed record StoragePhysicalUsage(
    long ChunkBytes,
    long StagingBytes,
    long MetadataBytes,
    int ChunkCount,
    long? AllocatedRootBytes = null);
