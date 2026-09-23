namespace Mk8.Sava.Storage;

public sealed record StorageMaintenanceResult(
    int CompletedCopies,
    int CompletedObjectReplications,
    int FailedObjectReplications,
    int RemovedObjectReplicas,
    int CompletedRehydrations,
    int CompletedSmartTierTransitions,
    int ExpiredBlobs,
    int PurgedSoftDeletedBlobs,
    int PurgedSoftDeletedContainers,
    int ExpiredUncommittedBlocks,
    int ReclaimedChunks,
    int ReclaimedStagingFiles,
    int RecompressedChunks,
    long RecompressionBytesSaved,
    int CompactedChunkPacks,
    long PackCompactionBytesSaved);
