namespace Mk8.Sava.Storage;

public sealed record BlobWriteOptions(
    BlobHttpProperties Http,
    Dictionary<string, string> Metadata,
    Dictionary<string, string>? Tags = null,
    string? AccessTier = null,
    DateTimeOffset? ImmutabilityUntil = null,
    bool ImmutabilityLocked = false,
    bool HasLegalHold = false,
    string? EncryptionScope = null,
    string? CustomerProvidedKeySha256 = null,
    byte[]? CustomerProvidedKey = null);

public sealed record PageRange(long Start, long End);

public sealed record PageRangeDiff(
    IReadOnlyList<PageRange> PageRanges,
    IReadOnlyList<PageRange> ClearRanges);

public sealed record BlobTierUpdate(BlobRecord Blob, bool Pending);

public sealed record StorageMaintenanceResult(
    int CompletedCopies,
    int CompletedRehydrations,
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

public sealed record StorageBackupValidation(
    string BackupPath,
    DateTimeOffset CreatedAt,
    int BlobRecordCount,
    int StagedBlockCount,
    int ChunkCount,
    long LogicalBytes,
    long PhysicalBytes);

public interface IStorageTelemetry
{
    StorageUsageSnapshot Usage { get; }

    StorageIntegritySnapshot Integrity { get; }

    void RecordRequest(int statusCode, long elapsedStopwatchTicks);

    void RecordMaintenance(StorageMaintenanceResult result, StorageUsageSnapshot usage);

    void RecordMaintenanceFailure();

    void RecordIntegrity(StorageIntegritySnapshot integrity);

    string RenderPrometheus();
}

public interface IStoragePaths
{
    string Root { get; }

    string Chunks { get; }

    string Packs { get; }

    string Staging { get; }

    string Database { get; }
}
