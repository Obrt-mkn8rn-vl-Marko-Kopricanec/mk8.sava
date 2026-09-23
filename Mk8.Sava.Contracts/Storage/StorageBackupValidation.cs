namespace Mk8.Sava.Storage;

public sealed record StorageBackupValidation(
    string BackupPath,
    DateTimeOffset CreatedAt,
    int BlobRecordCount,
    int StagedBlockCount,
    int ChunkCount,
    long LogicalBytes,
    long PhysicalBytes);
