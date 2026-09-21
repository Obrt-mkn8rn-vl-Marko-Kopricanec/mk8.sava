using System.Text.Json.Serialization;

namespace Mk8.Sava.Storage;

[JsonConverter(typeof(JsonStringEnumConverter<BlobKind>))]
public enum BlobKind
{
    BlockBlob,
    AppendBlob,
    PageBlob
}

[JsonConverter(typeof(JsonStringEnumConverter<LeaseState>))]
public enum LeaseState
{
    Available,
    Leased,
    Expired,
    Breaking,
    Broken
}

public sealed record ChunkReference(string Id, long Offset, long Length);

public sealed record ContentManifest(
    string Domain,
    long Length,
    string Sha256,
    IReadOnlyList<ChunkReference> Chunks)
{
    public const string SparseHash = "sparse";

    public static ContentManifest Empty(string domain) =>
        new(domain, 0, Convert.ToHexStringLower(SHA256.HashData([])), []);
}

public sealed record CommittedBlockRecord(string Id, ContentManifest Content);

public readonly record struct BlobEncryption(
    string? Scope,
    string? CustomerProvidedKeySha256,
    byte[]? CustomerProvidedKey = null);

public enum BlockListMode
{
    Latest,
    Committed,
    Uncommitted
}

public sealed record BlockListEntry(string Id, BlockListMode Mode);

public sealed record ContainerRecord
{
    public required string Account { get; init; }
    public required string Name { get; init; }
    public required string Revision { get; init; }
    public required string ETag { get; init; }
    public required DateTimeOffset CreatedAt { get; init; }
    public required DateTimeOffset LastModified { get; init; }
    public Dictionary<string, string> Metadata { get; init; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, StoredAccessPolicy> AccessPolicies { get; init; } = new(StringComparer.Ordinal);
    public string? PublicAccess { get; init; }
    public LeaseRecord Lease { get; init; } = LeaseRecord.Available;
    public DateTimeOffset? DeletedAt { get; init; }
    public DateTimeOffset? DeleteRetentionUntil { get; init; }
    public string? DeletedVersion { get; init; }
    public bool HasLegalHold { get; init; }
    public DateTimeOffset? ImmutabilityUntil { get; init; }
    public bool ImmutabilityLocked { get; init; }
}

public sealed record StoredAccessPolicy
{
    public DateTimeOffset? StartsAt { get; init; }
    public DateTimeOffset? ExpiresAt { get; init; }
    public required string Permission { get; init; }
}

public sealed record BlobRecord
{
    public required string Account { get; init; }
    public required string Container { get; init; }
    public required string Name { get; init; }
    public required string GenerationId { get; init; }
    public required string Revision { get; init; }
    public string? VersionId { get; init; }
    public string? Snapshot { get; init; }
    public bool IsCurrent { get; init; }
    public bool IsDeleted { get; init; }
    public DateTimeOffset? DeletedAt { get; init; }
    public DateTimeOffset? DeleteRetentionUntil { get; init; }
    public required BlobKind Kind { get; init; }
    public required ContentManifest Content { get; init; }
    public required string ETag { get; init; }
    public required DateTimeOffset CreatedAt { get; init; }
    public required DateTimeOffset LastModified { get; init; }
    public Dictionary<string, string> Metadata { get; init; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, string> Tags { get; init; } = new(StringComparer.Ordinal);
    public BlobHttpProperties Http { get; init; } = new();
    public LeaseRecord Lease { get; init; } = LeaseRecord.Available;
    public long SequenceNumber { get; init; }
    public bool IsSealed { get; init; }
    public string AccessTier { get; init; } = "Hot";
    public DateTimeOffset? AccessTierChangedAt { get; init; }
    public string? ArchiveStatus { get; init; }
    public string? RehydratePriority { get; init; }
    public DateTimeOffset? RehydrateCompleteAt { get; init; }
    public DateTimeOffset? ExpiresAt { get; init; }
    public DateTimeOffset? ImmutabilityUntil { get; init; }
    public bool ImmutabilityLocked { get; init; }
    public bool HasLegalHold { get; init; }
    public string? EncryptionScope { get; init; }
    public string? CustomerProvidedKeySha256 { get; init; }
    public CopyState? Copy { get; init; }
    public ContentManifest? PendingCopyContent { get; init; }
    public List<PageRange>? PendingCopyPageRanges { get; init; }
    public bool IsIncrementalCopy { get; init; }
    public string? IncrementalCopySource { get; init; }
    public string? IncrementalCopySourceSnapshot { get; init; }
    public DateTimeOffset? IncrementalCopySourceCreatedAt { get; init; }
    public string? CopyDestinationSnapshot { get; init; }
    public List<CommittedBlockRecord> CommittedBlocks { get; init; } = [];
    public int AppendBlockCount { get; init; }
    public List<PageRange> PageRanges { get; init; } = [];
}

public sealed record BlobHttpProperties
{
    public string ContentType { get; init; } = "application/octet-stream";
    public string? ContentEncoding { get; init; }
    public string? ContentLanguage { get; init; }
    public string? CacheControl { get; init; }
    public string? ContentDisposition { get; init; }
    public string? ContentMd5 { get; init; }
}

public sealed record LeaseRecord
{
    public static LeaseRecord Available { get; } = new();

    public string? Id { get; init; }
    public LeaseState State { get; init; } = LeaseState.Available;
    public int? DurationSeconds { get; init; }
    public DateTimeOffset? AcquiredAt { get; init; }
    public DateTimeOffset? ExpiresAt { get; init; }
    public DateTimeOffset? BreakEndsAt { get; init; }
}

public sealed record CopyState
{
    public required string Id { get; init; }
    public required string Source { get; init; }
    public required string Status { get; init; }
    public required long BytesCopied { get; init; }
    public required long TotalBytes { get; init; }
    public DateTimeOffset? CompletedAt { get; init; }
    public DateTimeOffset? ReadyAt { get; init; }
    public string? Description { get; init; }
    public bool IsIncremental { get; init; }
    public string? SourceSnapshot { get; init; }
}

public sealed record StagedBlockRecord
{
    public required string Account { get; init; }
    public required string Container { get; init; }
    public required string BlobName { get; init; }
    public required string BlockId { get; init; }
    public required ContentManifest Content { get; init; }
    public required DateTimeOffset CreatedAt { get; init; }
}

public sealed record StorageMetadataInventory(
    IReadOnlySet<string> ReachableChunkIds,
    long LogicalBlobBytes,
    long LogicalStagedBlockBytes,
    int BlobRecordCount,
    int StagedBlockCount);

public sealed record StorageInventorySummary(
    long LogicalBlobBytes,
    long LogicalStagedBlockBytes,
    int BlobRecordCount,
    int StagedBlockCount,
    int ReachableChunkCount);

internal sealed record ChunkIdPage(IReadOnlyList<string> Items, bool HasMore);

internal sealed record PhysicalChunkPage(IReadOnlyList<string> Items, bool HasMore);

internal sealed record KeysetPage<T>(IReadOnlyList<T> Items, bool HasMore);

internal readonly record struct ContainerKey(string Account, string Name);

internal sealed class MetadataBackupSnapshot(
    StorageMetadataInventory inventory,
    IDisposable contentPins) : IDisposable
{
    public StorageMetadataInventory Inventory { get; } = inventory;

    public void Dispose() => contentPins.Dispose();
}

internal sealed record MetadataDatabaseInspection(
    int SchemaVersion,
    StorageMetadataInventory Inventory);

public sealed record StorageUsageSnapshot(
    long LogicalBlobBytes,
    long LogicalStagedBlockBytes,
    long PhysicalChunkBytes,
    long StagingBytes,
    long MetadataBytes,
    int BlobRecordCount,
    int StagedBlockCount,
    int UniqueChunkCount,
    int ReachableChunkCount)
{
    public static StorageUsageSnapshot Empty { get; } = new(0, 0, 0, 0, 0, 0, 0, 0, 0);
}

public sealed record StoragePhysicalUsage(
    long ChunkBytes,
    long StagingBytes,
    long MetadataBytes,
    int ChunkCount);

public sealed record ChunkRecompressionResult(
    int ExaminedChunks,
    int RecompressedChunks,
    long BytesSaved)
{
    public static ChunkRecompressionResult Skipped { get; } = new(0, 0, 0);
    public static ChunkRecompressionResult Examined { get; } = new(1, 0, 0);
}

public enum ChunkIntegrityStatus
{
    Verified,
    RequiresCustomerKey,
    Missing,
    Corrupt
}

public sealed record StorageIntegritySnapshot(
    int ReachableChunks,
    int CheckedChunks,
    int VerifiedChunks,
    int CustomerKeyChunks,
    int MissingChunks,
    int CorruptChunks,
    bool Complete,
    DateTimeOffset CheckedAt)
{
    public static StorageIntegritySnapshot Pending { get; } =
        new(0, 0, 0, 0, 0, 0, false, DateTimeOffset.MinValue);

    public bool Healthy => MissingChunks == 0 && CorruptChunks == 0;
}

public sealed record ServiceProperties
{
    public bool VersioningEnabled { get; init; }
    public bool ContainerSoftDeleteEnabled { get; init; }
    public int ContainerSoftDeleteRetentionDays { get; init; } = 7;
    public bool BlobSoftDeleteEnabled { get; init; }
    public int BlobSoftDeleteRetentionDays { get; init; } = 7;
    public string? DefaultServiceVersion { get; init; }
    public List<CorsRule> Cors { get; init; } = [];
    public StaticWebsiteProperties StaticWebsite { get; init; } = new();
}

public sealed record CorsRule
{
    public required string AllowedOrigins { get; init; }
    public required string AllowedMethods { get; init; }
    public required string AllowedHeaders { get; init; }
    public required string ExposedHeaders { get; init; }
    public required int MaxAgeInSeconds { get; init; }
}

public sealed record StaticWebsiteProperties
{
    public bool Enabled { get; init; }
    public string? IndexDocument { get; init; }
    public string? ErrorDocument404Path { get; init; }
}
