using System.Text.Json.Serialization;

namespace Mk8.Sava.Storage;

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
    public bool IsDirectory { get; init; }
    public string Owner { get; init; } = "$superuser";
    public string Group { get; init; } = "$superuser";
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? AccessAcl { get; init; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool StickyBit { get; init; }
    [JsonIgnore]
    public string Permissions => PosixAccessControl.FormatMode(Acl, StickyBit);
    [JsonIgnore]
    public string Acl => AccessAcl ?? (IsDirectory
        ? "user::rwx,group::r-x,other::---"
        : "user::rw-,group::r--,other::---");
    public ulong? DeletionId { get; init; }
    public DateTimeOffset? DeletedAt { get; init; }
    public DateTimeOffset? DeleteRetentionUntil { get; init; }
    public required BlobKind Kind { get; init; }
    public required ContentManifest Content { get; init; }
    public required string ETag { get; init; }
    public required DateTimeOffset CreatedAt { get; init; }
    public required DateTimeOffset LastModified { get; init; }
    public IReadOnlyDictionary<string, string> Metadata { get; init; } =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    public IReadOnlyDictionary<string, string> Tags { get; init; } =
        new Dictionary<string, string>(StringComparer.Ordinal);
    public BlobHttpProperties Http { get; init; } = new();
    public LeaseRecord Lease { get; init; } = LeaseRecord.Available;
    public long SequenceNumber { get; init; }
    public bool IsSealed { get; init; }
    public string AccessTier { get; init; } = "Hot";
    public bool AccessTierInferred { get; init; }
    public string? SmartAccessTier { get; init; }
    public DateTimeOffset? SmartTierLastAccessedAt { get; init; }
    public DateTimeOffset? LastAccessedAt { get; init; }
    public DateTimeOffset? AccessTierChangedAt { get; init; }
    public string? ArchiveStatus { get; init; }
    public string? RehydratePriority { get; init; }
    public DateTimeOffset? RehydrateCompleteAt { get; init; }
    public DateTimeOffset? ExpiresAt { get; init; }
    public DateTimeOffset? ImmutabilityUntil { get; init; }
    public bool ImmutabilityLocked { get; init; }
    public bool HasLegalHold { get; init; }
    public string? EncryptionScope { get; init; }
    public string? EncryptionContext { get; init; }
    public string? CustomerProvidedKeySha256 { get; init; }
    public CopyState? Copy { get; init; }
    public ContentManifest? PendingCopyContent { get; init; }
    public IReadOnlyList<CommittedBlockRecord>? PendingCopyCommittedBlocks { get; init; }
    public int? PendingCopyAppendBlockCount { get; init; }
    public bool? PendingCopyIsSealed { get; init; }
    public IReadOnlyList<PageRange>? PendingCopyPageRanges { get; init; }
    public bool IsIncrementalCopy { get; init; }
    public string? IncrementalCopySource { get; init; }
    public string? IncrementalCopySourceSnapshot { get; init; }
    public DateTimeOffset? IncrementalCopySourceCreatedAt { get; init; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? IncrementalCopySourceIncarnationId { get; init; }
    public string? CopyDestinationSnapshot { get; init; }
    public IReadOnlyList<CommittedBlockRecord> CommittedBlocks { get; init; } = [];
    public int AppendBlockCount { get; init; }
    public IReadOnlyList<PageRange> PageRanges { get; init; } = [];
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? PageBlobIncarnationId { get; init; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public long PageMutationSequence { get; init; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IReadOnlyList<PageMutationRange>? PageMutationRanges { get; init; }
    public IReadOnlyDictionary<string, ObjectReplicationStatusRecord> ObjectReplicationStatuses { get; init; } =
        new Dictionary<string, ObjectReplicationStatusRecord>(StringComparer.Ordinal);
    public string? ObjectReplicationDestinationPolicyId { get; init; }
}
