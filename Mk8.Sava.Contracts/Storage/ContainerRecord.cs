using System.Text.Json.Serialization;

namespace Mk8.Sava.Storage;

public sealed record ContainerRecord
{
    public required string Account { get; init; }
    public required string Name { get; init; }
    public required string Revision { get; init; }
    public required string ETag { get; init; }
    public required DateTimeOffset CreatedAt { get; init; }
    public required DateTimeOffset LastModified { get; init; }
    public string Owner { get; init; } = "$superuser";
    public string Group { get; init; } = "$superuser";
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? AccessAcl { get; init; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool StickyBit { get; init; }
    [JsonIgnore]
    public string Acl => AccessAcl ?? "user::rwx,group::r-x,other::---";
    public IReadOnlyDictionary<string, string> Metadata { get; init; } =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    public IReadOnlyDictionary<string, StoredAccessPolicy> AccessPolicies { get; init; } =
        new Dictionary<string, StoredAccessPolicy>(StringComparer.Ordinal);
    public string? PublicAccess { get; init; }
    public string? DefaultEncryptionScope { get; init; }
    public bool PreventEncryptionScopeOverride { get; init; }
    public LeaseRecord Lease { get; init; } = LeaseRecord.Available;
    public DateTimeOffset? DeletedAt { get; init; }
    public DateTimeOffset? DeleteRetentionUntil { get; init; }
    public string? DeletedVersion { get; init; }
    public bool HasLegalHold { get; init; }
    public DateTimeOffset? ImmutabilityUntil { get; init; }
    public bool ImmutabilityLocked { get; init; }
    public bool ImmutableStorageWithVersioningEnabled { get; init; }
}
