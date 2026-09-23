using System.ComponentModel.DataAnnotations;

namespace Mk8.Sava.Configuration;

public sealed class SavaOptions : IValidatableObject
{
    public const string SectionName = "Sava";
    private static readonly DateTimeOffset EarliestObjectReplicationTime =
        new(1601, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Required]
    public string DataPath { get; init; } = "data";

    [Required]
    public string DefaultAccount { get; init; } = "devstoreaccount1";

    [Required]
    public Dictionary<string, string> Accounts { get; init; } = new(StringComparer.Ordinal);

    public Dictionary<string, string> DataEncryptionKeys { get; init; } = new(StringComparer.Ordinal);

    public Dictionary<string, StorageAccountCapabilities> AccountCapabilities { get; init; } = new(StringComparer.Ordinal);
    public List<ObjectReplicationPolicyOptions> ObjectReplicationPolicies { get; init; } = [];

    public bool AllowAnonymousPublicAccess { get; init; }
    public int MinimumChunkBytes { get; init; } = 64 * 1024;
    public int TargetChunkBytes { get; init; } = 256 * 1024;
    public int MaximumChunkBytes { get; init; } = 1024 * 1024;
    public int CompressionQuality { get; init; } = 5;
    public int CompressionMinimumSavingsBytes { get; init; } = 64;
    public int BackgroundCompressionQuality { get; init; } = 11;
    public int BackgroundCompressionMinimumSavingsBytes { get; init; } = 128;
    public TimeSpan BackgroundCompressionMinimumAge { get; init; } = TimeSpan.FromHours(1);
    public int BackgroundCompressionChunksPerMaintenancePass { get; init; } = 8;
    public bool EnableSmallChunkPacking { get; init; } = true;
    public int SmallChunkPackingThresholdBytes { get; init; } = 48 * 1024;
    public long ChunkPackTargetBytes { get; init; } = 16L * 1024 * 1024;
    public int ChunkPackMaximumRecords { get; init; } = 2048;
    public TimeSpan ChunkPackSealAge { get; init; } = TimeSpan.FromHours(1);
    public int ChunkPacksPerMaintenancePass { get; init; } = 2;
    public long ChunkPackCompactionMinimumSavingsBytes { get; init; } = 64 * 1024;
    public double ChunkPackCompactionMinimumDeadRatio { get; init; } = 0.20;
    public bool EnableCrossAccountDeduplication { get; init; }
    public string? CrossAccountEncryptionKey { get; init; }
    public long MaximumRequestBodyBytes { get; init; } = 5_000L * 1024 * 1024;
    public List<string> UrlTransferAllowedPrivateHosts { get; init; } = [];
    public int SoftDeleteRetentionDays { get; init; } = 7;
    public TimeSpan StandardRehydrationDelay { get; init; } = TimeSpan.FromHours(15);
    public TimeSpan HighPriorityRehydrationDelay { get; init; } = TimeSpan.FromHours(1);
    public TimeSpan AsyncCopyCompletionDelay { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan MaintenanceScanInterval { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan UncommittedBlockRetention { get; init; } = TimeSpan.FromDays(7);
    public TimeSpan AbandonedStagingRetention { get; init; } = TimeSpan.FromDays(1);
    public int MaximumStagingFilesPerMaintenancePass { get; init; } = 256;
    public int BlobRecordsPerMaintenancePass { get; init; } = 256;
    public int ContainerRecordsPerMaintenancePass { get; init; } = 256;
    public int UncommittedBlocksPerMaintenancePass { get; init; } = 256;
    public int GarbageCollectionChunksPerMaintenancePass { get; init; } = 256;
    public int IntegrityScanChunksPerMaintenancePass { get; init; } = 256;
    public BearerAuthenticationOptions BearerAuthentication { get; init; } = new();

    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (Accounts.Count == 0)
            yield return new ValidationResult("At least one storage account is required.", [nameof(Accounts)]);

        if (!Accounts.ContainsKey(DefaultAccount))
            yield return new ValidationResult("DefaultAccount must identify a configured account.", [nameof(DefaultAccount)]);

        foreach (var (accountName, accountKey) in Accounts)
        {
            if (!IsValidAccountName(accountName))
            {
                yield return new ValidationResult(
                    $"Storage account name '{accountName}' must have 3 to 24 lowercase ASCII letters or digits.",
                    [nameof(Accounts)]);
            }

            if (!TryDecodeKey(accountKey, out var keyBytes))
                yield return new ValidationResult($"The key for account '{accountName}' must be valid base64.", [nameof(Accounts)]);
            else if (keyBytes.Length < 32)
                yield return new ValidationResult($"The key for account '{accountName}' must contain at least 256 bits.", [nameof(Accounts)]);
        }

        foreach (var (accountName, dataKey) in DataEncryptionKeys)
        {
            if (!Accounts.ContainsKey(accountName))
            {
                yield return new ValidationResult(
                    $"DataEncryptionKeys references unknown account '{accountName}'.",
                    [nameof(DataEncryptionKeys)]);
            }
            if (!TryDecodeKey(dataKey, out var keyBytes) || keyBytes.Length < 32)
            {
                yield return new ValidationResult(
                    $"The data encryption key for account '{accountName}' must be valid base64 containing at least 256 bits.",
                    [nameof(DataEncryptionKeys)]);
            }
        }

        foreach (var (accountName, capabilities) in AccountCapabilities)
        {
            if (!Accounts.ContainsKey(accountName))
            {
                yield return new ValidationResult(
                    $"AccountCapabilities references unknown account '{accountName}'.",
                    [nameof(AccountCapabilities)]);
            }
            if (capabilities.HierarchicalNamespaceBlobIndexTagsEnabled &&
                !capabilities.HierarchicalNamespaceEnabled)
            {
                yield return new ValidationResult(
                    $"AccountCapabilities for '{accountName}' enables hierarchical blob index tags without hierarchical namespace.",
                    [nameof(AccountCapabilities)]);
            }
            if (capabilities.HierarchicalNamespaceBlobSnapshotsEnabled &&
                !capabilities.HierarchicalNamespaceEnabled)
            {
                yield return new ValidationResult(
                    $"AccountCapabilities for '{accountName}' enables hierarchical blob snapshots without hierarchical namespace.",
                    [nameof(AccountCapabilities)]);
            }
            var versionLevelImmutability = capabilities.ImmutableStorageWithVersioningEnabled ||
                                           capabilities.ImmutableStorageWithVersioningContainers.Count > 0;
            if (versionLevelImmutability && !capabilities.VersioningEnabled)
            {
                yield return new ValidationResult(
                    $"AccountCapabilities for '{accountName}' enables immutable storage with versioning without blob versioning.",
                    [nameof(AccountCapabilities)]);
            }
            if (versionLevelImmutability && capabilities.HierarchicalNamespaceEnabled)
            {
                yield return new ValidationResult(
                    $"AccountCapabilities for '{accountName}' combines immutable storage with versioning and hierarchical namespace.",
                    [nameof(AccountCapabilities)]);
            }
            if (versionLevelImmutability && capabilities.LastAccessTimeTrackingEnabled)
            {
                yield return new ValidationResult(
                    $"AccountCapabilities for '{accountName}' combines immutable storage with versioning and last-access-time tracking.",
                    [nameof(AccountCapabilities)]);
            }
            if (capabilities.ImmutableStorageWithVersioningContainers.Any(string.IsNullOrWhiteSpace))
            {
                yield return new ValidationResult(
                    $"AccountCapabilities for '{accountName}' contains a blank immutable-storage container name.",
                    [nameof(AccountCapabilities)]);
            }
            if (capabilities.SasExpirationPeriod is { } sasExpirationPeriod &&
                sasExpirationPeriod <= TimeSpan.Zero)
            {
                yield return new ValidationResult(
                    $"AccountCapabilities for '{accountName}' has a non-positive SAS expiration period.",
                    [nameof(AccountCapabilities)]);
            }
        }

        var replicationPolicyIds = new HashSet<Guid>();
        var replicationAccountPairs = new HashSet<string>(StringComparer.Ordinal);
        var replicationDestinationContainers = new HashSet<string>(StringComparer.Ordinal);
        foreach (var policy in ObjectReplicationPolicies)
        {
            if (!Guid.TryParseExact(policy.PolicyId, "D", out var policyId) || !replicationPolicyIds.Add(policyId))
            {
                yield return new ValidationResult(
                    $"Object replication policy ID '{policy.PolicyId}' must be a unique GUID.",
                    [nameof(ObjectReplicationPolicies)]);
            }
            if (!Accounts.ContainsKey(policy.SourceAccount) || !Accounts.ContainsKey(policy.DestinationAccount))
            {
                yield return new ValidationResult(
                    $"Object replication policy '{policy.PolicyId}' references an unknown account.",
                    [nameof(ObjectReplicationPolicies)]);
            }
            if (string.Equals(policy.SourceAccount, policy.DestinationAccount, StringComparison.Ordinal))
            {
                yield return new ValidationResult(
                    $"Object replication policy '{policy.PolicyId}' must use different source and destination accounts.",
                    [nameof(ObjectReplicationPolicies)]);
            }
            if (!replicationAccountPairs.Add($"{policy.SourceAccount}\n{policy.DestinationAccount}"))
            {
                yield return new ValidationResult(
                    $"Only one object replication policy is allowed for the account pair '{policy.SourceAccount}' and '{policy.DestinationAccount}'.",
                    [nameof(ObjectReplicationPolicies)]);
            }
            if (!policy.EnabledAt.HasValue)
            {
                yield return new ValidationResult(
                    $"Object replication policy '{policy.PolicyId}' requires its durable control-plane enablement time.",
                    [nameof(ObjectReplicationPolicies)]);
            }
            else if (policy.EnabledAt.Value < EarliestObjectReplicationTime)
            {
                yield return new ValidationResult(
                    $"Object replication policy '{policy.PolicyId}' has an enablement time before 1601-01-01T00:00:00Z.",
                    [nameof(ObjectReplicationPolicies)]);
            }
            if (policy.Rules.Count is 0 or > 1000)
            {
                yield return new ValidationResult(
                    $"Object replication policy '{policy.PolicyId}' must contain between one and 1,000 rules.",
                    [nameof(ObjectReplicationPolicies)]);
            }

            var sourceCapabilities = AccountCapabilities.GetValueOrDefault(policy.SourceAccount);
            var destinationCapabilities = AccountCapabilities.GetValueOrDefault(policy.DestinationAccount);
            if (sourceCapabilities is null ||
                !sourceCapabilities.VersioningEnabled ||
                !sourceCapabilities.ChangeFeedEnabled ||
                destinationCapabilities is null ||
                !destinationCapabilities.VersioningEnabled)
            {
                yield return new ValidationResult(
                    $"Object replication policy '{policy.PolicyId}' requires source change feed and blob versioning on both accounts.",
                    [nameof(ObjectReplicationPolicies)]);
            }
            if (sourceCapabilities?.HierarchicalNamespaceEnabled == true ||
                destinationCapabilities?.HierarchicalNamespaceEnabled == true)
            {
                yield return new ValidationResult(
                    $"Object replication policy '{policy.PolicyId}' cannot use a hierarchical-namespace account.",
                    [nameof(ObjectReplicationPolicies)]);
            }

            var ruleIds = new HashSet<Guid>();
            var sourceContainers = new HashSet<string>(StringComparer.Ordinal);
            var destinationContainers = new HashSet<string>(StringComparer.Ordinal);
            foreach (var rule in policy.Rules)
            {
                if (!Guid.TryParseExact(rule.RuleId, "D", out var ruleId) || !ruleIds.Add(ruleId))
                {
                    yield return new ValidationResult(
                        $"Object replication rule ID '{rule.RuleId}' in policy '{policy.PolicyId}' must be a unique GUID.",
                        [nameof(ObjectReplicationPolicies)]);
                }
                if (string.IsNullOrWhiteSpace(rule.SourceContainer) ||
                    string.IsNullOrWhiteSpace(rule.DestinationContainer))
                {
                    yield return new ValidationResult(
                        $"Object replication policy '{policy.PolicyId}' contains a rule with a blank container name.",
                        [nameof(ObjectReplicationPolicies)]);
                }
                var uniqueSourceContainer = sourceContainers.Add(rule.SourceContainer);
                var uniqueDestinationContainer = destinationContainers.Add(rule.DestinationContainer);
                if (!uniqueSourceContainer || !uniqueDestinationContainer)
                {
                    yield return new ValidationResult(
                        $"Object replication policy '{policy.PolicyId}' uses a source or destination container in more than one rule.",
                        [nameof(ObjectReplicationPolicies)]);
                }
                if (uniqueDestinationContainer &&
                    !replicationDestinationContainers.Add(
                        $"{policy.DestinationAccount}\n{rule.DestinationContainer}"))
                {
                    yield return new ValidationResult(
                        $"Object replication destination container '{rule.DestinationContainer}' in account '{policy.DestinationAccount}' participates in more than one policy.",
                        [nameof(ObjectReplicationPolicies)]);
                }
                if (rule.PrefixMatch.Count > 10)
                {
                    yield return new ValidationResult(
                        $"Object replication rule '{rule.RuleId}' contains more than 10 prefix filters.",
                        [nameof(ObjectReplicationPolicies)]);
                }
                if (rule.PrefixMatch.Any(string.IsNullOrEmpty))
                {
                    yield return new ValidationResult(
                        $"Object replication rule '{rule.RuleId}' contains an empty prefix filter.",
                        [nameof(ObjectReplicationPolicies)]);
                }
                if (rule.PrefixMatch.Distinct(StringComparer.Ordinal).Count() != rule.PrefixMatch.Count)
                {
                    yield return new ValidationResult(
                        $"Object replication rule '{rule.RuleId}' contains duplicate prefix filters.",
                        [nameof(ObjectReplicationPolicies)]);
                }
                if (rule.MinimumCreationTime is { } minimumCreationTime &&
                    minimumCreationTime < EarliestObjectReplicationTime)
                {
                    yield return new ValidationResult(
                        $"Object replication rule '{rule.RuleId}' has a minimum creation time before 1601-01-01T00:00:00Z.",
                        [nameof(ObjectReplicationPolicies)]);
                }
            }
        }

        foreach (var source in ObjectReplicationPolicies.GroupBy(policy => policy.SourceAccount, StringComparer.Ordinal))
        {
            if (source.Count() > 2)
            {
                yield return new ValidationResult(
                    $"Object replication source account '{source.Key}' participates in more than two policies.",
                    [nameof(ObjectReplicationPolicies)]);
            }
        }
        foreach (var destination in ObjectReplicationPolicies.GroupBy(policy => policy.DestinationAccount, StringComparer.Ordinal))
        {
            if (destination.Count() > 2)
            {
                yield return new ValidationResult(
                    $"Object replication destination account '{destination.Key}' participates in more than two policies.",
                    [nameof(ObjectReplicationPolicies)]);
            }
        }

        if (EnableCrossAccountDeduplication &&
            (!TryDecodeKey(CrossAccountEncryptionKey ?? string.Empty, out var crossAccountKey) || crossAccountKey.Length < 32))
        {
            yield return new ValidationResult(
                "CrossAccountEncryptionKey must contain at least 256 bits when cross-account deduplication is enabled.",
                [nameof(CrossAccountEncryptionKey)]);
        }

        if (MinimumChunkBytes < 4 * 1024 ||
            TargetChunkBytes < MinimumChunkBytes ||
            MaximumChunkBytes < TargetChunkBytes)
        {
            yield return new ValidationResult(
                "Chunk sizes must satisfy 4 KiB <= minimum <= target <= maximum.",
                [nameof(MinimumChunkBytes), nameof(TargetChunkBytes), nameof(MaximumChunkBytes)]);
        }

        if (CompressionQuality is < 0 or > 11)
            yield return new ValidationResult("CompressionQuality must be between 0 and 11.", [nameof(CompressionQuality)]);

        if (CompressionMinimumSavingsBytes < 0)
            yield return new ValidationResult("CompressionMinimumSavingsBytes cannot be negative.", [nameof(CompressionMinimumSavingsBytes)]);

        if (BackgroundCompressionQuality is < 0 or > 11 ||
            BackgroundCompressionQuality < CompressionQuality)
        {
            yield return new ValidationResult(
                "BackgroundCompressionQuality must be between CompressionQuality and 11.",
                [nameof(BackgroundCompressionQuality)]);
        }

        if (BackgroundCompressionMinimumSavingsBytes < 0)
        {
            yield return new ValidationResult(
                "BackgroundCompressionMinimumSavingsBytes cannot be negative.",
                [nameof(BackgroundCompressionMinimumSavingsBytes)]);
        }

        if (BackgroundCompressionMinimumAge < TimeSpan.Zero)
        {
            yield return new ValidationResult(
                "BackgroundCompressionMinimumAge cannot be negative.",
                [nameof(BackgroundCompressionMinimumAge)]);
        }

        if (BackgroundCompressionChunksPerMaintenancePass <= 0)
        {
            yield return new ValidationResult(
                "BackgroundCompressionChunksPerMaintenancePass must be positive.",
                [nameof(BackgroundCompressionChunksPerMaintenancePass)]);
        }

        if (SmallChunkPackingThresholdBytes <= 0 ||
            SmallChunkPackingThresholdBytes > MaximumChunkBytes)
        {
            yield return new ValidationResult(
                "SmallChunkPackingThresholdBytes must be positive and no larger than MaximumChunkBytes.",
                [nameof(SmallChunkPackingThresholdBytes)]);
        }

        if (ChunkPackTargetBytes < 2L * SmallChunkPackingThresholdBytes)
        {
            yield return new ValidationResult(
                "ChunkPackTargetBytes must hold at least two maximum-size packed chunks.",
                [nameof(ChunkPackTargetBytes)]);
        }

        if (ChunkPackMaximumRecords <= 0)
            yield return new ValidationResult("ChunkPackMaximumRecords must be positive.", [nameof(ChunkPackMaximumRecords)]);

        if (ChunkPackSealAge < TimeSpan.Zero)
            yield return new ValidationResult("ChunkPackSealAge cannot be negative.", [nameof(ChunkPackSealAge)]);

        if (ChunkPacksPerMaintenancePass <= 0)
            yield return new ValidationResult("ChunkPacksPerMaintenancePass must be positive.", [nameof(ChunkPacksPerMaintenancePass)]);

        if (ChunkPackCompactionMinimumSavingsBytes < 0)
        {
            yield return new ValidationResult(
                "ChunkPackCompactionMinimumSavingsBytes cannot be negative.",
                [nameof(ChunkPackCompactionMinimumSavingsBytes)]);
        }

        if (ChunkPackCompactionMinimumDeadRatio is < 0 or > 1)
        {
            yield return new ValidationResult(
                "ChunkPackCompactionMinimumDeadRatio must be between zero and one.",
                [nameof(ChunkPackCompactionMinimumDeadRatio)]);
        }

        if (MaximumRequestBodyBytes <= 0)
            yield return new ValidationResult("MaximumRequestBodyBytes must be positive.", [nameof(MaximumRequestBodyBytes)]);

        foreach (var host in UrlTransferAllowedPrivateHosts)
        {
            if (string.IsNullOrWhiteSpace(host) ||
                !string.Equals(host, host.Trim(), StringComparison.Ordinal) ||
                Uri.CheckHostName(host) == UriHostNameType.Unknown)
            {
                yield return new ValidationResult(
                    "UrlTransferAllowedPrivateHosts entries must be exact DNS names or IP addresses without schemes, ports, or wildcards.",
                    [nameof(UrlTransferAllowedPrivateHosts)]);
            }
        }

        if (StandardRehydrationDelay < TimeSpan.Zero ||
            HighPriorityRehydrationDelay < TimeSpan.Zero ||
            AsyncCopyCompletionDelay < TimeSpan.Zero)
        {
            yield return new ValidationResult(
                "Rehydration and copy-completion delays cannot be negative.",
                [nameof(StandardRehydrationDelay), nameof(HighPriorityRehydrationDelay), nameof(AsyncCopyCompletionDelay)]);
        }

        if (MaintenanceScanInterval <= TimeSpan.Zero)
            yield return new ValidationResult("MaintenanceScanInterval must be positive.", [nameof(MaintenanceScanInterval)]);

        if (UncommittedBlockRetention <= TimeSpan.Zero)
            yield return new ValidationResult("UncommittedBlockRetention must be positive.", [nameof(UncommittedBlockRetention)]);

        if (AbandonedStagingRetention <= TimeSpan.Zero)
            yield return new ValidationResult("AbandonedStagingRetention must be positive.", [nameof(AbandonedStagingRetention)]);

        if (MaximumStagingFilesPerMaintenancePass <= 0)
            yield return new ValidationResult(
                "MaximumStagingFilesPerMaintenancePass must be positive.",
                [nameof(MaximumStagingFilesPerMaintenancePass)]);

        if (BlobRecordsPerMaintenancePass <= 0)
            yield return new ValidationResult(
                "BlobRecordsPerMaintenancePass must be positive.",
                [nameof(BlobRecordsPerMaintenancePass)]);

        if (ContainerRecordsPerMaintenancePass <= 0)
            yield return new ValidationResult(
                "ContainerRecordsPerMaintenancePass must be positive.",
                [nameof(ContainerRecordsPerMaintenancePass)]);

        if (UncommittedBlocksPerMaintenancePass <= 0)
            yield return new ValidationResult(
                "UncommittedBlocksPerMaintenancePass must be positive.",
                [nameof(UncommittedBlocksPerMaintenancePass)]);

        if (GarbageCollectionChunksPerMaintenancePass <= 0)
            yield return new ValidationResult(
                "GarbageCollectionChunksPerMaintenancePass must be positive.",
                [nameof(GarbageCollectionChunksPerMaintenancePass)]);

        if (IntegrityScanChunksPerMaintenancePass <= 0)
            yield return new ValidationResult(
                "IntegrityScanChunksPerMaintenancePass must be positive.",
                [nameof(IntegrityScanChunksPerMaintenancePass)]);

        if (BearerAuthentication.Enabled)
        {
            if (BearerAuthentication.ValidAudiences.Count == 0)
                yield return new ValidationResult("At least one bearer-token audience is required.", [nameof(BearerAuthentication)]);
            if (BearerAuthentication.ValidIssuers.Count == 0)
                yield return new ValidationResult("At least one bearer-token issuer is required.", [nameof(BearerAuthentication)]);
            if (string.IsNullOrWhiteSpace(BearerAuthentication.Authority) &&
                string.IsNullOrWhiteSpace(BearerAuthentication.MetadataAddress) &&
                BearerAuthentication.SymmetricSigningKeys.Count == 0)
            {
                yield return new ValidationResult(
                    "Bearer authentication requires an authority, metadata address, or an explicitly configured signing key.",
                    [nameof(BearerAuthentication)]);
            }

            foreach (var (keyId, signingKey) in BearerAuthentication.SymmetricSigningKeys)
            {
                if (string.IsNullOrWhiteSpace(keyId) || !TryDecodeKey(signingKey, out var keyBytes) || keyBytes.Length < 32)
                    yield return new ValidationResult("Bearer symmetric signing keys must be named base64 values of at least 256 bits.", [nameof(BearerAuthentication)]);
            }
        }
    }

    private static bool TryDecodeKey(string value, out byte[] bytes)
    {
        try
        {
            bytes = Convert.FromBase64String(value);
            return true;
        }
        catch (FormatException)
        {
            bytes = [];
            return false;
        }
    }

    private static bool IsValidAccountName(string? name) =>
        name is { Length: >= 3 and <= 24 } &&
        name.All(character => character is >= 'a' and <= 'z' or >= '0' and <= '9');

    public string ResolveAccountDataEncryptionKey(string account) =>
        DataEncryptionKeys.TryGetValue(account, out var key)
            ? key
            : Accounts.TryGetValue(account, out key)
                ? key
                : throw new InvalidDataException($"The data encryption key for account '{account}' is not configured.");
}

public enum SasPolicyViolationAction
{
    None,
    Log,
    Block
}

public enum SasExpirationPolicyAction
{
    Log,
    Block
}

public sealed class StorageAccountCapabilities
{
    public bool EnableHttpsTrafficOnly { get; init; }
    public bool? AllowBlobPublicAccess { get; init; }
    public bool AllowSharedKeyAccess { get; init; } = true;
    public StorageSharedKeyAccessForServices AllowSharedKeyAccessForServices { get; init; } = new();
    public bool AllowCrossTenantDelegationSas { get; init; }
    public bool RequireUserBoundUserDelegationSas { get; init; }
    public SasPolicyViolationAction RequireUserBoundUserDelegationSasAction { get; init; } =
        SasPolicyViolationAction.Log;
    public TimeSpan? SasExpirationPeriod { get; init; }
    public SasExpirationPolicyAction SasExpirationAction { get; init; } = SasExpirationPolicyAction.Log;
    public bool HierarchicalNamespaceEnabled { get; init; }
    public bool HierarchicalNamespaceBlobIndexTagsEnabled { get; init; }
    public bool HierarchicalNamespaceBlobSnapshotsEnabled { get; init; }
    public bool LastAccessTimeTrackingEnabled { get; init; }
    public bool VersioningEnabled { get; init; }
    public bool ChangeFeedEnabled { get; init; }
    public bool ImmutableStorageWithVersioningEnabled { get; init; }
    public HashSet<string> ImmutableStorageWithVersioningContainers { get; init; } = new(StringComparer.Ordinal);
}

public sealed class StorageSharedKeyAccessForServices
{
    public StorageServiceSharedKeyAccess? Blob { get; init; }
}

public sealed class StorageServiceSharedKeyAccess
{
    public bool? Enabled { get; init; }
}

public sealed class ObjectReplicationPolicyOptions
{
    public string PolicyId { get; init; } = string.Empty;
    public string SourceAccount { get; init; } = string.Empty;
    public string DestinationAccount { get; init; } = string.Empty;
    public DateTimeOffset? EnabledAt { get; init; }
    public List<ObjectReplicationRuleOptions> Rules { get; init; } = [];
}

public sealed class ObjectReplicationRuleOptions
{
    public string RuleId { get; init; } = string.Empty;
    public string SourceContainer { get; init; } = string.Empty;
    public string DestinationContainer { get; init; } = string.Empty;
    public List<string> PrefixMatch { get; init; } = [];
    public DateTimeOffset? MinimumCreationTime { get; init; }
    public bool ReplicateBlobTags { get; init; }
}

public sealed class BearerAuthenticationOptions
{
    public bool Enabled { get; init; }
    public string? Authority { get; init; }
    public string? MetadataAddress { get; init; }
    public bool RequireHttpsMetadata { get; init; } = true;
    public List<string> ValidAudiences { get; init; } = ["https://storage.azure.com/"];
    public List<string> ValidIssuers { get; init; } = [];
    public Dictionary<string, string> SymmetricSigningKeys { get; init; } = new(StringComparer.Ordinal);
    public Dictionary<string, BearerPrincipalAccess> Principals { get; init; } = new(StringComparer.Ordinal);
    public Dictionary<string, string> RolePermissions { get; init; } = new(StringComparer.Ordinal);
}

public sealed class BearerPrincipalAccess
{
    public string Permissions { get; init; } = string.Empty;
    public List<string> Accounts { get; init; } = [];
    public List<string> Containers { get; init; } = [];
    public bool CanGenerateUserDelegationKey { get; init; }
    public bool CanManageOwnership { get; init; }
    public string? UserPrincipalName { get; init; }
}
