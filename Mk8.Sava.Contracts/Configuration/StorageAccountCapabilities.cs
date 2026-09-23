namespace Mk8.Sava.Configuration;

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
