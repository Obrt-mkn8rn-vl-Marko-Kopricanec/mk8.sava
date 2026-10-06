using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Hosting;

internal sealed class GatewayBlobCapabilities(IOptions<SavaOptions> options) : IBlobCapabilities
{
    private readonly SavaOptions _options = options.Value;

    public bool AllowsAnonymousPublicAccess(string account) =>
        _options.AccountCapabilities.TryGetValue(account, out var capabilities)
            ? capabilities.AllowBlobPublicAccess ?? _options.AllowAnonymousPublicAccess
            : _options.AllowAnonymousPublicAccess;

    public bool IsHierarchicalNamespaceEnabled(string account) =>
        _options.AccountCapabilities.TryGetValue(account, out var capabilities) &&
        capabilities.HierarchicalNamespaceEnabled;

    public bool SupportsBlobIndexTags(string account) => !IsHierarchicalNamespaceEnabled(account) ||
        _options.AccountCapabilities.TryGetValue(account, out var capabilities) &&
        capabilities.HierarchicalNamespaceBlobIndexTagsEnabled;

    public bool SupportsBlobSnapshots(string account) => !IsHierarchicalNamespaceEnabled(account) ||
        _options.AccountCapabilities.TryGetValue(account, out var capabilities) &&
        capabilities.HierarchicalNamespaceBlobSnapshotsEnabled;

    public bool IsLastAccessTimeTrackingEnabled(string account) =>
        _options.AccountCapabilities.TryGetValue(account, out var capabilities) &&
        capabilities.LastAccessTimeTrackingEnabled;

    public bool IsImmutableStorageWithVersioningEnabled(string account, string container) =>
        _options.AccountCapabilities.TryGetValue(account, out var capabilities) &&
        (capabilities.ImmutableStorageWithVersioningEnabled ||
         capabilities.ImmutableStorageWithVersioningContainers.Contains(container));

    public bool IsObjectReplicationDestinationContainer(string account, string container) =>
        _options.ObjectReplicationPolicies.Any(policy =>
            string.Equals(policy.DestinationAccount, account, StringComparison.Ordinal) &&
            policy.Rules.Any(rule => string.Equals(rule.DestinationContainer, container, StringComparison.Ordinal)));
}
