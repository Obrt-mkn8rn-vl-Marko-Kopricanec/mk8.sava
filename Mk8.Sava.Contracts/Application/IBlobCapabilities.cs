namespace Mk8.Sava.Application;

public interface IBlobCapabilities
{
    bool AllowsAnonymousPublicAccess(string account);
    bool IsHierarchicalNamespaceEnabled(string account);
    bool SupportsBlobIndexTags(string account);
    bool SupportsBlobSnapshots(string account);
    bool IsLastAccessTimeTrackingEnabled(string account);
    bool IsImmutableStorageWithVersioningEnabled(string account, string container);
    bool IsObjectReplicationDestinationContainer(string account, string container);
}
