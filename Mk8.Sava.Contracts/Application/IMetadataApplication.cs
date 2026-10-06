using Mk8.Sava.Storage;

namespace Mk8.Sava.Application;

public interface IMetadataApplication
{
    Task<ContainerRecord?> GetContainerAsync(string account, string name, bool includeDeleted,
        CancellationToken cancellationToken);
    Task<BlobRecord?> GetBlobAsync(string account, string container, string name,
        string? versionId, string? snapshot, bool includeDeleted, CancellationToken cancellationToken);
    Task<ServiceProperties> GetServicePropertiesAsync(string account, CancellationToken cancellationToken);
    Task EnsureHierarchicalDirectoriesAsync(string account, string container, CancellationToken cancellationToken);
    Task RecordUserDelegationRoleGrantsAsync(UserDelegationKeyIssue issue, CancellationToken cancellationToken);
    Task<IReadOnlyList<UserDelegationKeyCandidate>> ReadUserDelegationKeyCandidatesAsync(
        UserDelegationKeyIdentity identity, CancellationToken cancellationToken);
    Task<UserDelegationRoleGrants?> ReadUserDelegationRoleGrantsAsync(
        string keyFingerprint, CancellationToken cancellationToken);
}
