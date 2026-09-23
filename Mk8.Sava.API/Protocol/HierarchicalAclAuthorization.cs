using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

internal static class HierarchicalAclAuthorization
{
    internal static bool IsBlobReadOperation(HttpRequest http)
    {
        var component = http.Query["comp"].ToString();
        return ((HttpMethods.IsGet(http.Method) || HttpMethods.IsHead(http.Method)) &&
                (component.Length == 0 || component.Equals("metadata", StringComparison.OrdinalIgnoreCase))) ||
               (HttpMethods.IsPost(http.Method) && component.Equals("query", StringComparison.OrdinalIgnoreCase));
    }

    internal static void EnsureAuthorizedGeneration(StorageAuthorization authorization, string generationId)
    {
        if (authorization.AclReadChecked &&
            !string.Equals(authorization.AclAuthorizedGenerationId, generationId, StringComparison.Ordinal))
        {
            throw AzureStorageException.AuthorizationFailure();
        }
    }

    internal static async Task<string?> EnsureReadAsync(
        MetadataStore metadata,
        HttpRequest http,
        StorageRequestContext request,
        string objectId,
        IReadOnlySet<string> groups,
        string signedPermissions,
        CancellationToken cancellationToken)
    {
        if (request.ResourceKind != StorageResourceKind.Blob ||
            request.Container is null ||
            request.Blob is null ||
            !IsBlobReadOperation(http) ||
            !signedPermissions.Contains('r', StringComparison.Ordinal))
        {
            throw AzureStorageException.AuthorizationFailure();
        }

        var root = await metadata.GetContainerAsync(
            request.Account,
            request.Container,
            includeDeleted: false,
            cancellationToken);
        if (root is null ||
            !PosixAccessControl.Allows(root.Acl, root.Owner, root.Group, objectId, groups, 'x'))
            throw AzureStorageException.AuthorizationFailure();

        var blob = await metadata.GetBlobAsync(
            request.Account,
            request.Container,
            request.Blob,
            request.VersionId,
            request.Snapshot,
            includeDeleted: false,
            cancellationToken);
        var separator = request.Blob.IndexOf('/', StringComparison.Ordinal);
        var indexedParents = false;
        while (separator > 0)
        {
            var name = request.Blob[..separator];
            var directory = await metadata.GetBlobAsync(
                request.Account,
                request.Container,
                name,
                versionId: null,
                snapshot: null,
                includeDeleted: false,
                cancellationToken);
            if (directory is null && blob is not null && !indexedParents)
            {
                await metadata.EnsureHierarchicalDirectoriesAsync(
                    request.Account,
                    request.Container,
                    cancellationToken);
                indexedParents = true;
                directory = await metadata.GetBlobAsync(
                    request.Account,
                    request.Container,
                    name,
                    versionId: null,
                    snapshot: null,
                    includeDeleted: false,
                    cancellationToken);
            }
            if (directory is null && blob is null)
                return null;
            if (directory is null || !directory.IsDirectory ||
                !PosixAccessControl.Allows(
                    directory.Acl,
                    directory.Owner,
                    directory.Group,
                    objectId,
                    groups,
                    'x'))
            {
                throw AzureStorageException.AuthorizationFailure();
            }
            separator = request.Blob.IndexOf('/', separator + 1);
        }

        if (blob is not null &&
            (blob.IsDirectory ||
             !PosixAccessControl.Allows(
                 blob.Acl,
                 blob.Owner,
                 blob.Group,
                 objectId,
                 groups,
                 'r')))
        {
            throw AzureStorageException.AuthorizationFailure();
        }
        return blob?.GenerationId;
    }

}
