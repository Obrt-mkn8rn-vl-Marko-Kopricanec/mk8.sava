using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

internal static class HierarchicalAclAuthorization
{
    internal static async Task EnsureReadAsync(
        MetadataStore metadata,
        HttpRequest http,
        StorageRequestContext request,
        string objectId,
        string signedPermissions,
        CancellationToken cancellationToken)
    {
        if (request.ResourceKind != StorageResourceKind.Blob ||
            request.Container is null ||
            request.Blob is null ||
            !HttpMethods.IsGet(http.Method) && !HttpMethods.IsHead(http.Method) ||
            !string.IsNullOrEmpty(http.Query["comp"].ToString()) ||
            !signedPermissions.Contains('r', StringComparison.Ordinal))
        {
            throw AzureStorageException.AuthorizationFailure();
        }

        var root = await metadata.GetContainerAsync(
            request.Account,
            request.Container,
            includeDeleted: false,
            cancellationToken);
        if (root is null || !HasOwnerPermission(root.Owner, "rwxr-x---", objectId, 'x'))
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
                return;
            if (directory is null || !directory.IsDirectory ||
                !HasOwnerPermission(directory.Owner, directory.Permissions, objectId, 'x'))
            {
                throw AzureStorageException.AuthorizationFailure();
            }
            separator = request.Blob.IndexOf('/', separator + 1);
        }

        if (blob is not null &&
            (blob.IsDirectory || !HasOwnerPermission(blob.Owner, blob.Permissions, objectId, 'r')))
        {
            throw AzureStorageException.AuthorizationFailure();
        }
    }

    private static bool HasOwnerPermission(
        string owner,
        string permissions,
        string objectId,
        char permission)
    {
        var index = "rwx".IndexOf(permission);
        return index >= 0 &&
               permissions.Length >= 3 &&
               permissions[index] == permission &&
               string.Equals(owner, objectId, StringComparison.OrdinalIgnoreCase);
    }
}
