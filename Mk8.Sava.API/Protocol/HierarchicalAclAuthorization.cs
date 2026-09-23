using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

internal static class HierarchicalAclAuthorization
{
    internal static char? GetParentMutationPermission(HttpRequest http, StorageRequestContext request)
    {
        if (request.ResourceKind != StorageResourceKind.Blob ||
            request.Container is null ||
            request.Blob is null ||
            http.Query.ContainsKey("snapshot") ||
            http.Query.ContainsKey("versionid") ||
            http.Query.ContainsKey("deletetype"))
            return null;

        var component = http.Query["comp"].ToString();
        if (HttpMethods.IsPut(http.Method) &&
            (component.Length == 0 ||
             component.Equals("block", StringComparison.OrdinalIgnoreCase) ||
             component.Equals("blocklist", StringComparison.OrdinalIgnoreCase)))
            return 'w';
        return HttpMethods.IsDelete(http.Method) && component.Length == 0 ? 'd' : null;
    }

    internal static bool IsAppendOperation(HttpRequest http, StorageRequestContext request) =>
        request.ResourceKind == StorageResourceKind.Blob &&
        request.Container is not null &&
        request.Blob is not null &&
        HttpMethods.IsPut(http.Method) &&
        http.Query["comp"].ToString().Equals("appendblock", StringComparison.OrdinalIgnoreCase) &&
        !http.Query.ContainsKey("snapshot") &&
        !http.Query.ContainsKey("versionid");

    internal static async Task<string> EnsureAppendAsync(
        MetadataStore metadata,
        HttpRequest http,
        StorageRequestContext request,
        string objectId,
        IReadOnlySet<string> groups,
        string signedPermissions,
        CancellationToken cancellationToken)
    {
        if (!IsAppendOperation(http, request) ||
            !signedPermissions.Contains('a', StringComparison.Ordinal) &&
            !signedPermissions.Contains('w', StringComparison.Ordinal))
            throw AzureStorageException.AuthorizationFailure();

        var root = await metadata.GetContainerAsync(
            request.Account, request.Container!, includeDeleted: false, cancellationToken);
        if (root is null ||
            !PosixAccessControl.Allows(root.Acl, root.Owner, root.Group, objectId, groups, 'x'))
            throw AzureStorageException.AuthorizationFailure();

        var name = request.Blob!;
        var separator = name.IndexOf('/', StringComparison.Ordinal);
        while (separator > 0)
        {
            var parent = await metadata.GetBlobAsync(
                request.Account, request.Container!, name[..separator],
                versionId: null, snapshot: null, includeDeleted: false, cancellationToken);
            if (parent is null || !parent.IsDirectory ||
                !PosixAccessControl.Allows(parent.Acl, parent.Owner, parent.Group, objectId, groups, 'x'))
                throw AzureStorageException.AuthorizationFailure();
            separator = name.IndexOf('/', separator + 1);
        }

        var blob = await metadata.GetBlobAsync(
            request.Account, request.Container!, name,
            versionId: null, snapshot: null, includeDeleted: false, cancellationToken);
        if (blob is null || blob.IsDirectory ||
            !PosixAccessControl.Allows(blob.Acl, blob.Owner, blob.Group, objectId, groups, 'r') ||
            !PosixAccessControl.Allows(blob.Acl, blob.Owner, blob.Group, objectId, groups, 'w'))
            throw AzureStorageException.AuthorizationFailure();
        return blob.GenerationId;
    }

    internal static async Task EnsureParentMutationAsync(
        MetadataStore metadata,
        HttpRequest http,
        StorageRequestContext request,
        string objectId,
        IReadOnlySet<string> groups,
        string signedPermissions,
        CancellationToken cancellationToken)
    {
        var permission = GetParentMutationPermission(http, request);
        if (permission is null || !signedPermissions.Contains(permission.Value, StringComparison.Ordinal))
            throw AzureStorageException.AuthorizationFailure();

        var root = await metadata.GetContainerAsync(
            request.Account, request.Container!, includeDeleted: false, cancellationToken);
        if (root is null ||
            !PosixAccessControl.Allows(root.Acl, root.Owner, root.Group, objectId, groups, 'x'))
            throw AzureStorageException.AuthorizationFailure();

        var name = request.Blob!;
        var lastSeparator = name.LastIndexOf('/');
        if (lastSeparator == 0)
            throw AzureStorageException.AuthorizationFailure();
        if (lastSeparator < 0)
        {
            if (!PosixAccessControl.Allows(root.Acl, root.Owner, root.Group, objectId, groups, 'w'))
                throw AzureStorageException.AuthorizationFailure();
            return;
        }

        var separator = name.IndexOf('/', StringComparison.Ordinal);
        while (separator > 0)
        {
            var parent = await metadata.GetBlobAsync(
                request.Account, request.Container!, name[..separator],
                versionId: null, snapshot: null, includeDeleted: false, cancellationToken);
            if (parent is null || !parent.IsDirectory ||
                !PosixAccessControl.Allows(parent.Acl, parent.Owner, parent.Group, objectId, groups, 'x') ||
                separator == lastSeparator &&
                !PosixAccessControl.Allows(parent.Acl, parent.Owner, parent.Group, objectId, groups, 'w'))
                throw AzureStorageException.AuthorizationFailure();
            separator = name.IndexOf('/', separator + 1);
        }
    }

    internal static bool IsDirectoryListOperation(HttpRequest http, StorageRequestContext request)
    {
        if (request.ResourceKind != StorageResourceKind.Container ||
            !HttpMethods.IsGet(http.Method) ||
            !http.Query["comp"].ToString().Equals("list", StringComparison.OrdinalIgnoreCase) ||
            http.Query["delimiter"].ToString() != "/")
            return false;

        var prefix = http.Query["prefix"].ToString();
        if (prefix.Length > 0 &&
            (!prefix.EndsWith("/", StringComparison.Ordinal) ||
             prefix.StartsWith("/", StringComparison.Ordinal) ||
             prefix.Contains("//", StringComparison.Ordinal)))
            return false;

        if (http.Query["showonly"].ToString().Equals("deleted", StringComparison.OrdinalIgnoreCase))
            return false;
        return !http.Query["include"].ToString().Split(',', StringSplitOptions.TrimEntries)
            .Any(value => value.Equals("deleted", StringComparison.OrdinalIgnoreCase) ||
                          value.Equals("deletedwithversions", StringComparison.OrdinalIgnoreCase) ||
                          value.Equals("tags", StringComparison.OrdinalIgnoreCase));
    }

    internal static async Task EnsureDirectoryListAsync(
        MetadataStore metadata,
        HttpRequest http,
        StorageRequestContext request,
        string objectId,
        IReadOnlySet<string> groups,
        string signedPermissions,
        CancellationToken cancellationToken)
    {
        if (!IsDirectoryListOperation(http, request) ||
            request.Container is null ||
            !signedPermissions.Contains('l', StringComparison.Ordinal))
            throw AzureStorageException.AuthorizationFailure();

        var root = await metadata.GetContainerAsync(
            request.Account, request.Container, includeDeleted: false, cancellationToken);
        if (root is null ||
            !PosixAccessControl.Allows(root.Acl, root.Owner, root.Group, objectId, groups, 'x'))
            throw AzureStorageException.AuthorizationFailure();

        var prefix = http.Query["prefix"].ToString();
        if (prefix.Length == 0)
        {
            if (!PosixAccessControl.Allows(root.Acl, root.Owner, root.Group, objectId, groups, 'r'))
                throw AzureStorageException.AuthorizationFailure();
            return;
        }

        await metadata.EnsureHierarchicalDirectoriesAsync(
            request.Account, request.Container, cancellationToken);
        var target = prefix[..^1];
        var separator = target.IndexOf('/', StringComparison.Ordinal);
        while (separator > 0)
        {
            var parent = await metadata.GetBlobAsync(
                request.Account, request.Container, target[..separator],
                versionId: null, snapshot: null, includeDeleted: false, cancellationToken);
            if (parent is null || !parent.IsDirectory ||
                !PosixAccessControl.Allows(parent.Acl, parent.Owner, parent.Group, objectId, groups, 'x'))
                throw AzureStorageException.AuthorizationFailure();
            separator = target.IndexOf('/', separator + 1);
        }

        var directory = await metadata.GetBlobAsync(
            request.Account, request.Container, target,
            versionId: null, snapshot: null, includeDeleted: false, cancellationToken);
        if (directory is null || !directory.IsDirectory ||
            !PosixAccessControl.Allows(directory.Acl, directory.Owner, directory.Group, objectId, groups, 'r') ||
            !PosixAccessControl.Allows(directory.Acl, directory.Owner, directory.Group, objectId, groups, 'x'))
            throw AzureStorageException.AuthorizationFailure();
    }

    internal static bool IsBlobReadOperation(HttpRequest http)
    {
        var component = http.Query["comp"].ToString();
        return ((HttpMethods.IsGet(http.Method) || HttpMethods.IsHead(http.Method)) &&
                (component.Length == 0 || component.Equals("metadata", StringComparison.OrdinalIgnoreCase))) ||
               (HttpMethods.IsPost(http.Method) && component.Equals("query", StringComparison.OrdinalIgnoreCase));
    }

    internal static void EnsureAuthorizedGeneration(StorageAuthorization authorization, string generationId)
    {
        if ((authorization.AclReadChecked || authorization.AclAppendChecked) &&
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
