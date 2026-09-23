using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Storage;

public sealed class BlobService(
    MetadataStore metadata,
    ChunkStore chunks,
    LeaseService leases,
    StorageAnalyticsService analytics,
    IStorageTelemetry telemetry,
    IStorageFaultInjector faultInjector,
    IOptions<SavaOptions> configuredOptions) : IDisposable
{
    private static readonly JsonSerializerOptions AclManifestJsonOptions = new(JsonSerializerDefaults.Web)
    {
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow
    };

    private readonly SavaOptions _options = configuredOptions.Value;
    private readonly SemaphoreSlim _maintenanceGate = new(1, 1);
    private readonly SemaphoreSlim _hierarchicalDirectoryGate = new(1, 1);
    private readonly ConcurrentDictionary<ContainerKey, byte> _indexedHierarchicalContainers = [];
    private string? _integrityCursor;
    private int _integrityChecked;
    private int _integrityVerified;
    private int _integrityCustomerKey;
    private int _integrityMissing;
    private int _integrityCorrupt;
    private string? _blobMaintenanceCursor;
    private ContainerKey? _containerMaintenanceCursor;
    private string? _garbageCollectionCursor;
    private string? _recompressionCursor;
    private string? _packCompactionCursor;
    private ObjectReplicationStateKey? _objectReplicationStateCursor;
    private StoragePhysicalUsage? _lastPhysicalUsage;
    private long _lastPhysicalScanTicks;
    private long _lastPhysicalScanUnixSeconds;

    public void Dispose()
    {
        _maintenanceGate.Dispose();
        _hierarchicalDirectoryGate.Dispose();
        GC.SuppressFinalize(this);
    }

    public bool AllowsAnonymousPublicAccess(string account) =>
        _options.AccountCapabilities.TryGetValue(account, out var capabilities)
            ? capabilities.AllowBlobPublicAccess ?? _options.AllowAnonymousPublicAccess
            : _options.AllowAnonymousPublicAccess;

    public bool IsHierarchicalNamespaceEnabled(string account) =>
        _options.AccountCapabilities.TryGetValue(account, out var capabilities) &&
        capabilities.HierarchicalNamespaceEnabled;

    public bool SupportsBlobIndexTags(string account) =>
        !IsHierarchicalNamespaceEnabled(account) ||
        _options.AccountCapabilities.TryGetValue(account, out var capabilities) &&
        capabilities.HierarchicalNamespaceBlobIndexTagsEnabled;

    public bool SupportsBlobSnapshots(string account) =>
        !IsHierarchicalNamespaceEnabled(account) ||
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
            policy.Rules.Any(rule =>
                string.Equals(rule.DestinationContainer, container, StringComparison.Ordinal)));

    public async Task ApplyConfiguredAccountCapabilitiesAsync(CancellationToken cancellationToken = default)
    {
        await metadata.EnsureAccountNamespaceModesAsync(
            _options.Accounts.Keys.ToDictionary(
                account => account,
                IsHierarchicalNamespaceEnabled,
                StringComparer.Ordinal),
            cancellationToken).ConfigureAwait(false);

        foreach (var (account, capabilities) in _options.AccountCapabilities)
        {
            if (!capabilities.VersioningEnabled)
                continue;

            var properties = await metadata.GetServicePropertiesAsync(account, cancellationToken).ConfigureAwait(false);
            if (!properties.VersioningEnabled)
            {
                await metadata.PutServicePropertiesAsync(
                    account,
                    properties with { VersioningEnabled = true },
                    cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public async Task<int> ApplyHierarchicalAclManifestAsync(
        string manifestPath,
        CancellationToken cancellationToken)
    {
        const int maximumBytes = 4 * 1024 * 1024;
        var fullPath = Path.GetFullPath(manifestPath);
        var file = new FileStream(
            fullPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous);
        await using var fileDisposal = file.ConfigureAwait(false);
        if (file.Length > maximumBytes)
            throw new InvalidDataException("The HNS ACL manifest exceeds the 4 MiB limit.");
        var json = new byte[maximumBytes + 1];
        var length = 0;
        while (length < json.Length)
        {
            var read = await file.ReadAsync(json.AsMemory(length), cancellationToken).ConfigureAwait(false);
            if (read == 0)
                break;
            length += read;
        }
        if (length > maximumBytes)
            throw new InvalidDataException("The HNS ACL manifest exceeds the 4 MiB limit.");
        var manifest = JsonSerializer.Deserialize<HierarchicalAclManifest>(
            json.AsSpan(0, length), AclManifestJsonOptions)
            ?? throw new InvalidDataException("The HNS ACL manifest is empty.");
        if (manifest.SchemaVersion != 1 || manifest.Entries is not { Count: >= 1 and <= 4096 })
            throw new InvalidDataException("The HNS ACL manifest has an unsupported schema or entry count.");

        var targets = new HashSet<string>(StringComparer.Ordinal);
        for (var entryIndex = 0; entryIndex < manifest.Entries.Count; entryIndex++)
        {
            var entry = manifest.Entries[entryIndex];
            if (entry is null ||
                string.IsNullOrEmpty(entry.Account) ||
                string.IsNullOrEmpty(entry.Container) ||
                entry.Path is null ||
                string.IsNullOrEmpty(entry.AccessAcl) ||
                entry.AccessAcl.Length > 4096 ||
                entry.Path.Length > 1024 ||
                entry.Path.Any(char.IsControl) ||
                !string.IsNullOrEmpty(entry.Path) &&
                (entry.Path.StartsWith('/') ||
                 entry.Path.EndsWith('/')) ||
                !_options.Accounts.ContainsKey(entry.Account) ||
                !IsHierarchicalNamespaceEnabled(entry.Account))
            {
                throw new InvalidDataException("The HNS ACL manifest contains an invalid target or ACL.");
            }
            PosixAccessControl.ValidateStoredAcl(entry.AccessAcl, isDirectory: true);
            if (!targets.Add($"{entry.Account}\u001f{entry.Container}\u001f{entry.Path}"))
                throw new InvalidDataException("The HNS ACL manifest contains a duplicate target.");
        }

        await metadata.ApplyHierarchicalAclEntriesAsync(manifest.Entries, cancellationToken).ConfigureAwait(false);
        return manifest.Entries.Count;
    }

    public async Task<IReadOnlyList<ContainerRecord>> ListContainersAsync(
        string account,
        bool includeDeleted,
        CancellationToken cancellationToken)
    {
        var containers = await metadata.ListContainersAsync(account, includeDeleted, cancellationToken).ConfigureAwait(false);
        containers = containers
            .Where(container => !string.Equals(container.Name, StorageAnalyticsService.LogsContainerName, StringComparison.Ordinal))
            .ToArray();
        if (!includeDeleted || !containers.Any(item => item.DeletedAt.HasValue && !item.DeleteRetentionUntil.HasValue))
            return containers.Select(EffectiveContainer).ToArray();
        var properties = await metadata.GetServicePropertiesAsync(account, cancellationToken).ConfigureAwait(false);
        return containers.Select(container => container.DeletedAt.HasValue && !container.DeleteRetentionUntil.HasValue
            ? EffectiveContainer(container with
            {
                DeleteRetentionUntil = container.DeletedAt.Value.AddDays(properties.ContainerSoftDeleteRetentionDays)
            })
            : EffectiveContainer(container)).ToArray();
    }

    internal async Task<ContainerListPage> ListContainersPageAsync(
        string account,
        bool includeDeleted,
        bool includeSystem,
        string prefix,
        string marker,
        int maximum,
        CancellationToken cancellationToken)
    {
        var page = await metadata.ListContainersPageAsync(
            account,
            includeDeleted,
            includeSystem,
            prefix,
            marker,
            maximum,
            cancellationToken).ConfigureAwait(false);
        var needsRetention = includeDeleted && page.Items.Any(item =>
            item.DeletedAt.HasValue && !item.DeleteRetentionUntil.HasValue);
        var properties = needsRetention
            ? await metadata.GetServicePropertiesAsync(account, cancellationToken).ConfigureAwait(false)
            : null;
        return page with
        {
            Items = page.Items.Select(container =>
                    properties is not null && container.DeletedAt.HasValue && !container.DeleteRetentionUntil.HasValue
                        ? EffectiveContainer(container with
                        {
                            DeleteRetentionUntil = container.DeletedAt.Value.AddDays(
                                properties.ContainerSoftDeleteRetentionDays)
                        })
                        : EffectiveContainer(container))
                .ToArray()
        };
    }

    public async Task<ContainerRecord> CreateContainerAsync(
        string account,
        string name,
        IReadOnlyDictionary<string, string> userMetadata,
        string? publicAccess,
        string? defaultEncryptionScope,
        bool preventEncryptionScopeOverride,
        CancellationToken cancellationToken,
        string? creatorObjectId = null)
    {
        ArgumentNullException.ThrowIfNull(name);
        ValidateContainerName(name);
        if (publicAccess is not null && publicAccess is not ("blob" or "container"))
            throw AzureStorageException.InvalidHeader("x-ms-blob-public-access", publicAccess);
        EnsurePublicAccessAllowed(account, publicAccess);

        var now = metadata.GetUtcNow();
        var container = new ContainerRecord
        {
            Account = account,
            Name = name,
            Revision = MetadataStore.NewRevision(),
            ETag = MetadataStore.NewETag(),
            CreatedAt = now,
            LastModified = now,
            Owner = IsHierarchicalNamespaceEnabled(account) ? creatorObjectId ?? "$superuser" : "$superuser",
            Group = IsHierarchicalNamespaceEnabled(account) ? creatorObjectId ?? "$superuser" : "$superuser",
            Metadata = userMetadata,
            PublicAccess = publicAccess,
            DefaultEncryptionScope = defaultEncryptionScope,
            PreventEncryptionScopeOverride = preventEncryptionScopeOverride,
            ImmutableStorageWithVersioningEnabled =
                IsImmutableStorageWithVersioningEnabled(account, name)
        };

        if (!await metadata.TryCreateContainerAsync(container, cancellationToken).ConfigureAwait(false))
        {
            var existing = await metadata.GetContainerAsync(account, name, includeDeleted: true, cancellationToken).ConfigureAwait(false);
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                existing?.DeletedAt is null ? "ContainerAlreadyExists" : "ContainerBeingDeleted",
                existing?.DeletedAt is null
                    ? "The specified container already exists."
                    : "The specified container is being deleted.");
        }

        return container;
    }

    public async Task<ContainerRecord> GetContainerAsync(
        string account,
        string name,
        bool includeDeleted,
        CancellationToken cancellationToken)
    {
        var container = await metadata.GetContainerAsync(account, name, includeDeleted, cancellationToken).ConfigureAwait(false)
                        ?? throw AzureStorageException.ContainerNotFound();
        return EffectiveContainer(container);
    }

    public async Task<ContainerRecord> SetContainerMetadataAsync(
        ContainerRecord current,
        IReadOnlyDictionary<string, string> userMetadata,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        var updated = current with
        {
            Metadata = userMetadata,
            Revision = MetadataStore.NewRevision(),
            ETag = MetadataStore.NewETag(),
            LastModified = metadata.GetUtcNow()
        };
        await metadata.PutContainerAsync(updated, current.Revision, cancellationToken).ConfigureAwait(false);
        return updated;
    }

    public async Task<ContainerRecord> SetContainerAclAsync(
        ContainerRecord current,
        string? publicAccess,
        IReadOnlyDictionary<string, StoredAccessPolicy> policies,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        if (publicAccess is not null && publicAccess is not ("blob" or "container"))
            throw AzureStorageException.InvalidHeader("x-ms-blob-public-access", publicAccess);
        EnsurePublicAccessAllowed(current.Account, publicAccess);
        var updated = current with
        {
            PublicAccess = publicAccess,
            AccessPolicies = policies,
            Revision = MetadataStore.NewRevision(),
            ETag = MetadataStore.NewETag(),
            LastModified = metadata.GetUtcNow()
        };
        await metadata.PutContainerAsync(updated, current.Revision, cancellationToken).ConfigureAwait(false);
        return updated;
    }

    public async Task DeleteContainerAsync(ContainerRecord current, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        if (string.Equals(current.Name, StorageAnalyticsService.LogsContainerName, StringComparison.Ordinal))
        {
            throw new AzureStorageException(
                StatusCodes.Status403Forbidden,
                "ContainerOperationFailure",
                "The account being accessed does not have sufficient permissions to execute this operation.");
        }
        if (current.ImmutableStorageWithVersioningEnabled)
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "ContainerImmutableStorageWithVersioningEnabled",
                "The requested operation is not allowed because the container has immutable storage with versioning enabled.");
        }
        EnsureContainerMutable(current);
        var properties = await metadata.GetServicePropertiesAsync(current.Account, cancellationToken).ConfigureAwait(false);
        if (properties.ContainerSoftDeleteEnabled)
        {
            var deletedAt = metadata.GetUtcNow();
            var deleted = current with
            {
                Revision = MetadataStore.NewRevision(),
                DeletedAt = deletedAt,
                DeleteRetentionUntil = deletedAt.AddDays(properties.ContainerSoftDeleteRetentionDays),
                DeletedVersion = Guid.NewGuid().ToString("N"),
                ETag = MetadataStore.NewETag(),
                LastModified = deletedAt
            };
            await metadata.PutContainerAsync(deleted, current.Revision, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await metadata.DeleteContainerPermanentlyAsync(current.Account, current.Name, current.Revision, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task<ContainerRecord> RestoreContainerAsync(
        string account,
        string sourceName,
        string destinationName,
        string deletedVersion,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(destinationName);
        ValidateContainerName(destinationName);
        var current = await GetContainerAsync(account, sourceName, includeDeleted: true, cancellationToken).ConfigureAwait(false);
        if (current.DeletedAt is null || !string.Equals(current.DeletedVersion, deletedVersion, StringComparison.Ordinal))
            throw AzureStorageException.ContainerNotFound();
        var properties = await metadata.GetServicePropertiesAsync(account, cancellationToken).ConfigureAwait(false);
        var retentionUntil = current.DeleteRetentionUntil ??
                             current.DeletedAt.Value.AddDays(properties.ContainerSoftDeleteRetentionDays);
        if (retentionUntil <= metadata.GetUtcNow())
        {
            throw AzureStorageException.ContainerNotFound();
        }

        var restored = current with
        {
            Name = destinationName,
            Revision = MetadataStore.NewRevision(),
            DeletedAt = null,
            DeleteRetentionUntil = null,
            DeletedVersion = null,
            ETag = MetadataStore.NewETag(),
            LastModified = metadata.GetUtcNow()
        };
        if (!await metadata.TryRestoreContainerAsync(sourceName, restored, current.Revision, cancellationToken).ConfigureAwait(false))
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "ContainerAlreadyExists",
                "The specified container already exists.");
        }
        return restored;
    }

    public async Task<ContainerRecord> RenameContainerAsync(
        string account,
        string sourceName,
        string destinationName,
        string? sourceLeaseId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sourceName);
        ArgumentNullException.ThrowIfNull(destinationName);
        ValidateContainerName(sourceName);
        ValidateContainerName(destinationName);
        var current = await GetContainerAsync(account, sourceName, includeDeleted: false, cancellationToken).ConfigureAwait(false);
        if (string.Equals(current.Name, StorageAnalyticsService.LogsContainerName, StringComparison.Ordinal))
            throw AzureStorageException.ContainerNotFound();
        EnsureContainerMutable(current);
        leases.EnsureWriteAccess(
            current.Lease,
            sourceLeaseId,
            "container",
            "x-ms-source-lease-id");

        var renamed = current with
        {
            Name = destinationName,
            Revision = MetadataStore.NewRevision(),
            ETag = MetadataStore.NewETag(),
            LastModified = metadata.GetUtcNow()
        };
        if (!await metadata.TryRenameContainerAsync(
                sourceName,
                renamed,
                current.Revision,
                cancellationToken).ConfigureAwait(false))
        {
            var existing = await metadata.GetContainerAsync(
                account,
                destinationName,
                includeDeleted: true,
                cancellationToken).ConfigureAwait(false);
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                existing?.DeletedAt is null ? "ContainerAlreadyExists" : "ContainerBeingDeleted",
                existing?.DeletedAt is null
                    ? "The specified container already exists."
                    : "The specified container is being deleted.");
        }

        _indexedHierarchicalContainers.TryRemove(new ContainerKey(account, sourceName), out _);
        _indexedHierarchicalContainers.TryRemove(new ContainerKey(account, destinationName), out _);
        return renamed;
    }

    public async Task<ContainerRecord> SetContainerLeaseAsync(
        ContainerRecord current,
        LeaseRecord lease,
        bool updateProperties,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        var updated = current with
        {
            Lease = lease,
            Revision = MetadataStore.NewRevision(),
            ETag = updateProperties ? MetadataStore.NewETag() : current.ETag,
            LastModified = updateProperties ? metadata.GetUtcNow() : current.LastModified
        };
        await metadata.PutContainerAsync(updated, current.Revision, cancellationToken).ConfigureAwait(false);
        return updated;
    }

    public async Task<IReadOnlyList<BlobRecord>> ListBlobsAsync(
        string account,
        string container,
        bool includeVersions,
        bool includeSnapshots,
        bool includeDeleted,
        CancellationToken cancellationToken)
    {
        if (includeSnapshots && !SupportsBlobSnapshots(account))
            throw AzureStorageException.BlobOperationNotSupported();
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken).ConfigureAwait(false);
        await EnsureHierarchicalDirectoryIndexAsync(account, container, cancellationToken).ConfigureAwait(false);
        var blobs = await metadata.ListBlobsAsync(account, container, includeVersions, includeSnapshots, includeDeleted, cancellationToken).ConfigureAwait(false);
        var effective = new List<BlobRecord>(blobs.Count);
        var properties = includeDeleted && blobs.Any(item => item.DeletedAt.HasValue && !item.DeleteRetentionUntil.HasValue)
            ? await metadata.GetServicePropertiesAsync(account, cancellationToken).ConfigureAwait(false)
            : null;
        foreach (var blob in blobs)
        {
            var copy = await CompleteCopyIfDueAsync(blob, cancellationToken).ConfigureAwait(false);
            var rehydrated = await CompleteRehydrationIfDueAsync(copy, cancellationToken).ConfigureAwait(false);
            effective.Add(properties is not null && rehydrated.DeletedAt.HasValue && !rehydrated.DeleteRetentionUntil.HasValue
                ? EffectiveBlob(rehydrated with
                {
                    DeleteRetentionUntil = rehydrated.DeletedAt.Value.AddDays(properties.BlobSoftDeleteRetentionDays)
                })
                : EffectiveBlob(rehydrated));
        }
        return effective;
    }

    internal async Task<BlobListPage> ListBlobsPageAsync(
        string account,
        string container,
        BlobListShowOnly showOnly,
        bool includeVersions,
        bool includeSnapshots,
        bool includeDeleted,
        bool includeUncommitted,
        string prefix,
        string startFrom,
        string endBefore,
        string delimiter,
        BlobListingMarker marker,
        int maximum,
        CancellationToken cancellationToken)
    {
        if (includeSnapshots && !SupportsBlobSnapshots(account))
            throw AzureStorageException.BlobOperationNotSupported();
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken).ConfigureAwait(false);
        await EnsureHierarchicalDirectoryIndexAsync(account, container, cancellationToken).ConfigureAwait(false);
        var page = await metadata.ListBlobsPageAsync(
            account,
            container,
            IsHierarchicalNamespaceEnabled(account),
            showOnly,
            includeVersions,
            includeSnapshots,
            includeDeleted,
            includeUncommitted,
            prefix,
            startFrom,
            endBefore,
            delimiter,
            marker.Cursor,
            marker.LegacyOffset,
            maximum,
            cancellationToken).ConfigureAwait(false);
        ServiceProperties? properties = null;
        if (includeDeleted && page.Items.Any(item =>
                item.Blob is { DeletedAt: not null, DeleteRetentionUntil: null }))
        {
            properties = await metadata.GetServicePropertiesAsync(account, cancellationToken).ConfigureAwait(false);
        }

        var effective = new List<BlobListEntry>(page.Items.Count);
        foreach (var item in page.Items)
        {
            if (item.Blob is null)
            {
                effective.Add(item);
                continue;
            }

            var copy = await CompleteCopyIfDueAsync(item.Blob, cancellationToken).ConfigureAwait(false);
            var rehydrated = await CompleteRehydrationIfDueAsync(copy, cancellationToken).ConfigureAwait(false);
            var blob = properties is not null && rehydrated.DeletedAt.HasValue &&
                       !rehydrated.DeleteRetentionUntil.HasValue
                ? EffectiveBlob(rehydrated with
                {
                    DeleteRetentionUntil = rehydrated.DeletedAt.Value.AddDays(
                        properties.BlobSoftDeleteRetentionDays)
                })
                : EffectiveBlob(rehydrated);
            effective.Add(item with { Blob = blob });
        }
        return new BlobListPage(effective, page.HasMore);
    }

    internal async Task<TaggedBlobPage> FindBlobsByTagsPageAsync(
        string account,
        BlobTagFilter filter,
        BlobTagCursor? cursor,
        int maximum,
        CancellationToken cancellationToken)
    {
        var page = await metadata.FindBlobsByTagsPageAsync(
            account,
            filter,
            cursor,
            maximum,
            cancellationToken).ConfigureAwait(false);
        var effective = new List<BlobRecord>(page.Items.Count);
        foreach (var blob in page.Items)
        {
            var copy = await CompleteCopyIfDueAsync(blob, cancellationToken).ConfigureAwait(false);
            effective.Add(EffectiveBlob(await CompleteRehydrationIfDueAsync(copy, cancellationToken).ConfigureAwait(false)));
        }
        return new TaggedBlobPage(effective, page.HasMore);
    }

    public async Task<BlobRecord> GetBlobAsync(
        string account,
        string container,
        string name,
        string? versionId,
        string? snapshot,
        bool includeDeleted,
        CancellationToken cancellationToken)
    {
        ValidateBlobName(name);
        if (snapshot is not null && !SupportsBlobSnapshots(account))
            throw AzureStorageException.BlobOperationNotSupported();
        if (versionId is not null && IsHierarchicalNamespaceEnabled(account))
            throw AzureStorageException.BlobNotFound();
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken).ConfigureAwait(false);
        await EnsureHierarchicalDirectoryIndexAsync(account, container, cancellationToken).ConfigureAwait(false);
        var blob = await metadata.GetBlobAsync(account, container, name, versionId, snapshot, includeDeleted, cancellationToken).ConfigureAwait(false)
                   ?? throw AzureStorageException.BlobNotFound();
        blob = await CompleteCopyIfDueAsync(blob, cancellationToken).ConfigureAwait(false);
        return EffectiveBlob(await CompleteRehydrationIfDueAsync(blob, cancellationToken).ConfigureAwait(false));
    }

    public async Task<BlobRecord> RecordDataAccessAsync(
        BlobRecord current,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var now = metadata.GetUtcNow();
            var smartTier = string.Equals(current.AccessTier, "Smart", StringComparison.Ordinal);
            var trackLastAccess = IsLastAccessTimeTrackingEnabled(current.Account) &&
                                  (!current.LastAccessedAt.HasValue ||
                                   now - current.LastAccessedAt.Value >= TimeSpan.FromHours(24));
            if (!smartTier && !trackLastAccess)
                return current;

            var movedToHot = smartTier &&
                             !string.Equals(current.SmartAccessTier, "Hot", StringComparison.Ordinal);
            var updated = current with
            {
                Revision = MetadataStore.NewRevision(),
                SmartAccessTier = smartTier ? "Hot" : current.SmartAccessTier,
                SmartTierLastAccessedAt = smartTier ? now : current.SmartTierLastAccessedAt,
                LastAccessedAt = trackLastAccess ? now : current.LastAccessedAt,
                AccessTierChangedAt = movedToHot ? now : current.AccessTierChangedAt
            };
            try
            {
                await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken).ConfigureAwait(false);
                return updated;
            }
            catch (StorageConcurrencyException)
            {
                current = await metadata.GetBlobAsync(
                              current.Account,
                              current.Container,
                              current.Name,
                              current.VersionId,
                              current.Snapshot,
                              includeDeleted: false,
                              cancellationToken).ConfigureAwait(false)
                          ?? throw AzureStorageException.BlobNotFound();
            }
        }
    }

    public async Task<BlobRecord> PutBlockBlobAsync(
        string account,
        string container,
        string name,
        Stream source,
        BlobWriteOptions options,
        LeaseRecord destinationLease,
        string? expectedGeneration,
        string? expectedRevision,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        ValidateBlobName(name);
        ValidateEncryptionContext(account, options);
        options = await ApplyContainerEncryptionPolicyAsync(account, container, options, cancellationToken).ConfigureAwait(false);
        var encryption = EncryptionOf(options);
        using var content = await chunks.StorePinnedAsync(account, encryption, source, cancellationToken).ConfigureAwait(false);
        if (options.GenerateContentMd5 && options.Http.ContentMd5 is null)
        {
            options = options with
            {
                Http = options.Http with { ContentMd5 = content.ContentMd5 }
            };
        }
        var now = metadata.GetUtcNow();
        var proposed = NewBlob(account, container, name, BlobKind.BlockBlob, content.Manifest, options, now) with
        {
            Lease = leases.ResetAfterBlobWrite(destinationLease)
        };
        return await metadata.PublishBlobAsync(
            proposed,
            expectedGeneration,
            expectedRevision,
            IsHierarchicalNamespaceEnabled(account),
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<BlobRecord> CreateAppendBlobAsync(
        string account,
        string container,
        string name,
        BlobWriteOptions options,
        LeaseRecord destinationLease,
        string? expectedGeneration,
        string? expectedRevision,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        ValidateBlobName(name);
        ValidateEncryptionContext(account, options);
        options = await ApplyContainerEncryptionPolicyAsync(account, container, options, cancellationToken).ConfigureAwait(false);
        var now = metadata.GetUtcNow();
        var proposed = NewBlob(account, container, name, BlobKind.AppendBlob, chunks.Empty(account, EncryptionOf(options)), options, now) with
        {
            Lease = leases.ResetAfterBlobWrite(destinationLease)
        };
        return await metadata.PublishBlobAsync(
            proposed,
            expectedGeneration,
            expectedRevision,
            IsHierarchicalNamespaceEnabled(account),
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<BlobRecord> CreatePageBlobAsync(
        string account,
        string container,
        string name,
        long length,
        BlobWriteOptions options,
        long sequenceNumber,
        LeaseRecord destinationLease,
        string? expectedGeneration,
        string? expectedRevision,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        EnsureBlobKindSupported(account, BlobKind.PageBlob);
        ValidateBlobName(name);
        ValidateEncryptionContext(account, options);
        const long maximumPageBlobBytes = 8L * 1024 * 1024 * 1024 * 1024;
        if (length > maximumPageBlobBytes)
            throw new RequestBodyTooLargeException(maximumPageBlobBytes);
        if (length < 0 || length % 512 != 0)
            throw AzureStorageException.InvalidHeader("x-ms-blob-content-length", length.ToString(CultureInfo.InvariantCulture));
        if (sequenceNumber < 0)
            throw AzureStorageException.InvalidHeader("x-ms-blob-sequence-number", sequenceNumber.ToString(CultureInfo.InvariantCulture));
        options = await ApplyContainerEncryptionPolicyAsync(account, container, options, cancellationToken).ConfigureAwait(false);
        var now = metadata.GetUtcNow();
        var proposed = NewBlob(account, container, name, BlobKind.PageBlob, chunks.Sparse(account, EncryptionOf(options), length), options, now) with
        {
            Lease = leases.ResetAfterBlobWrite(destinationLease),
            SequenceNumber = sequenceNumber
        };
        return await metadata.PublishBlobAsync(
            proposed,
            expectedGeneration,
            expectedRevision,
            IsHierarchicalNamespaceEnabled(account),
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<BlobEncryption> StageBlockAsync(
        string account,
        string container,
        string name,
        string blockId,
        Stream source,
        BlobEncryption encryption,
        CancellationToken cancellationToken)
    {
        ValidateBlobName(name);
        var blockIdLength = ValidateBlockId(blockId);
        var containerRecord = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken).ConfigureAwait(false);
        var current = await metadata.GetBlobAsync(account, container, name, null, null, includeDeleted: false, cancellationToken).ConfigureAwait(false);
        encryption = ApplyContainerEncryptionPolicy(containerRecord, encryption, current);
        if (current is not null)
            EnsureNoPendingCopy(current);
        if (current is not null && current.Kind != BlobKind.BlockBlob)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
        if (current is not null && !chunks.IsInDomain(account, encryption, current.Content))
            throw AzureStorageException.BlobUsesCustomerSpecifiedEncryption();
        var staged = await metadata.ListStagedBlocksAsync(account, container, name, cancellationToken).ConfigureAwait(false);
        if (staged.Count >= BlobServiceLimits.MaximumUncommittedBlockCount &&
            staged.All(item => !string.Equals(item.BlockId, blockId, StringComparison.Ordinal)))
            throw new AzureStorageException(StatusCodes.Status409Conflict, "BlockCountExceedsLimit", "The uncommitted block count exceeds the maximum permitted value.");
        string? existingId = null;
        if (staged.Count > 0)
            existingId = staged[0].BlockId;
        else if (current?.CommittedBlocks.Count > 0)
            existingId = current.CommittedBlocks[0].Id;
        if (existingId is not null && ValidateBlockId(existingId) != blockIdLength)
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidBlobOrBlock", "All block IDs for a blob must have the same length.");
        if (staged.Any(item => !chunks.IsInDomain(account, encryption, item.Content)))
            throw AzureStorageException.BlobUsesCustomerSpecifiedEncryption();
        using var content = await chunks.StorePinnedAsync(account, encryption, source, cancellationToken).ConfigureAwait(false);
        await metadata.PutStagedBlockAsync(new StagedBlockRecord
        {
            Account = account,
            Container = container,
            BlobName = name,
            BlockId = blockId,
            Content = content.Manifest,
            CreatedAt = metadata.GetUtcNow()
        }, cancellationToken).ConfigureAwait(false);
        if (current is not null)
        {
            var written = PrepareBlobWrite(current);
            if (written.Lease != current.Lease)
            {
                try
                {
                    await metadata.PutBlobRecordAsync(
                        written with { Revision = MetadataStore.NewRevision() },
                        current.Revision,
                        cancellationToken).ConfigureAwait(false);
                }
                catch (StorageConcurrencyException)
                {
                    // A concurrent mutation owns the newer lease state.
                }
            }
        }
        return encryption;
    }

    public async Task<BlobRecord> CommitBlockListAsync(
        string account,
        string container,
        string name,
        IReadOnlyList<BlockListEntry> blockList,
        BlobWriteOptions options,
        string? expectedGeneration,
        string? expectedRevision,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(blockList);
        if (blockList.Count > BlobServiceLimits.MaximumCommittedBlockCount)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "BlockCountExceedsLimit", "The block list may not contain more than 50,000 blocks.");

        ValidateEncryptionContext(account, options);
        var current = await metadata.GetBlobAsync(account, container, name, null, null, includeDeleted: false, cancellationToken).ConfigureAwait(false);
        options = await ApplyContainerEncryptionPolicyAsync(
            account,
            container,
            options,
            cancellationToken,
            current).ConfigureAwait(false);
        var staged = await metadata.ListStagedBlocksAsync(account, container, name, cancellationToken).ConfigureAwait(false);
        var encryption = EncryptionOf(options);
        var stagedById = staged.ToDictionary(item => item.BlockId, StringComparer.Ordinal);
        var committedById = current?.CommittedBlocks
            .GroupBy(item => item.Id, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.Last(), StringComparer.Ordinal)
            ?? new Dictionary<string, CommittedBlockRecord>(StringComparer.Ordinal);

        var selected = new List<CommittedBlockRecord>(blockList.Count);
        int? blockIdLength = null;
        foreach (var entry in blockList)
        {
            var blockId = entry.Id;
            var currentIdLength = ValidateBlockId(blockId);
            if (blockIdLength.HasValue && currentIdLength != blockIdLength.Value)
                throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidBlobOrBlock", "All block IDs for a blob must have the same length.");
            blockIdLength = currentIdLength;
            StagedBlockRecord? stagedBlock;
            CommittedBlockRecord? committedBlock;
            var resolved = entry.Mode switch
            {
                BlockListMode.Uncommitted when stagedById.TryGetValue(blockId, out stagedBlock) => stagedBlock.Content,
                BlockListMode.Committed when committedById.TryGetValue(blockId, out committedBlock) => committedBlock.Content,
                BlockListMode.Latest when stagedById.TryGetValue(blockId, out stagedBlock) => stagedBlock.Content,
                BlockListMode.Latest when committedById.TryGetValue(blockId, out committedBlock) => committedBlock.Content,
                _ => null
            };
            if (resolved is null)
                throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidBlockList", "The specified block list is invalid.");
            if (!chunks.IsInDomain(account, encryption, resolved))
                throw AzureStorageException.BlobUsesCustomerSpecifiedEncryption();
            selected.Add(new CommittedBlockRecord(blockId, resolved));
        }

        var content = await chunks.ComposeAsync(account, encryption, selected.Select(item => item.Content).ToArray(), cancellationToken).ConfigureAwait(false);
        using var contentPin = chunks.Pin(content);
        var now = metadata.GetUtcNow();
        var proposed = NewBlob(account, container, name, BlobKind.BlockBlob, content, options, now) with
        {
            Lease = current is null ? LeaseRecord.Available : leases.ResetAfterBlobWrite(current.Lease),
            CommittedBlocks = selected
        };
        return await metadata.PublishBlockListAsync(
            proposed,
            expectedGeneration,
            expectedRevision,
            staged,
            IsHierarchicalNamespaceEnabled(account),
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<BlobRecord> AppendBlockAsync(
        BlobRecord current,
        Stream source,
        long? expectedPosition,
        long? expectedMaximumSize,
        BlobEncryption encryption,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        var container = await GetContainerAsync(current.Account, current.Container, includeDeleted: false, cancellationToken).ConfigureAwait(false);
        encryption = ApplyContainerEncryptionPolicy(container, encryption, current);
        EnsureNoPendingCopy(current);
        EnsureBlobMutable(current);
        current = PrepareBlobWrite(current);
        if (current.Kind != BlobKind.AppendBlob)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
        if (current.IsSealed)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "BlobIsSealed", "The specified append blob is sealed.");
        if (current.AppendBlockCount >= BlobServiceLimits.MaximumCommittedBlockCount)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "BlockCountExceedsLimit", "The append block count exceeds the maximum permitted value.");
        if (expectedPosition.HasValue && expectedPosition.Value != current.Content.Length)
            throw new AzureStorageException(StatusCodes.Status412PreconditionFailed, "AppendPositionConditionNotMet", "The append position condition specified was not met.");

        if (!chunks.IsInDomain(current.Account, encryption, current.Content))
            throw AzureStorageException.BlobUsesCustomerSpecifiedEncryption();
        using var appended = await chunks.StorePinnedAsync(current.Account, encryption, source, cancellationToken).ConfigureAwait(false);
        if (appended.Manifest.Length > 100L * 1024 * 1024)
            throw new AzureStorageException(StatusCodes.Status413PayloadTooLarge, "RequestBodyTooLarge", "An append block cannot exceed 100 MiB.");
        if (expectedMaximumSize.HasValue && current.Content.Length + appended.Manifest.Length > expectedMaximumSize.Value)
        {
            throw new AzureStorageException(
                StatusCodes.Status412PreconditionFailed,
                "MaxBlobSizeConditionNotMet",
                "The max blob size condition specified was not met.");
        }
        var content = await chunks.ComposeAsync(current.Account, encryption, [current.Content, appended.Manifest], cancellationToken).ConfigureAwait(false);
        using var contentPin = chunks.Pin(content);
        var now = metadata.GetUtcNow();
        var updated = current with
        {
            Revision = MetadataStore.NewRevision(),
            Content = content,
            ETag = MetadataStore.NewETag(),
            LastModified = now,
            LastAccessedAt = IsLastAccessTimeTrackingEnabled(current.Account)
                ? now
                : current.LastAccessedAt,
            Lease = current.Lease,
            AppendBlockCount = checked(current.AppendBlockCount + 1),
            Copy = null
        };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken).ConfigureAwait(false);
        return updated;
    }

    public async Task<BlobRecord> PutPageAsync(
        BlobRecord current,
        long start,
        long end,
        Stream? source,
        bool clear,
        BlobEncryption encryption,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        EnsureFlatNamespace(current.Account);
        var container = await GetContainerAsync(current.Account, current.Container, includeDeleted: false, cancellationToken).ConfigureAwait(false);
        encryption = ApplyContainerEncryptionPolicy(container, encryption, current);
        EnsureNoPendingCopy(current);
        EnsureBlobMutable(current);
        current = PrepareBlobWrite(current);
        if (current.Kind != BlobKind.PageBlob)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
        if (start < 0 || end < start || start % 512 != 0 || (end + 1) % 512 != 0 || end >= current.Content.Length)
            throw AzureStorageException.InvalidPageRange();
        if (!clear && end - start + 1 > 4L * 1024 * 1024)
            throw AzureStorageException.InvalidHeader("x-ms-range", $"bytes={start}-{end}");
        if (!chunks.IsInDomain(current.Account, encryption, current.Content))
            throw AzureStorageException.BlobUsesCustomerSpecifiedEncryption();

        StoredContent content;
        try
        {
            content = await chunks.ReplaceRangePinnedAsync(
                current.Account,
                encryption,
                current.Content,
                start,
                end - start + 1,
                source,
                clear,
                cancellationToken).ConfigureAwait(false);
        }
        catch (EndOfStreamException)
        {
            throw AzureStorageException.InvalidPageRange();
        }
        using (content)
        {
            var now = metadata.GetUtcNow();
            var updated = current with
            {
                Revision = MetadataStore.NewRevision(),
                Content = content.Manifest,
                PageRanges = UpdatePageRanges(current.PageRanges, start, end, clear),
                ETag = MetadataStore.NewETag(),
                LastModified = now,
                LastAccessedAt = IsLastAccessTimeTrackingEnabled(current.Account)
                    ? now
                    : current.LastAccessedAt
            };
            await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken).ConfigureAwait(false);
            return updated;
        }
    }

    public async Task<PageRangeDiff> GetPageRangeDiffAsync(
        BlobRecord current,
        BlobRecord previous,
        BlobEncryption encryption,
        long start,
        long end,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(previous);
        ArgumentNullException.ThrowIfNull(current);
        if (current.Kind != BlobKind.PageBlob || previous.Kind != BlobKind.PageBlob)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
        if (current.CreatedAt != previous.CreatedAt ||
            !string.Equals(current.Content.Domain, previous.Content.Domain, StringComparison.Ordinal))
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "BlobOverwritten",
                "The page blob was overwritten after the previous snapshot was created.");
        }
        if (start < 0 || end < start || start % 512 != 0 || (end + 1) % 512 != 0 || end >= current.Content.Length)
            throw AzureStorageException.InvalidHeader("x-ms-range", $"bytes={start}-{end}");
        if (!chunks.IsInDomain(current.Account, encryption, current.Content))
            throw AzureStorageException.BlobUsesCustomerSpecifiedEncryption();

        var changed = new List<PageRange>();
        var cleared = new List<PageRange>();
        var currentRanges = ClipPageRanges(current.PageRanges, start, end);
        var previousRanges = ClipPageRanges(previous.PageRanges, start, Math.Min(end, previous.Content.Length - 1));
        var currentIndex = 0;
        var previousIndex = 0;
        var cursor = start;
        var rangeEndExclusive = checked(end + 1);

        while (cursor < rangeEndExclusive)
        {
            while (currentIndex < currentRanges.Length && currentRanges[currentIndex].End < cursor)
                currentIndex++;
            while (previousIndex < previousRanges.Length && previousRanges[previousIndex].End < cursor)
                previousIndex++;

            var currentAllocated = currentIndex < currentRanges.Length && currentRanges[currentIndex].Start <= cursor;
            var previousAllocated = previousIndex < previousRanges.Length && previousRanges[previousIndex].Start <= cursor;
            var currentBoundary = currentAllocated
                ? checked(currentRanges[currentIndex].End + 1)
                : currentIndex < currentRanges.Length ? currentRanges[currentIndex].Start : rangeEndExclusive;
            var previousBoundary = previousAllocated
                ? checked(previousRanges[previousIndex].End + 1)
                : previousIndex < previousRanges.Length ? previousRanges[previousIndex].Start : rangeEndExclusive;
            var boundary = Math.Min(rangeEndExclusive, Math.Min(currentBoundary, previousBoundary));

            if (currentAllocated && !previousAllocated)
            {
                AddMergedPageRange(changed, cursor, boundary - 1);
            }
            else if (!currentAllocated && previousAllocated)
            {
                AddMergedPageRange(cleared, cursor, boundary - 1);
            }
            else if (currentAllocated)
            {
                await CompareAllocatedPagesAsync(
                    current,
                    previous,
                    encryption,
                    cursor,
                    boundary,
                    changed,
                    cancellationToken).ConfigureAwait(false);
            }

            cursor = boundary;
        }

        return new PageRangeDiff(changed, cleared);
    }

    public async Task WriteContentAsync(
        BlobRecord blob,
        BlobEncryption encryption,
        long offset,
        long length,
        Stream destination,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(blob);
        EnsureNoPendingCopy(blob);
        if (string.Equals(blob.AccessTier, "Archive", StringComparison.Ordinal))
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "BlobArchived",
                "This operation is not permitted on an archived blob.");
        }
        await chunks.WriteRangeAsync(blob.Content, encryption, offset, length, destination, cancellationToken).ConfigureAwait(false);
    }

    public async Task<BlobRecord> SetBlobMetadataAsync(
        BlobRecord current,
        IReadOnlyDictionary<string, string> userMetadata,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        EnsureNoPendingCopy(current);
        EnsureBlobMutable(current);
        current = PrepareBlobWrite(current);
        var updated = current with
        {
            Metadata = userMetadata,
            Revision = MetadataStore.NewRevision(),
            ETag = MetadataStore.NewETag(),
            LastModified = metadata.GetUtcNow(),
            Copy = null
        };
        var properties = await metadata.GetServicePropertiesAsync(current.Account, cancellationToken).ConfigureAwait(false);
        if (properties.VersioningEnabled && !IsHierarchicalNamespaceEnabled(current.Account))
        {
            return await metadata.PublishBlobAsync(
                updated with
                {
                    GenerationId = Guid.NewGuid().ToString("N"),
                    VersionId = null,
                    Snapshot = null
                },
                current.GenerationId,
                current.Revision,
                hierarchicalNamespace: false,
                cancellationToken).ConfigureAwait(false);
        }
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken).ConfigureAwait(false);
        return updated;
    }

    public async Task<BlobRecord> SetBlobTagsAsync(
        BlobRecord current,
        IReadOnlyDictionary<string, string> tags,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(tags);
        ArgumentNullException.ThrowIfNull(current);
        EnsureNoPendingCopy(current);
        EnsureBlobMutable(current);
        current = PrepareBlobWrite(current);
        if (tags.Count > 10)
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "TagsTooLarge", "The number of blob tags exceeds the permitted limit.");
        var updated = current with { Tags = tags, Copy = null, Revision = MetadataStore.NewRevision() };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken).ConfigureAwait(false);
        return updated;
    }

    public async Task<BlobRecord> SetBlobPropertiesAsync(
        BlobRecord current,
        BlobHttpProperties http,
        long? resizeTo,
        long? sequenceNumber,
        string? sequenceAction,
        BlobEncryption encryption,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        EnsureNoPendingCopy(current);
        EnsureBlobMutable(current);
        current = PrepareBlobWrite(current);
        var nextSequence = current.SequenceNumber;
        if (sequenceNumber.HasValue || sequenceAction is not null)
        {
            if (current.Kind != BlobKind.PageBlob)
                throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The sequence number is only valid for page blobs.");
            if (sequenceNumber < 0)
                throw AzureStorageException.InvalidHeader("x-ms-blob-sequence-number", sequenceNumber.Value.ToString(CultureInfo.InvariantCulture));
            nextSequence = (sequenceAction is null ? null : ProtocolLowercase(sequenceAction)) switch
            {
                "max" when sequenceNumber.HasValue => Math.Max(nextSequence, sequenceNumber.Value),
                "increment" => checked(nextSequence + 1),
                "update" when sequenceNumber.HasValue => sequenceNumber.Value,
                null when sequenceNumber.HasValue => sequenceNumber.Value,
                _ => throw AzureStorageException.InvalidHeader("x-ms-sequence-number-action", sequenceAction)
            };
        }

        var content = current.Content;
        StoredContent? resized = null;
        if (resizeTo.HasValue)
        {
            const long maximumPageBlobBytes = 8L * 1024 * 1024 * 1024 * 1024;
            if (current.Kind != BlobKind.PageBlob || resizeTo < 0 || resizeTo > maximumPageBlobBytes || resizeTo % 512 != 0)
                throw AzureStorageException.InvalidHeader("x-ms-blob-content-length", resizeTo.Value.ToString(CultureInfo.InvariantCulture));
            resized = await chunks.ResizeSparsePinnedAsync(current.Account, encryption, current.Content, resizeTo.Value, cancellationToken).ConfigureAwait(false);
            content = resized.Manifest;
        }

        var now = metadata.GetUtcNow();
        var updated = current with
        {
            Revision = MetadataStore.NewRevision(),
            Content = content,
            Http = http,
            SequenceNumber = nextSequence,
            PageRanges = resizeTo.HasValue
                ? current.PageRanges
                    .Where(range => range.Start < resizeTo.Value)
                    .Select(range => new PageRange(range.Start, Math.Min(range.End, resizeTo.Value - 1)))
                    .ToList()
                : current.PageRanges,
            Copy = null,
            ETag = MetadataStore.NewETag(),
            LastModified = now,
            LastAccessedAt = resizeTo.HasValue && IsLastAccessTimeTrackingEnabled(current.Account)
                ? now
                : current.LastAccessedAt
        };
        try
        {
            await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken).ConfigureAwait(false);
            return updated;
        }
        finally
        {
            resized?.Dispose();
        }
    }

    public async Task<BlobRecord> SealAppendBlobAsync(
        BlobRecord current,
        long? expectedPosition,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        EnsureNoPendingCopy(current);
        EnsureBlobMutable(current);
        current = PrepareBlobWrite(current);
        if (current.Kind != BlobKind.AppendBlob)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
        if (expectedPosition.HasValue && expectedPosition.Value != current.Content.Length)
            throw new AzureStorageException(StatusCodes.Status412PreconditionFailed, "AppendPositionConditionNotMet", "The append position condition specified was not met.");
        var updated = current with
        {
            IsSealed = true,
            Revision = MetadataStore.NewRevision(),
            ETag = MetadataStore.NewETag(),
            LastModified = metadata.GetUtcNow(),
            Copy = null
        };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken).ConfigureAwait(false);
        return updated;
    }

    public async Task<BlobTierUpdate> SetTierAsync(
        BlobRecord current,
        string tier,
        string? rehydratePriority,
        bool allowRehydratePriorityUpdate,
        bool supportsCustomerProvidedKey,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(tier);
        ArgumentNullException.ThrowIfNull(current);
        EnsureNoPendingCopy(current);
        current = PrepareBlobWrite(current);
        if (current.Kind != BlobKind.BlockBlob)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
        if (current.EncryptionScope is not null)
            throw EncryptionScopeTierChangeNotSupported();
        if (current.CustomerProvidedKeySha256 is not null && !supportsCustomerProvidedKey)
            throw AzureStorageException.BlobUsesCustomerSpecifiedEncryption();
        if (tier is not ("Hot" or "Cool" or "Cold" or "Smart" or "Archive"))
            throw AzureStorageException.InvalidHeader("x-ms-access-tier", tier);
        if (rehydratePriority is not null && rehydratePriority is not ("Standard" or "High"))
            throw AzureStorageException.InvalidHeader("x-ms-rehydrate-priority", rehydratePriority);

        var now = metadata.GetUtcNow();
        BlobRecord updated;
        var pending = false;
        if (string.Equals(current.AccessTier, "Archive", StringComparison.Ordinal) && !string.Equals(tier, "Archive", StringComparison.Ordinal))
        {
            if (!current.IsCurrent || current.Snapshot is not null)
                throw new AzureStorageException(StatusCodes.Status409Conflict, "BlobArchived", "This operation is not permitted on an archived blob.");

            var requestedStatus = $"rehydrate-pending-to-{ProtocolLowercase(tier)}";
            if (current.ArchiveStatus is not null && !string.Equals(current.ArchiveStatus, requestedStatus, StringComparison.Ordinal))
            {
                throw new AzureStorageException(
                    StatusCodes.Status409Conflict,
                    "BlobBeingRehydrated",
                    "This operation is not permitted because the blob is being rehydrated.");
            }

            var priority = current.RehydratePriority switch
            {
                "High" => "High",
                "Standard" when allowRehydratePriorityUpdate && string.Equals(rehydratePriority, "High", StringComparison.Ordinal) => "High",
                "Standard" => "Standard",
                _ => rehydratePriority ?? "Standard"
            };
            var delay = string.Equals(priority, "High", StringComparison.Ordinal) ? _options.HighPriorityRehydrationDelay : _options.StandardRehydrationDelay;
            var completion = now.Add(delay);
            if (current.RehydrateCompleteAt.HasValue && current.RehydrateCompleteAt.Value < completion)
            {
                completion = current.RehydrateCompleteAt.Value;
            }
            updated = current with
            {
                ArchiveStatus = requestedStatus,
                RehydratePriority = priority,
                RehydrateCompleteAt = completion,
                Revision = MetadataStore.NewRevision()
            };
            pending = true;
        }
        else
        {
            if (current.ArchiveStatus is not null)
            {
                throw new AzureStorageException(
                    StatusCodes.Status409Conflict,
                    "BlobBeingRehydrated",
                    "This operation is not permitted because the blob is being rehydrated.");
            }
            updated = current with
            {
                AccessTier = tier,
                AccessTierInferred = false,
                SmartAccessTier = string.Equals(tier, "Smart", StringComparison.Ordinal) ? "Hot" : null,
                SmartTierLastAccessedAt = string.Equals(tier, "Smart", StringComparison.Ordinal) ? now : null,
                ArchiveStatus = null,
                RehydratePriority = null,
                RehydrateCompleteAt = null,
                Revision = MetadataStore.NewRevision(),
                AccessTierChangedAt = now
            };
        }
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken).ConfigureAwait(false);
        return new BlobTierUpdate(updated, pending);
    }

    public async Task<BlobRecord> SetExpiryAsync(BlobRecord current, DateTimeOffset? expiresAt, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        if (!IsHierarchicalNamespaceEnabled(current.Account) || current.IsDirectory)
            throw AzureStorageException.BlobOperationNotSupported();
        EnsureNoPendingCopy(current);
        EnsureBlobMutable(current);
        current = PrepareBlobWrite(current);
        if (expiresAt <= metadata.GetUtcNow())
            throw AzureStorageException.InvalidHeader("x-ms-expiry-time", expiresAt.Value.ToString("R", CultureInfo.InvariantCulture));
        var updated = current with
        {
            ExpiresAt = expiresAt,
            Copy = null,
            Revision = MetadataStore.NewRevision(),
            ETag = MetadataStore.NewETag(),
            LastModified = metadata.GetUtcNow()
        };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken).ConfigureAwait(false);
        return updated;
    }

    public async Task<BlobRecord> SetBlobImmutabilityPolicyAsync(
        BlobRecord current,
        DateTimeOffset expiresOn,
        bool locked,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        await EnsureVersionLevelImmutabilityEnabledAsync(current, cancellationToken).ConfigureAwait(false);
        EnsureNoPendingCopy(current);
        current = PrepareBlobWrite(current);
        if (expiresOn <= metadata.GetUtcNow())
            throw AzureStorageException.InvalidHeader("x-ms-immutability-policy-until-date", expiresOn.ToString("R", CultureInfo.InvariantCulture));
        if (current.ImmutabilityLocked)
        {
            if (!locked || current.ImmutabilityUntil.HasValue && expiresOn < current.ImmutabilityUntil.Value)
                throw BlobImmutableDueToPolicy();
        }

        var updated = current with
        {
            ImmutabilityUntil = expiresOn,
            ImmutabilityLocked = locked,
            Copy = null,
            Revision = MetadataStore.NewRevision()
        };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken).ConfigureAwait(false);
        return updated;
    }

    public async Task<BlobRecord> DeleteBlobImmutabilityPolicyAsync(
        BlobRecord current,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        await EnsureVersionLevelImmutabilityEnabledAsync(current, cancellationToken).ConfigureAwait(false);
        EnsureNoPendingCopy(current);
        current = PrepareBlobWrite(current);
        if (current.ImmutabilityLocked)
            throw BlobImmutableDueToPolicy();
        var updated = current with
        {
            ImmutabilityUntil = null,
            ImmutabilityLocked = false,
            Copy = null,
            Revision = MetadataStore.NewRevision()
        };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken).ConfigureAwait(false);
        return updated;
    }

    public async Task<BlobRecord> SetBlobLegalHoldAsync(
        BlobRecord current,
        bool hasLegalHold,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        await EnsureVersionLevelImmutabilityEnabledAsync(current, cancellationToken).ConfigureAwait(false);
        EnsureNoPendingCopy(current);
        current = PrepareBlobWrite(current);
        var updated = current with
        {
            HasLegalHold = hasLegalHold,
            Copy = null,
            Revision = MetadataStore.NewRevision()
        };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken).ConfigureAwait(false);
        return updated;
    }

    public async Task<BlobRecord> SetBlobLeaseAsync(
        BlobRecord current,
        LeaseRecord lease,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        EnsureNoPendingCopy(current);
        var updated = current with
        {
            Lease = lease,
            Revision = MetadataStore.NewRevision()
        };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken).ConfigureAwait(false);
        return updated;
    }

    public Task<BlobRecord> CreateSnapshotAsync(
        BlobRecord current,
        IReadOnlyDictionary<string, string>? snapshotMetadata,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        EnsureNoPendingCopy(current);
        if (!SupportsBlobSnapshots(current.Account) || current.IsDirectory)
            throw AzureStorageException.BlobOperationNotSupported();
        if (string.Equals(current.AccessTier, "Archive", StringComparison.Ordinal))
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "BlobArchived",
                "This operation is not permitted on an archived blob.");
        }
        return metadata.CreateSnapshotAsync(
            current,
            snapshotMetadata,
            metadata.GetUtcNow(),
            IsHierarchicalNamespaceEnabled(current.Account),
            cancellationToken);
    }

    public async Task DeleteBlobAsync(
        BlobRecord current,
        bool hasExplicitSnapshotOrVersion,
        BlobDeleteSnapshotsOption deleteSnapshots,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        EnsureNoPendingCopy(current);
        EnsureBlobMutable(current);
        if ((current.Snapshot is not null || deleteSnapshots != BlobDeleteSnapshotsOption.Unspecified) &&
            !SupportsBlobSnapshots(current.Account))
        {
            throw AzureStorageException.BlobOperationNotSupported();
        }
        if (current.IsDirectory &&
            await metadata.HasActiveBlobDescendantsAsync(
                current.Account,
                current.Container,
                current.Name,
                cancellationToken).ConfigureAwait(false))
        {
            throw AzureStorageException.DirectoryIsNotEmpty();
        }
        var records = await metadata.ListBlobFamilyAsync(
            current.Account,
            current.Container,
            current.Name,
            includeDeleted: true,
            cancellationToken).ConfigureAwait(false);
        var relatedSnapshots = records.Where(item => string.Equals(item.Name, current.Name, StringComparison.Ordinal) && item.Snapshot is not null && !item.IsDeleted).ToArray();
        if (!hasExplicitSnapshotOrVersion &&
            relatedSnapshots.Length > 0 &&
            deleteSnapshots == BlobDeleteSnapshotsOption.Unspecified)
        {
            throw new AzureStorageException(StatusCodes.Status409Conflict, "SnapshotsPresent", "This operation is not permitted while the blob has snapshots.");
        }

        var targets = new List<BlobRecord>();
        if (hasExplicitSnapshotOrVersion)
        {
            targets.Add(current);
        }
        else
        {
            if (deleteSnapshots != BlobDeleteSnapshotsOption.Only)
                targets.Add(current);
            if (deleteSnapshots is BlobDeleteSnapshotsOption.Include or BlobDeleteSnapshotsOption.Only)
                targets.AddRange(relatedSnapshots);
        }

        for (var targetIndex = 0; targetIndex < targets.Count; targetIndex++)
            EnsureBlobMutable(targets[targetIndex]);

        var properties = await metadata.GetServicePropertiesAsync(current.Account, cancellationToken).ConfigureAwait(false);
        var hierarchicalNamespace = IsHierarchicalNamespaceEnabled(current.Account);
        var deletionIds = records
            .Where(item => item.DeletionId.HasValue)
            .Select(item => item.DeletionId!.Value)
            .ToHashSet();
        var mutations = new List<BlobRecordMutation>(targets.Count);
        for (var targetIndex = 0; targetIndex < targets.Count; targetIndex++)
        {
            var target = targets[targetIndex];
            BlobRecord? replacement;
            if (!hasExplicitSnapshotOrVersion &&
                target.IsCurrent &&
                target.Snapshot is null &&
                properties.VersioningEnabled &&
                !hierarchicalNamespace)
            {
                replacement = target with
                {
                    IsCurrent = false,
                    IsDeleted = false,
                    DeletedAt = null,
                    DeleteRetentionUntil = null,
                    VersionId = target.VersionId ?? MetadataStore.CreateVersionId(target.LastModified),
                    Lease = LeaseRecord.Available,
                    Revision = MetadataStore.NewRevision()
                };
            }
            else if (properties.BlobSoftDeleteEnabled)
            {
                var deletedAt = metadata.GetUtcNow();
                ulong? deletionId = null;
                var hierarchicalPathDelete = hierarchicalNamespace && target.Snapshot is null;
                if (hierarchicalPathDelete)
                {
                    do
                    {
                        deletionId = MetadataStore.NewDeletionId();
                    }
                    while (!deletionIds.Add(deletionId.Value));
                }
                replacement = target with
                {
                    IsDeleted = true,
                    DeletionId = deletionId,
                    DeletedAt = deletedAt,
                    DeleteRetentionUntil = deletedAt.AddDays(properties.BlobSoftDeleteRetentionDays),
                    IsCurrent = !hierarchicalNamespace && target.IsCurrent,
                    VersionId = hierarchicalNamespace ? null : target.VersionId,
                    Snapshot = hierarchicalPathDelete ? null : target.Snapshot,
                    Lease = LeaseRecord.Available,
                    Revision = MetadataStore.NewRevision()
                };
            }
            else
            {
                replacement = null;
            }
            mutations.Add(new BlobRecordMutation(target.GenerationId, target.Revision, replacement));
        }
        await metadata.ApplyBlobRecordMutationsAsync(
            mutations,
            cancellationToken,
            clearStagedBlocksForCurrentBlobs: true).ConfigureAwait(false);
    }

    public async Task PermanentlyDeleteBlobAsync(
        BlobRecord current,
        bool hasExplicitSnapshotOrVersion,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        var properties = await metadata.GetServicePropertiesAsync(current.Account, cancellationToken).ConfigureAwait(false);
        if (!properties.BlobPermanentDeleteEnabled || !properties.BlobSoftDeleteEnabled)
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "BlobOperationNotSupported",
                "Permanent deletion is not enabled for this storage account.");
        }
        if (!hasExplicitSnapshotOrVersion)
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "PermanentDeleteNotSupportedOnRootBlob",
                "Permanent delete is not supported on a root blob.");
        }
        if (!current.IsDeleted)
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "BlobOperationNotSupported",
                "Permanent delete is supported only for a soft-deleted blob snapshot or version.");
        }

        EnsureNoPendingCopy(current);
        EnsureBlobMutable(current);
        if (!await metadata.DeleteBlobRecordAsync(current.GenerationId, current.Revision, cancellationToken).ConfigureAwait(false))
            throw AzureStorageException.BlobNotFound();
    }

    public async Task UndeleteBlobAsync(
        string account,
        string container,
        string name,
        CancellationToken cancellationToken)
    {
        ValidateBlobName(name);
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken).ConfigureAwait(false);
        var records = await metadata.ListBlobFamilyAsync(account, container, name, includeDeleted: true, cancellationToken).ConfigureAwait(false);
        var properties = await metadata.GetServicePropertiesAsync(account, cancellationToken).ConfigureAwait(false);
        var now = metadata.GetUtcNow();
        var mutations = new List<BlobRecordMutation>();
        foreach (var record in records.Where(item => string.Equals(item.Name, name, StringComparison.Ordinal) && item.IsDeleted))
        {
            if (record.DeletedAt is null)
                continue;
            var retentionUntil = record.DeleteRetentionUntil ??
                                 record.DeletedAt.Value.AddDays(properties.BlobSoftDeleteRetentionDays);
            if (retentionUntil < now)
                continue;
            mutations.Add(new BlobRecordMutation(record.GenerationId, record.Revision, record with
            {
                IsDeleted = false,
                DeletedAt = null,
                DeleteRetentionUntil = null,
                Revision = MetadataStore.NewRevision()
            }));
        }
        if (mutations.Count == 0)
            throw AzureStorageException.BlobNotFound();
        await metadata.ApplyBlobRecordMutationsAsync(mutations, cancellationToken).ConfigureAwait(false);
    }

    public async Task UndeleteHierarchicalBlobAsync(
        string account,
        string container,
        string sourceName,
        string destinationName,
        ulong deletionId,
        CancellationToken cancellationToken)
    {
        ValidateBlobName(sourceName);
        ValidateBlobName(destinationName);
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken).ConfigureAwait(false);
        var records = await metadata.ListBlobFamilyAsync(
            account,
            container,
            sourceName,
            includeDeleted: true,
            cancellationToken).ConfigureAwait(false);
        var properties = await metadata.GetServicePropertiesAsync(account, cancellationToken).ConfigureAwait(false);
        var now = metadata.GetUtcNow();
        var source = records.FirstOrDefault(item =>
            item.IsDeleted &&
            item.DeletionId == deletionId &&
            item.DeletedAt.HasValue &&
            (item.DeleteRetentionUntil ??
             item.DeletedAt.Value.AddDays(properties.BlobSoftDeleteRetentionDays)) > now)
                     ?? throw AzureStorageException.BlobNotFound();
        var restored = source with
        {
            Name = destinationName,
            IsCurrent = true,
            IsDeleted = false,
            DeletionId = null,
            DeletedAt = null,
            DeleteRetentionUntil = null,
            Revision = MetadataStore.NewRevision()
        };
        if (!await metadata.TryRestoreDeletedBlobAsync(restored, source.Revision, cancellationToken).ConfigureAwait(false))
            throw AzureStorageException.PathAlreadyExists();
    }

    // Preserve the exact x-ms-copy-source text in the stored copy state.
#pragma warning disable CA1054
    public async Task<BlobRecord> BeginCopyFromBlobAsync(
        string account,
        string container,
        string name,
        BlobRecord source,
        bool destinationIsSealed,
        BlobWriteOptions options,
        string sourceUri,
        LeaseRecord destinationLease,
        string? expectedGeneration,
        string? expectedRevision,
        CancellationToken cancellationToken)
    {
#pragma warning restore CA1054
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(options);
        var sourceIsArchived = string.Equals(source.AccessTier, "Archive", StringComparison.Ordinal);
        ValidateCopyRehydrationOptions(source.Kind, sourceIsArchived, options);
        options = await ApplyContainerEncryptionPolicyAsync(account, container, options, cancellationToken).ConfigureAwait(false);
        using var prepared = await PrepareCopyContentAsync(
            account,
            source,
            EncryptionOf(options),
            preserveCommittedBlocks: true,
            cancellationToken).ConfigureAwait(false);
        return await BeginCopyAsync(
            account,
            container,
            name,
            source.Kind,
            sourceIsArchived,
            prepared.Content,
            source.SequenceNumber,
            destinationIsSealed,
            source.AppendBlockCount,
            prepared.CommittedBlocks,
            source.PageRanges,
            options,
            sourceUri,
            destinationLease,
            expectedGeneration,
            expectedRevision,
            cancellationToken).ConfigureAwait(false);
    }

    // Preserve the exact x-ms-copy-source text in the stored copy state.
#pragma warning disable CA1054
    public async Task<BlobRecord> CopyBlockBlobFromBlobAsync(
        string account,
        string container,
        string name,
        BlobRecord source,
        BlobWriteOptions options,
        string sourceUri,
        LeaseRecord destinationLease,
        string? expectedGeneration,
        string? expectedRevision,
        CancellationToken cancellationToken)
    {
#pragma warning restore CA1054
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(source);
        if (source.Kind != BlobKind.BlockBlob)
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "InvalidSourceBlobType",
                "The source blob type is invalid for this operation.");
        }
        options = await ApplyContainerEncryptionPolicyAsync(account, container, options, cancellationToken).ConfigureAwait(false);
        using var prepared = await PrepareCopyContentAsync(
            account,
            source,
            EncryptionOf(options),
            preserveCommittedBlocks: true,
            cancellationToken).ConfigureAwait(false);
        return await PublishSynchronousBlockCopyAsync(
            account,
            container,
            name,
            prepared.Content,
            prepared.CommittedBlocks,
            options,
            sourceUri,
            destinationLease,
            expectedGeneration,
            expectedRevision,
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<BlobRecord> CopyBlobFromBlobSynchronouslyAsync(
        string account,
        string container,
        string name,
        BlobRecord source,
        BlobWriteOptions options,
        LeaseRecord destinationLease,
        string? expectedGeneration,
        string? expectedRevision,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(source);
        EnsureBlobKindSupported(account, source.Kind);
        options = await ApplyContainerEncryptionPolicyAsync(account, container, options, cancellationToken).ConfigureAwait(false);
        using var prepared = await PrepareCopyContentAsync(
            account,
            source,
            EncryptionOf(options),
            preserveCommittedBlocks: true,
            cancellationToken).ConfigureAwait(false);
        ValidateBlobName(name);
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken).ConfigureAwait(false);
        var now = metadata.GetUtcNow();
        var proposed = NewBlob(account, container, name, source.Kind, prepared.Content, options, now) with
        {
            Lease = leases.ResetAfterBlobWrite(destinationLease),
            SequenceNumber = source.SequenceNumber,
            IsSealed = source.IsSealed,
            AppendBlockCount = source.AppendBlockCount,
            CommittedBlocks = [.. prepared.CommittedBlocks],
            PageRanges = [.. source.PageRanges]
        };
        return await metadata.PublishBlobAsync(
            proposed,
            expectedGeneration,
            expectedRevision,
            IsHierarchicalNamespaceEnabled(account),
            cancellationToken).ConfigureAwait(false);
    }

    // Preserve the exact x-ms-copy-source text in the stored copy state.
#pragma warning disable CA1054
    public async Task<BlobRecord> CopyBlockBlobFromStreamAsync(
        string account,
        string container,
        string name,
        Stream source,
        long contentLength,
        IReadOnlyList<CopySourceBlock> sourceBlocks,
        BlobWriteOptions options,
        string sourceUri,
        LeaseRecord destinationLease,
        string? expectedGeneration,
        string? expectedRevision,
        CancellationToken cancellationToken)
    {
#pragma warning restore CA1054
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(sourceBlocks);
        options = await ApplyContainerEncryptionPolicyAsync(account, container, options, cancellationToken).ConfigureAwait(false);
        using var content = await StoreRemoteBlockBlobAsync(
            account,
            source,
            contentLength,
            sourceBlocks,
            EncryptionOf(options),
            cancellationToken).ConfigureAwait(false);
        return await PublishSynchronousBlockCopyAsync(
            account,
            container,
            name,
            content.Content,
            content.CommittedBlocks,
            options,
            sourceUri,
            destinationLease,
            expectedGeneration,
            expectedRevision,
            cancellationToken).ConfigureAwait(false);
    }

    private async Task<BlobRecord> PublishSynchronousBlockCopyAsync(
        string account,
        string container,
        string name,
        ContentManifest content,
        IReadOnlyList<CommittedBlockRecord> committedBlocks,
        BlobWriteOptions options,
        string sourceUri,
        LeaseRecord destinationLease,
        string? expectedGeneration,
        string? expectedRevision,
        CancellationToken cancellationToken)
    {
        ValidateBlobName(name);
        var now = metadata.GetUtcNow();
        var proposed = NewBlob(account, container, name, BlobKind.BlockBlob, content, options, now) with
        {
            Lease = leases.ResetAfterBlobWrite(destinationLease),
            CommittedBlocks = [.. committedBlocks],
            Copy = new CopyState
            {
                Id = Guid.NewGuid().ToString(),
                Source = sourceUri,
                Status = "success",
                BytesCopied = content.Length,
                TotalBytes = content.Length,
                CompletedAt = now
            }
        };
        return await metadata.PublishBlobAsync(
            proposed,
            expectedGeneration,
            expectedRevision,
            IsHierarchicalNamespaceEnabled(account),
            cancellationToken).ConfigureAwait(false);
    }

    // Preserve the exact x-ms-copy-source text in the stored copy state.
#pragma warning disable CA1054
    public async Task<BlobRecord> BeginIncrementalCopyAsync(
        string account,
        string container,
        string name,
        BlobRecord source,
        BlobWriteOptions options,
        string sourceUri,
        BlobRecord? current,
        CancellationToken cancellationToken)
    {
#pragma warning restore CA1054
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(source);
        EnsureFlatNamespace(account);
        ValidateBlobName(name);
        options = await ApplyContainerEncryptionPolicyAsync(
            account,
            container,
            options,
            cancellationToken,
            current).ConfigureAwait(false);
        if (source.Kind != BlobKind.PageBlob)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidSourceBlobType", "The source blob type is invalid for incremental copy.");
        if (source.Snapshot is null)
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "IncrementalCopySourceMustBeSnapshot",
                "The source for an incremental copy must be a page blob snapshot.");
        }

        var sourceIdentity = $"{source.Account}/{source.Container}/{source.Name}";
        BlobEncryption encryption;
        if (current is not null)
        {
            EnsureNoPendingCopy(current);
            current = PrepareBlobWrite(current);
            if (!current.IsIncrementalCopy || current.Kind != BlobKind.PageBlob ||
                !string.Equals(current.IncrementalCopySource, sourceIdentity, StringComparison.Ordinal) ||
                current.IncrementalCopySourceCreatedAt != source.CreatedAt)
            {
                throw new AzureStorageException(
                    StatusCodes.Status409Conflict,
                    "IncrementalCopyBlobMismatch",
                    "The source blob does not match the source associated with this incremental copy blob.");
            }
            if (current.IncrementalCopySourceSnapshot is not null &&
                string.CompareOrdinal(source.Snapshot, current.IncrementalCopySourceSnapshot) <= 0)
            {
                throw new AzureStorageException(
                    StatusCodes.Status409Conflict,
                    "IncrementalCopyOfEarlierVersionSnapshotNotAllowed",
                    "The source snapshot must be newer than the last successfully copied snapshot.");
            }
            encryption = EncryptionOf(current);
        }
        else
        {
            encryption = EncryptionOf(options);
        }
        using var prepared = await PrepareCopyContentAsync(
            account,
            source,
            encryption,
            preserveCommittedBlocks: false,
            cancellationToken).ConfigureAwait(false);
        var now = metadata.GetUtcNow();
        var copyId = Guid.NewGuid().ToString();
        var pending = (current ?? NewBlob(
            account,
            container,
            name,
            BlobKind.PageBlob,
            chunks.Empty(account, encryption),
            options,
            now)) with
        {
            Revision = MetadataStore.NewRevision(),
            ETag = MetadataStore.NewETag(),
            LastModified = now,
            LastAccessedAt = IsLastAccessTimeTrackingEnabled(account)
                ? now
                : current?.LastAccessedAt,
            SequenceNumber = source.SequenceNumber,
            IsIncrementalCopy = true,
            IncrementalCopySource = sourceIdentity,
            IncrementalCopySourceCreatedAt = source.CreatedAt,
            PendingCopyContent = prepared.Content,
            PendingCopyPageRanges = [.. source.PageRanges],
            Copy = new CopyState
            {
                Id = copyId,
                Source = sourceUri,
                Status = "pending",
                BytesCopied = 0,
                TotalBytes = prepared.Content.Length,
                ReadyAt = now.Add(_options.AsyncCopyCompletionDelay),
                ExpiresAt = now.AddDays(14),
                IsIncremental = true,
                SourceSnapshot = source.Snapshot
            }
        };

        if (current is null)
            return await metadata.PublishBlobAsync(
                pending,
                null,
                null,
                IsHierarchicalNamespaceEnabled(account),
                cancellationToken).ConfigureAwait(false);
        await metadata.PutBlobRecordAsync(pending, current.Revision, cancellationToken).ConfigureAwait(false);
        return pending;
    }

    // Preserve the exact x-ms-copy-source text in the stored copy state.
#pragma warning disable CA1054
    public async Task<BlobRecord> BeginCopyFromStreamAsync(
        string account,
        string container,
        string name,
        Stream source,
        long contentLength,
        BlobKind sourceKind,
        bool sourceIsArchived,
        long sequenceNumber,
        bool destinationIsSealed,
        int appendBlockCount,
        IReadOnlyList<CopySourceBlock> sourceBlocks,
        IReadOnlyList<PageRange> pageRanges,
        BlobWriteOptions options,
        string sourceUri,
        LeaseRecord destinationLease,
        string? expectedGeneration,
        string? expectedRevision,
        CancellationToken cancellationToken)
    {
#pragma warning restore CA1054
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(sourceBlocks);
        ValidateCopyRehydrationOptions(sourceKind, sourceIsArchived, options);
        options = await ApplyContainerEncryptionPolicyAsync(account, container, options, cancellationToken).ConfigureAwait(false);
        var encryption = EncryptionOf(options);
        if (sourceKind == BlobKind.BlockBlob)
        {
            using var content = await StoreRemoteBlockBlobAsync(
                account,
                source,
                contentLength,
                sourceBlocks,
                encryption,
                cancellationToken).ConfigureAwait(false);
            return await BeginCopyAsync(
                account,
                container,
                name,
                sourceKind,
                sourceIsArchived,
                content.Content,
                sequenceNumber,
                destinationIsSealed,
                appendBlockCount,
                content.CommittedBlocks,
                pageRanges,
                options,
                sourceUri,
                destinationLease,
                expectedGeneration,
                expectedRevision,
                cancellationToken).ConfigureAwait(false);
        }
        using var stored = await chunks.StorePinnedAsync(account, encryption, source, cancellationToken).ConfigureAwait(false);
        if (stored.Manifest.Length != contentLength)
            throw CannotVerifyCopySource("The copy source length did not match its Content-Length value.");
        return await BeginCopyAsync(
            account,
            container,
            name,
            sourceKind,
            sourceIsArchived,
            stored.Manifest,
            sequenceNumber,
            destinationIsSealed,
            appendBlockCount,
            committedBlocks: [],
            pageRanges,
            options,
            sourceUri,
            destinationLease,
            expectedGeneration,
            expectedRevision,
            cancellationToken).ConfigureAwait(false);
    }

    private async Task<BlobRecord> BeginCopyAsync(
        string account,
        string container,
        string name,
        BlobKind sourceKind,
        bool sourceIsArchived,
        ContentManifest sourceContent,
        long sequenceNumber,
        bool destinationIsSealed,
        int appendBlockCount,
        IReadOnlyList<CommittedBlockRecord> committedBlocks,
        IReadOnlyList<PageRange> pageRanges,
        BlobWriteOptions options,
        string sourceUri,
        LeaseRecord destinationLease,
        string? expectedGeneration,
        string? expectedRevision,
        CancellationToken cancellationToken)
    {
        EnsureBlobKindSupported(account, sourceKind);
        ValidateCopyRehydrationOptions(sourceKind, sourceIsArchived, options);
        ValidateBlobName(name);
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken).ConfigureAwait(false);
        var encryption = EncryptionOf(options);
        if (!chunks.IsInDomain(account, encryption, sourceContent))
            throw UnsupportedEncryptionTransition();
        using var sourcePin = chunks.Pin(sourceContent);
        var now = metadata.GetUtcNow();
        var copyId = Guid.NewGuid().ToString();
        var copyReadyAt = now.Add(_options.AsyncCopyCompletionDelay);
        var rehydratePriority = sourceIsArchived
            ? options.RehydratePriority ?? "Standard"
            : null;
        var rehydrateCompleteAt = sourceIsArchived
            ? now.Add(string.Equals(rehydratePriority, "High"
, StringComparison.Ordinal) ? _options.HighPriorityRehydrationDelay
                : _options.StandardRehydrationDelay)
            : (DateTimeOffset?)null;
        if (rehydrateCompleteAt < copyReadyAt)
            rehydrateCompleteAt = copyReadyAt;
        var visibleContent = sourceKind == BlobKind.PageBlob
            ? chunks.Sparse(account, encryption, sourceContent.Length)
            : chunks.Empty(account, encryption);
        var proposed = NewBlob(account, container, name, sourceKind, visibleContent, options, now) with
        {
            Lease = leases.ResetAfterBlobWrite(destinationLease),
            SequenceNumber = sequenceNumber,
            IsSealed = false,
            AppendBlockCount = 0,
            CommittedBlocks = [],
            PageRanges = [],
            PendingCopyCommittedBlocks = sourceKind == BlobKind.BlockBlob ? [.. committedBlocks] : null,
            PendingCopyAppendBlockCount = sourceKind == BlobKind.AppendBlob ? appendBlockCount : null,
            PendingCopyIsSealed = sourceKind == BlobKind.AppendBlob ? destinationIsSealed : null,
            PendingCopyPageRanges = sourceKind == BlobKind.PageBlob ? [.. pageRanges] : null,
            PendingCopyContent = sourceContent,
            AccessTier = sourceIsArchived ? "Archive" : options.AccessTier ?? "Hot",
            AccessTierInferred = sourceIsArchived ? false : options.AccessTierInferred ?? options.AccessTier is null,
            ArchiveStatus = sourceIsArchived
                ? $"rehydrate-pending-to-{ProtocolLowercase(options.AccessTier!)}"
                : null,
            RehydratePriority = rehydratePriority,
            RehydrateCompleteAt = rehydrateCompleteAt,
            Copy = new CopyState
            {
                Id = copyId,
                Source = sourceUri,
                Status = "pending",
                BytesCopied = 0,
                TotalBytes = sourceContent.Length,
                ReadyAt = copyReadyAt,
                ExpiresAt = now.AddDays(14)
            }
        };
        return await metadata.PublishBlobAsync(
            proposed,
            expectedGeneration,
            expectedRevision,
            IsHierarchicalNamespaceEnabled(account),
            cancellationToken).ConfigureAwait(false);
    }

    private static void ValidateCopyRehydrationOptions(
        BlobKind sourceKind,
        bool sourceIsArchived,
        BlobWriteOptions options)
    {
        if (options.RehydratePriority is not null && sourceKind != BlobKind.BlockBlob)
            throw AzureStorageException.InvalidHeader("x-ms-rehydrate-priority", options.RehydratePriority);
        if (!sourceIsArchived)
            return;
        if (sourceKind == BlobKind.BlockBlob &&
            options.AccessTier is "Hot" or "Cool" or "Cold" or "Smart")
        {
            return;
        }
        throw new AzureStorageException(
            StatusCodes.Status409Conflict,
            "BlobArchived",
            "An archived copy source requires an explicit online destination access tier.");
    }

    private static async Task EnsureSourceExhaustedAsync(Stream source, CancellationToken cancellationToken)
    {
        var probe = new byte[1];
        if (await source.ReadAsync(probe, cancellationToken).ConfigureAwait(false) != 0)
            throw CannotVerifyCopySource("The copy source contained more data than its committed block list.");
    }

    private async Task<PreparedCopyContent> StoreRemoteBlockBlobAsync(
        string account,
        Stream source,
        long contentLength,
        IReadOnlyList<CopySourceBlock> sourceBlocks,
        BlobEncryption encryption,
        CancellationToken cancellationToken)
    {
        var contentLeases = new List<IDisposable>();
        try
        {
            if (sourceBlocks.Count == 0)
            {
                var stored = await chunks.StorePinnedAsync(account, encryption, source, cancellationToken).ConfigureAwait(false);
                contentLeases.Add(stored);
                if (stored.Manifest.Length != contentLength)
                    throw CannotVerifyCopySource("The copy source length did not match its Content-Length value.");
                return new PreparedCopyContent(stored.Manifest, [], contentLeases);
            }

            var copiedBlocks = new List<CommittedBlockRecord>(sourceBlocks.Count);
            foreach (var sourceBlock in sourceBlocks)
            {
                using var blockSource = new ExactLengthReadStream(source, sourceBlock.Length);
                StoredContent storedBlock;
                try
                {
                    storedBlock = await chunks.StorePinnedAsync(account, encryption, blockSource, cancellationToken).ConfigureAwait(false);
                }
                catch (EndOfStreamException)
                {
                    throw CannotVerifyCopySource("The copy source ended before its committed block did.");
                }
                contentLeases.Add(storedBlock);
                copiedBlocks.Add(new CommittedBlockRecord(sourceBlock.Id, storedBlock.Manifest));
            }
            await EnsureSourceExhaustedAsync(source, cancellationToken).ConfigureAwait(false);
            var content = await chunks.ComposeAsync(
                account,
                encryption,
                copiedBlocks.Select(block => block.Content).ToArray(),
                cancellationToken).ConfigureAwait(false);
            if (content.Length != contentLength)
                throw CannotVerifyCopySource("The copy source length did not match its committed block list.");
            return new PreparedCopyContent(content, copiedBlocks, contentLeases);
        }
        catch
        {
            for (var leaseIndex = 0; leaseIndex < contentLeases.Count; leaseIndex++)
                contentLeases[leaseIndex].Dispose();
            throw;
        }
    }

    private sealed class ExactLengthReadStream(Stream inner, long length) : Stream
    {
        private long _remaining = length >= 0 ? length : throw new ArgumentOutOfRangeException(nameof(length));

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => length;
        public override long Position
        {
            get => length - _remaining;
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            if (buffer.IsEmpty)
                return 0;
            if (_remaining == 0)
                return 0;
            var read = inner.Read(buffer[..(int)Math.Min(buffer.Length, _remaining)]);
            Record(read);
            return read;
        }

        public override async ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            if (buffer.IsEmpty)
                return 0;
            if (_remaining == 0)
                return 0;
            var read = await inner.ReadAsync(
                buffer[..(int)Math.Min(buffer.Length, _remaining)],
                cancellationToken).ConfigureAwait(false);
            Record(read);
            return read;
        }

        public override Task<int> ReadAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override void Flush() => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            // The outer transfer owns the source stream.
            base.Dispose(disposing);
        }

        private void Record(int read)
        {
            if (read == 0)
                throw new EndOfStreamException("The copy source ended before its committed block did.");
            _remaining -= read;
        }
    }

    private async Task<PreparedCopyContent> PrepareCopyContentAsync(
        string destinationAccount,
        BlobRecord source,
        BlobEncryption destinationEncryption,
        bool preserveCommittedBlocks,
        CancellationToken cancellationToken)
    {
        if (chunks.IsInDomain(destinationAccount, destinationEncryption, source.Content))
        {
            return new PreparedCopyContent(
                source.Content,
                preserveCommittedBlocks ? source.CommittedBlocks : [],
                [chunks.Pin(source.Content)]);
        }
        if (source.CustomerProvidedKeySha256 is not null)
            throw UnsupportedEncryptionTransition();

        var contentLeases = new List<IDisposable>();
        try
        {
            if (preserveCommittedBlocks && source.CommittedBlocks.Count > 0)
            {
                var copiedBlocks = new List<CommittedBlockRecord>(source.CommittedBlocks.Count);
                foreach (var block in source.CommittedBlocks)
                {
                    var copied = await chunks.CopyToDomainPinnedAsync(
                        destinationAccount,
                        EncryptionOf(source),
                        destinationEncryption,
                        block.Content,
                        cancellationToken).ConfigureAwait(false);
                    contentLeases.Add(copied);
                    copiedBlocks.Add(new CommittedBlockRecord(block.Id, copied.Manifest));
                }
                var content = await chunks.ComposeAsync(
                    destinationAccount,
                    destinationEncryption,
                    copiedBlocks.Select(block => block.Content).ToArray(),
                    cancellationToken).ConfigureAwait(false);
                return new PreparedCopyContent(content, copiedBlocks, contentLeases);
            }

            var copiedContent = await chunks.CopyToDomainPinnedAsync(
                destinationAccount,
                EncryptionOf(source),
                destinationEncryption,
                source.Content,
                cancellationToken).ConfigureAwait(false);
            contentLeases.Add(copiedContent);
            return new PreparedCopyContent(copiedContent.Manifest, [], contentLeases);
        }
        catch
        {
            for (var leaseIndex = 0; leaseIndex < contentLeases.Count; leaseIndex++)
                contentLeases[leaseIndex].Dispose();
            throw;
        }
    }

    private sealed class PreparedCopyContent(
        ContentManifest content,
        IReadOnlyList<CommittedBlockRecord> committedBlocks,
        IReadOnlyList<IDisposable> leases) : IDisposable
    {
        public ContentManifest Content { get; } = content;
        public IReadOnlyList<CommittedBlockRecord> CommittedBlocks { get; } = committedBlocks;

        public void Dispose()
        {
            foreach (var lease in leases)
                lease.Dispose();
        }
    }

    public async Task<BlobRecord> AbortCopyAsync(
        BlobRecord current,
        string copyId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        current = PrepareBlobWrite(current);
        if (current.Copy is null || !string.Equals(current.Copy.Status, "pending", StringComparison.Ordinal) || current.PendingCopyContent is null)
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "NoPendingCopyOperation",
                "There is currently no pending copy operation.");
        }
        if (!string.Equals(current.Copy.Id, copyId, StringComparison.Ordinal))
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "CopyIdMismatch",
                "The specified copy ID did not match the pending copy operation.");
        }

        var now = metadata.GetUtcNow();
        var abortedTier = RehydrationTargetTier(current.ArchiveStatus) ?? current.AccessTier;
        var updated = current with
        {
            Content = current.Copy.IsIncremental
                ? current.Content
                : chunks.Empty(current.Account, EncryptionOf(current)),
            PendingCopyContent = null,
            PendingCopyCommittedBlocks = null,
            PendingCopyAppendBlockCount = null,
            PendingCopyIsSealed = null,
            PendingCopyPageRanges = null,
            CommittedBlocks = current.Copy.IsIncremental ? current.CommittedBlocks : [],
            PageRanges = current.Copy.IsIncremental ? current.PageRanges : [],
            AppendBlockCount = current.Copy.IsIncremental ? current.AppendBlockCount : 0,
            IsSealed = current.Copy.IsIncremental && current.IsSealed,
            AccessTier = abortedTier,
            ArchiveStatus = null,
            RehydratePriority = null,
            RehydrateCompleteAt = null,
            Copy = current.Copy with
            {
                Status = "aborted",
                BytesCopied = 0,
                CompletedAt = now,
                ReadyAt = null,
                ExpiresAt = null
            },
            Revision = MetadataStore.NewRevision(),
            ETag = MetadataStore.NewETag(),
            LastModified = now
        };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken).ConfigureAwait(false);
        return updated;
    }

    public Task<IReadOnlyList<StagedBlockRecord>> ListStagedBlocksAsync(
        string account,
        string container,
        string name,
        CancellationToken cancellationToken) =>
        metadata.ListStagedBlocksAsync(account, container, name, cancellationToken);

    public async Task DeleteUncommittedBlobAsync(
        string account,
        string container,
        string name,
        CancellationToken cancellationToken)
    {
        ValidateBlobName(name);
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken).ConfigureAwait(false);
        if (!await metadata.DeleteStagedBlocksAsync(account, container, name, cancellationToken).ConfigureAwait(false))
            throw AzureStorageException.BlobNotFound();
    }

    public Task<ServiceProperties> GetServicePropertiesAsync(string account, CancellationToken cancellationToken) =>
        metadata.GetServicePropertiesAsync(account, cancellationToken);

    public async Task PutServicePropertiesAsync(
        string account,
        ServiceProperties properties,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(properties);
        if (!properties.VersioningEnabled &&
            _options.ObjectReplicationPolicies.Any(policy =>
                string.Equals(policy.SourceAccount, account, StringComparison.Ordinal) ||
                string.Equals(policy.DestinationAccount, account, StringComparison.Ordinal)))
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "BlobOperationNotSupported",
                "Blob versioning cannot be disabled while an object replication policy is active.");
        }
        await analytics.EnsureContainerAsync(account, cancellationToken).ConfigureAwait(false);
        if (properties.StaticWebsite.Enabled)
        {
            var websiteContainer = await metadata.GetContainerAsync(
                account,
                "$web",
                includeDeleted: true,
                cancellationToken).ConfigureAwait(false);
            if (websiteContainer?.DeletedAt is not null)
            {
                throw new AzureStorageException(
                    StatusCodes.Status409Conflict,
                    "ContainerBeingDeleted",
                    "The static website container is being deleted.");
            }
            if (websiteContainer is null)
            {
                _ = await CreateContainerAsync(
                    account,
                    "$web",
                    new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
                    publicAccess: null,
                    defaultEncryptionScope: null,
                    preventEncryptionScopeOverride: false,
                    cancellationToken).ConfigureAwait(false);
            }
        }
        await metadata.PutServicePropertiesAsync(account, properties, cancellationToken).ConfigureAwait(false);
    }

    public async Task<StorageMaintenanceResult> RunMaintenanceAsync(CancellationToken cancellationToken)
    {
        await _maintenanceGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await RunMaintenanceCoreAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _maintenanceGate.Release();
        }
    }

    private async Task<StorageMaintenanceResult> RunMaintenanceCoreAsync(CancellationToken cancellationToken)
    {
        var completedCopies = 0;
        var completedObjectReplications = 0;
        var failedObjectReplications = 0;
        var removedObjectReplicas = 0;
        var completedRehydrations = 0;
        var completedSmartTierTransitions = 0;
        var expiredBlobs = 0;
        var purgedBlobs = 0;
        var purgedContainers = 0;
        var now = metadata.GetUtcNow();
        var serviceProperties = new Dictionary<string, ServiceProperties>(StringComparer.Ordinal);

        var blobPage = await metadata.ListBlobMaintenancePageAsync(
            _blobMaintenanceCursor,
            _options.BlobRecordsPerMaintenancePass,
            cancellationToken).ConfigureAwait(false);
        foreach (var candidate in blobPage.Items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var blob = candidate;
                if (string.Equals(blob.Container, StorageAnalyticsService.LogsContainerName, StringComparison.Ordinal))
                {
                    if (!serviceProperties.TryGetValue(blob.Account, out var analyticsProperties))
                    {
                        analyticsProperties = await metadata.GetServicePropertiesAsync(blob.Account, cancellationToken).ConfigureAwait(false);
                        serviceProperties.Add(blob.Account, analyticsProperties);
                    }
                    if (analyticsProperties.Logging.RetentionPolicy is { Enabled: true, Days: { } retentionDays } &&
                        blob.CreatedAt.AddDays(retentionDays) <= now &&
                        await metadata.DeleteBlobRecordAsync(blob.GenerationId, blob.Revision, cancellationToken).ConfigureAwait(false))
                    {
                        continue;
                    }
                }
                if (string.Equals(blob.Copy?.Status, "pending", StringComparison.Ordinal) && blob.PendingCopyContent is not null)
                {
                    var pendingCopy = blob;
                    blob = await CompleteCopyIfDueAsync(blob, cancellationToken).ConfigureAwait(false);
                    if (string.Equals(pendingCopy.Copy?.Status, "pending", StringComparison.Ordinal) && string.Equals(blob.Copy?.Status, "success", StringComparison.Ordinal))
                        completedCopies++;
                }
                if (blob.RehydrateCompleteAt <= now)
                {
                    var rehydrated = await CompleteRehydrationIfDueAsync(blob, cancellationToken).ConfigureAwait(false);
                    if (rehydrated.RehydrateCompleteAt is null && blob.RehydrateCompleteAt is not null)
                        completedRehydrations++;
                    blob = rehydrated;
                }

                var smartTiered = await TransitionSmartTierIfDueAsync(blob, now, cancellationToken).ConfigureAwait(false);
                if (!string.Equals(smartTiered.SmartAccessTier, blob.SmartAccessTier, StringComparison.Ordinal))
                    completedSmartTierTransitions++;
                blob = smartTiered;

                if (blob.IsDeleted && blob.DeletedAt is not null)
                {
                    var retentionUntil = blob.DeleteRetentionUntil;
                    if (!retentionUntil.HasValue)
                    {
                        if (!serviceProperties.TryGetValue(blob.Account, out var properties))
                        {
                            properties = await metadata.GetServicePropertiesAsync(blob.Account, cancellationToken).ConfigureAwait(false);
                            serviceProperties.Add(blob.Account, properties);
                        }
                        retentionUntil = blob.DeletedAt.Value.AddDays(properties.BlobSoftDeleteRetentionDays);
                    }
                    if (retentionUntil <= now &&
                        await metadata.DeleteBlobRecordAsync(blob.GenerationId, blob.Revision, cancellationToken).ConfigureAwait(false))
                    {
                        purgedBlobs++;
                    }
                    continue;
                }

                if (blob.IsCurrent &&
                    blob.Snapshot is null &&
                    blob.ExpiresAt <= now &&
                    !blob.HasLegalHold &&
                    (!blob.ImmutabilityUntil.HasValue || blob.ImmutabilityUntil <= now) &&
                    await metadata.DeleteBlobRecordAsync(blob.GenerationId, blob.Revision, cancellationToken).ConfigureAwait(false))
                {
                    expiredBlobs++;
                    continue;
                }

                var replication = await ReplicateObjectIfConfiguredAsync(blob, now, cancellationToken).ConfigureAwait(false);
                completedObjectReplications += replication.Completed;
                failedObjectReplications += replication.Failed;
            }
            catch (StorageConcurrencyException)
            {
                // A concurrent request changed the resource; the next pass evaluates its new state.
            }
        }
        _blobMaintenanceCursor = blobPage.HasMore && blobPage.Items.Count > 0
            ? blobPage.Items[^1].GenerationId
            : null;

        var replicationStatePage = await metadata.ListObjectReplicationStatesPageAsync(
            _objectReplicationStateCursor,
            _options.BlobRecordsPerMaintenancePass,
            cancellationToken).ConfigureAwait(false);
        foreach (var state in replicationStatePage.Items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!IsConfiguredObjectReplicationState(state))
            {
                _ = await metadata.ForgetObjectReplicationStateAsync(state, cancellationToken).ConfigureAwait(false);
                continue;
            }
            if (await metadata.GetBlobByGenerationAsync(state.SourceGenerationId, cancellationToken).ConfigureAwait(false) is not null)
                continue;
            try
            {
                if (await metadata.RemoveObjectReplicaForMissingSourceAsync(state, cancellationToken).ConfigureAwait(false))
                    removedObjectReplicas++;
            }
            catch (StorageConcurrencyException)
            {
                // A request changed the replica while maintenance was evaluating it.
            }
            catch (StorageImmutabilityException)
            {
                // Retention on a destination version defers cleanup until a later pass.
            }
        }
        _objectReplicationStateCursor = replicationStatePage.HasMore && replicationStatePage.Items.Count > 0
            ? new ObjectReplicationStateKey(
                replicationStatePage.Items[^1].PolicyId,
                replicationStatePage.Items[^1].RuleId,
                replicationStatePage.Items[^1].SourceGenerationId)
            : null;

        var containerPage = await metadata.ListContainerMaintenancePageAsync(
            _containerMaintenanceCursor,
            _options.ContainerRecordsPerMaintenancePass,
            cancellationToken).ConfigureAwait(false);
        foreach (var container in containerPage.Items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (container.DeletedAt is null)
                continue;
            var retentionUntil = container.DeleteRetentionUntil;
            if (!retentionUntil.HasValue)
            {
                if (!serviceProperties.TryGetValue(container.Account, out var properties))
                {
                    properties = await metadata.GetServicePropertiesAsync(container.Account, cancellationToken).ConfigureAwait(false);
                    serviceProperties.Add(container.Account, properties);
                }
                retentionUntil = container.DeletedAt.Value.AddDays(properties.ContainerSoftDeleteRetentionDays);
            }
            if (retentionUntil > now)
                continue;
            try
            {
                if (await metadata.DeleteContainerPermanentlyAsync(
                        container.Account,
                        container.Name,
                        container.Revision,
                        cancellationToken).ConfigureAwait(false))
                {
                    purgedContainers++;
                }
            }
            catch (StorageConcurrencyException)
            {
                // A restore or mutation won the race; the next pass evaluates the current record.
            }
        }
        _containerMaintenanceCursor = containerPage.HasMore && containerPage.Items.Count > 0
            ? new ContainerKey(containerPage.Items[^1].Account, containerPage.Items[^1].Name)
            : null;

        var expiredBlocks = await metadata.DeleteStagedBlocksOlderThanAsync(
            now.Subtract(_options.UncommittedBlockRetention),
            _options.UncommittedBlocksPerMaintenancePass,
            cancellationToken).ConfigureAwait(false);
        var reclaimedStagingFiles = chunks.DeleteAbandonedStagingFiles(
            now.Subtract(_options.AbandonedStagingRetention),
            _options.MaximumStagingFilesPerMaintenancePass);
        var reclaimedChunks = await CollectGarbageBatchAsync(cancellationToken).ConfigureAwait(false);
        _ = await metadata.SealChunkPacksOlderThanAsync(
            now.Subtract(_options.ChunkPackSealAge),
            _options.ChunkPacksPerMaintenancePass,
            cancellationToken).ConfigureAwait(false);
        var orphanedPacks = await chunks.ReclaimOrphanedPacksAsync(
            now.Subtract(_options.ChunkPackSealAge),
            _options.ChunkPacksPerMaintenancePass,
            cancellationToken).ConfigureAwait(false);
        var packCompaction = await CompactChunkPacksAsync(cancellationToken).ConfigureAwait(false);
        var summary = await metadata.GetStorageInventorySummaryAsync(cancellationToken).ConfigureAwait(false);
        await ScanIntegrityAsync(summary.ReachableChunkCount, cancellationToken).ConfigureAwait(false);
        var recompression = await RecompressColdChunksAsync(now, cancellationToken).ConfigureAwait(false);
        if (chunks.IsPhysicalUsageScanInProgress ||
            _lastPhysicalUsage is null ||
            Stopwatch.GetElapsedTime(_lastPhysicalScanTicks) >= _options.PhysicalUsageScanInterval)
        {
            var completedPhysicalUsage = chunks.ScanPhysicalUsageBatch(
                _options.PhysicalUsageEntriesPerMaintenancePass);
            if (completedPhysicalUsage is not null)
            {
                _lastPhysicalUsage = completedPhysicalUsage;
                _lastPhysicalScanTicks = Stopwatch.GetTimestamp();
                _lastPhysicalScanUnixSeconds = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            }
        }
        var physical = _lastPhysicalUsage ?? new StoragePhysicalUsage(0, 0, 0, 0);
        var usage = new StorageUsageSnapshot(
            summary.LogicalBlobBytes,
            summary.LogicalStagedBlockBytes,
            physical.ChunkBytes,
            physical.StagingBytes,
            physical.MetadataBytes,
            summary.BlobRecordCount,
            summary.StagedBlockCount,
            physical.ChunkCount,
            summary.ReachableChunkCount,
            physical.AllocatedRootBytes,
            _lastPhysicalScanUnixSeconds);
        var result = new StorageMaintenanceResult(
            completedCopies,
            completedObjectReplications,
            failedObjectReplications,
            removedObjectReplicas,
            completedRehydrations,
            completedSmartTierTransitions,
            expiredBlobs,
            purgedBlobs,
            purgedContainers,
            expiredBlocks,
            reclaimedChunks,
            reclaimedStagingFiles,
            recompression.RecompressedChunks,
            recompression.BytesSaved,
            checked(packCompaction.CompactedPacks + orphanedPacks.ReclaimedPacks),
            checked(packCompaction.BytesSaved + orphanedPacks.BytesSaved));
        telemetry.RecordMaintenance(result, usage);
        return result;
    }

    private async Task<ObjectReplicationPassResult> ReplicateObjectIfConfiguredAsync(
        BlobRecord candidate,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        if (candidate.Kind != BlobKind.BlockBlob ||
            candidate.Snapshot is not null ||
            candidate.IsDeleted ||
string.Equals(candidate.Copy?.Status, "pending", StringComparison.Ordinal))
        {
            return default;
        }

        var matchingRules = _options.ObjectReplicationPolicies
            .Where(policy => string.Equals(policy.SourceAccount, candidate.Account, StringComparison.Ordinal))
            .SelectMany(policy => policy.Rules
                .Where(rule => string.Equals(rule.SourceContainer, candidate.Container, StringComparison.Ordinal))
                .Where(rule => candidate.CreatedAt >=
                               (rule.MinimumCreationTime ?? policy.EnabledAt ?? DateTimeOffset.MinValue))
                .Where(rule => rule.PrefixMatch.Count == 0 ||
                               rule.PrefixMatch.Any(prefix => candidate.Name.StartsWith(prefix, StringComparison.Ordinal)))
                .Select(rule => (Policy: policy, Rule: rule)))
            .ToArray();
        if (matchingRules.Length == 0)
            return default;

        var source = candidate;
        var completed = 0;
        var failed = 0;
        foreach (var (policy, rule) in matchingRules)
        {
            var fingerprint = ObjectReplicationFingerprint(source);
            var statusKey = $"{policy.PolicyId}_{rule.RuleId}";
            var stateKey = new ObjectReplicationStateKey(policy.PolicyId, rule.RuleId, source.GenerationId);
            var existingState = await metadata.GetObjectReplicationStateAsync(stateKey, cancellationToken).ConfigureAwait(false);
            if (existingState is not null && !ObjectReplicationStateMatches(policy, rule, source, existingState))
            {
                _ = await metadata.ForgetObjectReplicationStateAsync(existingState, cancellationToken).ConfigureAwait(false);
                existingState = null;
            }
            var sourceCannotReplicate = source.CustomerProvidedKeySha256 is not null ||
                                        string.Equals(source.AccessTier, "Archive", StringComparison.Ordinal) ||
                                        source.ArchiveStatus is not null;
            if (existingState is not null &&
                string.Equals(existingState.SourceFingerprint, fingerprint, StringComparison.Ordinal))
            {
                if (string.Equals(existingState.Status, "failed", StringComparison.Ordinal) ||
                    !sourceCannotReplicate)
                    continue;
            }

            var proposedState = new ObjectReplicationState
            {
                PolicyId = policy.PolicyId,
                RuleId = rule.RuleId,
                SourceGenerationId = source.GenerationId,
                SourceAccount = source.Account,
                SourceContainer = source.Container,
                SourceName = source.Name,
                DestinationAccount = policy.DestinationAccount,
                DestinationContainer = rule.DestinationContainer,
                DestinationGenerationId = existingState?.DestinationGenerationId,
                SourceFingerprint = fingerprint,
                Status = "pending",
                UpdatedAt = now
            };

            try
            {
                if (sourceCannotReplicate)
                    throw AzureStorageException.BlobOperationNotSupported();

                var mappedDestination = existingState?.DestinationGenerationId is { } destinationGeneration
                    ? await metadata.GetBlobByGenerationAsync(destinationGeneration, cancellationToken).ConfigureAwait(false)
                    : null;
                var currentDestination = await metadata.GetBlobAsync(
                    policy.DestinationAccount,
                    rule.DestinationContainer,
                    source.Name,
                    versionId: null,
                    snapshot: null,
                    includeDeleted: false,
                    cancellationToken).ConfigureAwait(false);
                if (IsArchivedObjectReplicationTarget(mappedDestination) ||
                    IsArchivedObjectReplicationTarget(currentDestination))
                {
                    throw AzureStorageException.BlobOperationNotSupported();
                }

                var options = await ApplyContainerEncryptionPolicyAsync(
                    policy.DestinationAccount,
                    rule.DestinationContainer,
                    new BlobWriteOptions(
                        source.Http,
                        new Dictionary<string, string>(source.Metadata, StringComparer.OrdinalIgnoreCase),
                        rule.ReplicateBlobTags
                            ? new Dictionary<string, string>(source.Tags, StringComparer.Ordinal)
                            : new Dictionary<string, string>(StringComparer.Ordinal)),
                    cancellationToken).ConfigureAwait(false);
                using var prepared = await PrepareCopyContentAsync(
                    policy.DestinationAccount,
                    source,
                    EncryptionOf(options),
                    preserveCommittedBlocks: true,
                    cancellationToken).ConfigureAwait(false);
                var proposedDestination = NewBlob(
                    policy.DestinationAccount,
                    rule.DestinationContainer,
                    source.Name,
                    BlobKind.BlockBlob,
                    prepared.Content,
                    options,
                    now) with
                {
                    CommittedBlocks = prepared.CommittedBlocks.ToList(),
                    ObjectReplicationDestinationPolicyId = policy.PolicyId
                };
                _ = await metadata.ApplyObjectReplicationAsync(
                    source,
                    proposedDestination,
                    proposedState,
                    statusKey,
                    cancellationToken).ConfigureAwait(false);
                completed++;
            }
            catch (StorageConcurrencyException)
            {
                continue;
            }
            catch (Exception exception) when (exception is AzureStorageException or StorageImmutabilityException)
            {
                if (await metadata.MarkObjectReplicationFailureAsync(
                        source,
                        proposedState,
                        statusKey,
                        cancellationToken).ConfigureAwait(false))
                {
                    failed++;
                }
            }

            source = await metadata.GetBlobByGenerationAsync(source.GenerationId, cancellationToken).ConfigureAwait(false)
                     ?? source;
        }

        return new ObjectReplicationPassResult(completed, failed);
    }

    private bool IsConfiguredObjectReplicationState(ObjectReplicationState state) =>
        _options.ObjectReplicationPolicies.Any(policy =>
            string.Equals(policy.PolicyId, state.PolicyId, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(policy.SourceAccount, state.SourceAccount, StringComparison.Ordinal) &&
            string.Equals(policy.DestinationAccount, state.DestinationAccount, StringComparison.Ordinal) &&
            policy.Rules.Any(rule =>
                string.Equals(rule.RuleId, state.RuleId, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(rule.SourceContainer, state.SourceContainer, StringComparison.Ordinal) &&
                string.Equals(rule.DestinationContainer, state.DestinationContainer, StringComparison.Ordinal)));

    private static bool ObjectReplicationStateMatches(
        ObjectReplicationPolicyOptions policy,
        ObjectReplicationRuleOptions rule,
        BlobRecord source,
        ObjectReplicationState state) =>
        string.Equals(state.SourceAccount, source.Account, StringComparison.Ordinal) &&
        string.Equals(state.SourceContainer, source.Container, StringComparison.Ordinal) &&
        string.Equals(state.SourceName, source.Name, StringComparison.Ordinal) &&
        string.Equals(state.DestinationAccount, policy.DestinationAccount, StringComparison.Ordinal) &&
        string.Equals(state.DestinationContainer, rule.DestinationContainer, StringComparison.Ordinal);

    private static bool IsArchivedObjectReplicationTarget(BlobRecord? blob) =>
        blob is not null &&
        (string.Equals(blob.AccessTier, "Archive", StringComparison.Ordinal) || blob.ArchiveStatus is not null);

    public async Task<int> CollectGarbageAsync(CancellationToken cancellationToken)
    {
        await _maintenanceGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await CollectGarbageBatchAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _maintenanceGate.Release();
        }
    }

    private async Task<int> CollectGarbageBatchAsync(
        CancellationToken cancellationToken)
    {
        var page = await chunks.EnumerateChunkIdsPageAsync(
            _garbageCollectionCursor,
            _options.GarbageCollectionChunksPerMaintenancePass,
            cancellationToken).ConfigureAwait(false);
        if (page.Items.Count == 0)
        {
            _garbageCollectionCursor = null;
            return 0;
        }

        var firstReachabilitySnapshot = await metadata.FindReachableChunkIdsAsync(page.Items, cancellationToken).ConfigureAwait(false);
        var reservations = new List<ChunkStore.ChunkMutationReservation>();
        var deleted = 0;
        try
        {
            foreach (var chunk in page.Items)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (firstReachabilitySnapshot.Contains(chunk))
                    continue;
                var reservation = chunks.TryReserveForMutation(chunk);
                if (reservation is not null)
                    reservations.Add(reservation);
            }

            var confirmedReachability = await metadata.FindReachableChunkIdsAsync(
                reservations.Select(reservation => reservation.Id).ToArray(),
                cancellationToken).ConfigureAwait(false);
            for (var reservationIndex = 0; reservationIndex < reservations.Count; reservationIndex++)
            {
                var reservation = reservations[reservationIndex];
                cancellationToken.ThrowIfCancellationRequested();
                if (!confirmedReachability.Contains(reservation.Id))
                {
                    faultInjector.Inject(StorageFaultPoint.BeforeGarbageCollectionDelete);
                    if (await reservation.TryDeleteAsync(cancellationToken).ConfigureAwait(false))
                        deleted++;
                }
            }
            _garbageCollectionCursor = page.HasMore ? page.Items[^1] : null;
            return deleted;
        }
        finally
        {
            for (var reservationIndex = 0; reservationIndex < reservations.Count; reservationIndex++)
                reservations[reservationIndex].Dispose();
        }
    }

    private async Task ScanIntegrityAsync(
        int reachableChunkCount,
        CancellationToken cancellationToken)
    {
        if (_integrityCursor is null)
            ResetIntegrityCycle();
        var page = await metadata.ListReachableChunkIdsAsync(
            _integrityCursor,
            _options.IntegrityScanChunksPerMaintenancePass,
            excludeCustomerProvidedKeyDomains: false,
            cancellationToken).ConfigureAwait(false);
        if (page.Items.Count == 0)
        {
            telemetry.RecordIntegrity(new StorageIntegritySnapshot(
                reachableChunkCount,
                _integrityChecked,
                _integrityVerified,
                _integrityCustomerKey,
                _integrityMissing,
                _integrityCorrupt,
                true,
                metadata.GetUtcNow()));
            _integrityCursor = null;
            return;
        }

        foreach (var id in page.Items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var status = await chunks.VerifyChunkAsync(id, cancellationToken).ConfigureAwait(false);
            _integrityChecked++;
            switch (status)
            {
                case ChunkIntegrityStatus.Verified:
                    _integrityVerified++;
                    break;
                case ChunkIntegrityStatus.RequiresCustomerKey:
                    _integrityCustomerKey++;
                    break;
                case ChunkIntegrityStatus.Missing:
                    _integrityMissing++;
                    break;
                case ChunkIntegrityStatus.Corrupt:
                    _integrityCorrupt++;
                    break;
                default:
                    throw new InvalidDataException($"Unexpected chunk integrity status: {status}.");
            }
        }

        var complete = !page.HasMore;
        var snapshot = new StorageIntegritySnapshot(
            reachableChunkCount,
            _integrityChecked,
            _integrityVerified,
            _integrityCustomerKey,
            _integrityMissing,
            _integrityCorrupt,
            complete,
            metadata.GetUtcNow());
        if (complete || !snapshot.Healthy || telemetry.Integrity.Healthy)
            telemetry.RecordIntegrity(snapshot);
        _integrityCursor = complete ? null : page.Items[^1];
    }

    private async Task<PackCompactionResult> CompactChunkPacksAsync(
        CancellationToken cancellationToken)
    {
        var page = await metadata.ListSealedChunkPacksAsync(
            _packCompactionCursor,
            _options.ChunkPacksPerMaintenancePass,
            cancellationToken).ConfigureAwait(false);
        if (page.Items.Count == 0)
        {
            _packCompactionCursor = null;
            return PackCompactionResult.Skipped;
        }

        var examined = 0;
        var compacted = 0;
        var reclaimedRecords = 0;
        long bytesSaved = 0;
        foreach (var pack in page.Items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var result = await chunks.TryCompactPackAsync(pack, cancellationToken).ConfigureAwait(false);
            examined += result.ExaminedPacks;
            compacted += result.CompactedPacks;
            reclaimedRecords += result.ReclaimedRecords;
            bytesSaved = checked(bytesSaved + result.BytesSaved);
        }
        _packCompactionCursor = page.HasMore ? page.Items[^1].PackId : null;
        return new PackCompactionResult(examined, compacted, reclaimedRecords, bytesSaved);
    }

    private void ResetIntegrityCycle()
    {
        _integrityCursor = null;
        _integrityChecked = 0;
        _integrityVerified = 0;
        _integrityCustomerKey = 0;
        _integrityMissing = 0;
        _integrityCorrupt = 0;
    }

    private async Task<ChunkRecompressionResult> RecompressColdChunksAsync(
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        var page = await metadata.ListReachableChunkIdsAsync(
            _recompressionCursor,
            _options.BackgroundCompressionChunksPerMaintenancePass,
            excludeCustomerProvidedKeyDomains: true,
            cancellationToken).ConfigureAwait(false);
        if (page.Items.Count == 0)
        {
            _recompressionCursor = null;
            return ChunkRecompressionResult.Skipped;
        }

        var examined = 0;
        var recompressed = 0;
        long bytesSaved = 0;
        foreach (var id in page.Items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var result = await chunks.TryRecompressChunkAsync(id, now, cancellationToken).ConfigureAwait(false);
            examined += result.ExaminedChunks;
            recompressed += result.RecompressedChunks;
            bytesSaved = checked(bytesSaved + result.BytesSaved);
        }

        _recompressionCursor = page.HasMore ? page.Items[^1] : null;
        return new ChunkRecompressionResult(examined, recompressed, bytesSaved);
    }

    private async Task<BlobRecord> CompleteCopyIfDueAsync(
        BlobRecord blob,
        CancellationToken cancellationToken)
    {
        var copy = blob.Copy;
        if (copy is null || !string.Equals(copy.Status, "pending", StringComparison.Ordinal) ||
            blob.PendingCopyContent is null)
        {
            return blob;
        }

        var now = metadata.GetUtcNow();
        if (copy.ExpiresAt <= now &&
            (!copy.ReadyAt.HasValue || copy.ReadyAt > copy.ExpiresAt))
        {
            var failedTier = RehydrationTargetTier(blob.ArchiveStatus) ?? blob.AccessTier;
            var failed = blob with
            {
                Content = copy.IsIncremental
                    ? blob.Content
                    : chunks.Empty(blob.Account, EncryptionOf(blob)),
                PendingCopyContent = null,
                PendingCopyCommittedBlocks = null,
                PendingCopyAppendBlockCount = null,
                PendingCopyIsSealed = null,
                PendingCopyPageRanges = null,
                CommittedBlocks = copy.IsIncremental ? blob.CommittedBlocks : [],
                PageRanges = copy.IsIncremental ? blob.PageRanges : [],
                AppendBlockCount = copy.IsIncremental ? blob.AppendBlockCount : 0,
                IsSealed = copy.IsIncremental && blob.IsSealed,
                AccessTier = failedTier,
                ArchiveStatus = null,
                RehydratePriority = null,
                RehydrateCompleteAt = null,
                Copy = copy with
                {
                    Status = "failed",
                    BytesCopied = 0,
                    Description = "500 (OperationCancelled)",
                    CompletedAt = now,
                    ReadyAt = null,
                    ExpiresAt = null
                },
                Revision = MetadataStore.NewRevision(),
                ETag = MetadataStore.NewETag(),
                LastModified = now
            };
            try
            {
                await metadata.PutBlobRecordAsync(failed, blob.Revision, cancellationToken).ConfigureAwait(false);
                return failed;
            }
            catch (StorageConcurrencyException)
            {
                return await metadata.GetBlobAsync(
                           blob.Account,
                           blob.Container,
                           blob.Name,
                           blob.VersionId,
                           blob.Snapshot,
                           includeDeleted: false,
                           cancellationToken).ConfigureAwait(false)
                       ?? throw AzureStorageException.BlobNotFound();
            }
        }

        if (!copy.ReadyAt.HasValue || copy.ReadyAt.Value > now)
            return blob;

        using var contentPin = chunks.Pin(blob.PendingCopyContent);
        var updated = blob with
        {
            Content = blob.PendingCopyContent,
            Lease = leases.ResetAfterBlobWrite(blob.Lease),
            PendingCopyContent = null,
            CommittedBlocks = blob.PendingCopyCommittedBlocks ?? blob.CommittedBlocks,
            PendingCopyCommittedBlocks = null,
            AppendBlockCount = blob.PendingCopyAppendBlockCount ?? blob.AppendBlockCount,
            PendingCopyAppendBlockCount = null,
            IsSealed = blob.PendingCopyIsSealed ?? blob.IsSealed,
            PendingCopyIsSealed = null,
            PageRanges = blob.PendingCopyPageRanges ?? blob.PageRanges,
            PendingCopyPageRanges = null,
            IncrementalCopySourceSnapshot = copy.IsIncremental
                ? copy.SourceSnapshot
                : blob.IncrementalCopySourceSnapshot,
            Copy = copy with
            {
                Status = "success",
                BytesCopied = copy.TotalBytes,
                CompletedAt = now,
                ReadyAt = null,
                ExpiresAt = null
            },
            Revision = MetadataStore.NewRevision(),
            ETag = MetadataStore.NewETag(),
            LastModified = now,
            LastAccessedAt = IsLastAccessTimeTrackingEnabled(blob.Account)
                ? now
                : blob.LastAccessedAt
        };
        try
        {
            if (copy.IsIncremental)
                return await metadata.CompleteIncrementalCopyAsync(updated, blob.Revision, now, cancellationToken).ConfigureAwait(false);
            await metadata.PutBlobRecordAsync(updated, blob.Revision, cancellationToken).ConfigureAwait(false);
            return updated;
        }
        catch (StorageConcurrencyException)
        {
            return await metadata.GetBlobAsync(
                       blob.Account,
                       blob.Container,
                       blob.Name,
                       blob.VersionId,
                       blob.Snapshot,
                       includeDeleted: false,
                       cancellationToken).ConfigureAwait(false)
                   ?? throw AzureStorageException.BlobNotFound();
        }
    }

    private async Task<BlobRecord> CompleteRehydrationIfDueAsync(
        BlobRecord blob,
        CancellationToken cancellationToken)
    {
        if (string.Equals(blob.Copy?.Status, "pending", StringComparison.Ordinal) ||
            blob.ArchiveStatus is null ||
            !blob.RehydrateCompleteAt.HasValue ||
            blob.RehydrateCompleteAt.Value > metadata.GetUtcNow())
        {
            return blob;
        }

        var targetTier = RehydrationTargetTier(blob.ArchiveStatus) switch
        {
            { } tier => tier,
            _ => throw new InvalidDataException("The blob has an invalid archive rehydration status.")
        };
        var now = metadata.GetUtcNow();
        var updated = blob with
        {
            AccessTier = targetTier,
            SmartAccessTier = string.Equals(targetTier, "Smart", StringComparison.Ordinal) ? "Hot" : null,
            SmartTierLastAccessedAt = string.Equals(targetTier, "Smart", StringComparison.Ordinal) ? now : null,
            AccessTierChangedAt = now,
            ArchiveStatus = null,
            RehydratePriority = null,
            RehydrateCompleteAt = null,
            Revision = MetadataStore.NewRevision()
        };
        try
        {
            await metadata.PutBlobRecordAsync(updated, blob.Revision, cancellationToken).ConfigureAwait(false);
            return updated;
        }
        catch (StorageConcurrencyException)
        {
            return await metadata.GetBlobAsync(
                       blob.Account,
                       blob.Container,
                       blob.Name,
                       blob.VersionId,
                       blob.Snapshot,
                       includeDeleted: false,
                       cancellationToken).ConfigureAwait(false)
                   ?? throw AzureStorageException.BlobNotFound();
        }
    }

    private static string? RehydrationTargetTier(string? archiveStatus) => archiveStatus switch
    {
        "rehydrate-pending-to-hot" => "Hot",
        "rehydrate-pending-to-cool" => "Cool",
        "rehydrate-pending-to-cold" => "Cold",
        "rehydrate-pending-to-smart" => "Smart",
        _ => null
    };

    private async Task<BlobRecord> TransitionSmartTierIfDueAsync(
        BlobRecord blob,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        if (blob.Kind != BlobKind.BlockBlob ||
            !string.Equals(blob.AccessTier, "Smart", StringComparison.Ordinal) ||
            blob.ArchiveStatus is not null)
        {
            return blob;
        }

        const long minimumManagedLength = 128L * 1024;
        var lastAccessedAt = blob.SmartTierLastAccessedAt
                             ?? blob.AccessTierChangedAt
                             ?? blob.CreatedAt;
        var inactiveFor = now - lastAccessedAt;
        var target = blob.Content.Length <= minimumManagedLength
            ? "Hot"
            : inactiveFor >= TimeSpan.FromDays(90)
                ? "Cold"
                : inactiveFor >= TimeSpan.FromDays(30)
                    ? "Cool"
                    : "Hot";
        if (string.Equals(blob.SmartAccessTier, target, StringComparison.Ordinal))
            return blob;

        var updated = blob with
        {
            Revision = MetadataStore.NewRevision(),
            SmartAccessTier = target,
            SmartTierLastAccessedAt = lastAccessedAt,
            AccessTierChangedAt = now
        };
        await metadata.PutBlobRecordAsync(updated, blob.Revision, cancellationToken).ConfigureAwait(false);
        return updated;
    }

    private BlobRecord NewBlob(
        string account,
        string container,
        string name,
        BlobKind kind,
        ContentManifest content,
        BlobWriteOptions options,
        DateTimeOffset now)
    {
        if (options.ImmutabilityLocked && !options.ImmutabilityUntil.HasValue || options.ImmutabilityUntil <= now)
        {
            throw AzureStorageException.InvalidHeader(
                "x-ms-immutability-policy-until-date",
                options.ImmutabilityUntil?.ToString("R", CultureInfo.InvariantCulture));
        }
        if (options.AccessTier is not null &&
            options.AccessTier is not ("Hot" or "Cool" or "Cold" or "Smart" or "Archive"))
        {
            throw AzureStorageException.InvalidHeader("x-ms-access-tier", options.AccessTier);
        }
        if (options.EncryptionScope is not null && options.AccessTierSpecified)
            throw EncryptionScopeTierChangeNotSupported();
        if (options.ExpiresAt.HasValue)
        {
            if (!IsHierarchicalNamespaceEnabled(account))
                throw AzureStorageException.InvalidHeader("x-ms-expiry-option");
            if (options.ExpiresAt <= now)
            {
                throw AzureStorageException.InvalidHeader(
                    "x-ms-expiry-time",
                    options.ExpiresAt.Value.ToString("R", CultureInfo.InvariantCulture));
            }
        }

        return new BlobRecord
        {
            Account = account,
            Container = container,
            Name = name,
            GenerationId = Guid.NewGuid().ToString("N"),
            Revision = MetadataStore.NewRevision(),
            IsCurrent = true,
            Kind = kind,
            Content = content,
            ETag = MetadataStore.NewETag(),
            CreatedAt = now,
            LastModified = now,
            Owner = IsHierarchicalNamespaceEnabled(account) ? options.CreatorObjectId ?? "$superuser" : "$superuser",
            Metadata = options.Metadata,
            Tags = options.Tags ?? new Dictionary<string, string>(StringComparer.Ordinal),
            Http = options.Http,
            AccessTier = options.AccessTier ?? "Hot",
            AccessTierInferred = kind == BlobKind.BlockBlob &&
                                 (options.AccessTierInferred ?? options.AccessTier is null),
            SmartAccessTier = string.Equals(options.AccessTier, "Smart", StringComparison.Ordinal) ? "Hot" : null,
            LastAccessedAt = IsLastAccessTimeTrackingEnabled(account) ? now : null,
            AccessTierChangedAt = options.AccessTierSpecified ? now : null,
            ImmutabilityUntil = options.ImmutabilityUntil,
            ImmutabilityLocked = options.ImmutabilityLocked,
            HasLegalHold = options.HasLegalHold,
            EncryptionScope = options.EncryptionScope,
            EncryptionContext = string.IsNullOrEmpty(options.EncryptionContext)
                ? null
                : options.EncryptionContext,
            ExpiresAt = options.ExpiresAt,
            CustomerProvidedKeySha256 = options.CustomerProvidedKeySha256,
            SmartTierLastAccessedAt = string.Equals(options.AccessTier, "Smart", StringComparison.Ordinal) ? now : null
        };
    }

    private void ValidateEncryptionContext(string account, BlobWriteOptions options)
    {
        if (string.IsNullOrEmpty(options.EncryptionContext))
            return;
        if (!IsHierarchicalNamespaceEnabled(account) || options.EncryptionContext.Length > 1024)
        {
            throw AzureStorageException.InvalidHeader(
                "x-ms-encryption-context",
                options.EncryptionContext);
        }
    }

    private async Task<BlobWriteOptions> ApplyContainerEncryptionPolicyAsync(
        string account,
        string container,
        BlobWriteOptions options,
        CancellationToken cancellationToken,
        BlobRecord? current = null)
    {
        var containerRecord = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken).ConfigureAwait(false);
        if (options.ImmutabilityUntil.HasValue || options.ImmutabilityLocked || options.HasLegalHold)
            EnsureVersionLevelImmutabilityEnabled(containerRecord);
        var resolved = ApplyContainerEncryptionPolicy(containerRecord, EncryptionOf(options), current);
        return options with
        {
            EncryptionScope = resolved.Scope,
            CustomerProvidedKeySha256 = resolved.CustomerProvidedKeySha256,
            CustomerProvidedKey = resolved.CustomerProvidedKey
        };
    }

    private static BlobEncryption ApplyContainerEncryptionPolicy(
        ContainerRecord container,
        BlobEncryption requested,
        BlobRecord? current = null)
    {
        if (container.PreventEncryptionScopeOverride &&
            requested.Scope is not null &&
            !string.Equals(requested.Scope, container.DefaultEncryptionScope, StringComparison.Ordinal))
        {
            throw AzureStorageException.RequestForbiddenByContainerEncryptionPolicy();
        }

        if (current is not null)
        {
            if (container.PreventEncryptionScopeOverride &&
                current.CustomerProvidedKeySha256 is null &&
                !string.Equals(current.EncryptionScope, container.DefaultEncryptionScope, StringComparison.Ordinal))
            {
                throw AzureStorageException.RequestForbiddenByContainerEncryptionPolicy();
            }

            if (requested.Scope is null &&
                requested.CustomerProvidedKeySha256 is null &&
                current.CustomerProvidedKeySha256 is null &&
                string.Equals(current.EncryptionScope, container.DefaultEncryptionScope, StringComparison.Ordinal))
            {
                requested = requested with { Scope = current.EncryptionScope };
            }
            if (!string.Equals(requested.Scope, current.EncryptionScope, StringComparison.Ordinal) ||
                !string.Equals(
                    requested.CustomerProvidedKeySha256,
                    current.CustomerProvidedKeySha256,
                    StringComparison.Ordinal))
            {
                throw AzureStorageException.BlobUsesCustomerSpecifiedEncryption();
            }
            return requested;
        }

        if (requested.CustomerProvidedKeySha256 is not null)
            return requested;

        return requested.Scope is null && container.DefaultEncryptionScope is not null
            ? requested with { Scope = container.DefaultEncryptionScope }
            : requested;
    }

    private static BlobEncryption EncryptionOf(BlobWriteOptions options) =>
        new(options.EncryptionScope, options.CustomerProvidedKeySha256, options.CustomerProvidedKey);

    private static BlobEncryption EncryptionOf(BlobRecord blob) =>
        new(blob.EncryptionScope, blob.CustomerProvidedKeySha256);

    private static AzureStorageException UnsupportedEncryptionTransition() => new(
        StatusCodes.Status409Conflict,
        "BlobOperationNotSupported",
        "The copy source and destination use different request-level encryption settings.");

    private void EnsureFlatNamespace(string account)
    {
        if (IsHierarchicalNamespaceEnabled(account))
            throw AzureStorageException.BlobOperationNotSupported();
    }

    private void EnsureBlobKindSupported(string account, BlobKind kind)
    {
        if (kind == BlobKind.PageBlob)
            EnsureFlatNamespace(account);
    }

    private static AzureStorageException EncryptionScopeTierChangeNotSupported() => new(
        StatusCodes.Status409Conflict,
        "BlobOperationNotSupported",
        "The access tier cannot be changed for a blob that uses an encryption scope.");

    private static AzureStorageException CannotVerifyCopySource(string message) => new(
        StatusCodes.Status500InternalServerError,
        "CannotVerifyCopySource",
        message);

    private static List<PageRange> UpdatePageRanges(
        IReadOnlyList<PageRange> current,
        long start,
        long end,
        bool clear)
    {
        if (clear)
        {
            var remaining = new List<PageRange>();
            foreach (var range in current)
            {
                if (range.End < start || range.Start > end)
                {
                    remaining.Add(range);
                    continue;
                }
                if (range.Start < start)
                    remaining.Add(new PageRange(range.Start, start - 1));
                if (range.End > end)
                    remaining.Add(new PageRange(end + 1, range.End));
            }
            return remaining;
        }

        var ordered = current.Append(new PageRange(start, end)).OrderBy(range => range.Start).ToArray();
        var merged = new List<PageRange>();
        foreach (var range in ordered)
        {
            if (merged.Count == 0 || range.Start > merged[^1].End + 1)
            {
                merged.Add(range);
                continue;
            }
            merged[^1] = new PageRange(merged[^1].Start, Math.Max(merged[^1].End, range.End));
        }
        return merged;
    }

    private async Task CompareAllocatedPagesAsync(
        BlobRecord current,
        BlobRecord previous,
        BlobEncryption encryption,
        long start,
        long endExclusive,
        List<PageRange> changed,
        CancellationToken cancellationToken)
    {
        const int comparisonBatchBytes = 4 * 1024 * 1024;
        var cursor = start;
        while (cursor < endExclusive)
        {
            var length = checked((int)Math.Min(comparisonBatchBytes, endExclusive - cursor));
            using var currentBytes = new MemoryStream(length);
            using var previousBytes = new MemoryStream(length);
            await chunks.WriteRangeAsync(current.Content, encryption, cursor, length, currentBytes, cancellationToken).ConfigureAwait(false);
            await chunks.WriteRangeAsync(previous.Content, encryption, cursor, length, previousBytes, cancellationToken).ConfigureAwait(false);

            var currentSpan = currentBytes.GetBuffer().AsSpan(0, length);
            var previousSpan = previousBytes.GetBuffer().AsSpan(0, length);
            for (var offset = 0; offset < length; offset += 512)
            {
                if (!currentSpan.Slice(offset, 512).SequenceEqual(previousSpan.Slice(offset, 512)))
                    AddMergedPageRange(changed, cursor + offset, cursor + offset + 511);
            }
            cursor += length;
        }
    }

    private static PageRange[] ClipPageRanges(
        IReadOnlyList<PageRange> ranges,
        long start,
        long end)
    {
        if (end < start)
            return [];
        return ranges
            .Where(range => range.End >= start && range.Start <= end)
            .Select(range => new PageRange(Math.Max(range.Start, start), Math.Min(range.End, end)))
            .ToArray();
    }

    private static void AddMergedPageRange(List<PageRange> ranges, long start, long end)
    {
        if (ranges.Count > 0 && ranges[^1].End + 1 == start)
        {
            ranges[^1] = ranges[^1] with { End = end };
            return;
        }
        ranges.Add(new PageRange(start, end));
    }

    private ContainerRecord EffectiveContainer(ContainerRecord container) =>
        container with
        {
            Lease = leases.GetEffective(container.Lease),
            ImmutableStorageWithVersioningEnabled =
                container.ImmutableStorageWithVersioningEnabled ||
                IsImmutableStorageWithVersioningEnabled(container.Account, container.Name)
        };

    private BlobRecord EffectiveBlob(BlobRecord blob)
    {
        var fingerprint = ObjectReplicationFingerprint(blob);
        return blob with
        {
            Lease = leases.GetEffective(blob.Lease),
            VersionId = IsHierarchicalNamespaceEnabled(blob.Account) ? null : blob.VersionId,
            ObjectReplicationStatuses = blob.ObjectReplicationStatuses
                .Where(pair => string.Equals(
                    pair.Value.SourceFingerprint,
                    fingerprint,
                    StringComparison.Ordinal))
                .ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal)
        };
    }

    private static string ObjectReplicationFingerprint(BlobRecord blob)
    {
        var builder = new StringBuilder(512);
        AppendFingerprintValue(builder, blob.GenerationId);
        AppendFingerprintValue(builder, blob.Kind.ToString());
        AppendFingerprintValue(builder, blob.IsCurrent ? "1" : "0");
        AppendFingerprintValue(builder, blob.IsDeleted ? "1" : "0");
        AppendFingerprintValue(builder, blob.ETag);
        AppendFingerprintValue(builder, blob.Content.Domain);
        AppendFingerprintValue(builder, blob.Content.Length.ToString(CultureInfo.InvariantCulture));
        AppendFingerprintValue(builder, blob.Content.Sha256);
        AppendFingerprintValue(builder, blob.Http.ContentType);
        AppendFingerprintValue(builder, blob.Http.ContentEncoding);
        AppendFingerprintValue(builder, blob.Http.ContentLanguage);
        AppendFingerprintValue(builder, blob.Http.CacheControl);
        AppendFingerprintValue(builder, blob.Http.ContentDisposition);
        AppendFingerprintValue(builder, blob.Http.ContentMd5);
        AppendFingerprintValue(builder, blob.CustomerProvidedKeySha256);
        foreach (var pair in blob.Metadata.OrderBy(pair => pair.Key, StringComparer.OrdinalIgnoreCase))
        {
            AppendFingerprintValue(builder, pair.Key.ToUpperInvariant());
            AppendFingerprintValue(builder, pair.Value);
        }
        foreach (var pair in blob.Tags.OrderBy(pair => pair.Key, StringComparer.Ordinal))
        {
            AppendFingerprintValue(builder, pair.Key);
            AppendFingerprintValue(builder, pair.Value);
        }
        return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString())));
    }

    private static void AppendFingerprintValue(StringBuilder builder, string? value)
    {
        builder.Append(value?.Length ?? -1)
            .Append(':')
            .Append(value)
            .Append(';');
    }

    private BlobRecord PrepareBlobWrite(BlobRecord blob) =>
        blob with { Lease = leases.ResetAfterBlobWrite(blob.Lease) };

    private static string ProtocolLowercase(string value)
    {
        // Azure's sequence actions and rehydration status tokens are lowercase wire values.
#pragma warning disable CA1308
        return value.ToLowerInvariant();
#pragma warning restore CA1308
    }

    [StructLayout(LayoutKind.Sequential)]
    private readonly record struct ObjectReplicationPassResult(int Completed, int Failed);

    private async Task EnsureHierarchicalDirectoryIndexAsync(
        string account,
        string container,
        CancellationToken cancellationToken)
    {
        if (!IsHierarchicalNamespaceEnabled(account))
            return;
        var key = new ContainerKey(account, container);
        if (_indexedHierarchicalContainers.ContainsKey(key))
            return;

        await _hierarchicalDirectoryGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_indexedHierarchicalContainers.ContainsKey(key))
                return;
            await metadata.EnsureHierarchicalDirectoriesAsync(account, container, cancellationToken).ConfigureAwait(false);
            _indexedHierarchicalContainers.TryAdd(key, 0);
        }
        finally
        {
            _hierarchicalDirectoryGate.Release();
        }
    }

    private static void ValidateContainerName(string name)
    {
        if (name is "$root" or "$web")
            return;
        if (name.Length is < 3 or > 63 ||
            name[0] == '-' || name[^1] == '-' ||
            name.Contains("--", StringComparison.Ordinal) ||
            name.Any(character => !(character is >= 'a' and <= 'z' or >= '0' and <= '9' or '-')))
        {
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidResourceName", "The specified resource name contains invalid characters.");
        }
    }

    private static void ValidateBlobName(string name)
    {
        if (string.IsNullOrEmpty(name) ||
            name.Length > 1024 ||
            name.Count(character => character == '/') + 1 > 254 ||
            name.Any(char.IsControl))
        {
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidResourceName", "The specified resource name contains invalid characters.");
        }
    }

    private void EnsurePublicAccessAllowed(string account, string? publicAccess)
    {
        if (publicAccess is not null && !AllowsAnonymousPublicAccess(account))
            throw AzureStorageException.PublicAccessNotPermitted();
    }

    private static int ValidateBlockId(string blockId)
    {
        try
        {
            var length = Convert.FromBase64String(blockId).Length;
            if (length is 0 or > BlobServiceLimits.MaximumBlockIdBytes)
                throw new FormatException();
            return length;
        }
        catch (FormatException)
        {
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidQueryParameterValue", "The specified block ID is invalid.");
        }
    }

    private static void EnsureContainerMutable(ContainerRecord container)
    {
        if (container.HasLegalHold)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "ContainerHasLegalHold", "The container has a legal hold.");
        if (container.ImmutabilityLocked && container.ImmutabilityUntil > DateTimeOffset.UtcNow)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "ContainerImmutabilityPolicyLocked", "The container has a locked immutability policy.");
    }

    private async Task EnsureVersionLevelImmutabilityEnabledAsync(
        BlobRecord blob,
        CancellationToken cancellationToken)
    {
        var container = await GetContainerAsync(
            blob.Account,
            blob.Container,
            includeDeleted: false,
            cancellationToken).ConfigureAwait(false);
        EnsureVersionLevelImmutabilityEnabled(container);
    }

    private static void EnsureVersionLevelImmutabilityEnabled(ContainerRecord container)
    {
        if (!container.ImmutableStorageWithVersioningEnabled)
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "BlobOperationNotSupported",
                "The operation requires immutable storage with versioning to be enabled on the container.");
        }
    }

    private void EnsureBlobMutable(BlobRecord blob)
    {
        if (blob.HasLegalHold)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "BlobImmutableDueToLegalHold", "This operation is not permitted because the blob has a legal hold.");
        if (blob.ImmutabilityUntil > metadata.GetUtcNow())
            throw BlobImmutableDueToPolicy();
    }

    private static void EnsureNoPendingCopy(BlobRecord blob)
    {
        if (string.Equals(blob.Copy?.Status, "pending", StringComparison.Ordinal))
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "PendingCopyOperation",
                "There is currently a pending copy operation.");
        }
    }

    private static AzureStorageException BlobImmutableDueToPolicy() => new(
        StatusCodes.Status409Conflict,
        "BlobImmutableDueToPolicy",
        "This operation is not permitted because the blob is immutable.");
}
