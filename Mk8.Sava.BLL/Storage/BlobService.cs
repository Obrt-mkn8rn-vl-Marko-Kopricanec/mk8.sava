using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
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
    IOptions<SavaOptions> configuredOptions)
{
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

    public bool AllowsAnonymousPublicAccess => _options.AllowAnonymousPublicAccess;

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

    public async Task<IReadOnlyList<ContainerRecord>> ListContainersAsync(
        string account,
        bool includeDeleted,
        CancellationToken cancellationToken)
    {
        var containers = await metadata.ListContainersAsync(account, includeDeleted, cancellationToken);
        containers = containers
            .Where(container => container.Name != StorageAnalyticsService.LogsContainerName)
            .ToArray();
        if (!includeDeleted || !containers.Any(item => item.DeletedAt.HasValue && !item.DeleteRetentionUntil.HasValue))
            return containers.Select(EffectiveContainer).ToArray();
        var properties = await metadata.GetServicePropertiesAsync(account, cancellationToken);
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
            cancellationToken);
        var needsRetention = includeDeleted && page.Items.Any(item =>
            item.DeletedAt.HasValue && !item.DeleteRetentionUntil.HasValue);
        var properties = needsRetention
            ? await metadata.GetServicePropertiesAsync(account, cancellationToken)
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
        Dictionary<string, string> userMetadata,
        string? publicAccess,
        string? defaultEncryptionScope,
        bool preventEncryptionScopeOverride,
        CancellationToken cancellationToken)
    {
        ValidateContainerName(name);
        if (publicAccess is not null && publicAccess is not ("blob" or "container"))
            throw AzureStorageException.InvalidHeader("x-ms-blob-public-access", publicAccess);
        EnsurePublicAccessAllowed(publicAccess);

        var now = metadata.GetUtcNow();
        var container = new ContainerRecord
        {
            Account = account,
            Name = name,
            Revision = MetadataStore.NewRevision(),
            ETag = MetadataStore.NewETag(),
            CreatedAt = now,
            LastModified = now,
            Metadata = userMetadata,
            PublicAccess = publicAccess,
            DefaultEncryptionScope = defaultEncryptionScope,
            PreventEncryptionScopeOverride = preventEncryptionScopeOverride
        };

        if (!await metadata.TryCreateContainerAsync(container, cancellationToken))
        {
            var existing = await metadata.GetContainerAsync(account, name, includeDeleted: true, cancellationToken);
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
        var container = await metadata.GetContainerAsync(account, name, includeDeleted, cancellationToken)
                        ?? throw AzureStorageException.ContainerNotFound();
        return EffectiveContainer(container);
    }

    public async Task<ContainerRecord> SetContainerMetadataAsync(
        ContainerRecord current,
        Dictionary<string, string> userMetadata,
        CancellationToken cancellationToken)
    {
        var updated = current with
        {
            Metadata = userMetadata,
            Revision = MetadataStore.NewRevision(),
            ETag = MetadataStore.NewETag(),
            LastModified = metadata.GetUtcNow()
        };
        await metadata.PutContainerAsync(updated, current.Revision, cancellationToken);
        return updated;
    }

    public async Task<ContainerRecord> SetContainerAclAsync(
        ContainerRecord current,
        string? publicAccess,
        Dictionary<string, StoredAccessPolicy> policies,
        CancellationToken cancellationToken)
    {
        if (publicAccess is not null && publicAccess is not ("blob" or "container"))
            throw AzureStorageException.InvalidHeader("x-ms-blob-public-access", publicAccess);
        EnsurePublicAccessAllowed(publicAccess);
        var updated = current with
        {
            PublicAccess = publicAccess,
            AccessPolicies = policies,
            Revision = MetadataStore.NewRevision(),
            ETag = MetadataStore.NewETag(),
            LastModified = metadata.GetUtcNow()
        };
        await metadata.PutContainerAsync(updated, current.Revision, cancellationToken);
        return updated;
    }

    public async Task DeleteContainerAsync(ContainerRecord current, CancellationToken cancellationToken)
    {
        if (current.Name == StorageAnalyticsService.LogsContainerName)
        {
            throw new AzureStorageException(
                StatusCodes.Status403Forbidden,
                "ContainerOperationFailure",
                "The account being accessed does not have sufficient permissions to execute this operation.");
        }
        EnsureContainerMutable(current);
        var properties = await metadata.GetServicePropertiesAsync(current.Account, cancellationToken);
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
            await metadata.PutContainerAsync(deleted, current.Revision, cancellationToken);
        }
        else
        {
            await metadata.DeleteContainerPermanentlyAsync(current.Account, current.Name, current.Revision, cancellationToken);
        }
    }

    public async Task<ContainerRecord> RestoreContainerAsync(
        string account,
        string sourceName,
        string destinationName,
        string deletedVersion,
        CancellationToken cancellationToken)
    {
        ValidateContainerName(destinationName);
        var current = await GetContainerAsync(account, sourceName, includeDeleted: true, cancellationToken);
        if (current.DeletedAt is null || !string.Equals(current.DeletedVersion, deletedVersion, StringComparison.Ordinal))
            throw AzureStorageException.ContainerNotFound();
        var properties = await metadata.GetServicePropertiesAsync(account, cancellationToken);
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
        if (!await metadata.TryRestoreContainerAsync(sourceName, restored, current.Revision, cancellationToken))
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "ContainerAlreadyExists",
                "The specified container already exists.");
        }
        return restored;
    }

    public async Task<ContainerRecord> SetContainerLeaseAsync(
        ContainerRecord current,
        LeaseRecord lease,
        bool updateProperties,
        CancellationToken cancellationToken)
    {
        var updated = current with
        {
            Lease = lease,
            Revision = MetadataStore.NewRevision(),
            ETag = updateProperties ? MetadataStore.NewETag() : current.ETag,
            LastModified = updateProperties ? metadata.GetUtcNow() : current.LastModified
        };
        await metadata.PutContainerAsync(updated, current.Revision, cancellationToken);
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
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
        await EnsureHierarchicalDirectoryIndexAsync(account, container, cancellationToken);
        var blobs = await metadata.ListBlobsAsync(account, container, includeVersions, includeSnapshots, includeDeleted, cancellationToken);
        var effective = new List<BlobRecord>(blobs.Count);
        var properties = includeDeleted && blobs.Any(item => item.DeletedAt.HasValue && !item.DeleteRetentionUntil.HasValue)
            ? await metadata.GetServicePropertiesAsync(account, cancellationToken)
            : null;
        foreach (var blob in blobs)
        {
            var copy = await CompleteCopyIfDueAsync(blob, cancellationToken);
            var rehydrated = await CompleteRehydrationIfDueAsync(copy, cancellationToken);
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
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
        await EnsureHierarchicalDirectoryIndexAsync(account, container, cancellationToken);
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
            cancellationToken);
        ServiceProperties? properties = null;
        if (includeDeleted && page.Items.Any(item =>
                item.Blob is { DeletedAt: not null, DeleteRetentionUntil: null }))
        {
            properties = await metadata.GetServicePropertiesAsync(account, cancellationToken);
        }

        var effective = new List<BlobListEntry>(page.Items.Count);
        foreach (var item in page.Items)
        {
            if (item.Blob is null)
            {
                effective.Add(item);
                continue;
            }

            var copy = await CompleteCopyIfDueAsync(item.Blob, cancellationToken);
            var rehydrated = await CompleteRehydrationIfDueAsync(copy, cancellationToken);
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
            cancellationToken);
        var effective = new List<BlobRecord>(page.Items.Count);
        foreach (var blob in page.Items)
        {
            var copy = await CompleteCopyIfDueAsync(blob, cancellationToken);
            effective.Add(EffectiveBlob(await CompleteRehydrationIfDueAsync(copy, cancellationToken)));
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
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
        await EnsureHierarchicalDirectoryIndexAsync(account, container, cancellationToken);
        var blob = await metadata.GetBlobAsync(account, container, name, versionId, snapshot, includeDeleted, cancellationToken)
                   ?? throw AzureStorageException.BlobNotFound();
        blob = await CompleteCopyIfDueAsync(blob, cancellationToken);
        return EffectiveBlob(await CompleteRehydrationIfDueAsync(blob, cancellationToken));
    }

    public async Task<BlobRecord> RecordSmartTierAccessAsync(
        BlobRecord current,
        CancellationToken cancellationToken)
    {
        while (string.Equals(current.AccessTier, "Smart", StringComparison.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var now = metadata.GetUtcNow();
            var movedToHot = !string.Equals(current.SmartAccessTier, "Hot", StringComparison.Ordinal);
            var updated = current with
            {
                Revision = MetadataStore.NewRevision(),
                SmartAccessTier = "Hot",
                SmartTierLastAccessedAt = now,
                AccessTierChangedAt = movedToHot ? now : current.AccessTierChangedAt
            };
            try
            {
                await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
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
                              cancellationToken)
                          ?? throw AzureStorageException.BlobNotFound();
            }
        }
        return current;
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
        ValidateBlobName(name);
        options = await ApplyContainerEncryptionPolicyAsync(account, container, options, cancellationToken);
        var encryption = EncryptionOf(options);
        using var content = await chunks.StorePinnedAsync(account, encryption, source, cancellationToken);
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
            cancellationToken);
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
        ValidateBlobName(name);
        options = await ApplyContainerEncryptionPolicyAsync(account, container, options, cancellationToken);
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
            cancellationToken);
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
        EnsureBlobKindSupported(account, BlobKind.PageBlob);
        ValidateBlobName(name);
        const long maximumPageBlobBytes = 8L * 1024 * 1024 * 1024 * 1024;
        if (length > maximumPageBlobBytes)
            throw new RequestBodyTooLargeException(maximumPageBlobBytes);
        if (length < 0 || length % 512 != 0)
            throw AzureStorageException.InvalidHeader("x-ms-blob-content-length", length.ToString(CultureInfo.InvariantCulture));
        if (sequenceNumber < 0)
            throw AzureStorageException.InvalidHeader("x-ms-blob-sequence-number", sequenceNumber.ToString(CultureInfo.InvariantCulture));
        options = await ApplyContainerEncryptionPolicyAsync(account, container, options, cancellationToken);
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
            cancellationToken);
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
        var containerRecord = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
        var current = await metadata.GetBlobAsync(account, container, name, null, null, includeDeleted: false, cancellationToken);
        encryption = ApplyContainerEncryptionPolicy(containerRecord, encryption, current);
        if (current is not null)
            EnsureNoPendingCopy(current);
        if (current is not null && current.Kind != BlobKind.BlockBlob)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
        if (current is not null && !chunks.IsInDomain(account, encryption, current.Content))
            throw AzureStorageException.BlobUsesCustomerSpecifiedEncryption();
        var staged = await metadata.ListStagedBlocksAsync(account, container, name, cancellationToken);
        if (staged.Count >= BlobServiceLimits.MaximumUncommittedBlockCount &&
            staged.All(item => !string.Equals(item.BlockId, blockId, StringComparison.Ordinal)))
            throw new AzureStorageException(StatusCodes.Status409Conflict, "BlockCountExceedsLimit", "The uncommitted block count exceeds the maximum permitted value.");
        var existingId = staged.Select(item => item.BlockId).FirstOrDefault()
                         ?? current?.CommittedBlocks.FirstOrDefault()?.Id;
        if (existingId is not null && ValidateBlockId(existingId) != blockIdLength)
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidBlobOrBlock", "All block IDs for a blob must have the same length.");
        if (staged.Any(item => !chunks.IsInDomain(account, encryption, item.Content)))
            throw AzureStorageException.BlobUsesCustomerSpecifiedEncryption();
        using var content = await chunks.StorePinnedAsync(account, encryption, source, cancellationToken);
        await metadata.PutStagedBlockAsync(new StagedBlockRecord
        {
            Account = account,
            Container = container,
            BlobName = name,
            BlockId = blockId,
            Content = content.Manifest,
            CreatedAt = metadata.GetUtcNow()
        }, cancellationToken);
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
                        cancellationToken);
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
        if (blockList.Count > BlobServiceLimits.MaximumCommittedBlockCount)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "BlockCountExceedsLimit", "The block list may not contain more than 50,000 blocks.");

        var current = await metadata.GetBlobAsync(account, container, name, null, null, includeDeleted: false, cancellationToken);
        options = await ApplyContainerEncryptionPolicyAsync(
            account,
            container,
            options,
            cancellationToken,
            current);
        var staged = await metadata.ListStagedBlocksAsync(account, container, name, cancellationToken);
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

        var content = await chunks.ComposeAsync(account, encryption, selected.Select(item => item.Content).ToArray(), cancellationToken);
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
            cancellationToken);
    }

    public async Task<BlobRecord> AppendBlockAsync(
        BlobRecord current,
        Stream source,
        long? expectedPosition,
        long? expectedMaximumSize,
        BlobEncryption encryption,
        CancellationToken cancellationToken)
    {
        var container = await GetContainerAsync(current.Account, current.Container, includeDeleted: false, cancellationToken);
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
        using var appended = await chunks.StorePinnedAsync(current.Account, encryption, source, cancellationToken);
        if (appended.Manifest.Length > 100L * 1024 * 1024)
            throw new AzureStorageException(StatusCodes.Status413PayloadTooLarge, "RequestBodyTooLarge", "An append block cannot exceed 100 MiB.");
        if (expectedMaximumSize.HasValue && current.Content.Length + appended.Manifest.Length > expectedMaximumSize.Value)
        {
            throw new AzureStorageException(
                StatusCodes.Status412PreconditionFailed,
                "MaxBlobSizeConditionNotMet",
                "The max blob size condition specified was not met.");
        }
        var content = await chunks.ComposeAsync(current.Account, encryption, [current.Content, appended.Manifest], cancellationToken);
        using var contentPin = chunks.Pin(content);
        var updated = current with
        {
            GenerationId = Guid.NewGuid().ToString("N"),
            Revision = MetadataStore.NewRevision(),
            Content = content,
            ETag = MetadataStore.NewETag(),
            LastModified = metadata.GetUtcNow(),
            Lease = current.Lease,
            AppendBlockCount = checked(current.AppendBlockCount + 1),
            Copy = null
        };
        return await metadata.PublishBlobAsync(
            updated,
            current.GenerationId,
            current.Revision,
            IsHierarchicalNamespaceEnabled(current.Account),
            cancellationToken);
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
        EnsureFlatNamespace(current.Account);
        var container = await GetContainerAsync(current.Account, current.Container, includeDeleted: false, cancellationToken);
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
                cancellationToken);
        }
        catch (EndOfStreamException)
        {
            throw AzureStorageException.InvalidPageRange();
        }
        using (content)
        {
            var updated = current with
            {
                GenerationId = Guid.NewGuid().ToString("N"),
                Revision = MetadataStore.NewRevision(),
                Content = content.Manifest,
                PageRanges = UpdatePageRanges(current.PageRanges, start, end, clear),
                ETag = MetadataStore.NewETag(),
                LastModified = metadata.GetUtcNow()
            };
            return await metadata.PublishBlobAsync(
                updated,
                current.GenerationId,
                current.Revision,
                IsHierarchicalNamespaceEnabled(current.Account),
                cancellationToken);
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
            while (currentIndex < currentRanges.Count && currentRanges[currentIndex].End < cursor)
                currentIndex++;
            while (previousIndex < previousRanges.Count && previousRanges[previousIndex].End < cursor)
                previousIndex++;

            var currentAllocated = currentIndex < currentRanges.Count && currentRanges[currentIndex].Start <= cursor;
            var previousAllocated = previousIndex < previousRanges.Count && previousRanges[previousIndex].Start <= cursor;
            var currentBoundary = currentAllocated
                ? checked(currentRanges[currentIndex].End + 1)
                : currentIndex < currentRanges.Count ? currentRanges[currentIndex].Start : rangeEndExclusive;
            var previousBoundary = previousAllocated
                ? checked(previousRanges[previousIndex].End + 1)
                : previousIndex < previousRanges.Count ? previousRanges[previousIndex].Start : rangeEndExclusive;
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
                    cancellationToken);
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
        EnsureNoPendingCopy(blob);
        if (string.Equals(blob.AccessTier, "Archive", StringComparison.Ordinal))
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "BlobArchived",
                "This operation is not permitted on an archived blob.");
        }
        await chunks.WriteRangeAsync(blob.Content, encryption, offset, length, destination, cancellationToken);
    }

    public async Task<BlobRecord> SetBlobMetadataAsync(
        BlobRecord current,
        Dictionary<string, string> userMetadata,
        CancellationToken cancellationToken)
    {
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
        var properties = await metadata.GetServicePropertiesAsync(current.Account, cancellationToken);
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
                cancellationToken);
        }
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
        return updated;
    }

    public async Task<BlobRecord> SetBlobTagsAsync(
        BlobRecord current,
        Dictionary<string, string> tags,
        CancellationToken cancellationToken)
    {
        EnsureNoPendingCopy(current);
        EnsureBlobMutable(current);
        current = PrepareBlobWrite(current);
        if (tags.Count > 10)
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "TagsTooLarge", "The number of blob tags exceeds the permitted limit.");
        var updated = current with { Tags = tags, Copy = null, Revision = MetadataStore.NewRevision() };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
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
            nextSequence = sequenceAction?.ToLowerInvariant() switch
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
            resized = await chunks.ResizeSparsePinnedAsync(current.Account, encryption, current.Content, resizeTo.Value, cancellationToken);
            content = resized.Manifest;
        }

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
            LastModified = metadata.GetUtcNow()
        };
        try
        {
            await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
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
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
        return updated;
    }

    public async Task<BlobTierUpdate> SetTierAsync(
        BlobRecord current,
        string tier,
        string? rehydratePriority,
        CancellationToken cancellationToken)
    {
        EnsureNoPendingCopy(current);
        current = PrepareBlobWrite(current);
        if (current.Kind != BlobKind.BlockBlob)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
        if (current.EncryptionScope is not null)
            throw EncryptionScopeTierChangeNotSupported();
        if (tier is not ("Hot" or "Cool" or "Cold" or "Smart" or "Archive"))
            throw AzureStorageException.InvalidHeader("x-ms-access-tier", tier);
        if (rehydratePriority is not null && rehydratePriority is not ("Standard" or "High"))
            throw AzureStorageException.InvalidHeader("x-ms-rehydrate-priority", rehydratePriority);

        var now = metadata.GetUtcNow();
        BlobRecord updated;
        var pending = false;
        if (string.Equals(current.AccessTier, "Archive", StringComparison.Ordinal) && tier != "Archive")
        {
            if (!current.IsCurrent || current.Snapshot is not null)
                throw new AzureStorageException(StatusCodes.Status409Conflict, "BlobArchived", "This operation is not permitted on an archived blob.");

            var requestedStatus = $"rehydrate-pending-to-{tier.ToLowerInvariant()}";
            if (current.ArchiveStatus is not null && !string.Equals(current.ArchiveStatus, requestedStatus, StringComparison.Ordinal))
            {
                throw new AzureStorageException(
                    StatusCodes.Status409Conflict,
                    "BlobBeingRehydrated",
                    "This operation is not permitted because the blob is being rehydrated.");
            }

            var priority = current.RehydratePriority == "High" || rehydratePriority == "High" ? "High" : "Standard";
            var delay = priority == "High" ? _options.HighPriorityRehydrationDelay : _options.StandardRehydrationDelay;
            var completion = now.Add(delay);
            if (current.RehydrateCompleteAt.HasValue &&
                current.RehydratePriority == priority &&
                current.RehydrateCompleteAt.Value < completion)
            {
                completion = current.RehydrateCompleteAt.Value;
            }
            updated = current with
            {
                ArchiveStatus = requestedStatus,
                RehydratePriority = priority,
                RehydrateCompleteAt = completion,
                Copy = null,
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
                SmartAccessTier = tier == "Smart" ? "Hot" : null,
                SmartTierLastAccessedAt = tier == "Smart" ? now : null,
                ArchiveStatus = null,
                RehydratePriority = null,
                RehydrateCompleteAt = null,
                Copy = null,
                Revision = MetadataStore.NewRevision(),
                AccessTierChangedAt = now
            };
        }
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
        return new BlobTierUpdate(updated, pending);
    }

    public async Task<BlobRecord> SetExpiryAsync(BlobRecord current, DateTimeOffset? expiresAt, CancellationToken cancellationToken)
    {
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
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
        return updated;
    }

    public async Task<BlobRecord> SetBlobImmutabilityPolicyAsync(
        BlobRecord current,
        DateTimeOffset expiresOn,
        bool locked,
        CancellationToken cancellationToken)
    {
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
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
        return updated;
    }

    public async Task<BlobRecord> DeleteBlobImmutabilityPolicyAsync(
        BlobRecord current,
        CancellationToken cancellationToken)
    {
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
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
        return updated;
    }

    public async Task<BlobRecord> SetBlobLegalHoldAsync(
        BlobRecord current,
        bool hasLegalHold,
        CancellationToken cancellationToken)
    {
        EnsureNoPendingCopy(current);
        current = PrepareBlobWrite(current);
        var updated = current with
        {
            HasLegalHold = hasLegalHold,
            Copy = null,
            Revision = MetadataStore.NewRevision()
        };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
        return updated;
    }

    public async Task<BlobRecord> SetBlobLeaseAsync(
        BlobRecord current,
        LeaseRecord lease,
        CancellationToken cancellationToken)
    {
        EnsureNoPendingCopy(current);
        var updated = current with
        {
            Lease = lease,
            Revision = MetadataStore.NewRevision()
        };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
        return updated;
    }

    public Task<BlobRecord> CreateSnapshotAsync(
        BlobRecord current,
        Dictionary<string, string>? snapshotMetadata,
        CancellationToken cancellationToken)
    {
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
                cancellationToken))
        {
            throw AzureStorageException.DirectoryIsNotEmpty();
        }
        var records = await metadata.ListBlobFamilyAsync(
            current.Account,
            current.Container,
            current.Name,
            includeDeleted: true,
            cancellationToken);
        var relatedSnapshots = records.Where(item => item.Name == current.Name && item.Snapshot is not null && !item.IsDeleted).ToArray();
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

        foreach (var target in targets)
            EnsureBlobMutable(target);

        var properties = await metadata.GetServicePropertiesAsync(current.Account, cancellationToken);
        var hierarchicalNamespace = IsHierarchicalNamespaceEnabled(current.Account);
        var deletionIds = records
            .Where(item => item.DeletionId.HasValue)
            .Select(item => item.DeletionId!.Value)
            .ToHashSet();
        var mutations = new List<BlobRecordMutation>(targets.Count);
        foreach (var target in targets)
        {
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
            clearStagedBlocksForCurrentBlobs: true);
    }

    public async Task PermanentlyDeleteBlobAsync(
        BlobRecord current,
        bool hasExplicitSnapshotOrVersion,
        CancellationToken cancellationToken)
    {
        var properties = await metadata.GetServicePropertiesAsync(current.Account, cancellationToken);
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
        if (!await metadata.DeleteBlobRecordAsync(current.GenerationId, current.Revision, cancellationToken))
            throw AzureStorageException.BlobNotFound();
    }

    public async Task UndeleteBlobAsync(
        string account,
        string container,
        string name,
        CancellationToken cancellationToken)
    {
        ValidateBlobName(name);
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
        var records = await metadata.ListBlobFamilyAsync(account, container, name, includeDeleted: true, cancellationToken);
        var properties = await metadata.GetServicePropertiesAsync(account, cancellationToken);
        var now = metadata.GetUtcNow();
        var mutations = new List<BlobRecordMutation>();
        foreach (var record in records.Where(item => item.Name == name && item.IsDeleted))
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
        await metadata.ApplyBlobRecordMutationsAsync(mutations, cancellationToken);
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
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
        var records = await metadata.ListBlobFamilyAsync(
            account,
            container,
            sourceName,
            includeDeleted: true,
            cancellationToken);
        var properties = await metadata.GetServicePropertiesAsync(account, cancellationToken);
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
        if (!await metadata.TryRestoreDeletedBlobAsync(restored, source.Revision, cancellationToken))
            throw AzureStorageException.PathAlreadyExists();
    }

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
        options = await ApplyContainerEncryptionPolicyAsync(account, container, options, cancellationToken);
        using var prepared = await PrepareCopyContentAsync(
            account,
            source,
            EncryptionOf(options),
            preserveCommittedBlocks: true,
            cancellationToken);
        return await BeginCopyAsync(
            account,
            container,
            name,
            source.Kind,
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
            cancellationToken);
    }

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
        if (source.Kind != BlobKind.BlockBlob)
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "InvalidSourceBlobType",
                "The source blob type is invalid for this operation.");
        }
        options = await ApplyContainerEncryptionPolicyAsync(account, container, options, cancellationToken);
        using var prepared = await PrepareCopyContentAsync(
            account,
            source,
            EncryptionOf(options),
            preserveCommittedBlocks: true,
            cancellationToken);
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
            cancellationToken);
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
        EnsureBlobKindSupported(account, source.Kind);
        options = await ApplyContainerEncryptionPolicyAsync(account, container, options, cancellationToken);
        using var prepared = await PrepareCopyContentAsync(
            account,
            source,
            EncryptionOf(options),
            preserveCommittedBlocks: true,
            cancellationToken);
        ValidateBlobName(name);
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
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
            cancellationToken);
    }

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
        options = await ApplyContainerEncryptionPolicyAsync(account, container, options, cancellationToken);
        using var content = await StoreRemoteBlockBlobAsync(
            account,
            source,
            contentLength,
            sourceBlocks,
            EncryptionOf(options),
            cancellationToken);
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
            cancellationToken);
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
            cancellationToken);
    }

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
        EnsureFlatNamespace(account);
        ValidateBlobName(name);
        options = await ApplyContainerEncryptionPolicyAsync(
            account,
            container,
            options,
            cancellationToken,
            current);
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
            cancellationToken);
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
                cancellationToken);
        await metadata.PutBlobRecordAsync(pending, current.Revision, cancellationToken);
        return pending;
    }

    public async Task<BlobRecord> BeginCopyFromStreamAsync(
        string account,
        string container,
        string name,
        Stream source,
        long contentLength,
        BlobKind sourceKind,
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
        options = await ApplyContainerEncryptionPolicyAsync(account, container, options, cancellationToken);
        var encryption = EncryptionOf(options);
        if (sourceKind == BlobKind.BlockBlob)
        {
            using var content = await StoreRemoteBlockBlobAsync(
                account,
                source,
                contentLength,
                sourceBlocks,
                encryption,
                cancellationToken);
            return await BeginCopyAsync(
                account,
                container,
                name,
                sourceKind,
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
                cancellationToken);
        }
        using var stored = await chunks.StorePinnedAsync(account, encryption, source, cancellationToken);
        if (stored.Manifest.Length != contentLength)
            throw CannotVerifyCopySource("The copy source length did not match its Content-Length value.");
        return await BeginCopyAsync(
            account,
            container,
            name,
            sourceKind,
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
            cancellationToken);
    }

    private async Task<BlobRecord> BeginCopyAsync(
        string account,
        string container,
        string name,
        BlobKind sourceKind,
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
        ValidateBlobName(name);
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
        var encryption = EncryptionOf(options);
        if (!chunks.IsInDomain(account, encryption, sourceContent))
            throw UnsupportedEncryptionTransition();
        using var sourcePin = chunks.Pin(sourceContent);
        var now = metadata.GetUtcNow();
        var copyId = Guid.NewGuid().ToString();
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
            Copy = new CopyState
            {
                Id = copyId,
                Source = sourceUri,
                Status = "pending",
                BytesCopied = 0,
                TotalBytes = sourceContent.Length,
                ReadyAt = now.Add(_options.AsyncCopyCompletionDelay),
                ExpiresAt = now.AddDays(14)
            }
        };
        return await metadata.PublishBlobAsync(
            proposed,
            expectedGeneration,
            expectedRevision,
            IsHierarchicalNamespaceEnabled(account),
            cancellationToken);
    }

    private static async Task EnsureSourceExhaustedAsync(Stream source, CancellationToken cancellationToken)
    {
        var probe = new byte[1];
        if (await source.ReadAsync(probe, cancellationToken) != 0)
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
        var leases = new List<IDisposable>();
        try
        {
            if (sourceBlocks.Count == 0)
            {
                var stored = await chunks.StorePinnedAsync(account, encryption, source, cancellationToken);
                leases.Add(stored);
                if (stored.Manifest.Length != contentLength)
                    throw CannotVerifyCopySource("The copy source length did not match its Content-Length value.");
                return new PreparedCopyContent(stored.Manifest, [], leases);
            }

            var copiedBlocks = new List<CommittedBlockRecord>(sourceBlocks.Count);
            foreach (var sourceBlock in sourceBlocks)
            {
                using var blockSource = new ExactLengthReadStream(source, sourceBlock.Length);
                StoredContent storedBlock;
                try
                {
                    storedBlock = await chunks.StorePinnedAsync(account, encryption, blockSource, cancellationToken);
                }
                catch (EndOfStreamException)
                {
                    throw CannotVerifyCopySource("The copy source ended before its committed block did.");
                }
                leases.Add(storedBlock);
                copiedBlocks.Add(new CommittedBlockRecord(sourceBlock.Id, storedBlock.Manifest));
            }
            await EnsureSourceExhaustedAsync(source, cancellationToken);
            var content = await chunks.ComposeAsync(
                account,
                encryption,
                copiedBlocks.Select(block => block.Content).ToArray(),
                cancellationToken);
            if (content.Length != contentLength)
                throw CannotVerifyCopySource("The copy source length did not match its committed block list.");
            return new PreparedCopyContent(content, copiedBlocks, leases);
        }
        catch
        {
            foreach (var lease in leases)
                lease.Dispose();
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
                cancellationToken);
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

        var leases = new List<IDisposable>();
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
                        cancellationToken);
                    leases.Add(copied);
                    copiedBlocks.Add(new CommittedBlockRecord(block.Id, copied.Manifest));
                }
                var content = await chunks.ComposeAsync(
                    destinationAccount,
                    destinationEncryption,
                    copiedBlocks.Select(block => block.Content).ToArray(),
                    cancellationToken);
                return new PreparedCopyContent(content, copiedBlocks, leases);
            }

            var copiedContent = await chunks.CopyToDomainPinnedAsync(
                destinationAccount,
                EncryptionOf(source),
                destinationEncryption,
                source.Content,
                cancellationToken);
            leases.Add(copiedContent);
            return new PreparedCopyContent(copiedContent.Manifest, [], leases);
        }
        catch
        {
            foreach (var lease in leases)
                lease.Dispose();
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
        current = PrepareBlobWrite(current);
        if (current.Copy is null || current.Copy.Status != "pending" || current.PendingCopyContent is null)
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
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
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
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
        if (!await metadata.DeleteStagedBlocksAsync(account, container, name, cancellationToken))
            throw AzureStorageException.BlobNotFound();
    }

    public Task<ServiceProperties> GetServicePropertiesAsync(string account, CancellationToken cancellationToken) =>
        metadata.GetServicePropertiesAsync(account, cancellationToken);

    public async Task PutServicePropertiesAsync(
        string account,
        ServiceProperties properties,
        CancellationToken cancellationToken)
    {
        await analytics.EnsureContainerAsync(account, cancellationToken);
        if (properties.StaticWebsite.Enabled)
        {
            var websiteContainer = await metadata.GetContainerAsync(
                account,
                "$web",
                includeDeleted: true,
                cancellationToken);
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
                    cancellationToken);
            }
        }
        await metadata.PutServicePropertiesAsync(account, properties, cancellationToken);
    }

    public async Task<StorageMaintenanceResult> RunMaintenanceAsync(CancellationToken cancellationToken)
    {
        await _maintenanceGate.WaitAsync(cancellationToken);
        try
        {
            return await RunMaintenanceCoreAsync(cancellationToken);
        }
        finally
        {
            _maintenanceGate.Release();
        }
    }

    private async Task<StorageMaintenanceResult> RunMaintenanceCoreAsync(CancellationToken cancellationToken)
    {
        var completedCopies = 0;
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
            cancellationToken);
        foreach (var candidate in blobPage.Items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var blob = candidate;
                if (blob.Container == StorageAnalyticsService.LogsContainerName)
                {
                    if (!serviceProperties.TryGetValue(blob.Account, out var analyticsProperties))
                    {
                        analyticsProperties = await metadata.GetServicePropertiesAsync(blob.Account, cancellationToken);
                        serviceProperties.Add(blob.Account, analyticsProperties);
                    }
                    if (analyticsProperties.Logging.RetentionPolicy is { Enabled: true, Days: { } retentionDays } &&
                        blob.CreatedAt.AddDays(retentionDays) <= now &&
                        await metadata.DeleteBlobRecordAsync(blob.GenerationId, blob.Revision, cancellationToken))
                    {
                        continue;
                    }
                }
                if (blob.Copy?.Status == "pending" && blob.PendingCopyContent is not null)
                {
                    var pendingCopy = blob;
                    blob = await CompleteCopyIfDueAsync(blob, cancellationToken);
                    if (pendingCopy.Copy.Status == "pending" && blob.Copy?.Status == "success")
                        completedCopies++;
                }
                if (blob.RehydrateCompleteAt <= now)
                {
                    var rehydrated = await CompleteRehydrationIfDueAsync(blob, cancellationToken);
                    if (rehydrated.RehydrateCompleteAt is null && blob.RehydrateCompleteAt is not null)
                        completedRehydrations++;
                    blob = rehydrated;
                }

                var smartTiered = await TransitionSmartTierIfDueAsync(blob, now, cancellationToken);
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
                            properties = await metadata.GetServicePropertiesAsync(blob.Account, cancellationToken);
                            serviceProperties.Add(blob.Account, properties);
                        }
                        retentionUntil = blob.DeletedAt.Value.AddDays(properties.BlobSoftDeleteRetentionDays);
                    }
                    if (retentionUntil <= now &&
                        await metadata.DeleteBlobRecordAsync(blob.GenerationId, blob.Revision, cancellationToken))
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
                    await metadata.DeleteBlobRecordAsync(blob.GenerationId, blob.Revision, cancellationToken))
                {
                    expiredBlobs++;
                }
            }
            catch (StorageConcurrencyException)
            {
                // A concurrent request changed the resource; the next pass evaluates its new state.
            }
        }
        _blobMaintenanceCursor = blobPage.HasMore && blobPage.Items.Count > 0
            ? blobPage.Items[^1].GenerationId
            : null;

        var containerPage = await metadata.ListContainerMaintenancePageAsync(
            _containerMaintenanceCursor,
            _options.ContainerRecordsPerMaintenancePass,
            cancellationToken);
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
                    properties = await metadata.GetServicePropertiesAsync(container.Account, cancellationToken);
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
                        cancellationToken))
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
            cancellationToken);
        var reclaimedStagingFiles = chunks.DeleteAbandonedStagingFiles(
            now.Subtract(_options.AbandonedStagingRetention),
            _options.MaximumStagingFilesPerMaintenancePass);
        var reclaimedChunks = await CollectGarbageBatchAsync(cancellationToken);
        _ = await metadata.SealChunkPacksOlderThanAsync(
            now.Subtract(_options.ChunkPackSealAge),
            _options.ChunkPacksPerMaintenancePass,
            cancellationToken);
        var orphanedPacks = await chunks.ReclaimOrphanedPacksAsync(
            now.Subtract(_options.ChunkPackSealAge),
            _options.ChunkPacksPerMaintenancePass,
            cancellationToken);
        var packCompaction = await CompactChunkPacksAsync(cancellationToken);
        var summary = await metadata.GetStorageInventorySummaryAsync(cancellationToken);
        await ScanIntegrityAsync(summary.ReachableChunkCount, cancellationToken);
        var recompression = await RecompressColdChunksAsync(now, cancellationToken);
        var physical = chunks.MeasurePhysicalUsage();
        var usage = new StorageUsageSnapshot(
            summary.LogicalBlobBytes,
            summary.LogicalStagedBlockBytes,
            physical.ChunkBytes,
            physical.StagingBytes,
            physical.MetadataBytes,
            summary.BlobRecordCount,
            summary.StagedBlockCount,
            physical.ChunkCount,
            summary.ReachableChunkCount);
        var result = new StorageMaintenanceResult(
            completedCopies,
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

    public async Task<int> CollectGarbageAsync(CancellationToken cancellationToken)
    {
        await _maintenanceGate.WaitAsync(cancellationToken);
        try
        {
            return await CollectGarbageBatchAsync(cancellationToken);
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
            cancellationToken);
        if (page.Items.Count == 0)
        {
            _garbageCollectionCursor = null;
            return 0;
        }

        var firstReachabilitySnapshot = await metadata.FindReachableChunkIdsAsync(page.Items, cancellationToken);
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
                cancellationToken);
            foreach (var reservation in reservations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!confirmedReachability.Contains(reservation.Id) &&
                    await reservation.TryDeleteAsync(cancellationToken))
                {
                    deleted++;
                }
            }
            _garbageCollectionCursor = page.HasMore ? page.Items[^1] : null;
            return deleted;
        }
        finally
        {
            foreach (var reservation in reservations)
                reservation.Dispose();
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
            cancellationToken);
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
            var status = await chunks.VerifyChunkAsync(id, cancellationToken);
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
                    throw new ArgumentOutOfRangeException(nameof(status));
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
            cancellationToken);
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
            var result = await chunks.TryCompactPackAsync(pack, cancellationToken);
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
            cancellationToken);
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
            var result = await chunks.TryRecompressChunkAsync(id, now, cancellationToken);
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
        if (blob.Copy?.Status != "pending" ||
            blob.PendingCopyContent is null)
        {
            return blob;
        }

        var now = metadata.GetUtcNow();
        if (blob.Copy.ExpiresAt <= now &&
            (!blob.Copy.ReadyAt.HasValue || blob.Copy.ReadyAt > blob.Copy.ExpiresAt))
        {
            var failed = blob with
            {
                Content = blob.Copy.IsIncremental
                    ? blob.Content
                    : chunks.Empty(blob.Account, EncryptionOf(blob)),
                PendingCopyContent = null,
                PendingCopyCommittedBlocks = null,
                PendingCopyAppendBlockCount = null,
                PendingCopyIsSealed = null,
                PendingCopyPageRanges = null,
                CommittedBlocks = blob.Copy.IsIncremental ? blob.CommittedBlocks : [],
                PageRanges = blob.Copy.IsIncremental ? blob.PageRanges : [],
                AppendBlockCount = blob.Copy.IsIncremental ? blob.AppendBlockCount : 0,
                IsSealed = blob.Copy.IsIncremental && blob.IsSealed,
                Copy = blob.Copy with
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
                await metadata.PutBlobRecordAsync(failed, blob.Revision, cancellationToken);
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
                           cancellationToken)
                       ?? throw AzureStorageException.BlobNotFound();
            }
        }

        if (!blob.Copy.ReadyAt.HasValue || blob.Copy.ReadyAt.Value > now)
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
            IncrementalCopySourceSnapshot = blob.Copy.IsIncremental
                ? blob.Copy.SourceSnapshot
                : blob.IncrementalCopySourceSnapshot,
            Copy = blob.Copy with
            {
                Status = "success",
                BytesCopied = blob.Copy.TotalBytes,
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
            if (blob.Copy.IsIncremental)
                return await metadata.CompleteIncrementalCopyAsync(updated, blob.Revision, now, cancellationToken);
            await metadata.PutBlobRecordAsync(updated, blob.Revision, cancellationToken);
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
                       cancellationToken)
                   ?? throw AzureStorageException.BlobNotFound();
        }
    }

    private async Task<BlobRecord> CompleteRehydrationIfDueAsync(
        BlobRecord blob,
        CancellationToken cancellationToken)
    {
        if (blob.ArchiveStatus is null ||
            !blob.RehydrateCompleteAt.HasValue ||
            blob.RehydrateCompleteAt.Value > metadata.GetUtcNow())
        {
            return blob;
        }

        var targetTier = blob.ArchiveStatus switch
        {
            "rehydrate-pending-to-hot" => "Hot",
            "rehydrate-pending-to-cool" => "Cool",
            "rehydrate-pending-to-cold" => "Cold",
            "rehydrate-pending-to-smart" => "Smart",
            _ => throw new InvalidDataException("The blob has an invalid archive rehydration status.")
        };
        var now = metadata.GetUtcNow();
        var updated = blob with
        {
            AccessTier = targetTier,
            SmartAccessTier = targetTier == "Smart" ? "Hot" : null,
            SmartTierLastAccessedAt = targetTier == "Smart" ? now : null,
            AccessTierChangedAt = now,
            ArchiveStatus = null,
            RehydratePriority = null,
            RehydrateCompleteAt = null,
            Revision = MetadataStore.NewRevision()
        };
        try
        {
            await metadata.PutBlobRecordAsync(updated, blob.Revision, cancellationToken);
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
                       cancellationToken)
                   ?? throw AzureStorageException.BlobNotFound();
        }
    }

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
        await metadata.PutBlobRecordAsync(updated, blob.Revision, cancellationToken);
        return updated;
    }

    private static BlobRecord NewBlob(
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
            Metadata = options.Metadata,
            Tags = options.Tags ?? new Dictionary<string, string>(StringComparer.Ordinal),
            Http = options.Http,
            AccessTier = options.AccessTier ?? "Hot",
            AccessTierInferred = kind == BlobKind.BlockBlob &&
                                 (options.AccessTierInferred ?? options.AccessTier is null),
            SmartAccessTier = options.AccessTier == "Smart" ? "Hot" : null,
            ImmutabilityUntil = options.ImmutabilityUntil,
            ImmutabilityLocked = options.ImmutabilityLocked,
            HasLegalHold = options.HasLegalHold,
            EncryptionScope = options.EncryptionScope,
            CustomerProvidedKeySha256 = options.CustomerProvidedKeySha256,
            SmartTierLastAccessedAt = options.AccessTier == "Smart" ? now : null
        };
    }

    private async Task<BlobWriteOptions> ApplyContainerEncryptionPolicyAsync(
        string account,
        string container,
        BlobWriteOptions options,
        CancellationToken cancellationToken,
        BlobRecord? current = null)
    {
        var containerRecord = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
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
            await chunks.WriteRangeAsync(current.Content, encryption, cursor, length, currentBytes, cancellationToken);
            await chunks.WriteRangeAsync(previous.Content, encryption, cursor, length, previousBytes, cancellationToken);

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

    private static IReadOnlyList<PageRange> ClipPageRanges(
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
        container with { Lease = leases.GetEffective(container.Lease) };

    private BlobRecord EffectiveBlob(BlobRecord blob) =>
        blob with
        {
            Lease = leases.GetEffective(blob.Lease),
            VersionId = IsHierarchicalNamespaceEnabled(blob.Account) ? null : blob.VersionId
        };

    private BlobRecord PrepareBlobWrite(BlobRecord blob) =>
        blob with { Lease = leases.ResetAfterBlobWrite(blob.Lease) };

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

        await _hierarchicalDirectoryGate.WaitAsync(cancellationToken);
        try
        {
            if (_indexedHierarchicalContainers.ContainsKey(key))
                return;
            await metadata.EnsureHierarchicalDirectoriesAsync(account, container, cancellationToken);
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

    private void EnsurePublicAccessAllowed(string? publicAccess)
    {
        if (publicAccess is not null && !_options.AllowAnonymousPublicAccess)
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "PublicAccessNotPermitted",
                "Public access is not permitted on this storage account.");
        }
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

    private void EnsureBlobMutable(BlobRecord blob)
    {
        if (blob.HasLegalHold)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "BlobImmutableDueToLegalHold", "This operation is not permitted because the blob has a legal hold.");
        if (blob.ImmutabilityUntil > metadata.GetUtcNow())
            throw BlobImmutableDueToPolicy();
    }

    private static void EnsureNoPendingCopy(BlobRecord blob)
    {
        if (blob.Copy?.Status == "pending")
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
