using System.Globalization;
using System.Text;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Storage;

public sealed class BlobService(
    MetadataStore metadata,
    ChunkStore chunks,
    IStorageTelemetry telemetry,
    IOptions<SavaOptions> configuredOptions)
{
    private readonly SavaOptions _options = configuredOptions.Value;
    private readonly SemaphoreSlim _maintenanceGate = new(1, 1);
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

    public async Task<IReadOnlyList<ContainerRecord>> ListContainersAsync(
        string account,
        bool includeDeleted,
        CancellationToken cancellationToken)
    {
        var containers = await metadata.ListContainersAsync(account, includeDeleted, cancellationToken);
        if (!includeDeleted || !containers.Any(item => item.DeletedAt.HasValue && !item.DeleteRetentionUntil.HasValue))
            return containers;
        var properties = await metadata.GetServicePropertiesAsync(account, cancellationToken);
        return containers.Select(container => container.DeletedAt.HasValue && !container.DeleteRetentionUntil.HasValue
            ? container with
            {
                DeleteRetentionUntil = container.DeletedAt.Value.AddDays(properties.ContainerSoftDeleteRetentionDays)
            }
            : container).ToArray();
    }

    internal async Task<ContainerListPage> ListContainersPageAsync(
        string account,
        bool includeDeleted,
        string prefix,
        string marker,
        int maximum,
        CancellationToken cancellationToken)
    {
        var page = await metadata.ListContainersPageAsync(
            account,
            includeDeleted,
            prefix,
            marker,
            maximum,
            cancellationToken);
        if (!includeDeleted || !page.Items.Any(item =>
                item.DeletedAt.HasValue && !item.DeleteRetentionUntil.HasValue))
        {
            return page;
        }

        var properties = await metadata.GetServicePropertiesAsync(account, cancellationToken);
        return page with
        {
            Items = page.Items.Select(container =>
                    container.DeletedAt.HasValue && !container.DeleteRetentionUntil.HasValue
                        ? container with
                        {
                            DeleteRetentionUntil = container.DeletedAt.Value.AddDays(
                                properties.ContainerSoftDeleteRetentionDays)
                        }
                        : container)
                .ToArray()
        };
    }

    public async Task<ContainerRecord> CreateContainerAsync(
        string account,
        string name,
        Dictionary<string, string> userMetadata,
        string? publicAccess,
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
            PublicAccess = publicAccess
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
        CancellationToken cancellationToken) =>
        await metadata.GetContainerAsync(account, name, includeDeleted, cancellationToken)
        ?? throw AzureStorageException.ContainerNotFound();

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
        string name,
        string deletedVersion,
        CancellationToken cancellationToken)
    {
        var current = await GetContainerAsync(account, name, includeDeleted: true, cancellationToken);
        if (current.DeletedAt is null || !string.Equals(current.DeletedVersion, deletedVersion, StringComparison.Ordinal))
            throw AzureStorageException.ContainerNotFound();
        var properties = await metadata.GetServicePropertiesAsync(account, cancellationToken);
        var retentionUntil = current.DeleteRetentionUntil ??
                             current.DeletedAt.Value.AddDays(properties.ContainerSoftDeleteRetentionDays);
        if (retentionUntil < metadata.GetUtcNow())
        {
            throw AzureStorageException.ContainerNotFound();
        }

        var restored = current with
        {
            Revision = MetadataStore.NewRevision(),
            DeletedAt = null,
            DeleteRetentionUntil = null,
            DeletedVersion = null,
            ETag = MetadataStore.NewETag(),
            LastModified = metadata.GetUtcNow()
        };
        await metadata.PutContainerAsync(restored, current.Revision, cancellationToken);
        return restored;
    }

    public async Task<ContainerRecord> SetContainerLeaseAsync(
        ContainerRecord current,
        LeaseRecord lease,
        CancellationToken cancellationToken)
    {
        var updated = current with
        {
            Lease = lease,
            Revision = MetadataStore.NewRevision()
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
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
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
                ? rehydrated with
                {
                    DeleteRetentionUntil = rehydrated.DeletedAt.Value.AddDays(properties.BlobSoftDeleteRetentionDays)
                }
                : rehydrated);
        }
        return effective;
    }

    internal async Task<BlobListPage> ListBlobsPageAsync(
        string account,
        string container,
        bool includeVersions,
        bool includeSnapshots,
        bool includeDeleted,
        string prefix,
        string startFrom,
        string delimiter,
        BlobListingMarker marker,
        int maximum,
        CancellationToken cancellationToken)
    {
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
        var page = await metadata.ListBlobsPageAsync(
            account,
            container,
            includeVersions,
            includeSnapshots,
            includeDeleted,
            prefix,
            startFrom,
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
                ? rehydrated with
                {
                    DeleteRetentionUntil = rehydrated.DeletedAt.Value.AddDays(
                        properties.BlobSoftDeleteRetentionDays)
                }
                : rehydrated;
            effective.Add(new BlobListEntry(blob, null));
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
            effective.Add(await CompleteRehydrationIfDueAsync(copy, cancellationToken));
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
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
        var blob = await metadata.GetBlobAsync(account, container, name, versionId, snapshot, includeDeleted, cancellationToken)
                   ?? throw AzureStorageException.BlobNotFound();
        blob = await CompleteCopyIfDueAsync(blob, cancellationToken);
        return await CompleteRehydrationIfDueAsync(blob, cancellationToken);
    }

    public async Task<BlobRecord> PutBlockBlobAsync(
        string account,
        string container,
        string name,
        Stream source,
        BlobWriteOptions options,
        string? expectedGeneration,
        string? expectedRevision,
        CancellationToken cancellationToken)
    {
        ValidateBlobName(name);
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
        var encryption = EncryptionOf(options);
        using var content = await chunks.StorePinnedAsync(account, encryption, source, cancellationToken);
        var now = metadata.GetUtcNow();
        var proposed = NewBlob(account, container, name, BlobKind.BlockBlob, content.Manifest, options, now);
        return await metadata.PublishBlobAsync(proposed, expectedGeneration, expectedRevision, cancellationToken);
    }

    public async Task<BlobRecord> CreateAppendBlobAsync(
        string account,
        string container,
        string name,
        BlobWriteOptions options,
        string? expectedGeneration,
        string? expectedRevision,
        CancellationToken cancellationToken)
    {
        ValidateBlobName(name);
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
        var now = metadata.GetUtcNow();
        var proposed = NewBlob(account, container, name, BlobKind.AppendBlob, chunks.Empty(account, EncryptionOf(options)), options, now);
        return await metadata.PublishBlobAsync(proposed, expectedGeneration, expectedRevision, cancellationToken);
    }

    public async Task<BlobRecord> CreatePageBlobAsync(
        string account,
        string container,
        string name,
        long length,
        BlobWriteOptions options,
        long sequenceNumber,
        string? expectedGeneration,
        string? expectedRevision,
        CancellationToken cancellationToken)
    {
        ValidateBlobName(name);
        const long maximumPageBlobBytes = 8L * 1024 * 1024 * 1024 * 1024;
        if (length < 0 || length > maximumPageBlobBytes || length % 512 != 0)
            throw AzureStorageException.InvalidHeader("x-ms-blob-content-length", length.ToString(CultureInfo.InvariantCulture));
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
        var now = metadata.GetUtcNow();
        var proposed = NewBlob(account, container, name, BlobKind.PageBlob, chunks.Sparse(account, EncryptionOf(options), length), options, now) with
        {
            SequenceNumber = sequenceNumber
        };
        return await metadata.PublishBlobAsync(proposed, expectedGeneration, expectedRevision, cancellationToken);
    }

    public async Task StageBlockAsync(
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
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
        var current = await metadata.GetBlobAsync(account, container, name, null, null, includeDeleted: false, cancellationToken);
        if (current is not null && current.Kind != BlobKind.BlockBlob)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
        if (current is not null && !chunks.IsInDomain(account, encryption, current.Content))
            throw CustomerProvidedKeyMismatch();
        var staged = await metadata.ListStagedBlocksAsync(account, container, name, cancellationToken);
        if (staged.Count >= 100_000 && staged.All(item => !string.Equals(item.BlockId, blockId, StringComparison.Ordinal)))
            throw new AzureStorageException(StatusCodes.Status409Conflict, "BlockCountExceedsLimit", "The uncommitted block count exceeds the maximum permitted value.");
        var existingId = staged.Select(item => item.BlockId).FirstOrDefault()
                         ?? current?.CommittedBlocks.FirstOrDefault()?.Id;
        if (existingId is not null && ValidateBlockId(existingId) != blockIdLength)
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidBlobOrBlock", "All block IDs for a blob must have the same length.");
        if (staged.Any(item => !chunks.IsInDomain(account, encryption, item.Content)))
            throw CustomerProvidedKeyMismatch();
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
        if (blockList.Count > 50_000)
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "BlockCountExceedsLimit", "The block list may not contain more than 50,000 blocks.");

        var staged = await metadata.ListStagedBlocksAsync(account, container, name, cancellationToken);
        var encryption = EncryptionOf(options);
        var stagedById = staged.ToDictionary(item => item.BlockId, StringComparer.Ordinal);
        var current = await metadata.GetBlobAsync(account, container, name, null, null, includeDeleted: false, cancellationToken);
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
                throw CustomerProvidedKeyMismatch();
            selected.Add(new CommittedBlockRecord(blockId, resolved));
        }

        var content = await chunks.ComposeAsync(account, encryption, selected.Select(item => item.Content).ToArray(), cancellationToken);
        using var contentPin = chunks.Pin(content);
        var now = metadata.GetUtcNow();
        var proposed = NewBlob(account, container, name, BlobKind.BlockBlob, content, options, now) with
        {
            CommittedBlocks = selected
        };
        return await metadata.PublishBlockListAsync(
            proposed,
            expectedGeneration,
            expectedRevision,
            staged,
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
        EnsureNoPendingCopy(current);
        EnsureBlobMutable(current);
        if (current.Kind != BlobKind.AppendBlob)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
        if (current.IsSealed)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "BlobIsSealed", "The specified append blob is sealed.");
        if (current.AppendBlockCount >= 50_000)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "BlockCountExceedsLimit", "The append block count exceeds the maximum permitted value.");
        if (expectedPosition.HasValue && expectedPosition.Value != current.Content.Length)
            throw new AzureStorageException(StatusCodes.Status412PreconditionFailed, "AppendPositionConditionNotMet", "The append position condition specified was not met.");

        if (!chunks.IsInDomain(current.Account, encryption, current.Content))
            throw CustomerProvidedKeyMismatch();
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
        return await metadata.PublishBlobAsync(updated, current.GenerationId, current.Revision, cancellationToken);
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
        EnsureNoPendingCopy(current);
        EnsureBlobMutable(current);
        if (current.Kind != BlobKind.PageBlob)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
        if (start < 0 || end < start || start % 512 != 0 || (end + 1) % 512 != 0 || end >= current.Content.Length)
            throw AzureStorageException.InvalidHeader("x-ms-range", $"bytes={start}-{end}");
        if (!clear && end - start + 1 > 4L * 1024 * 1024)
            throw AzureStorageException.InvalidHeader("x-ms-range", $"bytes={start}-{end}");
        if (!chunks.IsInDomain(current.Account, encryption, current.Content))
            throw CustomerProvidedKeyMismatch();

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
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "InvalidPageRange",
                "The request body length must match the page range length.");
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
            return await metadata.PublishBlobAsync(updated, current.GenerationId, current.Revision, cancellationToken);
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
            throw CustomerProvidedKeyMismatch();

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
        var updated = current with
        {
            Metadata = userMetadata,
            Revision = MetadataStore.NewRevision(),
            ETag = MetadataStore.NewETag(),
            LastModified = metadata.GetUtcNow(),
            Copy = null
        };
        var properties = await metadata.GetServicePropertiesAsync(current.Account, cancellationToken);
        if (properties.VersioningEnabled)
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

    public async Task<BlobRecord> SealAppendBlobAsync(BlobRecord current, CancellationToken cancellationToken)
    {
        EnsureNoPendingCopy(current);
        EnsureBlobMutable(current);
        if (current.Kind != BlobKind.AppendBlob)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
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
        if (current.Kind != BlobKind.BlockBlob)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
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
                SmartAccessTier = tier == "Smart" ? "Hot" : null,
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
        return metadata.CreateSnapshotAsync(current, snapshotMetadata, metadata.GetUtcNow(), cancellationToken);
    }

    public async Task DeleteBlobAsync(
        BlobRecord current,
        string? deleteSnapshots,
        CancellationToken cancellationToken)
    {
        EnsureNoPendingCopy(current);
        EnsureBlobMutable(current);
        var records = await metadata.ListBlobFamilyAsync(
            current.Account,
            current.Container,
            current.Name,
            includeDeleted: true,
            cancellationToken);
        var relatedSnapshots = records.Where(item => item.Name == current.Name && item.Snapshot is not null && !item.IsDeleted).ToArray();
        if (current.Snapshot is null && relatedSnapshots.Length > 0 && deleteSnapshots is null)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "SnapshotsPresent", "This operation is not permitted while the blob has snapshots.");

        var targets = new List<BlobRecord>();
        if (current.Snapshot is not null || current.VersionId is not null && !current.IsCurrent)
        {
            targets.Add(current);
        }
        else
        {
            if (!string.Equals(deleteSnapshots, "only", StringComparison.OrdinalIgnoreCase))
                targets.Add(current);
            if (deleteSnapshots is "include" or "only")
                targets.AddRange(relatedSnapshots);
        }

        foreach (var target in targets)
            EnsureBlobMutable(target);

        var properties = await metadata.GetServicePropertiesAsync(current.Account, cancellationToken);
        var mutations = new List<BlobRecordMutation>(targets.Count);
        foreach (var target in targets)
        {
            BlobRecord? replacement;
            if (target.IsCurrent &&
                target.Snapshot is null &&
                properties.VersioningEnabled)
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
                replacement = target with
                {
                    IsDeleted = true,
                    DeletedAt = deletedAt,
                    DeleteRetentionUntil = deletedAt.AddDays(properties.BlobSoftDeleteRetentionDays),
                    IsCurrent = target.IsCurrent,
                    Revision = MetadataStore.NewRevision()
                };
            }
            else
            {
                replacement = null;
            }
            mutations.Add(new BlobRecordMutation(target.GenerationId, target.Revision, replacement));
        }
        await metadata.ApplyBlobRecordMutationsAsync(mutations, cancellationToken);
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

    public async Task<BlobRecord> BeginCopyFromBlobAsync(
        string account,
        string container,
        string name,
        BlobRecord source,
        BlobWriteOptions options,
        string sourceUri,
        string? expectedGeneration,
        string? expectedRevision,
        CancellationToken cancellationToken)
    {
        var encryption = EncryptionOf(options);
        if (!chunks.IsInDomain(account, encryption, source.Content))
            throw UnsupportedEncryptionTransition();
        using var sourcePin = chunks.Pin(source.Content);
        return await BeginCopyAsync(
            account,
            container,
            name,
            source.Kind,
            source.Content,
            source.SequenceNumber,
            source.IsSealed,
            source.AppendBlockCount,
            source.CommittedBlocks,
            source.PageRanges,
            options,
            sourceUri,
            expectedGeneration,
            expectedRevision,
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
        ValidateBlobName(name);
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
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
        if (!chunks.IsInDomain(account, encryption, source.Content))
            throw UnsupportedEncryptionTransition();

        using var sourcePin = chunks.Pin(source.Content);
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
            PendingCopyContent = source.Content,
            PendingCopyPageRanges = [.. source.PageRanges],
            Copy = new CopyState
            {
                Id = copyId,
                Source = sourceUri,
                Status = "pending",
                BytesCopied = 0,
                TotalBytes = source.Content.Length,
                ReadyAt = now.Add(_options.AsyncCopyCompletionDelay),
                IsIncremental = true,
                SourceSnapshot = source.Snapshot
            }
        };

        if (current is null)
            return await metadata.PublishBlobAsync(pending, null, null, cancellationToken);
        await metadata.PutBlobRecordAsync(pending, current.Revision, cancellationToken);
        return pending;
    }

    public async Task<BlobRecord> BeginCopyFromStreamAsync(
        string account,
        string container,
        string name,
        Stream source,
        BlobWriteOptions options,
        string sourceUri,
        string? expectedGeneration,
        string? expectedRevision,
        CancellationToken cancellationToken)
    {
        using var content = await chunks.StorePinnedAsync(account, EncryptionOf(options), source, cancellationToken);
        return await BeginCopyAsync(
            account,
            container,
            name,
            BlobKind.BlockBlob,
            content.Manifest,
            sequenceNumber: 0,
            isSealed: false,
            appendBlockCount: 0,
            committedBlocks: [],
            pageRanges: [],
            options,
            sourceUri,
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
        bool isSealed,
        int appendBlockCount,
        IReadOnlyList<CommittedBlockRecord> committedBlocks,
        IReadOnlyList<PageRange> pageRanges,
        BlobWriteOptions options,
        string sourceUri,
        string? expectedGeneration,
        string? expectedRevision,
        CancellationToken cancellationToken)
    {
        ValidateBlobName(name);
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
        var encryption = EncryptionOf(options);
        if (!chunks.IsInDomain(account, encryption, sourceContent))
            throw UnsupportedEncryptionTransition();
        using var sourcePin = chunks.Pin(sourceContent);
        var now = metadata.GetUtcNow();
        var copyId = Guid.NewGuid().ToString();
        var proposed = NewBlob(account, container, name, sourceKind, chunks.Empty(account, encryption), options, now) with
        {
            SequenceNumber = sequenceNumber,
            IsSealed = isSealed,
            AppendBlockCount = appendBlockCount,
            CommittedBlocks = [.. committedBlocks],
            PageRanges = [.. pageRanges],
            PendingCopyContent = sourceContent,
            Copy = new CopyState
            {
                Id = copyId,
                Source = sourceUri,
                Status = "pending",
                BytesCopied = 0,
                TotalBytes = sourceContent.Length,
                ReadyAt = now.Add(_options.AsyncCopyCompletionDelay)
            }
        };
        return await metadata.PublishBlobAsync(proposed, expectedGeneration, expectedRevision, cancellationToken);
    }

    public async Task<BlobRecord> AbortCopyAsync(
        BlobRecord current,
        string copyId,
        CancellationToken cancellationToken)
    {
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
                ReadyAt = null
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

    public Task<ServiceProperties> GetServicePropertiesAsync(string account, CancellationToken cancellationToken) =>
        metadata.GetServicePropertiesAsync(account, cancellationToken);

    public Task PutServicePropertiesAsync(string account, ServiceProperties properties, CancellationToken cancellationToken) =>
        metadata.PutServicePropertiesAsync(account, properties, cancellationToken);

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
                if (blob.Copy?.Status == "pending" && blob.PendingCopyContent is not null)
                {
                    var pendingCopy = blob;
                    blob = await CompleteCopyIfDueAsync(blob, cancellationToken);
                    if (pendingCopy.Copy.Status == "pending" && blob.Copy?.Status == "success")
                        completedCopies++;
                }
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

                if (blob.RehydrateCompleteAt <= now)
                {
                    var rehydrated = await CompleteRehydrationIfDueAsync(blob, cancellationToken);
                    if (rehydrated.RehydrateCompleteAt is null && blob.RehydrateCompleteAt is not null)
                        completedRehydrations++;
                    blob = rehydrated;
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
            blob.PendingCopyContent is null ||
            !blob.Copy.ReadyAt.HasValue ||
            blob.Copy.ReadyAt.Value > metadata.GetUtcNow())
        {
            return blob;
        }

        using var contentPin = chunks.Pin(blob.PendingCopyContent);
        var now = metadata.GetUtcNow();
        var updated = blob with
        {
            Content = blob.PendingCopyContent,
            PendingCopyContent = null,
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
                ReadyAt = null
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
        var updated = blob with
        {
            AccessTier = targetTier,
            SmartAccessTier = targetTier == "Smart" ? "Hot" : null,
            AccessTierChangedAt = metadata.GetUtcNow(),
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
            SmartAccessTier = options.AccessTier == "Smart" ? "Hot" : null,
            ImmutabilityUntil = options.ImmutabilityUntil,
            ImmutabilityLocked = options.ImmutabilityLocked,
            HasLegalHold = options.HasLegalHold,
            EncryptionScope = options.EncryptionScope,
            CustomerProvidedKeySha256 = options.CustomerProvidedKeySha256
        };
    }

    private static BlobEncryption EncryptionOf(BlobWriteOptions options) =>
        new(options.EncryptionScope, options.CustomerProvidedKeySha256, options.CustomerProvidedKey);

    private static BlobEncryption EncryptionOf(BlobRecord blob) =>
        new(blob.EncryptionScope, blob.CustomerProvidedKeySha256);

    private static AzureStorageException CustomerProvidedKeyMismatch() => new(
        StatusCodes.Status409Conflict,
        "CustomerProvidedKeyInUse",
        "The blob is encrypted with a customer-provided key that does not match this request.");

    private static AzureStorageException UnsupportedEncryptionTransition() => new(
        StatusCodes.Status409Conflict,
        "BlobOperationNotSupported",
        "The copy source and destination use different request-level encryption settings.");

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

    private static void ValidateContainerName(string name)
    {
        if (name == "$root")
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
        if (string.IsNullOrEmpty(name) || Encoding.UTF8.GetByteCount(name) > 1024 || name.Any(char.IsControl))
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidResourceName", "The specified resource name contains invalid characters.");
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
            if (length is 0 or > 64)
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
