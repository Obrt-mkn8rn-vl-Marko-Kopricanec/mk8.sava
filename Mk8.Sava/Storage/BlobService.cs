using System.Globalization;
using System.Text;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Storage;

public sealed record BlobWriteOptions(
    BlobHttpProperties Http,
    Dictionary<string, string> Metadata,
    Dictionary<string, string>? Tags = null,
    string? AccessTier = null,
    DateTimeOffset? ImmutabilityUntil = null,
    bool ImmutabilityLocked = false,
    bool HasLegalHold = false);

public sealed record PageRange(long Start, long End);

public sealed class BlobService(MetadataStore metadata, ChunkStore chunks, IOptions<SavaOptions> configuredOptions)
{
    private readonly SavaOptions _options = configuredOptions.Value;

    public bool AllowsAnonymousPublicAccess => _options.AllowAnonymousPublicAccess;

    public Task<IReadOnlyList<ContainerRecord>> ListContainersAsync(
        string account,
        bool includeDeleted,
        CancellationToken cancellationToken) =>
        metadata.ListContainersAsync(account, includeDeleted, cancellationToken);

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
            var deleted = current with
            {
                Revision = MetadataStore.NewRevision(),
                DeletedAt = metadata.GetUtcNow(),
                DeletedVersion = Guid.NewGuid().ToString("N"),
                ETag = MetadataStore.NewETag(),
                LastModified = metadata.GetUtcNow()
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
        if (!properties.ContainerSoftDeleteEnabled ||
            current.DeletedAt.Value.AddDays(properties.ContainerSoftDeleteRetentionDays) < metadata.GetUtcNow())
        {
            throw AzureStorageException.ContainerNotFound();
        }

        var restored = current with
        {
            Revision = MetadataStore.NewRevision(),
            DeletedAt = null,
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
        return await metadata.ListBlobsAsync(account, container, includeVersions, includeSnapshots, includeDeleted, cancellationToken);
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
        return await metadata.GetBlobAsync(account, container, name, versionId, snapshot, includeDeleted, cancellationToken)
               ?? throw AzureStorageException.BlobNotFound();
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
        using var content = await chunks.StorePinnedAsync(account, source, cancellationToken);
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
        var proposed = NewBlob(account, container, name, BlobKind.AppendBlob, chunks.Empty(account), options, now);
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
        var proposed = NewBlob(account, container, name, BlobKind.PageBlob, chunks.Sparse(account, length), options, now) with
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
        CancellationToken cancellationToken)
    {
        ValidateBlobName(name);
        var blockIdLength = ValidateBlockId(blockId);
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
        var current = await metadata.GetBlobAsync(account, container, name, null, null, includeDeleted: false, cancellationToken);
        if (current is not null && current.Kind != BlobKind.BlockBlob)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
        var staged = await metadata.ListStagedBlocksAsync(account, container, name, cancellationToken);
        if (staged.Count >= 100_000 && staged.All(item => !string.Equals(item.BlockId, blockId, StringComparison.Ordinal)))
            throw new AzureStorageException(StatusCodes.Status409Conflict, "BlockCountExceedsLimit", "The uncommitted block count exceeds the maximum permitted value.");
        var existingId = staged.Select(item => item.BlockId).FirstOrDefault()
                         ?? current?.CommittedBlocks.FirstOrDefault()?.Id;
        if (existingId is not null && ValidateBlockId(existingId) != blockIdLength)
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidBlobOrBlock", "All block IDs for a blob must have the same length.");
        using var content = await chunks.StorePinnedAsync(account, source, cancellationToken);
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
            selected.Add(new CommittedBlockRecord(blockId, resolved));
        }

        var content = await chunks.ComposeAsync(account, selected.Select(item => item.Content).ToArray(), cancellationToken);
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
        CancellationToken cancellationToken)
    {
        EnsureBlobMutable(current);
        if (current.Kind != BlobKind.AppendBlob)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
        if (current.IsSealed)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "BlobIsSealed", "The specified append blob is sealed.");
        if (current.AppendBlockCount >= 50_000)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "BlockCountExceedsLimit", "The append block count exceeds the maximum permitted value.");
        if (expectedPosition.HasValue && expectedPosition.Value != current.Content.Length)
            throw new AzureStorageException(StatusCodes.Status412PreconditionFailed, "AppendPositionConditionNotMet", "The append position condition specified was not met.");

        using var appended = await chunks.StorePinnedAsync(current.Account, source, cancellationToken);
        if (appended.Manifest.Length > 100L * 1024 * 1024)
            throw new AzureStorageException(StatusCodes.Status413PayloadTooLarge, "RequestBodyTooLarge", "An append block cannot exceed 100 MiB.");
        if (expectedMaximumSize.HasValue && current.Content.Length + appended.Manifest.Length > expectedMaximumSize.Value)
        {
            throw new AzureStorageException(
                StatusCodes.Status412PreconditionFailed,
                "MaxBlobSizeConditionNotMet",
                "The max blob size condition specified was not met.");
        }
        var content = await chunks.ComposeAsync(current.Account, [current.Content, appended.Manifest], cancellationToken);
        using var contentPin = chunks.Pin(content);
        var updated = current with
        {
            GenerationId = Guid.NewGuid().ToString("N"),
            Revision = MetadataStore.NewRevision(),
            Content = content,
            ETag = MetadataStore.NewETag(),
            LastModified = metadata.GetUtcNow(),
            Lease = current.Lease,
            AppendBlockCount = checked(current.AppendBlockCount + 1)
        };
        return await metadata.PublishBlobAsync(updated, current.GenerationId, current.Revision, cancellationToken);
    }

    public async Task<BlobRecord> PutPageAsync(
        BlobRecord current,
        long start,
        long end,
        Stream? source,
        bool clear,
        CancellationToken cancellationToken)
    {
        EnsureBlobMutable(current);
        if (current.Kind != BlobKind.PageBlob)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
        if (start < 0 || end < start || start % 512 != 0 || (end + 1) % 512 != 0 || end >= current.Content.Length)
            throw AzureStorageException.InvalidHeader("x-ms-range", $"bytes={start}-{end}");

        using var content = await chunks.ReplaceRangePinnedAsync(
            current.Account,
            current.Content,
            start,
            end - start + 1,
            source,
            clear,
            cancellationToken);
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

    public async Task WriteContentAsync(
        BlobRecord blob,
        long offset,
        long length,
        Stream destination,
        CancellationToken cancellationToken) =>
        await chunks.WriteRangeAsync(blob.Content, offset, length, destination, cancellationToken);

    public async Task<BlobRecord> SetBlobMetadataAsync(
        BlobRecord current,
        Dictionary<string, string> userMetadata,
        CancellationToken cancellationToken)
    {
        EnsureBlobMutable(current);
        var updated = current with
        {
            Metadata = userMetadata,
            Revision = MetadataStore.NewRevision(),
            ETag = MetadataStore.NewETag(),
            LastModified = metadata.GetUtcNow()
        };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
        return updated;
    }

    public async Task<BlobRecord> SetBlobTagsAsync(
        BlobRecord current,
        Dictionary<string, string> tags,
        CancellationToken cancellationToken)
    {
        EnsureBlobMutable(current);
        if (tags.Count > 10)
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "TagsTooLarge", "The number of blob tags exceeds the permitted limit.");
        var updated = current with { Tags = tags, Revision = MetadataStore.NewRevision() };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
        return updated;
    }

    public async Task<BlobRecord> SetBlobPropertiesAsync(
        BlobRecord current,
        BlobHttpProperties http,
        long? resizeTo,
        long? sequenceNumber,
        string? sequenceAction,
        CancellationToken cancellationToken)
    {
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
            resized = await chunks.ResizeSparsePinnedAsync(current.Account, current.Content, resizeTo.Value, cancellationToken);
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
        EnsureBlobMutable(current);
        if (current.Kind != BlobKind.AppendBlob)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
        var updated = current with
        {
            IsSealed = true,
            Revision = MetadataStore.NewRevision(),
            ETag = MetadataStore.NewETag(),
            LastModified = metadata.GetUtcNow()
        };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
        return updated;
    }

    public async Task<BlobRecord> SetTierAsync(BlobRecord current, string tier, CancellationToken cancellationToken)
    {
        if (tier is not ("Hot" or "Cool" or "Cold" or "Archive"))
            throw AzureStorageException.InvalidHeader("x-ms-access-tier", tier);
        var updated = current with
        {
            AccessTier = tier,
            Revision = MetadataStore.NewRevision(),
            AccessTierChangedAt = metadata.GetUtcNow()
        };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
        return updated;
    }

    public async Task<BlobRecord> SetExpiryAsync(BlobRecord current, DateTimeOffset? expiresAt, CancellationToken cancellationToken)
    {
        EnsureBlobMutable(current);
        var updated = current with { ExpiresAt = expiresAt, Revision = MetadataStore.NewRevision() };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
        return updated;
    }

    public async Task<BlobRecord> SetBlobImmutabilityPolicyAsync(
        BlobRecord current,
        DateTimeOffset expiresOn,
        bool locked,
        CancellationToken cancellationToken)
    {
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
            Revision = MetadataStore.NewRevision()
        };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
        return updated;
    }

    public async Task<BlobRecord> DeleteBlobImmutabilityPolicyAsync(
        BlobRecord current,
        CancellationToken cancellationToken)
    {
        if (current.ImmutabilityLocked)
            throw BlobImmutableDueToPolicy();
        var updated = current with
        {
            ImmutabilityUntil = null,
            ImmutabilityLocked = false,
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
        var updated = current with
        {
            HasLegalHold = hasLegalHold,
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
        var updated = current with
        {
            Lease = lease,
            Revision = MetadataStore.NewRevision()
        };
        await metadata.PutBlobRecordAsync(updated, current.Revision, cancellationToken);
        return updated;
    }

    public Task<BlobRecord> CreateSnapshotAsync(BlobRecord current, CancellationToken cancellationToken) =>
        metadata.CreateSnapshotAsync(current, metadata.GetUtcNow(), cancellationToken);

    public async Task DeleteBlobAsync(
        BlobRecord current,
        string? deleteSnapshots,
        CancellationToken cancellationToken)
    {
        EnsureBlobMutable(current);
        var records = await metadata.ListBlobsAsync(
            current.Account,
            current.Container,
            includeVersions: true,
            includeSnapshots: true,
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
        foreach (var target in targets)
        {
            if (properties.BlobSoftDeleteEnabled)
            {
                await metadata.PutBlobRecordAsync(target with
                {
                    IsDeleted = true,
                    DeletedAt = metadata.GetUtcNow(),
                    IsCurrent = target.IsCurrent,
                    Revision = MetadataStore.NewRevision()
                }, target.Revision, cancellationToken);
            }
            else
            {
                await metadata.DeleteBlobRecordAsync(target.GenerationId, target.Revision, cancellationToken);
            }
        }
    }

    public async Task UndeleteBlobAsync(
        string account,
        string container,
        string name,
        CancellationToken cancellationToken)
    {
        var records = await metadata.ListBlobsAsync(account, container, true, true, true, cancellationToken);
        var properties = await metadata.GetServicePropertiesAsync(account, cancellationToken);
        var now = metadata.GetUtcNow();
        var restored = false;
        foreach (var record in records.Where(item => item.Name == name && item.IsDeleted))
        {
            if (!properties.BlobSoftDeleteEnabled ||
                record.DeletedAt is null ||
                record.DeletedAt.Value.AddDays(properties.BlobSoftDeleteRetentionDays) < now)
                continue;
            await metadata.PutBlobRecordAsync(record with
            {
                IsDeleted = false,
                DeletedAt = null,
                Revision = MetadataStore.NewRevision()
            }, record.Revision, cancellationToken);
            restored = true;
        }
        if (!restored)
            throw AzureStorageException.BlobNotFound();
    }

    public async Task<BlobRecord> CopyFromAsync(
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
        _ = await GetContainerAsync(account, container, includeDeleted: false, cancellationToken);
        if (!chunks.IsInDomain(account, source.Content))
            throw new InvalidOperationException("Cross-account copies must pass through a verified plaintext transfer.");
        using var sourcePin = chunks.Pin(source.Content);
        var now = metadata.GetUtcNow();
        var proposed = NewBlob(account, container, name, source.Kind, source.Content, options, now) with
        {
            SequenceNumber = source.SequenceNumber,
            IsSealed = source.IsSealed,
            AppendBlockCount = source.AppendBlockCount,
            CommittedBlocks = source.CommittedBlocks,
            PageRanges = [.. source.PageRanges],
            Copy = new CopyState
            {
                Id = Guid.NewGuid().ToString(),
                Source = sourceUri,
                Status = "success",
                BytesCopied = source.Content.Length,
                TotalBytes = source.Content.Length,
                CompletedAt = now
            }
        };
        return await metadata.PublishBlobAsync(proposed, expectedGeneration, expectedRevision, cancellationToken);
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

    public async Task<int> CollectGarbageAsync(CancellationToken cancellationToken)
    {
        var reachable = await metadata.GetReachableChunkIdsAsync(cancellationToken);
        var deleted = 0;
        foreach (var chunk in chunks.EnumerateChunkIds())
        {
            if (!reachable.Contains(chunk) && chunks.DeleteChunk(chunk))
                deleted++;
        }
        return deleted;
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
            ImmutabilityUntil = options.ImmutabilityUntil,
            ImmutabilityLocked = options.ImmutabilityLocked,
            HasLegalHold = options.HasLegalHold
        };
    }

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

    private static AzureStorageException BlobImmutableDueToPolicy() => new(
        StatusCodes.Status409Conflict,
        "BlobImmutableDueToPolicy",
        "This operation is not permitted because the blob is immutable.");
}
