using Mk8.Sava.Storage;

namespace Mk8.Sava.Application;

public interface IBlobApplication : IBlobCapabilities
{
    Task<IReadOnlyList<ContainerRecord>> ListContainersAsync(
        string account, bool includeDeleted, CancellationToken cancellationToken);
    Task<ContainerListPage> ListContainersPageAsync(
        string account, bool includeDeleted, bool includeSystem, string prefix, string marker,
        int maximum, CancellationToken cancellationToken);
    Task<ContainerRecord> CreateContainerAsync(
        string account, string name, IReadOnlyDictionary<string, string> userMetadata,
        string? publicAccess, string? defaultEncryptionScope, bool preventEncryptionScopeOverride,
        CancellationToken cancellationToken, string? creatorObjectId = null);
    Task<ContainerRecord> GetContainerAsync(
        string account, string name, bool includeDeleted, CancellationToken cancellationToken);
    Task<ContainerRecord> SetContainerMetadataAsync(
        ContainerRecord current, IReadOnlyDictionary<string, string> userMetadata,
        CancellationToken cancellationToken);
    Task<ContainerRecord> SetContainerAclAsync(
        ContainerRecord current, string? publicAccess, IReadOnlyDictionary<string, StoredAccessPolicy> policies,
        CancellationToken cancellationToken);
    Task DeleteContainerAsync(ContainerRecord current, CancellationToken cancellationToken);
    Task<ContainerRecord> RestoreContainerAsync(
        string account, string sourceName, string destinationName, string deletedVersion,
        CancellationToken cancellationToken);
    Task<ContainerRecord> RenameContainerAsync(
        string account, string sourceName, string destinationName, string? sourceLeaseId,
        CancellationToken cancellationToken);
    Task<ContainerLeaseUpdate> ApplyContainerLeaseAsync(
        ContainerRecord current, LeaseAction action, int? durationSeconds, int? breakPeriodSeconds,
        string? suppliedId, string? proposedId, bool useLegacySemantics, bool updateProperties,
        CancellationToken cancellationToken);
    Task<IReadOnlyList<BlobRecord>> ListBlobsAsync(
        string account, string container, bool includeVersions, bool includeSnapshots, bool includeDeleted,
        CancellationToken cancellationToken);
    Task<BlobListPage> ListBlobsPageAsync(
        string account, string container, BlobListShowOnly showOnly, bool includeVersions,
        bool includeSnapshots, bool includeDeleted, bool includeUncommitted, string prefix, string startFrom,
        string endBefore, string delimiter, BlobListingMarker marker, int maximum,
        CancellationToken cancellationToken);
    Task<TaggedBlobPage> FindBlobsByTagsPageAsync(
        string account, BlobTagFilter filter, BlobTagCursor? cursor, int maximum,
        CancellationToken cancellationToken);
    Task<BlobRecord> GetBlobAsync(
        string account, string container, string name, string? versionId, string? snapshot,
        bool includeDeleted, CancellationToken cancellationToken);
    Task<BlobRecord> RecordDataAccessAsync(BlobRecord current, CancellationToken cancellationToken);
    Task<BlobRecord> PutBlockBlobAsync(
        string account, string container, string name, Stream source, BlobWriteOptions options,
        LeaseRecord destinationLease, string? expectedGeneration, string? expectedRevision,
        CancellationToken cancellationToken);
    Task<BlobRecord> CreateAppendBlobAsync(
        string account, string container, string name, BlobWriteOptions options,
        LeaseRecord destinationLease, string? expectedGeneration, string? expectedRevision,
        CancellationToken cancellationToken);
    Task<BlobRecord> CreatePageBlobAsync(
        string account, string container, string name, long length, BlobWriteOptions options,
        long sequenceNumber, LeaseRecord destinationLease, string? expectedGeneration, string? expectedRevision,
        CancellationToken cancellationToken);
    Task<BlobEncryption> StageBlockAsync(
        string account, string container, string name, string blockId, Stream source,
        BlobEncryption encryption, CancellationToken cancellationToken);
    Task<BlobRecord> CommitBlockListAsync(
        string account, string container, string name, IReadOnlyList<BlockListEntry> blockList,
        BlobWriteOptions options, string? expectedGeneration, string? expectedRevision,
        CancellationToken cancellationToken);
    Task<BlobRecord> AppendBlockAsync(
        BlobRecord current, Stream source, long? expectedPosition, long? expectedMaximumSize,
        BlobEncryption encryption, CancellationToken cancellationToken);
    Task<BlobRecord> PutPageAsync(
        BlobRecord current, long start, long rangeEnd, Stream? source, bool clear,
        BlobEncryption encryption, CancellationToken cancellationToken);
    Task<PageRangeDiff> GetPageRangeDiffAsync(
        BlobRecord current, BlobRecord previous, BlobEncryption encryption, long start, long rangeEnd,
        CancellationToken cancellationToken);
    Task WriteContentAsync(
        BlobRecord blob, BlobEncryption encryption, long offset, long length, Stream destination,
        CancellationToken cancellationToken);
    Task<BlobRecord> SetBlobMetadataAsync(
        BlobRecord current, IReadOnlyDictionary<string, string> userMetadata, CancellationToken cancellationToken);
    Task<BlobRecord> SetBlobTagsAsync(
        BlobRecord current, IReadOnlyDictionary<string, string> tags, CancellationToken cancellationToken);
    Task<BlobRecord> SetBlobPropertiesAsync(
        BlobRecord current, BlobHttpProperties http, long? resizeTo, long? sequenceNumber,
        string? sequenceAction, BlobEncryption encryption, CancellationToken cancellationToken);
    Task<BlobRecord> SealAppendBlobAsync(
        BlobRecord current, long? expectedPosition, CancellationToken cancellationToken);
    Task<BlobTierUpdate> SetTierAsync(
        BlobRecord current, string tier, string? rehydratePriority, bool allowRehydratePriorityUpdate,
        bool supportsCustomerProvidedKey, CancellationToken cancellationToken);
    Task<BlobRecord> SetExpiryAsync(BlobRecord current, DateTimeOffset? expiresAt, CancellationToken cancellationToken);
    Task<BlobRecord> SetBlobImmutabilityPolicyAsync(
        BlobRecord current, DateTimeOffset expiresOn, bool locked, CancellationToken cancellationToken);
    Task<BlobRecord> DeleteBlobImmutabilityPolicyAsync(BlobRecord current, CancellationToken cancellationToken);
    Task<BlobRecord> SetBlobLegalHoldAsync(BlobRecord current, bool hasLegalHold, CancellationToken cancellationToken);
    Task<BlobLeaseUpdate> ApplyBlobLeaseAsync(
        BlobRecord current, LeaseAction action, int? durationSeconds, int? breakPeriodSeconds,
        string? suppliedId, string? proposedId, bool useLegacySemantics, CancellationToken cancellationToken);
    Task<BlobRecord> CreateSnapshotAsync(
        BlobRecord current, IReadOnlyDictionary<string, string>? snapshotMetadata, CancellationToken cancellationToken);
    Task DeleteBlobAsync(
        BlobRecord current, bool hasExplicitSnapshotOrVersion, BlobDeleteSnapshotsOption deleteSnapshots,
        CancellationToken cancellationToken);
    Task PermanentlyDeleteBlobAsync(
        BlobRecord current, bool hasExplicitSnapshotOrVersion, CancellationToken cancellationToken);
    Task UndeleteBlobAsync(string account, string container, string name, CancellationToken cancellationToken);
    Task UndeleteHierarchicalBlobAsync(
        string account, string container, string sourceName, string destinationName, ulong deletionId,
        CancellationToken cancellationToken);
#pragma warning disable CA1054 // Preserve the exact Azure copy-source text, including legacy relative paths and encoded SAS fields.
    Task<BlobRecord> BeginCopyFromBlobAsync(
        string account, string container, string name, BlobRecord source, bool destinationIsSealed,
        BlobWriteOptions options, string sourceUri, LeaseRecord destinationLease,
        string? expectedGeneration, string? expectedRevision, CancellationToken cancellationToken);
#pragma warning restore CA1054
#pragma warning disable CA1054 // Preserve the exact Azure copy-source text rather than re-escaping it through Uri.
    Task<BlobRecord> CopyBlockBlobFromBlobAsync(
        string account, string container, string name, BlobRecord source, BlobWriteOptions options,
        string sourceUri, LeaseRecord destinationLease, string? expectedGeneration, string? expectedRevision,
        CancellationToken cancellationToken);
#pragma warning restore CA1054
    Task<BlobRecord> CopyBlobFromBlobSynchronouslyAsync(
        string account, string container, string name, BlobRecord source, BlobWriteOptions options,
        LeaseRecord destinationLease, string? expectedGeneration, string? expectedRevision,
        CancellationToken cancellationToken);
#pragma warning disable CA1054 // Copy metadata stores the exact, sanitized source text from the Azure protocol layer.
    Task<BlobRecord> CopyBlockBlobFromStreamAsync(
        string account, string container, string name, Stream source, long contentLength,
        IReadOnlyList<CopySourceBlock> sourceBlocks, BlobWriteOptions options, string sourceUri,
        LeaseRecord destinationLease, string? expectedGeneration, string? expectedRevision,
        CancellationToken cancellationToken);
#pragma warning restore CA1054
#pragma warning disable CA1054 // Copy metadata stores the exact, sanitized source text from the Azure protocol layer.
    Task<BlobRecord> BeginIncrementalCopyAsync(
        string account, string container, string name, BlobRecord source, BlobWriteOptions options,
        string sourceUri, BlobRecord? current, CancellationToken cancellationToken);
#pragma warning restore CA1054
#pragma warning disable CA1054 // Copy metadata stores the exact, sanitized source text from the Azure protocol layer.
    Task<BlobRecord> BeginIncrementalCopyFromPageRangesAsync(
        string account, string container, string name, long sourceLength, string sourceSnapshot,
        string sourceIdentity, DateTimeOffset? sourceCreatedAt, long sourceSequenceNumber,
        IReadOnlyList<PageRange> sourcePageRanges, BlobWriteOptions options, string sourceUri,
        BlobRecord? current, IPageCopySource pageSource, CancellationToken cancellationToken);
#pragma warning restore CA1054
#pragma warning disable CA1054 // Copy metadata stores the exact, sanitized source text from the Azure protocol layer.
    Task<BlobRecord> BeginCopyFromStreamAsync(
        string account, string container, string name, Stream source, long contentLength,
        BlobKind sourceKind, bool sourceIsArchived, long sequenceNumber, bool destinationIsSealed,
        int appendBlockCount, IReadOnlyList<CopySourceBlock> sourceBlocks, IReadOnlyList<PageRange> pageRanges,
        BlobWriteOptions options, string sourceUri, LeaseRecord destinationLease,
        string? expectedGeneration, string? expectedRevision, CancellationToken cancellationToken);
#pragma warning restore CA1054
    Task<BlobRecord> AbortCopyAsync(BlobRecord current, string copyId, CancellationToken cancellationToken);
    Task<IReadOnlyList<StagedBlockRecord>> ListStagedBlocksAsync(
        string account, string container, string name, CancellationToken cancellationToken);
    Task DeleteUncommittedBlobAsync(
        string account, string container, string name, CancellationToken cancellationToken);
    Task<ServiceProperties> GetServicePropertiesAsync(string account, CancellationToken cancellationToken);
    Task PutServicePropertiesAsync(string account, ServiceProperties properties, CancellationToken cancellationToken);
}
