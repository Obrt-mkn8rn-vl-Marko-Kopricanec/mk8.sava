using Microsoft.Data.Sqlite;

namespace Mk8.Sava.Storage;

public sealed partial class MetadataStore
{
    private static async Task<(BlobRecord Source, BlobRecord? Mapped, BlobRecord? Current)> LoadObjectReplicationContextAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord expectedSource,
        BlobRecord proposedDestination,
        ObjectReplicationState proposedState,
        CancellationToken cancellationToken)
    {
        var source = await GetBlobByGenerationAsync(
            connection, transaction, expectedSource.GenerationId, cancellationToken).ConfigureAwait(false);
        if (source is null || !string.Equals(source.Revision, expectedSource.Revision, StringComparison.Ordinal))
            throw new StorageConcurrencyException();
        var key = new ObjectReplicationStateKey(
            proposedState.PolicyId, proposedState.RuleId, source.GenerationId);
        var state = await GetObjectReplicationStateAsync(connection, transaction, key, cancellationToken).ConfigureAwait(false);
        var mapped = state?.DestinationGenerationId is { } mappedGeneration
            ? await GetBlobByGenerationAsync(connection, transaction, mappedGeneration, cancellationToken).ConfigureAwait(false)
            : null;
        var current = await GetCurrentBlobAsync(
            connection, transaction, proposedDestination.Account, proposedDestination.Container,
            proposedDestination.Name, cancellationToken).ConfigureAwait(false);
        return (source, mapped, current);
    }

    private static Task<BlobRecord> WriteObjectReplicaAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord source,
        BlobRecord? mapped,
        BlobRecord? current,
        BlobRecord proposedDestination,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        if (source.IsCurrent && mapped?.IsCurrent != true)
            return CreateCurrentObjectReplicaAsync(
                connection, transaction, current, proposedDestination, now, cancellationToken);
        return mapped is null
            ? CreateHistoricalObjectReplicaAsync(connection, transaction, proposedDestination, cancellationToken)
            : UpdateMappedObjectReplicaAsync(
                connection, transaction, source, mapped, proposedDestination, now, cancellationToken);
    }

    private static async Task<BlobRecord> CreateCurrentObjectReplicaAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord? current,
        BlobRecord proposedDestination,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        if (current is not null)
        {
            EnsureObjectReplicationTargetMutable(current, now);
            var historical = current with
            {
                IsCurrent = false,
                VersionId = current.VersionId ?? await CreateUniqueVersionIdAsync(
                    connection, transaction, current.Account, current.Container, current.Name,
                    current.LastModified, cancellationToken).ConfigureAwait(false),
                Lease = LeaseRecord.Available,
                Revision = NewRevision()
            };
            await UpdateBlobRowAsync(connection, transaction, historical, cancellationToken).ConfigureAwait(false);
        }

        var replicated = proposedDestination with
        {
            IsCurrent = true,
            IsDeleted = false,
            VersionId = await CreateUniqueVersionIdAsync(
                connection, transaction, proposedDestination.Account, proposedDestination.Container,
                proposedDestination.Name, proposedDestination.LastModified,
                cancellationToken).ConfigureAwait(false),
            Snapshot = null,
            Lease = LeaseRecord.Available
        };
        await InsertBlobRowAsync(connection, transaction, replicated, cancellationToken).ConfigureAwait(false);
        return replicated;
    }

    private static async Task<BlobRecord> CreateHistoricalObjectReplicaAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord proposedDestination,
        CancellationToken cancellationToken)
    {
        var replicated = proposedDestination with
        {
            IsCurrent = false,
            IsDeleted = false,
            VersionId = await CreateUniqueVersionIdAsync(
                connection, transaction, proposedDestination.Account, proposedDestination.Container,
                proposedDestination.Name, proposedDestination.LastModified,
                cancellationToken).ConfigureAwait(false),
            Snapshot = null,
            Lease = LeaseRecord.Available
        };
        await InsertBlobRowAsync(connection, transaction, replicated, cancellationToken).ConfigureAwait(false);
        return replicated;
    }

    private static async Task<BlobRecord> UpdateMappedObjectReplicaAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord source,
        BlobRecord mapped,
        BlobRecord proposedDestination,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        EnsureObjectReplicationTargetMutable(mapped, now);
        var replicated = proposedDestination with
        {
            GenerationId = mapped.GenerationId,
            CreatedAt = mapped.CreatedAt,
            VersionId = mapped.VersionId ?? await CreateUniqueVersionIdAsync(
                connection, transaction, mapped.Account, mapped.Container, mapped.Name,
                mapped.LastModified, cancellationToken).ConfigureAwait(false),
            Snapshot = null,
            IsCurrent = source.IsCurrent,
            IsDeleted = false,
            AccessTier = mapped.AccessTier,
            AccessTierInferred = mapped.AccessTierInferred,
            SmartAccessTier = mapped.SmartAccessTier,
            SmartTierLastAccessedAt = mapped.SmartTierLastAccessedAt,
            AccessTierChangedAt = mapped.AccessTierChangedAt,
            ArchiveStatus = mapped.ArchiveStatus,
            RehydratePriority = mapped.RehydratePriority,
            RehydrateCompleteAt = mapped.RehydrateCompleteAt,
            Lease = LeaseRecord.Available
        };
        await UpdateBlobRowAsync(connection, transaction, replicated, cancellationToken).ConfigureAwait(false);
        return replicated;
    }

    private static async Task RecordObjectReplicationSuccessAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord source,
        ObjectReplicationState proposedState,
        string statusKey,
        BlobRecord replicated,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        var statuses = new Dictionary<string, ObjectReplicationStatusRecord>(
            source.ObjectReplicationStatuses, StringComparer.Ordinal)
        {
            [statusKey] = new ObjectReplicationStatusRecord
            {
                Status = "complete",
                SourceFingerprint = proposedState.SourceFingerprint
            }
        };
        await UpdateBlobRowAsync(connection, transaction, source with
        {
            Revision = NewRevision(),
            ObjectReplicationStatuses = statuses
        }, cancellationToken).ConfigureAwait(false);
        await UpsertObjectReplicationStateAsync(
            connection, transaction, proposedState with
            {
                DestinationGenerationId = replicated.GenerationId,
                Status = "complete",
                UpdatedAt = now
            }, cancellationToken).ConfigureAwait(false);
    }
}
