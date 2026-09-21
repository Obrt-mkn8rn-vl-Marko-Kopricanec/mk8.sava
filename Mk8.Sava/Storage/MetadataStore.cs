using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Data.Sqlite;

namespace Mk8.Sava.Storage;

public sealed class MetadataStore(StoragePaths paths, TimeProvider? timeProvider = null)
{
    public const int CurrentSchemaVersion = 2;

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly string _connectionString = new SqliteConnectionStringBuilder
    {
        DataSource = paths.Database,
        Mode = SqliteOpenMode.ReadWriteCreate,
        Cache = SqliteCacheMode.Shared,
        Pooling = true
    }.ToString();
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await ExecuteNonQueryAsync(connection, "PRAGMA journal_mode=WAL;", cancellationToken);
            await ExecuteNonQueryAsync(connection, "PRAGMA synchronous=FULL;", cancellationToken);
            await ExecuteNonQueryAsync(connection, "PRAGMA foreign_keys=ON;", cancellationToken);
            await ExecuteNonQueryAsync(connection, "PRAGMA busy_timeout=30000;", cancellationToken);
            var schemaVersion = await ReadSchemaVersionAsync(connection, cancellationToken);
            if (schemaVersion > CurrentSchemaVersion)
            {
                throw new InvalidDataException(
                    $"The metadata schema version {schemaVersion} is newer than the supported version {CurrentSchemaVersion}.");
            }
            await ExecuteNonQueryAsync(connection, """
                CREATE TABLE IF NOT EXISTS containers (
                    account TEXT NOT NULL,
                    name TEXT NOT NULL,
                    deleted INTEGER NOT NULL,
                    modified_ticks INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    PRIMARY KEY (account, name)
                );
                CREATE INDEX IF NOT EXISTS ix_containers_listing
                    ON containers(account, deleted, name);

                CREATE TABLE IF NOT EXISTS blobs (
                    generation_id TEXT PRIMARY KEY,
                    account TEXT NOT NULL,
                    container TEXT NOT NULL,
                    name TEXT NOT NULL,
                    version_id TEXT NULL,
                    snapshot TEXT NULL,
                    is_current INTEGER NOT NULL,
                    is_deleted INTEGER NOT NULL,
                    modified_ticks INTEGER NOT NULL,
                    logical_length INTEGER NOT NULL,
                    pending_copy_length INTEGER NOT NULL,
                    data TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_blobs_current
                    ON blobs(account, container, name) WHERE is_current = 1;
                CREATE UNIQUE INDEX IF NOT EXISTS ux_blobs_version
                    ON blobs(account, container, name, version_id) WHERE version_id IS NOT NULL;
                CREATE UNIQUE INDEX IF NOT EXISTS ux_blobs_snapshot
                    ON blobs(account, container, name, snapshot) WHERE snapshot IS NOT NULL;
                CREATE INDEX IF NOT EXISTS ix_blobs_listing
                    ON blobs(account, container, is_current, is_deleted, name);

                CREATE TABLE IF NOT EXISTS staged_blocks (
                    account TEXT NOT NULL,
                    container TEXT NOT NULL,
                    blob_name TEXT NOT NULL,
                    block_id TEXT NOT NULL,
                    created_ticks INTEGER NOT NULL,
                    logical_length INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    PRIMARY KEY (account, container, blob_name, block_id)
                );
                CREATE INDEX IF NOT EXISTS ix_staged_blocks_age
                    ON staged_blocks(created_ticks);

                CREATE TABLE IF NOT EXISTS blob_chunk_references (
                    generation_id TEXT NOT NULL,
                    chunk_id TEXT NOT NULL,
                    PRIMARY KEY (generation_id, chunk_id),
                    FOREIGN KEY (generation_id) REFERENCES blobs(generation_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS ix_blob_chunk_references_chunk
                    ON blob_chunk_references(chunk_id);

                CREATE TABLE IF NOT EXISTS staged_block_chunk_references (
                    account TEXT NOT NULL,
                    container TEXT NOT NULL,
                    blob_name TEXT NOT NULL,
                    block_id TEXT NOT NULL,
                    chunk_id TEXT NOT NULL,
                    PRIMARY KEY (account, container, blob_name, block_id, chunk_id),
                    FOREIGN KEY (account, container, blob_name, block_id)
                        REFERENCES staged_blocks(account, container, blob_name, block_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS ix_staged_block_chunk_references_chunk
                    ON staged_block_chunk_references(chunk_id);

                CREATE TABLE IF NOT EXISTS service_properties (
                    account TEXT PRIMARY KEY,
                    data TEXT NOT NULL
                );
                """, cancellationToken);
            if (schemaVersion == 1)
                await MigrateVersion1ToVersion2Async(connection, cancellationToken);
            else if (schemaVersion == 0)
                await ExecuteNonQueryAsync(connection, $"PRAGMA user_version={CurrentSchemaVersion};", cancellationToken);
            await VerifyForeignKeysAsync(connection, cancellationToken);
            _ = await ReadVerifiedStorageInventoryAsync(connection, cancellationToken);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task<bool> IsReadyAsync(CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT 1;";
            return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture) == 1;
        }
        catch (SqliteException)
        {
            return false;
        }
    }

    public async Task<IReadOnlyList<ContainerRecord>> ListContainersAsync(
        string account,
        bool includeDeleted,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = includeDeleted
            ? "SELECT data FROM containers WHERE account = $account ORDER BY name;"
            : "SELECT data FROM containers WHERE account = $account AND deleted = 0 ORDER BY name;";
        command.Parameters.AddWithValue("$account", account);
        return await ReadJsonRowsAsync<ContainerRecord>(command, cancellationToken);
    }

    public async Task<ContainerRecord?> GetContainerAsync(
        string account,
        string name,
        bool includeDeleted,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        return await GetContainerAsync(connection, transaction: null, account, name, includeDeleted, cancellationToken);
    }

    private static async Task<ContainerRecord?> GetContainerAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string account,
        string name,
        bool includeDeleted,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = includeDeleted
            ? "SELECT data FROM containers WHERE account = $account AND name = $name;"
            : "SELECT data FROM containers WHERE account = $account AND name = $name AND deleted = 0;";
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$name", name);
        return await ReadSingleJsonAsync<ContainerRecord>(command, cancellationToken);
    }

    public async Task<bool> TryCreateContainerAsync(ContainerRecord container, CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.CommandText = """
                INSERT OR IGNORE INTO containers(account, name, deleted, modified_ticks, data)
                VALUES ($account, $name, $deleted, $modified, $data);
                """;
            AddContainerParameters(command, container);
            return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task PutContainerAsync(
        ContainerRecord container,
        string expectedRevision,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var current = await GetContainerAsync(connection, transaction, container.Account, container.Name, includeDeleted: true, cancellationToken);
            if (current is null || !string.Equals(current.Revision, expectedRevision, StringComparison.Ordinal))
                throw new StorageConcurrencyException();
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                UPDATE containers SET deleted = $deleted, modified_ticks = $modified, data = $data
                WHERE account = $account AND name = $name;
                """;
            AddContainerParameters(command, container);
            if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
                throw new StorageConcurrencyException();
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task<bool> DeleteContainerPermanentlyAsync(
        string account,
        string name,
        string expectedRevision,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var current = await GetContainerAsync(connection, transaction, account, name, includeDeleted: true, cancellationToken);
            if (current is null || !string.Equals(current.Revision, expectedRevision, StringComparison.Ordinal))
                throw new StorageConcurrencyException();
            await using var blobs = connection.CreateCommand();
            blobs.Transaction = transaction;
            blobs.CommandText = "DELETE FROM blobs WHERE account = $account AND container = $container;";
            blobs.Parameters.AddWithValue("$account", account);
            blobs.Parameters.AddWithValue("$container", name);
            await blobs.ExecuteNonQueryAsync(cancellationToken);

            await using var blocks = connection.CreateCommand();
            blocks.Transaction = transaction;
            blocks.CommandText = "DELETE FROM staged_blocks WHERE account = $account AND container = $container;";
            blocks.Parameters.AddWithValue("$account", account);
            blocks.Parameters.AddWithValue("$container", name);
            await blocks.ExecuteNonQueryAsync(cancellationToken);

            await using var container = connection.CreateCommand();
            container.Transaction = transaction;
            container.CommandText = "DELETE FROM containers WHERE account = $account AND name = $name;";
            container.Parameters.AddWithValue("$account", account);
            container.Parameters.AddWithValue("$name", name);
            var deleted = await container.ExecuteNonQueryAsync(cancellationToken) == 1;
            await transaction.CommitAsync(cancellationToken);
            return deleted;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task<BlobRecord?> GetBlobAsync(
        string account,
        string container,
        string name,
        string? versionId,
        string? snapshot,
        bool includeDeleted,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        var deletedClause = includeDeleted ? string.Empty : " AND is_deleted = 0";
        if (versionId is not null)
        {
            command.CommandText = $"SELECT data FROM blobs WHERE account = $account AND container = $container AND name = $name AND version_id = $version{deletedClause};";
            command.Parameters.AddWithValue("$version", versionId);
        }
        else if (snapshot is not null)
        {
            command.CommandText = $"SELECT data FROM blobs WHERE account = $account AND container = $container AND name = $name AND snapshot = $snapshot{deletedClause};";
            command.Parameters.AddWithValue("$snapshot", snapshot);
        }
        else
        {
            command.CommandText = $"SELECT data FROM blobs WHERE account = $account AND container = $container AND name = $name AND is_current = 1{deletedClause};";
        }

        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", container);
        command.Parameters.AddWithValue("$name", name);
        return await ReadSingleJsonAsync<BlobRecord>(command, cancellationToken);
    }

    public async Task<IReadOnlyList<BlobRecord>> ListBlobsAsync(
        string account,
        string container,
        bool includeVersions,
        bool includeSnapshots,
        bool includeDeleted,
        CancellationToken cancellationToken)
    {
        var predicates = new List<string> { "account = $account", "container = $container" };
        if (!includeVersions && !includeSnapshots)
            predicates.Add("is_current = 1");
        else if (!includeVersions)
            predicates.Add("(is_current = 1 OR snapshot IS NOT NULL)");
        else if (!includeSnapshots)
            predicates.Add("snapshot IS NULL");
        if (!includeDeleted)
            predicates.Add("is_deleted = 0");

        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT data FROM blobs WHERE {string.Join(" AND ", predicates)} ORDER BY name, modified_ticks DESC;";
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", container);
        return await ReadJsonRowsAsync<BlobRecord>(command, cancellationToken);
    }

    public async Task<IReadOnlyList<BlobRecord>> ListPendingCopiesAsync(CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT data FROM blobs WHERE is_deleted = 0;";
        var blobs = await ReadJsonRowsAsync<BlobRecord>(command, cancellationToken);
        return blobs.Where(blob => blob.Copy?.Status == "pending" && blob.PendingCopyContent is not null).ToArray();
    }

    public async Task<IReadOnlyList<BlobRecord>> ListBlobsForMaintenanceAsync(CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT data FROM blobs ORDER BY modified_ticks;";
        return await ReadJsonRowsAsync<BlobRecord>(command, cancellationToken);
    }

    public async Task<IReadOnlyList<ContainerRecord>> ListDeletedContainersForMaintenanceAsync(
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT data FROM containers WHERE deleted = 1 ORDER BY modified_ticks;";
        return await ReadJsonRowsAsync<ContainerRecord>(command, cancellationToken);
    }

    public async Task<BlobRecord> PublishBlobAsync(
        BlobRecord proposed,
        string? expectedCurrentGeneration,
        string? expectedCurrentRevision,
        CancellationToken cancellationToken) =>
        await PublishBlobCoreAsync(
            proposed,
            expectedCurrentGeneration,
            expectedCurrentRevision,
            stagedBlockSnapshot: null,
            cancellationToken);

    public async Task<BlobRecord> PublishBlockListAsync(
        BlobRecord proposed,
        string? expectedCurrentGeneration,
        string? expectedCurrentRevision,
        IReadOnlyList<StagedBlockRecord> stagedBlockSnapshot,
        CancellationToken cancellationToken) =>
        await PublishBlobCoreAsync(
            proposed,
            expectedCurrentGeneration,
            expectedCurrentRevision,
            stagedBlockSnapshot,
            cancellationToken);

    private async Task<BlobRecord> PublishBlobCoreAsync(
        BlobRecord proposed,
        string? expectedCurrentGeneration,
        string? expectedCurrentRevision,
        IReadOnlyList<StagedBlockRecord>? stagedBlockSnapshot,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var current = await GetCurrentBlobAsync(connection, transaction, proposed.Account, proposed.Container, proposed.Name, cancellationToken);
            var activeCurrent = current is { IsDeleted: false } ? current : null;
            if (!string.Equals(activeCurrent?.GenerationId, expectedCurrentGeneration, StringComparison.Ordinal) ||
                !string.Equals(activeCurrent?.Revision, expectedCurrentRevision, StringComparison.Ordinal))
                throw new StorageConcurrencyException();

            if (stagedBlockSnapshot is not null)
            {
                var actualBlocks = await ListStagedBlocksAsync(
                    connection,
                    transaction,
                    proposed.Account,
                    proposed.Container,
                    proposed.Name,
                    cancellationToken);
                if (!EquivalentStagedBlocks(actualBlocks, stagedBlockSnapshot))
                    throw new StorageConcurrencyException();
            }

            var serviceProperties = await GetServicePropertiesAsync(connection, transaction, proposed.Account, cancellationToken);
            if (current is not null)
            {
                if (!current.IsDeleted && current.Copy?.Status == "pending")
                    throw new StoragePendingCopyException();
                var now = _timeProvider.GetUtcNow();
                if (current.IsDeleted)
                {
                    if (current.Kind == proposed.Kind)
                    {
                        var historical = current with
                        {
                            IsCurrent = false,
                            VersionId = null,
                            Snapshot = current.Snapshot ?? await CreateUniqueSnapshotIdAsync(
                                connection,
                                transaction,
                                current.Account,
                                current.Container,
                                current.Name,
                                current.DeletedAt ?? now,
                                cancellationToken),
                            Lease = LeaseRecord.Available,
                            Revision = NewRevision()
                        };
                        await UpdateBlobRowAsync(connection, transaction, historical, cancellationToken);
                    }
                    else
                    {
                        await DeleteSoftDeletedBlobRowsAsync(
                            connection,
                            transaction,
                            current.Account,
                            current.Container,
                            current.Name,
                            cancellationToken);
                    }
                }
                else
                {
                    var createsHistoricalVersion = serviceProperties.VersioningEnabled;
                    var protectedByRetention = current.HasLegalHold || current.ImmutabilityUntil > now;
                    if (protectedByRetention && !createsHistoricalVersion)
                        throw new StorageImmutabilityException(current.HasLegalHold);

                    if (createsHistoricalVersion)
                    {
                        var historical = current with
                        {
                            IsCurrent = false,
                            VersionId = current.VersionId ?? CreateVersionId(current.LastModified),
                            Lease = LeaseRecord.Available,
                            Revision = NewRevision()
                        };
                        await UpdateBlobRowAsync(connection, transaction, historical, cancellationToken);
                    }
                    else if (serviceProperties.BlobSoftDeleteEnabled)
                    {
                        var historical = current with
                        {
                            IsCurrent = false,
                            IsDeleted = true,
                            DeletedAt = now,
                            DeleteRetentionUntil = now.AddDays(serviceProperties.BlobSoftDeleteRetentionDays),
                            VersionId = null,
                            Snapshot = await CreateUniqueSnapshotIdAsync(
                                connection,
                                transaction,
                                current.Account,
                                current.Container,
                                current.Name,
                                now,
                                cancellationToken),
                            Lease = LeaseRecord.Available,
                            Revision = NewRevision()
                        };
                        await UpdateBlobRowAsync(connection, transaction, historical, cancellationToken);
                    }
                    else
                    {
                        await DeleteBlobRowAsync(connection, transaction, current.GenerationId, cancellationToken);
                    }
                }
            }

            var published = proposed with
            {
                IsCurrent = true,
                VersionId = serviceProperties.VersioningEnabled
                    ? proposed.VersionId ?? CreateVersionId(proposed.LastModified)
                    : null,
                Snapshot = null
            };
            await InsertBlobRowAsync(connection, transaction, published, cancellationToken);
            if (stagedBlockSnapshot is not null)
            {
                await using var clearBlocks = connection.CreateCommand();
                clearBlocks.Transaction = transaction;
                clearBlocks.CommandText = """
                    DELETE FROM staged_blocks
                    WHERE account = $account AND container = $container AND blob_name = $blob;
                    """;
                clearBlocks.Parameters.AddWithValue("$account", proposed.Account);
                clearBlocks.Parameters.AddWithValue("$container", proposed.Container);
                clearBlocks.Parameters.AddWithValue("$blob", proposed.Name);
                await clearBlocks.ExecuteNonQueryAsync(cancellationToken);
            }
            await transaction.CommitAsync(cancellationToken);
            return published;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task PutBlobRecordAsync(
        BlobRecord record,
        string expectedRevision,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var current = await GetBlobByGenerationAsync(connection, transaction, record.GenerationId, cancellationToken);
            if (current is null || !string.Equals(current.Revision, expectedRevision, StringComparison.Ordinal))
                throw new StorageConcurrencyException();
            await UpdateBlobRowAsync(connection, transaction, record, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task<BlobRecord> CompleteIncrementalCopyAsync(
        BlobRecord record,
        string expectedRevision,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var current = await GetBlobByGenerationAsync(connection, transaction, record.GenerationId, cancellationToken);
            if (current is null ||
                !current.IsCurrent ||
                !string.Equals(current.Revision, expectedRevision, StringComparison.Ordinal))
            {
                throw new StorageConcurrencyException();
            }

            var snapshotId = await CreateUniqueSnapshotIdAsync(
                connection,
                transaction,
                record.Account,
                record.Container,
                record.Name,
                now,
                cancellationToken);
            var completed = record with { CopyDestinationSnapshot = snapshotId };
            await UpdateBlobRowAsync(connection, transaction, completed, cancellationToken);
            var snapshot = completed with
            {
                GenerationId = Guid.NewGuid().ToString("N"),
                Revision = NewRevision(),
                VersionId = null,
                Snapshot = snapshotId,
                IsCurrent = false,
                Lease = LeaseRecord.Available
            };
            await InsertBlobRowAsync(connection, transaction, snapshot, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return completed;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task<BlobRecord> CreateSnapshotAsync(
        BlobRecord source,
        Dictionary<string, string>? snapshotMetadata,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var current = await GetCurrentBlobAsync(connection, transaction, source.Account, source.Container, source.Name, cancellationToken);
            if (current?.GenerationId != source.GenerationId || current.Revision != source.Revision)
                throw new StorageConcurrencyException();

            var snapshotId = await CreateUniqueSnapshotIdAsync(
                connection,
                transaction,
                source.Account,
                source.Container,
                source.Name,
                now,
                cancellationToken);
            var snapshot = source with
            {
                GenerationId = Guid.NewGuid().ToString("N"),
                Revision = NewRevision(),
                IsCurrent = false,
                VersionId = null,
                Snapshot = snapshotId,
                Metadata = snapshotMetadata ?? source.Metadata,
                Lease = LeaseRecord.Available
            };

            var properties = await GetServicePropertiesAsync(connection, transaction, source.Account, cancellationToken);
            string? newVersionId = null;
            if (properties.VersioningEnabled)
            {
                var historical = source with
                {
                    IsCurrent = false,
                    VersionId = source.VersionId ?? CreateVersionId(source.LastModified),
                    Lease = LeaseRecord.Available,
                    Revision = NewRevision()
                };
                await UpdateBlobRowAsync(connection, transaction, historical, cancellationToken);
                newVersionId = await CreateUniqueVersionIdAsync(
                    connection,
                    transaction,
                    source.Account,
                    source.Container,
                    source.Name,
                    now,
                    cancellationToken);
                var newCurrent = source with
                {
                    GenerationId = Guid.NewGuid().ToString("N"),
                    Revision = NewRevision(),
                    VersionId = newVersionId,
                    Snapshot = null,
                    IsCurrent = true
                };
                await InsertBlobRowAsync(connection, transaction, newCurrent, cancellationToken);
            }
            await InsertBlobRowAsync(connection, transaction, snapshot, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return newVersionId is null ? snapshot : snapshot with { VersionId = newVersionId };
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task<bool> DeleteBlobRecordAsync(
        string generationId,
        string expectedRevision,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var current = await GetBlobByGenerationAsync(connection, transaction, generationId, cancellationToken);
            if (current is null)
                return false;
            if (!string.Equals(current.Revision, expectedRevision, StringComparison.Ordinal))
                throw new StorageConcurrencyException();
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "DELETE FROM blobs WHERE generation_id = $generation;";
            command.Parameters.AddWithValue("$generation", generationId);
            var deleted = await command.ExecuteNonQueryAsync(cancellationToken) == 1;
            await transaction.CommitAsync(cancellationToken);
            return deleted;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task PutStagedBlockAsync(StagedBlockRecord block, CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                INSERT INTO staged_blocks(
                    account, container, blob_name, block_id, created_ticks, logical_length, data)
                VALUES ($account, $container, $blob, $block, $created, $logical, $data)
                ON CONFLICT(account, container, blob_name, block_id) DO UPDATE SET
                    created_ticks = excluded.created_ticks,
                    logical_length = excluded.logical_length,
                    data = excluded.data;
                """;
            command.Parameters.AddWithValue("$account", block.Account);
            command.Parameters.AddWithValue("$container", block.Container);
            command.Parameters.AddWithValue("$blob", block.BlobName);
            command.Parameters.AddWithValue("$block", block.BlockId);
            command.Parameters.AddWithValue("$created", block.CreatedAt.UtcTicks);
            command.Parameters.AddWithValue("$logical", block.Content.Length);
            command.Parameters.AddWithValue("$data", Serialize(block));
            await command.ExecuteNonQueryAsync(cancellationToken);
            await ReplaceStagedBlockChunkReferencesAsync(connection, transaction, block, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task<IReadOnlyList<StagedBlockRecord>> ListStagedBlocksAsync(
        string account,
        string container,
        string blobName,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        return await ListStagedBlocksAsync(connection, transaction: null, account, container, blobName, cancellationToken);
    }

    private static async Task<IReadOnlyList<StagedBlockRecord>> ListStagedBlocksAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string account,
        string container,
        string blobName,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT data FROM staged_blocks
            WHERE account = $account AND container = $container AND blob_name = $blob
            ORDER BY created_ticks;
            """;
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", container);
        command.Parameters.AddWithValue("$blob", blobName);
        return await ReadJsonRowsAsync<StagedBlockRecord>(command, cancellationToken);
    }

    public async Task CommitStagedBlocksAsync(
        string account,
        string container,
        string blobName,
        IReadOnlyCollection<string> committedIds,
        IReadOnlyCollection<string> removeIds,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            foreach (var blockId in committedIds.Concat(removeIds).Distinct(StringComparer.Ordinal))
            {
                await using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = """
                    DELETE FROM staged_blocks
                    WHERE account = $account AND container = $container AND blob_name = $blob AND block_id = $block;
                    """;
                command.Parameters.AddWithValue("$account", account);
                command.Parameters.AddWithValue("$container", container);
                command.Parameters.AddWithValue("$blob", blobName);
                command.Parameters.AddWithValue("$block", blockId);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task<int> DeleteStagedBlocksOlderThanAsync(
        DateTimeOffset cutoff,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.CommandText = "DELETE FROM staged_blocks WHERE created_ticks < $cutoff;";
            command.Parameters.AddWithValue("$cutoff", cutoff.UtcTicks);
            return await command.ExecuteNonQueryAsync(cancellationToken);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task<ServiceProperties> GetServicePropertiesAsync(string account, CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        return await GetServicePropertiesAsync(connection, transaction: null, account, cancellationToken);
    }

    public async Task PutServicePropertiesAsync(string account, ServiceProperties properties, CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.CommandText = """
                INSERT INTO service_properties(account, data) VALUES ($account, $data)
                ON CONFLICT(account) DO UPDATE SET data = excluded.data;
                """;
            command.Parameters.AddWithValue("$account", account);
            command.Parameters.AddWithValue("$data", Serialize(properties));
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    internal async Task<ChunkIdPage> ListReachableChunkIdsAsync(
        string? after,
        int maximum,
        bool excludeCustomerProvidedKeyDomains,
        CancellationToken cancellationToken)
    {
        if (maximum <= 0)
            throw new ArgumentOutOfRangeException(nameof(maximum));
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT chunk_id
            FROM (
                SELECT chunk_id FROM blob_chunk_references
                UNION
                SELECT chunk_id FROM staged_block_chunk_references
            )
            WHERE chunk_id > $after
              AND chunk_id NOT LIKE '%/$zero'
              {(excludeCustomerProvidedKeyDomains ? "AND instr(chunk_id, '/$cpk-') = 0" : string.Empty)}
            ORDER BY chunk_id
            LIMIT $limit;
            """;
        command.Parameters.AddWithValue("$after", after ?? string.Empty);
        command.Parameters.AddWithValue("$limit", checked(maximum + 1));
        var ids = new List<string>(maximum + 1);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            ids.Add(reader.GetString(0));
        var hasMore = ids.Count > maximum;
        if (hasMore)
            ids.RemoveAt(ids.Count - 1);
        return new ChunkIdPage(ids, hasMore);
    }

    internal async Task<IReadOnlySet<string>> FindReachableChunkIdsAsync(
        IReadOnlyList<string> candidates,
        CancellationToken cancellationToken)
    {
        var reachable = new HashSet<string>(StringComparer.Ordinal);
        if (candidates.Count == 0)
            return reachable;

        const int maximumParametersPerQuery = 512;
        var uniqueCandidates = candidates.Distinct(StringComparer.Ordinal).ToArray();
        await using var connection = await OpenAsync(cancellationToken);
        for (var offset = 0; offset < uniqueCandidates.Length; offset += maximumParametersPerQuery)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var count = Math.Min(maximumParametersPerQuery, uniqueCandidates.Length - offset);
            await using var command = connection.CreateCommand();
            var parameterNames = new string[count];
            for (var index = 0; index < count; index++)
            {
                parameterNames[index] = $"$chunk{index}";
                command.Parameters.AddWithValue(parameterNames[index], uniqueCandidates[offset + index]);
            }
            var values = string.Join(',', parameterNames);
            command.CommandText = $"""
                SELECT chunk_id FROM blob_chunk_references WHERE chunk_id IN ({values})
                UNION
                SELECT chunk_id FROM staged_block_chunk_references WHERE chunk_id IN ({values});
                """;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                reachable.Add(reader.GetString(0));
        }
        return reachable;
    }

    public async Task<StorageInventorySummary> GetStorageInventorySummaryAsync(
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        long logicalBlobBytes;
        int blobRecordCount;
        await using (var blobs = connection.CreateCommand())
        {
            blobs.CommandText = """
                SELECT COALESCE(SUM(logical_length + pending_copy_length), 0), COUNT(*)
                FROM blobs;
                """;
            await using var reader = await blobs.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
                throw new InvalidDataException("The blob inventory query returned no aggregate row.");
            logicalBlobBytes = reader.GetInt64(0);
            blobRecordCount = checked((int)reader.GetInt64(1));
        }

        long logicalStagedBlockBytes;
        int stagedBlockCount;
        await using (var blocks = connection.CreateCommand())
        {
            blocks.CommandText = "SELECT COALESCE(SUM(logical_length), 0), COUNT(*) FROM staged_blocks;";
            await using var reader = await blocks.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
                throw new InvalidDataException("The staged-block inventory query returned no aggregate row.");
            logicalStagedBlockBytes = reader.GetInt64(0);
            stagedBlockCount = checked((int)reader.GetInt64(1));
        }

        int reachableChunkCount;
        await using (var chunks = connection.CreateCommand())
        {
            chunks.CommandText = """
                SELECT COUNT(*)
                FROM (
                    SELECT chunk_id FROM blob_chunk_references
                    UNION
                    SELECT chunk_id FROM staged_block_chunk_references
                )
                WHERE chunk_id NOT LIKE '%/$zero';
                """;
            reachableChunkCount = checked(Convert.ToInt32(
                await chunks.ExecuteScalarAsync(cancellationToken),
                CultureInfo.InvariantCulture));
        }

        return new StorageInventorySummary(
            logicalBlobBytes,
            logicalStagedBlockBytes,
            blobRecordCount,
            stagedBlockCount,
            reachableChunkCount);
    }

    public async Task<StorageMetadataInventory> GetStorageInventoryAsync(CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        return await ReadStorageInventoryAsync(connection, cancellationToken);
    }

    internal async Task<MetadataBackupSnapshot> CreateBackupSnapshotAsync(
        string destinationDatabasePath,
        Func<IReadOnlySet<string>, IDisposable> acquireContentPins,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        IDisposable? pins = null;
        try
        {
            await using var source = await OpenAsync(cancellationToken);
            var inventory = await ReadVerifiedStorageInventoryAsync(source, cancellationToken);
            pins = acquireContentPins(inventory.ReachableChunkIds);
            var destinationConnectionString = new SqliteConnectionStringBuilder
            {
                DataSource = destinationDatabasePath,
                Mode = SqliteOpenMode.ReadWriteCreate,
                Cache = SqliteCacheMode.Private,
                Pooling = false
            }.ToString();
            await using (var destination = new SqliteConnection(destinationConnectionString))
            {
                await destination.OpenAsync(cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                source.BackupDatabase(destination);
                cancellationToken.ThrowIfCancellationRequested();
                await using var journalMode = destination.CreateCommand();
                journalMode.CommandText = "PRAGMA journal_mode=DELETE;";
                var selectedMode = Convert.ToString(
                    await journalMode.ExecuteScalarAsync(cancellationToken),
                    CultureInfo.InvariantCulture);
                if (!string.Equals(selectedMode, "delete", StringComparison.OrdinalIgnoreCase))
                    throw new InvalidDataException("The metadata backup could not be normalized to a standalone database file.");
            }
            var snapshot = new MetadataBackupSnapshot(inventory, pins);
            pins = null;
            return snapshot;
        }
        finally
        {
            pins?.Dispose();
            _writeGate.Release();
        }
    }

    internal static async Task<MetadataDatabaseInspection> InspectDatabaseAsync(
        string databasePath,
        CancellationToken cancellationToken)
    {
        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadOnly,
            Cache = SqliteCacheMode.Private,
            Pooling = false
        }.ToString();
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using (var integrity = connection.CreateCommand())
        {
            integrity.CommandText = "PRAGMA integrity_check;";
            await using var reader = await integrity.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var result = reader.GetString(0);
                if (!string.Equals(result, "ok", StringComparison.OrdinalIgnoreCase))
                    throw new InvalidDataException($"The metadata database failed SQLite integrity checking: {result}");
            }
        }
        await VerifyForeignKeysAsync(connection, cancellationToken);

        var schemaVersion = await ReadSchemaVersionAsync(connection, cancellationToken);
        var inventory = await ReadVerifiedStorageInventoryAsync(connection, cancellationToken);
        return new MetadataDatabaseInspection(schemaVersion, inventory);
    }

    private static async Task<StorageMetadataInventory> ReadStorageInventoryAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        var schemaVersion = await ReadSchemaVersionAsync(connection, cancellationToken);
        return schemaVersion >= 2
            ? await ReadIndexedStorageInventoryAsync(connection, cancellationToken)
            : await ReadManifestStorageInventoryAsync(connection, cancellationToken);
    }

    private static async Task<StorageMetadataInventory> ReadVerifiedStorageInventoryAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        var authoritative = await ReadManifestStorageInventoryAsync(connection, cancellationToken);
        if (await ReadSchemaVersionAsync(connection, cancellationToken) < 2)
            return authoritative;

        var indexed = await ReadIndexedStorageInventoryAsync(connection, cancellationToken);
        if (authoritative.LogicalBlobBytes != indexed.LogicalBlobBytes ||
            authoritative.LogicalStagedBlockBytes != indexed.LogicalStagedBlockBytes ||
            authoritative.BlobRecordCount != indexed.BlobRecordCount ||
            authoritative.StagedBlockCount != indexed.StagedBlockCount ||
            !authoritative.ReachableChunkIds.SetEquals(indexed.ReachableChunkIds))
        {
            throw new InvalidDataException("The metadata chunk-reference index does not match the authoritative manifests.");
        }
        return authoritative;
    }

    private static async Task<StorageMetadataInventory> ReadIndexedStorageInventoryAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        var reachable = new HashSet<string>(StringComparer.Ordinal);
        await using (var chunks = connection.CreateCommand())
        {
            chunks.CommandText = """
                SELECT chunk_id FROM blob_chunk_references
                UNION
                SELECT chunk_id FROM staged_block_chunk_references;
                """;
            await using var reader = await chunks.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                reachable.Add(reader.GetString(0));
        }

        long logicalBlobBytes;
        int blobRecordCount;
        await using (var blobs = connection.CreateCommand())
        {
            blobs.CommandText = """
                SELECT COALESCE(SUM(logical_length + pending_copy_length), 0), COUNT(*)
                FROM blobs;
                """;
            await using var reader = await blobs.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
                throw new InvalidDataException("The blob inventory query returned no aggregate row.");
            logicalBlobBytes = reader.GetInt64(0);
            blobRecordCount = reader.GetInt32(1);
        }

        long logicalStagedBlockBytes;
        int stagedBlockCount;
        await using (var blocks = connection.CreateCommand())
        {
            blocks.CommandText = "SELECT COALESCE(SUM(logical_length), 0), COUNT(*) FROM staged_blocks;";
            await using var reader = await blocks.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
                throw new InvalidDataException("The staged-block inventory query returned no aggregate row.");
            logicalStagedBlockBytes = reader.GetInt64(0);
            stagedBlockCount = reader.GetInt32(1);
        }

        return new StorageMetadataInventory(
            reachable,
            logicalBlobBytes,
            logicalStagedBlockBytes,
            blobRecordCount,
            stagedBlockCount);
    }

    private static async Task<StorageMetadataInventory> ReadManifestStorageInventoryAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        var reachable = new HashSet<string>(StringComparer.Ordinal);
        long logicalBlobBytes = 0;
        long logicalStagedBlockBytes = 0;
        var blobRecordCount = 0;
        var stagedBlockCount = 0;

        await using (var blobs = connection.CreateCommand())
        {
            blobs.CommandText = "SELECT data FROM blobs;";
            await using var reader = await blobs.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var blob = Deserialize<BlobRecord>(reader.GetString(0));
                blobRecordCount++;
                logicalBlobBytes = checked(logicalBlobBytes + blob.Content.Length);
                foreach (var chunk in blob.Content.Chunks)
                    reachable.Add(chunk.Id);
                if (blob.PendingCopyContent is not null)
                {
                    logicalBlobBytes = checked(logicalBlobBytes + blob.PendingCopyContent.Length);
                    foreach (var chunk in blob.PendingCopyContent.Chunks)
                        reachable.Add(chunk.Id);
                }
            }
        }

        await using (var blocks = connection.CreateCommand())
        {
            blocks.CommandText = "SELECT data FROM staged_blocks;";
            await using var reader = await blocks.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var block = Deserialize<StagedBlockRecord>(reader.GetString(0));
                stagedBlockCount++;
                logicalStagedBlockBytes = checked(logicalStagedBlockBytes + block.Content.Length);
                foreach (var chunk in block.Content.Chunks)
                    reachable.Add(chunk.Id);
            }
        }

        return new StorageMetadataInventory(
            reachable,
            logicalBlobBytes,
            logicalStagedBlockBytes,
            blobRecordCount,
            stagedBlockCount);
    }

    public DateTimeOffset GetUtcNow() => _timeProvider.GetUtcNow();

    public static string NewETag() => $"\"0x{Convert.ToHexString(RandomNumberGenerator.GetBytes(8))}\"";

    public static string NewRevision() => Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(16));

    public static string CreateVersionId(DateTimeOffset time) =>
        time.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ", CultureInfo.InvariantCulture);

    private static async Task MigrateVersion1ToVersion2Async(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<BlobRecord> blobs;
        IReadOnlyList<StagedBlockRecord> blocks;
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT data FROM blobs;";
            blobs = await ReadJsonRowsAsync<BlobRecord>(command, cancellationToken);
        }
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT data FROM staged_blocks;";
            blocks = await ReadJsonRowsAsync<StagedBlockRecord>(command, cancellationToken);
        }

        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
        await ExecuteNonQueryAsync(
            connection,
            transaction,
            "ALTER TABLE blobs ADD COLUMN logical_length INTEGER NOT NULL DEFAULT 0;",
            cancellationToken);
        await ExecuteNonQueryAsync(
            connection,
            transaction,
            "ALTER TABLE blobs ADD COLUMN pending_copy_length INTEGER NOT NULL DEFAULT 0;",
            cancellationToken);
        await ExecuteNonQueryAsync(
            connection,
            transaction,
            "ALTER TABLE staged_blocks ADD COLUMN logical_length INTEGER NOT NULL DEFAULT 0;",
            cancellationToken);

        foreach (var blob in blobs)
        {
            await using var update = connection.CreateCommand();
            update.Transaction = transaction;
            update.CommandText = """
                UPDATE blobs
                SET logical_length = $logical, pending_copy_length = $pending
                WHERE generation_id = $generation;
                """;
            update.Parameters.AddWithValue("$logical", blob.Content.Length);
            update.Parameters.AddWithValue("$pending", blob.PendingCopyContent?.Length ?? 0);
            update.Parameters.AddWithValue("$generation", blob.GenerationId);
            if (await update.ExecuteNonQueryAsync(cancellationToken) != 1)
                throw new InvalidDataException("A blob changed while migrating the metadata schema.");
            await ReplaceBlobChunkReferencesAsync(connection, transaction, blob, cancellationToken);
        }

        foreach (var block in blocks)
        {
            await using var update = connection.CreateCommand();
            update.Transaction = transaction;
            update.CommandText = """
                UPDATE staged_blocks
                SET logical_length = $logical
                WHERE account = $account AND container = $container
                  AND blob_name = $blob AND block_id = $block;
                """;
            update.Parameters.AddWithValue("$logical", block.Content.Length);
            update.Parameters.AddWithValue("$account", block.Account);
            update.Parameters.AddWithValue("$container", block.Container);
            update.Parameters.AddWithValue("$blob", block.BlobName);
            update.Parameters.AddWithValue("$block", block.BlockId);
            if (await update.ExecuteNonQueryAsync(cancellationToken) != 1)
                throw new InvalidDataException("A staged block changed while migrating the metadata schema.");
            await ReplaceStagedBlockChunkReferencesAsync(connection, transaction, block, cancellationToken);
        }

        await ExecuteNonQueryAsync(
            connection,
            transaction,
            $"PRAGMA user_version={CurrentSchemaVersion};",
            cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    private async Task<SqliteConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await ExecuteNonQueryAsync(connection, "PRAGMA foreign_keys=ON;", cancellationToken);
        await ExecuteNonQueryAsync(connection, "PRAGMA busy_timeout=30000;", cancellationToken);
        return connection;
    }

    private static async Task ExecuteNonQueryAsync(SqliteConnection connection, string text, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = text;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task ExecuteNonQueryAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string text,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = text;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<int> ReadSchemaVersionAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA user_version;";
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
    }

    private static async Task VerifyForeignKeysAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA foreign_key_check;";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
            throw new InvalidDataException("The metadata database contains an invalid chunk-reference relationship.");
    }

    private static void AddContainerParameters(SqliteCommand command, ContainerRecord container)
    {
        command.Parameters.AddWithValue("$account", container.Account);
        command.Parameters.AddWithValue("$name", container.Name);
        command.Parameters.AddWithValue("$deleted", container.DeletedAt.HasValue ? 1 : 0);
        command.Parameters.AddWithValue("$modified", container.LastModified.UtcTicks);
        command.Parameters.AddWithValue("$data", Serialize(container));
    }

    private static async Task<BlobRecord?> GetCurrentBlobAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string account,
        string container,
        string name,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT data FROM blobs
            WHERE account = $account AND container = $container AND name = $name AND is_current = 1;
            """;
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", container);
        command.Parameters.AddWithValue("$name", name);
        return await ReadSingleJsonAsync<BlobRecord>(command, cancellationToken);
    }

    private static async Task<BlobRecord?> GetBlobByGenerationAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string generationId,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT data FROM blobs WHERE generation_id = $generation;";
        command.Parameters.AddWithValue("$generation", generationId);
        return await ReadSingleJsonAsync<BlobRecord>(command, cancellationToken);
    }

    private static async Task DeleteSoftDeletedBlobRowsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string account,
        string container,
        string name,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            DELETE FROM blobs
            WHERE account = $account AND container = $container AND name = $name AND is_deleted = 1;
            """;
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", container);
        command.Parameters.AddWithValue("$name", name);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<string> CreateUniqueSnapshotIdAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string account,
        string container,
        string name,
        DateTimeOffset time,
        CancellationToken cancellationToken)
    {
        for (var tickOffset = 0L; ; tickOffset++)
        {
            var candidate = CreateVersionId(time.AddTicks(tickOffset));
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                SELECT 1 FROM blobs
                WHERE account = $account AND container = $container AND name = $name AND snapshot = $snapshot
                LIMIT 1;
                """;
            command.Parameters.AddWithValue("$account", account);
            command.Parameters.AddWithValue("$container", container);
            command.Parameters.AddWithValue("$name", name);
            command.Parameters.AddWithValue("$snapshot", candidate);
            if (await command.ExecuteScalarAsync(cancellationToken) is null)
                return candidate;
        }
    }

    private static async Task<string> CreateUniqueVersionIdAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string account,
        string container,
        string name,
        DateTimeOffset time,
        CancellationToken cancellationToken)
    {
        for (var tickOffset = 0L; ; tickOffset++)
        {
            var candidate = CreateVersionId(time.AddTicks(tickOffset));
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                SELECT 1 FROM blobs
                WHERE account = $account AND container = $container AND name = $name AND version_id = $version
                LIMIT 1;
                """;
            command.Parameters.AddWithValue("$account", account);
            command.Parameters.AddWithValue("$container", container);
            command.Parameters.AddWithValue("$name", name);
            command.Parameters.AddWithValue("$version", candidate);
            if (await command.ExecuteScalarAsync(cancellationToken) is null)
                return candidate;
        }
    }

    private static async Task InsertBlobRowAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord record,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO blobs(
                generation_id, account, container, name, version_id, snapshot,
                is_current, is_deleted, modified_ticks, logical_length,
                pending_copy_length, data)
            VALUES (
                $generation, $account, $container, $name, $version, $snapshot,
                $current, $deleted, $modified, $logical, $pending, $data);
            """;
        AddBlobParameters(command, record);
        await command.ExecuteNonQueryAsync(cancellationToken);
        await ReplaceBlobChunkReferencesAsync(connection, transaction, record, cancellationToken);
    }

    private static async Task UpdateBlobRowAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord record,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE blobs SET
                account = $account,
                container = $container,
                name = $name,
                version_id = $version,
                snapshot = $snapshot,
                is_current = $current,
                is_deleted = $deleted,
                modified_ticks = $modified,
                logical_length = $logical,
                pending_copy_length = $pending,
                data = $data
            WHERE generation_id = $generation;
            """;
        AddBlobParameters(command, record);
        if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
            throw new StorageConcurrencyException();
        await ReplaceBlobChunkReferencesAsync(connection, transaction, record, cancellationToken);
    }

    private static async Task DeleteBlobRowAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string generationId,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "DELETE FROM blobs WHERE generation_id = $generation;";
        command.Parameters.AddWithValue("$generation", generationId);
        if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
            throw new StorageConcurrencyException();
    }

    private static async Task ReplaceBlobChunkReferencesAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord record,
        CancellationToken cancellationToken)
    {
        await using (var clear = connection.CreateCommand())
        {
            clear.Transaction = transaction;
            clear.CommandText = "DELETE FROM blob_chunk_references WHERE generation_id = $generation;";
            clear.Parameters.AddWithValue("$generation", record.GenerationId);
            await clear.ExecuteNonQueryAsync(cancellationToken);
        }

        foreach (var chunkId in EnumerateChunkIds(record).Distinct(StringComparer.Ordinal))
        {
            await using var insert = connection.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText = """
                INSERT INTO blob_chunk_references(generation_id, chunk_id)
                VALUES ($generation, $chunk);
                """;
            insert.Parameters.AddWithValue("$generation", record.GenerationId);
            insert.Parameters.AddWithValue("$chunk", chunkId);
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static async Task ReplaceStagedBlockChunkReferencesAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        StagedBlockRecord block,
        CancellationToken cancellationToken)
    {
        await using (var clear = connection.CreateCommand())
        {
            clear.Transaction = transaction;
            clear.CommandText = """
                DELETE FROM staged_block_chunk_references
                WHERE account = $account AND container = $container
                  AND blob_name = $blob AND block_id = $block;
                """;
            clear.Parameters.AddWithValue("$account", block.Account);
            clear.Parameters.AddWithValue("$container", block.Container);
            clear.Parameters.AddWithValue("$blob", block.BlobName);
            clear.Parameters.AddWithValue("$block", block.BlockId);
            await clear.ExecuteNonQueryAsync(cancellationToken);
        }

        foreach (var chunkId in block.Content.Chunks.Select(chunk => chunk.Id).Distinct(StringComparer.Ordinal))
        {
            await using var insert = connection.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText = """
                INSERT INTO staged_block_chunk_references(
                    account, container, blob_name, block_id, chunk_id)
                VALUES ($account, $container, $blob, $block, $chunk);
                """;
            insert.Parameters.AddWithValue("$account", block.Account);
            insert.Parameters.AddWithValue("$container", block.Container);
            insert.Parameters.AddWithValue("$blob", block.BlobName);
            insert.Parameters.AddWithValue("$block", block.BlockId);
            insert.Parameters.AddWithValue("$chunk", chunkId);
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static IEnumerable<string> EnumerateChunkIds(BlobRecord record)
    {
        foreach (var chunk in record.Content.Chunks)
            yield return chunk.Id;
        if (record.PendingCopyContent is not null)
        {
            foreach (var chunk in record.PendingCopyContent.Chunks)
                yield return chunk.Id;
        }
    }

    private static void AddBlobParameters(SqliteCommand command, BlobRecord record)
    {
        command.Parameters.AddWithValue("$generation", record.GenerationId);
        command.Parameters.AddWithValue("$account", record.Account);
        command.Parameters.AddWithValue("$container", record.Container);
        command.Parameters.AddWithValue("$name", record.Name);
        command.Parameters.AddWithValue("$version", (object?)record.VersionId ?? DBNull.Value);
        command.Parameters.AddWithValue("$snapshot", (object?)record.Snapshot ?? DBNull.Value);
        command.Parameters.AddWithValue("$current", record.IsCurrent ? 1 : 0);
        command.Parameters.AddWithValue("$deleted", record.IsDeleted ? 1 : 0);
        command.Parameters.AddWithValue("$modified", record.LastModified.UtcTicks);
        command.Parameters.AddWithValue("$logical", record.Content.Length);
        command.Parameters.AddWithValue("$pending", record.PendingCopyContent?.Length ?? 0);
        command.Parameters.AddWithValue("$data", Serialize(record));
    }

    private static async Task<ServiceProperties> GetServicePropertiesAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string account,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT data FROM service_properties WHERE account = $account;";
        command.Parameters.AddWithValue("$account", account);
        return await ReadSingleJsonAsync<ServiceProperties>(command, cancellationToken) ?? new ServiceProperties();
    }

    private static async Task<T?> ReadSingleJsonAsync<T>(SqliteCommand command, CancellationToken cancellationToken)
        where T : class
    {
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is string json ? Deserialize<T>(json) : null;
    }

    private static async Task<IReadOnlyList<T>> ReadJsonRowsAsync<T>(SqliteCommand command, CancellationToken cancellationToken)
    {
        var values = new List<T>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            values.Add(Deserialize<T>(reader.GetString(0)));
        return values;
    }

    private static bool EquivalentStagedBlocks(
        IReadOnlyList<StagedBlockRecord> left,
        IReadOnlyList<StagedBlockRecord> right)
    {
        if (left.Count != right.Count)
            return false;
        var expected = right.ToDictionary(item => item.BlockId, StringComparer.Ordinal);
        return left.All(item =>
            expected.TryGetValue(item.BlockId, out var candidate) &&
            item.CreatedAt == candidate.CreatedAt &&
            item.Content.Length == candidate.Content.Length &&
            string.Equals(item.Content.Domain, candidate.Content.Domain, StringComparison.Ordinal) &&
            string.Equals(item.Content.Sha256, candidate.Content.Sha256, StringComparison.Ordinal));
    }

    private static string Serialize<T>(T value) => JsonSerializer.Serialize(value, JsonOptions);

    private static T Deserialize<T>(string value) =>
        JsonSerializer.Deserialize<T>(value, JsonOptions)
        ?? throw new InvalidDataException($"Stored {typeof(T).Name} metadata was null.");
}

public sealed class StorageConcurrencyException : Exception
{
    public StorageConcurrencyException() : base("The logical storage resource changed concurrently.") { }
}

public sealed class StorageImmutabilityException(bool legalHold) : Exception(
    legalHold
        ? "The blob is protected by a legal hold."
        : "The blob is protected by a time-based retention policy.")
{
    public bool LegalHold { get; } = legalHold;
}

public sealed class StoragePendingCopyException : Exception
{
    public StoragePendingCopyException() : base("There is currently a pending copy operation.") { }
}
