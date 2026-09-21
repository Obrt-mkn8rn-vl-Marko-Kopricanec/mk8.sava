using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Data.Sqlite;

namespace Mk8.Sava.Storage;

public sealed class MetadataStore(StoragePaths paths, TimeProvider? timeProvider = null)
{
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
                    data TEXT NOT NULL,
                    PRIMARY KEY (account, container, blob_name, block_id)
                );
                CREATE INDEX IF NOT EXISTS ix_staged_blocks_age
                    ON staged_blocks(created_ticks);

                CREATE TABLE IF NOT EXISTS service_properties (
                    account TEXT PRIMARY KEY,
                    data TEXT NOT NULL
                );
                """, cancellationToken);
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
            if (!string.Equals(current?.GenerationId, expectedCurrentGeneration, StringComparison.Ordinal) ||
                !string.Equals(current?.Revision, expectedCurrentRevision, StringComparison.Ordinal))
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
                if (serviceProperties.VersioningEnabled)
                {
                    var historical = current with
                    {
                        IsCurrent = false,
                        VersionId = current.VersionId ?? CreateVersionId(current.LastModified)
                    };
                    await UpdateBlobRowAsync(connection, transaction, historical, cancellationToken);
                }
                else
                {
                    await DeleteBlobRowAsync(connection, transaction, current.GenerationId, cancellationToken);
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

    public async Task<BlobRecord> CreateSnapshotAsync(BlobRecord source, DateTimeOffset now, CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var current = await GetCurrentBlobAsync(connection, transaction, source.Account, source.Container, source.Name, cancellationToken);
            if (current?.GenerationId != source.GenerationId || current.Revision != source.Revision)
                throw new StorageConcurrencyException();

            var snapshot = source with
            {
                GenerationId = Guid.NewGuid().ToString("N"),
                Revision = NewRevision(),
                IsCurrent = false,
                VersionId = null,
                Snapshot = CreateVersionId(now),
                Lease = LeaseRecord.Available
            };
            await InsertBlobRowAsync(connection, transaction, snapshot, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return snapshot;
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
            await using var command = connection.CreateCommand();
            command.CommandText = """
                INSERT INTO staged_blocks(account, container, blob_name, block_id, created_ticks, data)
                VALUES ($account, $container, $blob, $block, $created, $data)
                ON CONFLICT(account, container, blob_name, block_id) DO UPDATE SET
                    created_ticks = excluded.created_ticks,
                    data = excluded.data;
                """;
            command.Parameters.AddWithValue("$account", block.Account);
            command.Parameters.AddWithValue("$container", block.Container);
            command.Parameters.AddWithValue("$blob", block.BlobName);
            command.Parameters.AddWithValue("$block", block.BlockId);
            command.Parameters.AddWithValue("$created", block.CreatedAt.UtcTicks);
            command.Parameters.AddWithValue("$data", Serialize(block));
            await command.ExecuteNonQueryAsync(cancellationToken);
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

    public async Task<IReadOnlySet<string>> GetReachableChunkIdsAsync(CancellationToken cancellationToken)
    {
        var reachable = new HashSet<string>(StringComparer.Ordinal);
        await using var connection = await OpenAsync(cancellationToken);

        await using (var blobs = connection.CreateCommand())
        {
            blobs.CommandText = "SELECT data FROM blobs;";
            await using var reader = await blobs.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var blob = Deserialize<BlobRecord>(reader.GetString(0));
                foreach (var chunk in blob.Content.Chunks)
                    reachable.Add(chunk.Id);
            }
        }

        await using (var blocks = connection.CreateCommand())
        {
            blocks.CommandText = "SELECT data FROM staged_blocks;";
            await using var reader = await blocks.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var block = Deserialize<StagedBlockRecord>(reader.GetString(0));
                foreach (var chunk in block.Content.Chunks)
                    reachable.Add(chunk.Id);
            }
        }

        return reachable;
    }

    public DateTimeOffset GetUtcNow() => _timeProvider.GetUtcNow();

    public static string NewETag() => $"\"0x{Convert.ToHexString(RandomNumberGenerator.GetBytes(8))}\"";

    public static string NewRevision() => Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(16));

    public static string CreateVersionId(DateTimeOffset time) =>
        time.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ", CultureInfo.InvariantCulture);

    private async Task<SqliteConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }

    private static async Task ExecuteNonQueryAsync(SqliteConnection connection, string text, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = text;
        await command.ExecuteNonQueryAsync(cancellationToken);
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

    private static async Task InsertBlobRowAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord record,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO blobs(generation_id, account, container, name, version_id, snapshot, is_current, is_deleted, modified_ticks, data)
            VALUES ($generation, $account, $container, $name, $version, $snapshot, $current, $deleted, $modified, $data);
            """;
        AddBlobParameters(command, record);
        await command.ExecuteNonQueryAsync(cancellationToken);
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
                data = $data
            WHERE generation_id = $generation;
            """;
        AddBlobParameters(command, record);
        if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
            throw new StorageConcurrencyException();
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
