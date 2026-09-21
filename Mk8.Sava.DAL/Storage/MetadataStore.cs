using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Data.Sqlite;

namespace Mk8.Sava.Storage;

public sealed class MetadataStore(IStoragePaths paths, TimeProvider? timeProvider = null)
{
    public const int CurrentSchemaVersion = 4;
    private const int ChunkIndexSchemaVersion = 2;
    private const int TagIndexSchemaVersion = 3;

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
                CREATE INDEX IF NOT EXISTS ix_containers_enumeration
                    ON containers(account, name);

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
                CREATE INDEX IF NOT EXISTS ix_blobs_enumeration
                    ON blobs(account, container, name);

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

                CREATE TABLE IF NOT EXISTS blob_tags (
                    generation_id TEXT NOT NULL,
                    tag_key TEXT NOT NULL,
                    tag_value TEXT NOT NULL,
                    PRIMARY KEY (generation_id, tag_key),
                    FOREIGN KEY (generation_id) REFERENCES blobs(generation_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS ix_blob_tags_search
                    ON blob_tags(tag_key, tag_value, generation_id);

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

                CREATE TABLE IF NOT EXISTS chunk_packs (
                    pack_id TEXT PRIMARY KEY,
                    domain TEXT NOT NULL,
                    created_ticks INTEGER NOT NULL,
                    sealed INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_chunk_packs_domain_state
                    ON chunk_packs(domain, sealed, created_ticks, pack_id);

                CREATE TABLE IF NOT EXISTS packed_chunks (
                    chunk_id TEXT PRIMARY KEY,
                    pack_id TEXT NOT NULL,
                    record_offset INTEGER NOT NULL,
                    record_length INTEGER NOT NULL,
                    payload_offset INTEGER NOT NULL,
                    payload_length INTEGER NOT NULL,
                    FOREIGN KEY (pack_id) REFERENCES chunk_packs(pack_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS ix_packed_chunks_pack
                    ON packed_chunks(pack_id, record_offset, chunk_id);
                """, cancellationToken);
            if (schemaVersion == 1)
            {
                await MigrateVersion1ToVersion2Async(connection, cancellationToken);
                schemaVersion = ChunkIndexSchemaVersion;
            }
            if (schemaVersion == ChunkIndexSchemaVersion)
            {
                await MigrateVersion2ToVersion3Async(connection, cancellationToken);
                schemaVersion = TagIndexSchemaVersion;
            }
            if (schemaVersion == TagIndexSchemaVersion)
                await ExecuteNonQueryAsync(connection, $"PRAGMA user_version={CurrentSchemaVersion};", cancellationToken);
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

    internal async Task<ContainerListPage> ListContainersPageAsync(
        string account,
        bool includeDeleted,
        string prefix,
        string marker,
        int maximum,
        CancellationToken cancellationToken)
    {
        if (maximum <= 0)
            throw new ArgumentOutOfRangeException(nameof(maximum));
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT data FROM containers
            WHERE account = $account
              {(includeDeleted ? string.Empty : "AND deleted = 0")}
              AND name >= $prefix
              AND substr(name, 1, length($prefix)) = $prefix
              AND name > $marker
            ORDER BY name
            LIMIT $limit;
            """;
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$prefix", prefix);
        command.Parameters.AddWithValue("$marker", marker);
        command.Parameters.AddWithValue("$limit", checked(maximum + 1));
        var records = (await ReadJsonRowsAsync<ContainerRecord>(command, cancellationToken)).ToList();
        var hasMore = records.Count > maximum;
        if (hasMore)
            records.RemoveAt(records.Count - 1);
        return new ContainerListPage(records, hasMore);
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

    internal async Task<IReadOnlyList<BlobRecord>> ListBlobFamilyAsync(
        string account,
        string container,
        string name,
        bool includeDeleted,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT data FROM blobs
            WHERE account = $account AND container = $container AND name = $name
                  {(includeDeleted ? string.Empty : "AND is_deleted = 0")}
            ORDER BY is_current DESC, modified_ticks DESC, generation_id;
            """;
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", container);
        command.Parameters.AddWithValue("$name", name);
        return await ReadJsonRowsAsync<BlobRecord>(command, cancellationToken);
    }

    internal bool PackedChunkExists(string chunkId)
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM packed_chunks WHERE chunk_id = $chunk LIMIT 1;";
        command.Parameters.AddWithValue("$chunk", chunkId);
        return command.ExecuteScalar() is not null;
    }

    internal bool ChunkPackExists(string packId)
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM chunk_packs WHERE pack_id = $pack LIMIT 1;";
        command.Parameters.AddWithValue("$pack", packId);
        return command.ExecuteScalar() is not null;
    }

    internal int CountPackedChunks()
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM packed_chunks;";
        return Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture);
    }

    internal async Task<PackedChunkLocation?> GetPackedChunkLocationAsync(
        string chunkId,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT chunk_id, pack_id, record_offset, record_length, payload_offset, payload_length
            FROM packed_chunks
            WHERE chunk_id = $chunk;
            """;
        command.Parameters.AddWithValue("$chunk", chunkId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadPackedChunkLocation(reader) : null;
    }

    internal async Task<ChunkPackRecord?> GetActiveChunkPackAsync(
        string domain,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT pack_id, domain, created_ticks, sealed
            FROM chunk_packs
            WHERE domain = $domain AND sealed = 0
            ORDER BY created_ticks DESC, pack_id DESC
            LIMIT 1;
            """;
        command.Parameters.AddWithValue("$domain", domain);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadChunkPack(reader) : null;
    }

    internal async Task<int> CountPackedChunksAsync(
        string packId,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM packed_chunks WHERE pack_id = $pack;";
        command.Parameters.AddWithValue("$pack", packId);
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
    }

    internal async Task<bool> TryRegisterPackedChunkAsync(
        ChunkPackRecord pack,
        PackedChunkLocation location,
        CancellationToken cancellationToken)
    {
        if (!string.Equals(pack.PackId, location.PackId, StringComparison.Ordinal))
            throw new ArgumentException("The packed chunk location does not belong to the supplied pack.", nameof(location));

        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await using (var addPack = connection.CreateCommand())
            {
                addPack.Transaction = transaction;
                addPack.CommandText = """
                    INSERT INTO chunk_packs(pack_id, domain, created_ticks, sealed)
                    VALUES ($pack, $domain, $created, $sealed)
                    ON CONFLICT(pack_id) DO NOTHING;
                    """;
                addPack.Parameters.AddWithValue("$pack", pack.PackId);
                addPack.Parameters.AddWithValue("$domain", pack.Domain);
                addPack.Parameters.AddWithValue("$created", pack.CreatedAt.UtcTicks);
                addPack.Parameters.AddWithValue("$sealed", pack.Sealed ? 1 : 0);
                await addPack.ExecuteNonQueryAsync(cancellationToken);
            }

            await using var addLocation = connection.CreateCommand();
            addLocation.Transaction = transaction;
            addLocation.CommandText = """
                INSERT INTO packed_chunks(
                    chunk_id, pack_id, record_offset, record_length, payload_offset, payload_length)
                VALUES ($chunk, $pack, $record_offset, $record_length, $payload_offset, $payload_length)
                ON CONFLICT(chunk_id) DO NOTHING;
                """;
            AddPackedChunkLocationParameters(addLocation, location);
            var inserted = await addLocation.ExecuteNonQueryAsync(cancellationToken) == 1;
            await transaction.CommitAsync(cancellationToken);
            return inserted;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    internal async Task SealChunkPackAsync(string packId, CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.CommandText = "UPDATE chunk_packs SET sealed = 1 WHERE pack_id = $pack;";
            command.Parameters.AddWithValue("$pack", packId);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    internal async Task<int> SealChunkPacksOlderThanAsync(
        DateTimeOffset olderThan,
        int maximum,
        CancellationToken cancellationToken)
    {
        if (maximum <= 0)
            throw new ArgumentOutOfRangeException(nameof(maximum));
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.CommandText = """
                UPDATE chunk_packs SET sealed = 1
                WHERE pack_id IN (
                    SELECT pack_id FROM chunk_packs
                    WHERE sealed = 0 AND created_ticks <= $older
                    ORDER BY created_ticks, pack_id
                    LIMIT $limit
                );
                """;
            command.Parameters.AddWithValue("$older", olderThan.UtcTicks);
            command.Parameters.AddWithValue("$limit", maximum);
            return await command.ExecuteNonQueryAsync(cancellationToken);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    internal async Task<bool> DeletePackedChunkLocationAsync(
        string chunkId,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.CommandText = "DELETE FROM packed_chunks WHERE chunk_id = $chunk;";
            command.Parameters.AddWithValue("$chunk", chunkId);
            return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    internal async Task<ChunkIdPage> ListPackedChunkIdsAsync(
        string? after,
        int maximum,
        CancellationToken cancellationToken)
    {
        if (maximum <= 0)
            throw new ArgumentOutOfRangeException(nameof(maximum));
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT chunk_id FROM packed_chunks
            WHERE $has_after = 0 OR chunk_id > $after
            ORDER BY chunk_id
            LIMIT $limit;
            """;
        command.Parameters.AddWithValue("$has_after", after is null ? 0 : 1);
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

    internal async Task<ChunkPackPage> ListSealedChunkPacksAsync(
        string? after,
        int maximum,
        CancellationToken cancellationToken)
    {
        if (maximum <= 0)
            throw new ArgumentOutOfRangeException(nameof(maximum));
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT pack_id, domain, created_ticks, sealed
            FROM chunk_packs
            WHERE sealed = 1 AND ($has_after = 0 OR pack_id > $after)
            ORDER BY pack_id
            LIMIT $limit;
            """;
        command.Parameters.AddWithValue("$has_after", after is null ? 0 : 1);
        command.Parameters.AddWithValue("$after", after ?? string.Empty);
        command.Parameters.AddWithValue("$limit", checked(maximum + 1));
        var packs = new List<ChunkPackRecord>(maximum + 1);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            packs.Add(ReadChunkPack(reader));
        var hasMore = packs.Count > maximum;
        if (hasMore)
            packs.RemoveAt(packs.Count - 1);
        return new ChunkPackPage(packs, hasMore);
    }

    internal async Task<IReadOnlyList<PackedChunkLocation>> ListPackedChunkLocationsAsync(
        string packId,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT chunk_id, pack_id, record_offset, record_length, payload_offset, payload_length
            FROM packed_chunks
            WHERE pack_id = $pack
            ORDER BY record_offset, chunk_id;
            """;
        command.Parameters.AddWithValue("$pack", packId);
        var locations = new List<PackedChunkLocation>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            locations.Add(ReadPackedChunkLocation(reader));
        return locations;
    }

    internal async Task ReplaceChunkPackAsync(
        ChunkPackRecord oldPack,
        ChunkPackRecord? replacementPack,
        IReadOnlyList<PackedChunkLocation> oldLocations,
        IReadOnlyList<PackedChunkLocation> replacementLocations,
        CancellationToken cancellationToken)
    {
        if (replacementPack is null && replacementLocations.Count != 0 ||
            replacementPack is not null && replacementLocations.Any(location =>
                !string.Equals(location.PackId, replacementPack.PackId, StringComparison.Ordinal)) ||
            oldLocations.Count != replacementLocations.Count)
        {
            throw new ArgumentException("The replacement pack and chunk locations are inconsistent.", nameof(replacementLocations));
        }

        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var actual = await ListPackedChunkLocationsAsync(connection, transaction, oldPack.PackId, cancellationToken);
            if (!EquivalentPackedLocations(actual, oldLocations))
                throw new StorageConcurrencyException();

            if (replacementPack is not null)
            {
                await using var addPack = connection.CreateCommand();
                addPack.Transaction = transaction;
                addPack.CommandText = """
                    INSERT INTO chunk_packs(pack_id, domain, created_ticks, sealed)
                    VALUES ($pack, $domain, $created, 1);
                    """;
                addPack.Parameters.AddWithValue("$pack", replacementPack.PackId);
                addPack.Parameters.AddWithValue("$domain", replacementPack.Domain);
                addPack.Parameters.AddWithValue("$created", replacementPack.CreatedAt.UtcTicks);
                await addPack.ExecuteNonQueryAsync(cancellationToken);

                foreach (var location in replacementLocations)
                {
                    await using var update = connection.CreateCommand();
                    update.Transaction = transaction;
                    update.CommandText = """
                        UPDATE packed_chunks SET
                            pack_id = $pack,
                            record_offset = $record_offset,
                            record_length = $record_length,
                            payload_offset = $payload_offset,
                            payload_length = $payload_length
                        WHERE chunk_id = $chunk AND pack_id = $old_pack;
                        """;
                    AddPackedChunkLocationParameters(update, location);
                    update.Parameters.AddWithValue("$old_pack", oldPack.PackId);
                    if (await update.ExecuteNonQueryAsync(cancellationToken) != 1)
                        throw new StorageConcurrencyException();
                }
            }

            await using var removeOld = connection.CreateCommand();
            removeOld.Transaction = transaction;
            removeOld.CommandText = "DELETE FROM chunk_packs WHERE pack_id = $pack;";
            removeOld.Parameters.AddWithValue("$pack", oldPack.PackId);
            if (await removeOld.ExecuteNonQueryAsync(cancellationToken) != 1)
                throw new StorageConcurrencyException();
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    internal async Task<BlobListPage> ListBlobsPageAsync(
        string account,
        string container,
        bool includeVersions,
        bool includeSnapshots,
        bool includeDeleted,
        string prefix,
        string delimiter,
        BlobListCursor? cursor,
        int legacyOffset,
        int maximum,
        CancellationToken cancellationToken)
    {
        if (maximum <= 0)
            throw new ArgumentOutOfRangeException(nameof(maximum));
        if (legacyOffset < 0)
            throw new ArgumentOutOfRangeException(nameof(legacyOffset));

        var predicates = new List<string>
        {
            "account = $account",
            "container = $container",
            "name >= $prefix",
            "substr(name, 1, length($prefix)) = $prefix"
        };
        if (!includeVersions && !includeSnapshots)
            predicates.Add("is_current = 1");
        else if (!includeVersions)
            predicates.Add("(is_current = 1 OR snapshot IS NOT NULL)");
        else if (!includeSnapshots)
            predicates.Add("snapshot IS NULL");
        if (!includeDeleted)
            predicates.Add("is_deleted = 0");

        const string rankExpression = """
            CASE
                WHEN is_current = 1 THEN 0
                WHEN version_id IS NOT NULL THEN 1
                WHEN snapshot IS NULL THEN 2
                ELSE 3
            END
            """;
        var eligible = $"""
            SELECT data, name, version_id, snapshot, generation_id,
                   {rankExpression} AS rank
            FROM blobs
            WHERE {string.Join(" AND ", predicates)}
            """;
        var entries = string.IsNullOrEmpty(delimiter)
            ? $"""
                WITH entries AS (
                    SELECT data, name AS entry_name, 1 AS entry_type, rank,
                           version_id, snapshot, generation_id
                    FROM ({eligible})
                )
                """
            : $"""
                WITH eligible AS (
                    SELECT source.*,
                           instr(substr(name, length($prefix) + 1), $delimiter) AS delimiter_offset
                    FROM ({eligible}) AS source
                ),
                entries AS (
                    SELECT DISTINCT
                           NULL AS data,
                           substr(
                               name,
                               1,
                               length($prefix) + delimiter_offset + length($delimiter) - 1) AS entry_name,
                           0 AS entry_type,
                           -1 AS rank,
                           NULL AS version_id,
                           NULL AS snapshot,
                           '' AS generation_id
                    FROM eligible
                    WHERE delimiter_offset > 0
                    UNION ALL
                    SELECT data, name AS entry_name, 1 AS entry_type, rank,
                           version_id, snapshot, generation_id
                    FROM eligible
                    WHERE delimiter_offset = 0
                )
                """;

        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            {entries}
            SELECT data, entry_name, entry_type, rank, version_id, snapshot, generation_id
            FROM entries
            WHERE $has_cursor = 0
               OR ($name_complete = 1 AND entry_name > $cursor_name)
               OR ($name_complete = 0 AND (
                    entry_name > $cursor_name
                    OR (entry_name = $cursor_name AND (
                        entry_type > $cursor_type
                        OR (entry_type = $cursor_type AND entry_type = 1 AND (
                            rank > $cursor_rank
                            OR (rank = $cursor_rank AND (
                                ($cursor_rank = 1 AND (
                                    version_id < $ordered_id
                                    OR (version_id = $ordered_id AND generation_id > $generation_id)
                                ))
                                OR ($cursor_rank = 3 AND (
                                    snapshot > $ordered_id
                                    OR (snapshot = $ordered_id AND generation_id > $generation_id)
                                ))
                                OR ($cursor_rank NOT IN (1, 3) AND generation_id > $generation_id)
                            ))
                        ))
                    ))
               ))
            ORDER BY entry_name COLLATE BINARY,
                     entry_type,
                     rank,
                     CASE WHEN rank = 1 THEN version_id END DESC,
                     CASE WHEN rank = 3 THEN snapshot END,
                     generation_id
            LIMIT $limit OFFSET $offset;
            """;
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", container);
        command.Parameters.AddWithValue("$prefix", prefix);
        command.Parameters.AddWithValue("$delimiter", delimiter);
        command.Parameters.AddWithValue("$has_cursor", cursor is null ? 0 : 1);
        command.Parameters.AddWithValue("$name_complete", cursor?.NameComplete == true ? 1 : 0);
        command.Parameters.AddWithValue("$cursor_name", cursor?.Name ?? string.Empty);
        command.Parameters.AddWithValue("$cursor_type", cursor?.IsPrefix == true ? 0 : 1);
        command.Parameters.AddWithValue("$cursor_rank", cursor?.Rank ?? -1);
        command.Parameters.AddWithValue("$ordered_id", cursor?.OrderedId ?? string.Empty);
        command.Parameters.AddWithValue("$generation_id", cursor?.GenerationId ?? string.Empty);
        command.Parameters.AddWithValue("$limit", checked(maximum + 1));
        command.Parameters.AddWithValue("$offset", legacyOffset);

        var items = new List<BlobListEntry>(maximum + 1);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(reader.GetInt32(2) == 0
                ? new BlobListEntry(null, reader.GetString(1))
                : new BlobListEntry(Deserialize<BlobRecord>(reader.GetString(0)), null));
        }
        var hasMore = items.Count > maximum;
        if (hasMore)
            items.RemoveAt(items.Count - 1);
        return new BlobListPage(items, hasMore);
    }

    internal async Task<TaggedBlobPage> FindBlobsByTagsPageAsync(
        string account,
        BlobTagFilter filter,
        BlobTagCursor? cursor,
        int maximum,
        CancellationToken cancellationToken)
    {
        if (maximum <= 0)
            throw new ArgumentOutOfRangeException(nameof(maximum));
        if (filter.Predicates.Count == 0)
            throw new ArgumentException("At least one tag predicate is required.", nameof(filter));

        var predicates = new List<string>
        {
            "blob.account = $account",
            "blob.is_current = 1",
            "blob.is_deleted = 0",
            "blob.snapshot IS NULL"
        };
        if (filter.Container is not null)
            predicates.Add("blob.container = $container");
        for (var index = 0; index < filter.Predicates.Count; index++)
        {
            var comparison = filter.Predicates[index].Comparison switch
            {
                BlobTagComparison.Equal => "=",
                BlobTagComparison.GreaterThan => ">",
                BlobTagComparison.GreaterThanOrEqual => ">=",
                BlobTagComparison.LessThan => "<",
                BlobTagComparison.LessThanOrEqual => "<=",
                _ => throw new ArgumentOutOfRangeException(nameof(filter))
            };
            predicates.Add($"""
                EXISTS (
                    SELECT 1 FROM blob_tags AS tag{index}
                    WHERE tag{index}.generation_id = blob.generation_id
                      AND tag{index}.tag_key = $key{index}
                      AND tag{index}.tag_value {comparison} $value{index}
                )
                """);
        }

        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT blob.data
            FROM blobs AS blob
            WHERE {string.Join(" AND ", predicates)}
              AND ($has_cursor = 0
                   OR blob.container > $cursor_container
                   OR (blob.container = $cursor_container AND blob.name > $cursor_name)
                   OR (blob.container = $cursor_container AND blob.name = $cursor_name
                       AND blob.generation_id > $cursor_generation))
            ORDER BY blob.container COLLATE BINARY,
                     blob.name COLLATE BINARY,
                     blob.generation_id
            LIMIT $limit;
            """;
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", filter.Container ?? string.Empty);
        command.Parameters.AddWithValue("$has_cursor", cursor is null ? 0 : 1);
        command.Parameters.AddWithValue("$cursor_container", cursor?.Container ?? string.Empty);
        command.Parameters.AddWithValue("$cursor_name", cursor?.Name ?? string.Empty);
        command.Parameters.AddWithValue("$cursor_generation", cursor?.GenerationId ?? string.Empty);
        command.Parameters.AddWithValue("$limit", checked(maximum + 1));
        for (var index = 0; index < filter.Predicates.Count; index++)
        {
            command.Parameters.AddWithValue($"$key{index}", filter.Predicates[index].Key);
            command.Parameters.AddWithValue($"$value{index}", filter.Predicates[index].Value);
        }

        var records = (await ReadJsonRowsAsync<BlobRecord>(command, cancellationToken)).ToList();
        var hasMore = records.Count > maximum;
        if (hasMore)
            records.RemoveAt(records.Count - 1);
        return new TaggedBlobPage(records, hasMore);
    }

    internal async Task<KeysetPage<BlobRecord>> ListBlobMaintenancePageAsync(
        string? afterGenerationId,
        int maximum,
        CancellationToken cancellationToken)
    {
        if (maximum <= 0)
            throw new ArgumentOutOfRangeException(nameof(maximum));
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT data FROM blobs
            WHERE generation_id > $after
            ORDER BY generation_id
            LIMIT $limit;
            """;
        command.Parameters.AddWithValue("$after", afterGenerationId ?? string.Empty);
        command.Parameters.AddWithValue("$limit", checked(maximum + 1));
        var records = (await ReadJsonRowsAsync<BlobRecord>(command, cancellationToken)).ToList();
        var hasMore = records.Count > maximum;
        if (hasMore)
            records.RemoveAt(records.Count - 1);
        return new KeysetPage<BlobRecord>(records, hasMore);
    }

    internal async Task<KeysetPage<ContainerRecord>> ListContainerMaintenancePageAsync(
        ContainerKey? after,
        int maximum,
        CancellationToken cancellationToken)
    {
        if (maximum <= 0)
            throw new ArgumentOutOfRangeException(nameof(maximum));
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT data FROM containers
            WHERE $has_after = 0
               OR account > $account
               OR (account = $account AND name > $name)
            ORDER BY account, name
            LIMIT $limit;
            """;
        command.Parameters.AddWithValue("$has_after", after.HasValue ? 1 : 0);
        command.Parameters.AddWithValue("$account", after?.Account ?? string.Empty);
        command.Parameters.AddWithValue("$name", after?.Name ?? string.Empty);
        command.Parameters.AddWithValue("$limit", checked(maximum + 1));
        var records = (await ReadJsonRowsAsync<ContainerRecord>(command, cancellationToken)).ToList();
        var hasMore = records.Count > maximum;
        if (hasMore)
            records.RemoveAt(records.Count - 1);
        return new KeysetPage<ContainerRecord>(records, hasMore);
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

    internal async Task ApplyBlobRecordMutationsAsync(
        IReadOnlyList<BlobRecordMutation> mutations,
        CancellationToken cancellationToken)
    {
        if (mutations.Count == 0)
            return;
        if (mutations.Select(item => item.GenerationId).Distinct(StringComparer.Ordinal).Count() != mutations.Count ||
            mutations.Any(item => item.Replacement is not null &&
                                  !string.Equals(
                                      item.GenerationId,
                                      item.Replacement.GenerationId,
                                      StringComparison.Ordinal)))
        {
            throw new ArgumentException("Blob record mutations must target unique, stable generation identities.", nameof(mutations));
        }

        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            foreach (var mutation in mutations)
            {
                var current = await GetBlobByGenerationAsync(
                    connection,
                    transaction,
                    mutation.GenerationId,
                    cancellationToken);
                if (current is null ||
                    !string.Equals(current.Revision, mutation.ExpectedRevision, StringComparison.Ordinal))
                {
                    throw new StorageConcurrencyException();
                }
            }

            foreach (var mutation in mutations)
            {
                if (mutation.Replacement is null)
                {
                    await DeleteBlobRowAsync(
                        connection,
                        transaction,
                        mutation.GenerationId,
                        cancellationToken);
                }
                else
                {
                    await UpdateBlobRowAsync(
                        connection,
                        transaction,
                        mutation.Replacement,
                        cancellationToken);
                }
            }
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
        int maximum,
        CancellationToken cancellationToken)
    {
        if (maximum <= 0)
            throw new ArgumentOutOfRangeException(nameof(maximum));
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.CommandText = """
                DELETE FROM staged_blocks
                WHERE rowid IN (
                    SELECT rowid FROM staged_blocks
                    WHERE created_ticks < $cutoff
                    ORDER BY created_ticks, account, container, blob_name, block_id
                    LIMIT $limit
                );
                """;
            command.Parameters.AddWithValue("$cutoff", cutoff.UtcTicks);
            command.Parameters.AddWithValue("$limit", maximum);
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

    internal static async Task NormalizePackedLocationsForStandaloneBackupAsync(
        string databasePath,
        CancellationToken cancellationToken)
    {
        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadWrite,
            Cache = SqliteCacheMode.Private,
            Pooling = false
        }.ToString();
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using (var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken))
        {
            await ExecuteNonQueryAsync(connection, transaction, "DELETE FROM packed_chunks;", cancellationToken);
            await ExecuteNonQueryAsync(connection, transaction, "DELETE FROM chunk_packs;", cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        await ExecuteNonQueryAsync(connection, "VACUUM;", cancellationToken);
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
        var schemaVersion = await ReadSchemaVersionAsync(connection, cancellationToken);
        if (schemaVersion < ChunkIndexSchemaVersion)
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
        if (schemaVersion >= 3)
            await VerifyBlobTagIndexAsync(connection, cancellationToken);
        return authoritative;
    }

    private static async Task VerifyBlobTagIndexAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        var expected = new Dictionary<(string GenerationId, string Key), string>();
        await using (var blobs = connection.CreateCommand())
        {
            blobs.CommandText = "SELECT data FROM blobs;";
            await using var reader = await blobs.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var blob = Deserialize<BlobRecord>(reader.GetString(0));
                foreach (var (key, value) in blob.Tags)
                    expected.Add((blob.GenerationId, key), value);
            }
        }

        var actual = new Dictionary<(string GenerationId, string Key), string>();
        await using (var tags = connection.CreateCommand())
        {
            tags.CommandText = "SELECT generation_id, tag_key, tag_value FROM blob_tags;";
            await using var reader = await tags.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                actual.Add((reader.GetString(0), reader.GetString(1)), reader.GetString(2));
        }

        if (expected.Count != actual.Count ||
            expected.Any(pair => !actual.TryGetValue(pair.Key, out var value) ||
                                 !string.Equals(pair.Value, value, StringComparison.Ordinal)))
        {
            throw new InvalidDataException("The metadata blob-tag index does not match the authoritative blob records.");
        }
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
            $"PRAGMA user_version={ChunkIndexSchemaVersion};",
            cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    private static async Task MigrateVersion2ToVersion3Async(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<BlobRecord> blobs;
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT data FROM blobs;";
            blobs = await ReadJsonRowsAsync<BlobRecord>(command, cancellationToken);
        }

        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
        await ExecuteNonQueryAsync(
            connection,
            transaction,
            "DELETE FROM blob_tags;",
            cancellationToken);
        foreach (var blob in blobs)
            await ReplaceBlobTagsAsync(connection, transaction, blob, cancellationToken);
        await ExecuteNonQueryAsync(
            connection,
            transaction,
            $"PRAGMA user_version={TagIndexSchemaVersion};",
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
        await ReplaceBlobTagsAsync(connection, transaction, record, cancellationToken);
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
        await ReplaceBlobTagsAsync(connection, transaction, record, cancellationToken);
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

    private static async Task ReplaceBlobTagsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord record,
        CancellationToken cancellationToken)
    {
        await using (var clear = connection.CreateCommand())
        {
            clear.Transaction = transaction;
            clear.CommandText = "DELETE FROM blob_tags WHERE generation_id = $generation;";
            clear.Parameters.AddWithValue("$generation", record.GenerationId);
            await clear.ExecuteNonQueryAsync(cancellationToken);
        }

        foreach (var (key, value) in record.Tags)
        {
            await using var insert = connection.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText = """
                INSERT INTO blob_tags(generation_id, tag_key, tag_value)
                VALUES ($generation, $key, $value);
                """;
            insert.Parameters.AddWithValue("$generation", record.GenerationId);
            insert.Parameters.AddWithValue("$key", key);
            insert.Parameters.AddWithValue("$value", value);
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static async Task<IReadOnlyList<PackedChunkLocation>> ListPackedChunkLocationsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string packId,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT chunk_id, pack_id, record_offset, record_length, payload_offset, payload_length
            FROM packed_chunks
            WHERE pack_id = $pack
            ORDER BY record_offset, chunk_id;
            """;
        command.Parameters.AddWithValue("$pack", packId);
        var locations = new List<PackedChunkLocation>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            locations.Add(ReadPackedChunkLocation(reader));
        return locations;
    }

    private static bool EquivalentPackedLocations(
        IReadOnlyList<PackedChunkLocation> left,
        IReadOnlyList<PackedChunkLocation> right) =>
        left.Count == right.Count && left.Zip(right).All(pair => pair.First == pair.Second);

    private static ChunkPackRecord ReadChunkPack(SqliteDataReader reader) => new(
        reader.GetString(0),
        reader.GetString(1),
        new DateTimeOffset(reader.GetInt64(2), TimeSpan.Zero),
        reader.GetInt64(3) != 0);

    private static PackedChunkLocation ReadPackedChunkLocation(SqliteDataReader reader) => new(
        reader.GetString(0),
        reader.GetString(1),
        reader.GetInt64(2),
        reader.GetInt32(3),
        reader.GetInt64(4),
        reader.GetInt32(5));

    private static void AddPackedChunkLocationParameters(
        SqliteCommand command,
        PackedChunkLocation location)
    {
        command.Parameters.AddWithValue("$chunk", location.ChunkId);
        command.Parameters.AddWithValue("$pack", location.PackId);
        command.Parameters.AddWithValue("$record_offset", location.RecordOffset);
        command.Parameters.AddWithValue("$record_length", location.RecordLength);
        command.Parameters.AddWithValue("$payload_offset", location.PayloadOffset);
        command.Parameters.AddWithValue("$payload_length", location.PayloadLength);
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
