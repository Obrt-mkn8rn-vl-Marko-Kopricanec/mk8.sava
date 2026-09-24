using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Data.Sqlite;

namespace Mk8.Sava.Storage;

public sealed partial class MetadataStore(
    IStoragePaths paths,
    IStorageFaultInjector faultInjector,
    TimeProvider? timeProvider = null) : IDisposable
{
    public const int CurrentSchemaVersion = 7;
    private const int RetainedWalLimitBytes = 1024 * 1024;
    private const int ChunkIndexSchemaVersion = 2;
    private const int TagIndexSchemaVersion = 3;
    private const int PackIndexSchemaVersion = 4;
    private const int ObjectReplicationSchemaVersion = 5;
    private const int DataKeyContinuitySchemaVersion = 6;

    private const string InitialSchemaSql = """
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

                CREATE TABLE IF NOT EXISTS data_encryption_keys (
                    key_id TEXT PRIMARY KEY,
                    fingerprint TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS account_namespace_modes (
                    account TEXT PRIMARY KEY,
                    hierarchical_namespace_enabled INTEGER NOT NULL CHECK (hierarchical_namespace_enabled IN (0, 1))
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

                CREATE TABLE IF NOT EXISTS object_replication_states (
                    policy_id TEXT NOT NULL,
                    rule_id TEXT NOT NULL,
                    source_generation_id TEXT NOT NULL,
                    source_account TEXT NOT NULL,
                    source_container TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    destination_account TEXT NOT NULL,
                    destination_container TEXT NOT NULL,
                    destination_generation_id TEXT NULL,
                    source_fingerprint TEXT NOT NULL,
                    status TEXT NOT NULL,
                    updated_ticks INTEGER NOT NULL,
                    PRIMARY KEY (policy_id, rule_id, source_generation_id)
                );
                CREATE INDEX IF NOT EXISTS ix_object_replication_source
                    ON object_replication_states(source_account, source_container, source_name);
                CREATE INDEX IF NOT EXISTS ix_object_replication_destination
                    ON object_replication_states(destination_generation_id)
                    WHERE destination_generation_id IS NOT NULL;
                """;

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

    public void Dispose()
    {
        _writeGate.Dispose();
        GC.SuppressFinalize(this);
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            await ExecuteNonQueryAsync(connection, "PRAGMA journal_mode=WAL;", cancellationToken).ConfigureAwait(false);
            await ExecuteNonQueryAsync(connection, "PRAGMA synchronous=FULL;", cancellationToken).ConfigureAwait(false);
            var schemaVersion = await ReadSchemaVersionAsync(connection, cancellationToken).ConfigureAwait(false);
            if (schemaVersion > CurrentSchemaVersion)
            {
                throw new InvalidDataException(
                    $"The metadata schema version {schemaVersion} is newer than the supported version {CurrentSchemaVersion}.");
            }
            await ExecuteNonQueryAsync(connection, InitialSchemaSql, cancellationToken).ConfigureAwait(false);
            if (schemaVersion == 1)
            {
                await MigrateVersion1ToVersion2Async(connection, cancellationToken).ConfigureAwait(false);
                schemaVersion = ChunkIndexSchemaVersion;
            }
            if (schemaVersion == ChunkIndexSchemaVersion)
            {
                await MigrateVersion2ToVersion3Async(connection, cancellationToken).ConfigureAwait(false);
                schemaVersion = TagIndexSchemaVersion;
            }
            if (schemaVersion is TagIndexSchemaVersion or PackIndexSchemaVersion or
                ObjectReplicationSchemaVersion or DataKeyContinuitySchemaVersion)
                await ExecuteNonQueryAsync(connection, $"PRAGMA user_version={CurrentSchemaVersion};", cancellationToken).ConfigureAwait(false);
            else if (schemaVersion == 0)
                await ExecuteNonQueryAsync(connection, $"PRAGMA user_version={CurrentSchemaVersion};", cancellationToken).ConfigureAwait(false);
            await VerifyForeignKeysAsync(connection, cancellationToken).ConfigureAwait(false);
            _ = await ReadVerifiedStorageInventoryAsync(connection, cancellationToken).ConfigureAwait(false);
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
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var command = connection.CreateCommand();
            await using var commandDisposal = command.ConfigureAwait(false);
            command.CommandText = "SELECT 1;";
            return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture) == 1;
        }
        catch (SqliteException)
        {
            return false;
        }
    }

    public async Task EnsureDataEncryptionKeyFingerprintsAsync(
        IReadOnlyDictionary<string, string> expected,
        Func<string, CancellationToken, Task> verifyUnrecordedKey,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(expected);
        ArgumentNullException.ThrowIfNull(verifyUnrecordedKey);
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var recorded = new Dictionary<string, string>(StringComparer.Ordinal);
            var read = connection.CreateCommand();
            await using (read.ConfigureAwait(false))
            {
                read.Transaction = transaction;
                read.CommandText = "SELECT key_id, fingerprint FROM data_encryption_keys;";
                var reader = (await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
                await using var readerDisposal = reader.ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    recorded.Add(reader.GetString(0), reader.GetString(1));
            }

            foreach (var (keyId, fingerprint) in expected)
            {
                if (recorded.TryGetValue(keyId, out var previous))
                {
                    if (!string.Equals(previous, fingerprint, StringComparison.Ordinal))
                    {
                        throw new InvalidDataException(
                            $"The configured data encryption key for '{keyId}' differs from the key recorded for stored content.");
                    }
                    continue;
                }

                await verifyUnrecordedKey(keyId, cancellationToken).ConfigureAwait(false);
                var insert = connection.CreateCommand();
                await using var insertDisposal = insert.ConfigureAwait(false);
                insert.Transaction = transaction;
                insert.CommandText = "INSERT INTO data_encryption_keys(key_id, fingerprint) VALUES ($key, $fingerprint);";
                insert.Parameters.AddWithValue("$key", keyId);
                insert.Parameters.AddWithValue("$fingerprint", fingerprint);
                await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            foreach (var keyId in recorded.Keys.Except(expected.Keys, StringComparer.Ordinal))
            {
                var delete = connection.CreateCommand();
                await using var deleteDisposal = delete.ConfigureAwait(false);
                delete.Transaction = transaction;
                delete.CommandText = "DELETE FROM data_encryption_keys WHERE key_id = $key;";
                delete.Parameters.AddWithValue("$key", keyId);
                await delete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task EnsureAccountNamespaceModesAsync(
        IReadOnlyDictionary<string, bool> expected,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(expected);
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var recorded = new Dictionary<string, bool>(StringComparer.Ordinal);
            var read = connection.CreateCommand();
            await using (read.ConfigureAwait(false))
            {
                read.Transaction = transaction;
                read.CommandText = "SELECT account, hierarchical_namespace_enabled FROM account_namespace_modes;";
                var reader = (await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
                await using var readerDisposal = reader.ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    recorded.Add(reader.GetString(0), reader.GetInt32(1) == 1);
            }

            foreach (var (account, enabled) in expected)
            {
                if (recorded.TryGetValue(account, out var previous))
                {
                    if (previous != enabled)
                    {
                        throw new InvalidDataException(
                            $"The configured hierarchical namespace mode for account '{account}' differs from its recorded mode. " +
                            "Changing account namespace mode requires an explicit offline migration.");
                    }
                    continue;
                }

                var insert = connection.CreateCommand();
                await using var insertDisposal = insert.ConfigureAwait(false);
                insert.Transaction = transaction;
                insert.CommandText = """
                    INSERT INTO account_namespace_modes(account, hierarchical_namespace_enabled)
                    VALUES ($account, $enabled);
                    """;
                insert.Parameters.AddWithValue("$account", account);
                insert.Parameters.AddWithValue("$enabled", enabled ? 1 : 0);
                await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task<IReadOnlyList<ContainerRecord>> ListContainersAsync(
        string account,
        bool includeDeleted,
        CancellationToken cancellationToken)
    {
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        if (includeDeleted)
            command.CommandText = "SELECT data FROM containers WHERE account = $account ORDER BY name;";
        else
            command.CommandText = "SELECT data FROM containers WHERE account = $account AND deleted = 0 ORDER BY name;";
        command.Parameters.AddWithValue("$account", account);
        return await ReadJsonRowsAsync<ContainerRecord>(command, cancellationToken).ConfigureAwait(false);
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
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximum);
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.CommandText = $"""
            SELECT data FROM containers
            WHERE account = $account
              {(includeDeleted ? string.Empty : "AND deleted = 0")}
              {(includeSystem ? string.Empty : "AND name <> '$logs'")}
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
        var records = (await ReadJsonRowsAsync<ContainerRecord>(command, cancellationToken).ConfigureAwait(false)).ToList();
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
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        return await GetContainerAsync(connection, transaction: null, account, name, includeDeleted, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<ContainerRecord?> GetContainerAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string account,
        string name,
        bool includeDeleted,
        CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Transaction = transaction;
        if (includeDeleted)
            command.CommandText = "SELECT data FROM containers WHERE account = $account AND name = $name;";
        else
            command.CommandText = "SELECT data FROM containers WHERE account = $account AND name = $name AND deleted = 0;";
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$name", name);
        return await ReadSingleJsonAsync<ContainerRecord>(command, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> TryCreateContainerAsync(ContainerRecord container, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(container);
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var command = connection.CreateCommand();
            await using var commandDisposal = command.ConfigureAwait(false);
            command.CommandText = """
                INSERT OR IGNORE INTO containers(account, name, deleted, modified_ticks, data)
                VALUES ($account, $name, $deleted, $modified, $data);
                """;
            AddContainerParameters(command, container);
            return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
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
        ArgumentNullException.ThrowIfNull(container);
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var current = await GetContainerAsync(connection, transaction, container.Account, container.Name, includeDeleted: true, cancellationToken).ConfigureAwait(false);
            if (current is null || !string.Equals(current.Revision, expectedRevision, StringComparison.Ordinal))
                throw new StorageConcurrencyException();
            var command = connection.CreateCommand();
            await using var commandDisposal = command.ConfigureAwait(false);
            command.Transaction = transaction;
            command.CommandText = """
                UPDATE containers SET deleted = $deleted, modified_ticks = $modified, data = $data
                WHERE account = $account AND name = $name;
                """;
            AddContainerParameters(command, container);
            if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
                throw new StorageConcurrencyException();
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    internal async Task ApplyHierarchicalAclEntriesAsync(
        IReadOnlyList<HierarchicalAclManifestEntry> entries,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            foreach (var entry in entries)
                await ApplyHierarchicalAclEntryAsync(connection, transaction, entry, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    private async Task ApplyHierarchicalAclEntryAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        HierarchicalAclManifestEntry entry,
        CancellationToken cancellationToken)
    {
        if (entry.Path.Length == 0)
        {
            var container = await GetContainerAsync(
                connection, transaction, entry.Account, entry.Container,
                includeDeleted: false, cancellationToken).ConfigureAwait(false);
            if (container is null)
                throw new InvalidDataException($"The HNS ACL target container '{entry.Account}/{entry.Container}' does not exist.");
            PosixAccessControl.ValidateStoredAcl(entry.AccessAcl, isDirectory: true);
            var stickyBit = entry.StickyBit ?? container.StickyBit;
            if (string.Equals(container.AccessAcl, entry.AccessAcl, StringComparison.Ordinal) &&
                container.StickyBit == stickyBit)
                return;

            var update = connection.CreateCommand();
            await using var updateDisposal = update.ConfigureAwait(false);
            update.Transaction = transaction;
            update.CommandText = """
                UPDATE containers SET modified_ticks = $modified, data = $data
                WHERE account = $account AND name = $name AND deleted = 0;
                """;
            AddContainerParameters(update, container with
            {
                AccessAcl = entry.AccessAcl,
                StickyBit = stickyBit,
                Revision = NewRevision(),
                ETag = NewETag(),
                LastModified = _timeProvider.GetUtcNow()
            });
            if (await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
                throw new StorageConcurrencyException();
            return;
        }

        var blob = await GetCurrentBlobAsync(
            connection, transaction, entry.Account, entry.Container, entry.Path, cancellationToken).ConfigureAwait(false);
        if (blob is null || blob.IsDeleted)
            throw new InvalidDataException($"The HNS ACL target path '{entry.Account}/{entry.Container}/{entry.Path}' does not exist.");
        PosixAccessControl.ValidateStoredAcl(entry.AccessAcl, blob.IsDirectory);
        if (entry.StickyBit.HasValue && !blob.IsDirectory)
            throw new InvalidDataException("The HNS ACL manifest cannot set a sticky bit on a file.");
        var blobStickyBit = entry.StickyBit ?? blob.StickyBit;
        if (string.Equals(blob.AccessAcl, entry.AccessAcl, StringComparison.Ordinal) &&
            blob.StickyBit == blobStickyBit)
            return;
        await UpdateBlobRowAsync(connection, transaction, blob with
        {
            AccessAcl = entry.AccessAcl,
            StickyBit = blobStickyBit,
            Revision = NewRevision(),
            ETag = NewETag(),
            LastModified = _timeProvider.GetUtcNow()
        }, cancellationToken).ConfigureAwait(false);
    }

    public Task<bool> TryRestoreContainerAsync(
        string sourceName,
        ContainerRecord restored,
        string expectedSourceRevision,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(restored);
        return TryRelocateContainerAsync(
            sourceName,
            restored,
            expectedSourceRevision,
            allowSameName: true,
            cancellationToken);
    }

    public Task<bool> TryRenameContainerAsync(
        string sourceName,
        ContainerRecord renamed,
        string expectedSourceRevision,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(renamed);
        return TryRelocateContainerAsync(
            sourceName,
            renamed,
            expectedSourceRevision,
            allowSameName: false,
            cancellationToken);
    }

    private async Task<bool> TryRelocateContainerAsync(
        string sourceName,
        ContainerRecord destination,
        string expectedSourceRevision,
        bool allowSameName,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var source = await GetContainerAsync(
                connection,
                transaction,
                destination.Account,
                sourceName,
                includeDeleted: true,
                cancellationToken).ConfigureAwait(false);
            if (source is null || !string.Equals(source.Revision, expectedSourceRevision, StringComparison.Ordinal))
                throw new StorageConcurrencyException();

            if (string.Equals(sourceName, destination.Name, StringComparison.Ordinal))
            {
                if (!allowSameName)
                    return false;
                var update = connection.CreateCommand();
                await using var updateDisposal = update.ConfigureAwait(false);
                update.Transaction = transaction;
                update.CommandText = """
                    UPDATE containers SET deleted = $deleted, modified_ticks = $modified, data = $data
                    WHERE account = $account AND name = $name;
                    """;
                AddContainerParameters(update, destination);
                if (await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
                    throw new StorageConcurrencyException();
            }
            else
            {
                if (!await RelocateContainerNameAsync(
                    connection, transaction, sourceName, destination, cancellationToken).ConfigureAwait(false))
                    return false;
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return true;
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
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var current = await GetContainerAsync(connection, transaction, account, name, includeDeleted: true, cancellationToken).ConfigureAwait(false);
            if (current is null || !string.Equals(current.Revision, expectedRevision, StringComparison.Ordinal))
                throw new StorageConcurrencyException();
            var blobs = connection.CreateCommand();
            await using var blobsDisposal = blobs.ConfigureAwait(false);
            blobs.Transaction = transaction;
            blobs.CommandText = "DELETE FROM blobs WHERE account = $account AND container = $container;";
            blobs.Parameters.AddWithValue("$account", account);
            blobs.Parameters.AddWithValue("$container", name);
            await blobs.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            var blocks = connection.CreateCommand();
            await using var blocksDisposal = blocks.ConfigureAwait(false);
            blocks.Transaction = transaction;
            blocks.CommandText = "DELETE FROM staged_blocks WHERE account = $account AND container = $container;";
            blocks.Parameters.AddWithValue("$account", account);
            blocks.Parameters.AddWithValue("$container", name);
            await blocks.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            var container = connection.CreateCommand();
            await using var containerDisposal = container.ConfigureAwait(false);
            container.Transaction = transaction;
            container.CommandText = "DELETE FROM containers WHERE account = $account AND name = $name;";
            container.Parameters.AddWithValue("$account", account);
            container.Parameters.AddWithValue("$name", name);
            var deleted = await container.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
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
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
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
        return await ReadSingleJsonAsync<BlobRecord>(command, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<string?> GetLastBlobNameAsync(
        string account,
        string container,
        string prefix,
        CancellationToken cancellationToken)
    {
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.CommandText = """
            SELECT name FROM blobs
            WHERE account = $account
              AND container = $container
              AND substr(name, 1, length($prefix)) = $prefix
            ORDER BY name DESC
            LIMIT 1;
            """;
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", container);
        command.Parameters.AddWithValue("$prefix", prefix);
        return (string?)await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
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

        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        // Every predicate is a fixed SQL fragment; account and container are bound parameters.
#pragma warning disable CA2100
        command.CommandText = $"SELECT data FROM blobs WHERE {string.Join(" AND ", predicates)} ORDER BY name, modified_ticks DESC;";
#pragma warning restore CA2100
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", container);
        return await ReadJsonRowsAsync<BlobRecord>(command, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<IReadOnlyList<BlobRecord>> ListBlobFamilyAsync(
        string account,
        string container,
        string name,
        bool includeDeleted,
        CancellationToken cancellationToken)
    {
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.CommandText = $"""
            SELECT data FROM blobs
            WHERE account = $account AND container = $container AND name = $name
                  {(includeDeleted ? string.Empty : "AND is_deleted = 0")}
            ORDER BY is_current DESC, modified_ticks DESC, generation_id;
            """;
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", container);
        command.Parameters.AddWithValue("$name", name);
        return await ReadJsonRowsAsync<BlobRecord>(command, cancellationToken).ConfigureAwait(false);
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

    internal async Task<int> CountPackedChunksAsync(CancellationToken cancellationToken)
    {
        var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.CommandText = "SELECT COUNT(*) FROM packed_chunks;";
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture);
    }

    internal async Task<PackedChunkLocation?> GetPackedChunkLocationAsync(
        string chunkId,
        CancellationToken cancellationToken)
    {
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.CommandText = """
            SELECT chunk_id, pack_id, record_offset, record_length, payload_offset, payload_length
            FROM packed_chunks
            WHERE chunk_id = $chunk;
            """;
        command.Parameters.AddWithValue("$chunk", chunkId);
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? ReadPackedChunkLocation(reader) : null;
    }

    internal async Task<ChunkPackRecord?> GetActiveChunkPackAsync(
        string domain,
        CancellationToken cancellationToken)
    {
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.CommandText = """
            SELECT pack_id, domain, created_ticks, sealed
            FROM chunk_packs
            WHERE domain = $domain AND sealed = 0
            ORDER BY created_ticks DESC, pack_id DESC
            LIMIT 1;
            """;
        command.Parameters.AddWithValue("$domain", domain);
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? ReadChunkPack(reader) : null;
    }

    internal async Task<int> CountPackedChunksAsync(
        string packId,
        CancellationToken cancellationToken)
    {
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.CommandText = "SELECT COUNT(*) FROM packed_chunks WHERE pack_id = $pack;";
        command.Parameters.AddWithValue("$pack", packId);
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture);
    }

    internal async Task<long> GetPackIndexedLengthAsync(
        string packId,
        CancellationToken cancellationToken)
    {
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.CommandText = "SELECT COALESCE(MAX(record_offset + record_length), 0) FROM packed_chunks WHERE pack_id = $pack;";
        command.Parameters.AddWithValue("$pack", packId);
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture);
    }

    internal async Task<bool> TryRegisterPackedChunkAsync(
        ChunkPackRecord pack,
        PackedChunkLocation location,
        CancellationToken cancellationToken)
    {
        if (!string.Equals(pack.PackId, location.PackId, StringComparison.Ordinal))
            throw new ArgumentException("The packed chunk location does not belong to the supplied pack.", nameof(location));

        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var addPack = connection.CreateCommand();
            await using (addPack.ConfigureAwait(false))
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
                await addPack.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            var addLocation = connection.CreateCommand();
            await using var addLocationDisposal = addLocation.ConfigureAwait(false);
            addLocation.Transaction = transaction;
            addLocation.CommandText = """
                INSERT INTO packed_chunks(
                    chunk_id, pack_id, record_offset, record_length, payload_offset, payload_length)
                VALUES ($chunk, $pack, $record_offset, $record_length, $payload_offset, $payload_length)
                ON CONFLICT(chunk_id) DO NOTHING;
                """;
            AddPackedChunkLocationParameters(addLocation, location);
            var inserted = await addLocation.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return inserted;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    internal async Task SealChunkPackAsync(string packId, CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var command = connection.CreateCommand();
            await using var commandDisposal = command.ConfigureAwait(false);
            command.CommandText = "UPDATE chunk_packs SET sealed = 1 WHERE pack_id = $pack;";
            command.Parameters.AddWithValue("$pack", packId);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
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
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximum);
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var command = connection.CreateCommand();
            await using var commandDisposal = command.ConfigureAwait(false);
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
            return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
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
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var command = connection.CreateCommand();
            await using var commandDisposal = command.ConfigureAwait(false);
            command.CommandText = "DELETE FROM packed_chunks WHERE chunk_id = $chunk;";
            command.Parameters.AddWithValue("$chunk", chunkId);
            return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
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
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximum);
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
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
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximum);
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
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
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.CommandText = """
            SELECT chunk_id, pack_id, record_offset, record_length, payload_offset, payload_length
            FROM packed_chunks
            WHERE pack_id = $pack
            ORDER BY record_offset, chunk_id;
            """;
        command.Parameters.AddWithValue("$pack", packId);
        var locations = new List<PackedChunkLocation>();
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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

        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var actual = await ListPackedChunkLocationsAsync(connection, transaction, oldPack.PackId, cancellationToken).ConfigureAwait(false);
            if (!EquivalentPackedLocations(actual, oldLocations))
                throw new StorageConcurrencyException();

            if (replacementPack is not null)
                await ReplacePackedChunkLocationsAsync(
                    connection, transaction, oldPack.PackId, replacementPack,
                    replacementLocations, cancellationToken).ConfigureAwait(false);

            var removeOld = connection.CreateCommand();
            await using var removeOldDisposal = removeOld.ConfigureAwait(false);
            removeOld.Transaction = transaction;
            removeOld.CommandText = "DELETE FROM chunk_packs WHERE pack_id = $pack;";
            removeOld.Parameters.AddWithValue("$pack", oldPack.PackId);
            if (await removeOld.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
                throw new StorageConcurrencyException();
            faultInjector.Inject(StorageFaultPoint.BeforePackMetadataCommit);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            faultInjector.Inject(StorageFaultPoint.AfterPackMetadataCommit);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    private static async Task ReplacePackedChunkLocationsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string oldPackId,
        ChunkPackRecord replacementPack,
        IReadOnlyList<PackedChunkLocation> replacementLocations,
        CancellationToken cancellationToken)
    {
        var addPack = connection.CreateCommand();
        await using var addPackDisposal = addPack.ConfigureAwait(false);
        addPack.Transaction = transaction;
        addPack.CommandText = """
            INSERT INTO chunk_packs(pack_id, domain, created_ticks, sealed)
            VALUES ($pack, $domain, $created, 1);
            """;
        addPack.Parameters.AddWithValue("$pack", replacementPack.PackId);
        addPack.Parameters.AddWithValue("$domain", replacementPack.Domain);
        addPack.Parameters.AddWithValue("$created", replacementPack.CreatedAt.UtcTicks);
        await addPack.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        foreach (var location in replacementLocations)
        {
            var update = connection.CreateCommand();
            await using var updateDisposal = update.ConfigureAwait(false);
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
            update.Parameters.AddWithValue("$old_pack", oldPackId);
            if (await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
                throw new StorageConcurrencyException();
        }
    }

    internal async Task<BlobListPage> ListBlobsPageAsync(
        string account,
        string container,
        bool hierarchicalNamespace,
        BlobListShowOnly showOnly,
        bool includeVersions,
        bool includeSnapshots,
        bool includeDeleted,
        bool includeUncommitted,
        string prefix,
        string startFrom,
        string endBefore,
        string delimiter,
        BlobListCursor? cursor,
        int legacyOffset,
        int maximum,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximum);
        ArgumentOutOfRangeException.ThrowIfNegative(legacyOffset);

        var hierarchicalRecursive = hierarchicalNamespace && string.IsNullOrEmpty(delimiter);
        var predicates = BuildBlobListingPredicates(
            hierarchicalNamespace, hierarchicalRecursive, showOnly, includeVersions, includeSnapshots,
            includeDeleted, endBefore);

        var eligible = BuildEligibleBlobListingSql(predicates, endBefore, hierarchicalRecursive);
        var entries = BuildBlobListingEntriesSql(eligible, delimiter, hierarchicalNamespace);

        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        // The query is assembled only from fixed SQL fragments; all request values are bound below.
#pragma warning disable CA2100
        command.CommandText = BuildBlobListingPageSql(entries);
#pragma warning restore CA2100
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", container);
        command.Parameters.AddWithValue("$prefix", prefix);
        command.Parameters.AddWithValue("$start_from", startFrom);
        command.Parameters.AddWithValue("$end_before", endBefore);
        command.Parameters.AddWithValue("$delimiter", delimiter);
        command.Parameters.AddWithValue("$hns_recursive", hierarchicalRecursive ? 1 : 0);
        command.Parameters.AddWithValue(
            "$include_uncommitted",
            includeUncommitted && showOnly is not (BlobListShowOnly.Deleted or BlobListShowOnly.Directories) ? 1 : 0);
        command.Parameters.AddWithValue(
            "$include_directory_properties",
            hierarchicalNamespace && showOnly != BlobListShowOnly.Deleted ? 1 : 0);
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
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            items.Add(await ReadBlobListEntryAsync(reader, cancellationToken).ConfigureAwait(false));
        var hasMore = items.Count > maximum;
        if (hasMore)
            items.RemoveAt(items.Count - 1);
        return new BlobListPage(items, hasMore);
    }

    private static async Task<BlobListEntry> ReadBlobListEntryAsync(
        SqliteDataReader reader,
        CancellationToken cancellationToken)
    {
        var name = reader.GetString(1);
        if (reader.GetInt32(2) == 0)
        {
            var blob = await reader.IsDBNullAsync(0, cancellationToken).ConfigureAwait(false)
                ? null : Deserialize<BlobRecord>(reader.GetString(0));
            return new BlobListEntry(blob, name);
        }
        return reader.GetInt32(7) == 1
            ? new BlobListEntry(null, null, name)
            : new BlobListEntry(Deserialize<BlobRecord>(reader.GetString(0)), null);
    }

    private static List<string> BuildBlobListingPredicates(
        bool hierarchicalNamespace,
        bool hierarchicalRecursive,
        BlobListShowOnly showOnly,
        bool includeVersions,
        bool includeSnapshots,
        bool includeDeleted,
        string endBefore)
    {
        var predicates = new List<string>
        {
            "account = $account",
            "container = $container",
            "name >= $prefix",
            $"{BlobListingOrderExpression("name", hierarchicalRecursive)} >= " +
            BlobListingOrderExpression("$start_from", hierarchicalRecursive),
            "substr(name, 1, length($prefix)) = $prefix"
        };
        if (hierarchicalNamespace)
        {
            var activePredicate = includeSnapshots
                ? "((is_current = 1 OR snapshot IS NOT NULL) AND is_deleted = 0)"
                : "is_current = 1 AND is_deleted = 0";
            var deletedPredicate = includeSnapshots
                ? "is_deleted = 1"
                : "is_deleted = 1 AND snapshot IS NULL";
            predicates.Add(showOnly == BlobListShowOnly.Deleted
                ? deletedPredicate
                : includeDeleted
                    ? $"({activePredicate} OR ({deletedPredicate}))"
                    : activePredicate);
            if (showOnly == BlobListShowOnly.Files)
                predicates.Add("COALESCE(json_extract(data, '$.isDirectory'), 0) = 0");
            else if (showOnly == BlobListShowOnly.Directories)
                predicates.Add("COALESCE(json_extract(data, '$.isDirectory'), 0) = 1");
        }
        else
        {
            if (!includeVersions && !includeSnapshots)
                predicates.Add("is_current = 1");
            else if (!includeVersions)
                predicates.Add("(is_current = 1 OR snapshot IS NOT NULL)");
            else if (!includeSnapshots)
                predicates.Add("snapshot IS NULL");
            if (!includeDeleted)
                predicates.Add("is_deleted = 0");
        }
        if (!string.IsNullOrEmpty(endBefore))
        {
            predicates.Add($"{BlobListingOrderExpression("name", hierarchicalRecursive)} < " +
                           BlobListingOrderExpression("$end_before", hierarchicalRecursive));
        }
        return predicates;
    }

    private static string BuildEligibleBlobListingSql(
        IReadOnlyList<string> predicates,
        string endBefore,
        bool hierarchicalRecursive)
    {
        const string rankExpression = """
            CASE
                WHEN is_current = 1 THEN 0
                WHEN version_id IS NOT NULL THEN 1
                WHEN snapshot IS NULL THEN 2
                ELSE 3
            END
            """;
        var eligibleBlobs = $"""
            SELECT data, name, version_id, snapshot, generation_id,
                   {rankExpression} AS rank,
                   0 AS is_uncommitted,
                   COALESCE(json_extract(data, '$.isDirectory'), 0) AS is_directory
            FROM blobs
            WHERE {string.Join(" AND ", predicates)}
            """;
        var uncommittedPredicates = new List<string>
        {
            "staged.account = $account",
            "staged.container = $container",
            "staged.blob_name >= $prefix",
            $"{BlobListingOrderExpression("staged.blob_name", hierarchicalRecursive)} >= " +
            BlobListingOrderExpression("$start_from", hierarchicalRecursive),
            "substr(staged.blob_name, 1, length($prefix)) = $prefix"
        };
        if (!string.IsNullOrEmpty(endBefore))
        {
            uncommittedPredicates.Add(
                $"{BlobListingOrderExpression("staged.blob_name", hierarchicalRecursive)} < " +
                BlobListingOrderExpression("$end_before", hierarchicalRecursive));
        }
        var eligibleUncommitted = $"""
            SELECT NULL AS data, staged.blob_name AS name,
                   NULL AS version_id, NULL AS snapshot, '' AS generation_id,
                   -1 AS rank, 1 AS is_uncommitted, 0 AS is_directory
            FROM staged_blocks AS staged
            WHERE $include_uncommitted = 1
              AND {string.Join(" AND ", uncommittedPredicates)}
              AND NOT EXISTS (
                  SELECT 1
                  FROM blobs AS current
                  WHERE current.account = staged.account
                    AND current.container = staged.container
                    AND current.name = staged.blob_name
                    AND current.is_current = 1
                    AND current.is_deleted = 0
              )
            GROUP BY staged.blob_name
            """;
        return $"""
            SELECT * FROM ({eligibleBlobs})
            UNION ALL
            SELECT * FROM ({eligibleUncommitted})
            """;
    }

    private static string BlobListingOrderExpression(string name, bool hierarchicalRecursive) =>
        hierarchicalRecursive ? $"replace({name}, '/', char(0))" : name;

    private static string BuildBlobListingEntriesSql(
        string eligible,
        string delimiter,
        bool hierarchicalNamespace)
    {
        if (string.IsNullOrEmpty(delimiter))
        {
            return $"""
                WITH entries AS (
                    SELECT data, name AS entry_name, 1 AS entry_type, rank,
                           version_id, snapshot, generation_id, is_uncommitted, is_directory
                    FROM ({eligible})
                )
                """;
        }
        return hierarchicalNamespace
            ? BuildHierarchicalBlobListingEntriesSql(eligible)
            : BuildFlatBlobListingEntriesSql(eligible);
    }

    private static string BuildHierarchicalBlobListingEntriesSql(string eligible) => $"""
        WITH eligible AS (
            SELECT source.*,
                   instr(substr(name, length($prefix) + 1), $delimiter) AS delimiter_offset
            FROM ({eligible}) AS source
        ),
        candidates AS (
            SELECT data,
                   CASE
                       WHEN is_directory = 1 AND delimiter_offset = 0 THEN name || '/'
                       WHEN delimiter_offset > 0 THEN substr(
                           name,
                           1,
                           length($prefix) + delimiter_offset)
                       ELSE name
                   END AS entry_name,
                   CASE
                       WHEN is_directory = 1 OR delimiter_offset > 0 THEN 0
                       ELSE 1
                   END AS entry_type,
                   CASE
                       WHEN is_directory = 1 AND delimiter_offset = 0 THEN data
                       ELSE NULL
                   END AS prefix_data,
                   rank, version_id, snapshot, generation_id,
                   is_uncommitted, is_directory
            FROM eligible
        ),
        entries AS (
            SELECT COALESCE(
                       MAX(prefix_data),
                       CASE WHEN $include_directory_properties = 1 THEN (
                           SELECT directory.data
                           FROM blobs AS directory
                           WHERE directory.account = $account
                             AND directory.container = $container
                             AND directory.name = substr(entry_name, 1, length(entry_name) - 1)
                             AND directory.is_current = 1
                             AND directory.is_deleted = 0
                             AND COALESCE(json_extract(directory.data, '$.isDirectory'), 0) = 1
                           LIMIT 1
                       ) END) AS data,
                   entry_name, 0 AS entry_type,
                   -1 AS rank, NULL AS version_id, NULL AS snapshot,
                   '' AS generation_id, 0 AS is_uncommitted, 1 AS is_directory
            FROM candidates
            WHERE entry_type = 0
            GROUP BY entry_name
            UNION ALL
            SELECT data, entry_name, 1 AS entry_type, rank,
                   version_id, snapshot, generation_id, is_uncommitted, is_directory
            FROM candidates
            WHERE entry_type = 1
        )
        """;

    private static string BuildFlatBlobListingEntriesSql(string eligible) => $"""
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
                   '' AS generation_id,
                   0 AS is_uncommitted,
                   0 AS is_directory
            FROM eligible
            WHERE delimiter_offset > 0
            UNION ALL
            SELECT data, name AS entry_name, 1 AS entry_type, rank,
                   version_id, snapshot, generation_id, is_uncommitted, is_directory
            FROM eligible
            WHERE delimiter_offset = 0
        )
        """;

    private static string BuildBlobListingPageSql(string entries) => $"""
        {entries}
        , ordered_entries AS (
            SELECT *,
                   CASE WHEN $hns_recursive = 1 THEN replace(entry_name, '/', char(0))
                        ELSE entry_name END AS sort_name
            FROM entries
        )
        SELECT data, entry_name, entry_type, rank, version_id, snapshot, generation_id,
               is_uncommitted, is_directory
        FROM ordered_entries
        WHERE $has_cursor = 0
           OR ($name_complete = 1 AND sort_name >
               CASE WHEN $hns_recursive = 1 THEN replace($cursor_name, '/', char(0))
                    ELSE $cursor_name END)
           OR ($name_complete = 0 AND (
                sort_name > CASE WHEN $hns_recursive = 1 THEN replace($cursor_name, '/', char(0))
                                 ELSE $cursor_name END
                OR (sort_name = CASE WHEN $hns_recursive = 1 THEN replace($cursor_name, '/', char(0))
                                     ELSE $cursor_name END AND (
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
        ORDER BY sort_name COLLATE BINARY,
                 entry_type,
                 rank,
                 CASE WHEN rank = 1 THEN version_id END DESC,
                 CASE WHEN rank = 3 THEN snapshot END,
                 generation_id
        LIMIT $limit OFFSET $offset;
        """;

    internal async Task<TaggedBlobPage> FindBlobsByTagsPageAsync(
        string account,
        BlobTagFilter filter,
        BlobTagCursor? cursor,
        int maximum,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximum);
        var predicates = BuildTagSearchPredicates(filter);

        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        // Predicate operators come from a closed enum; tag keys and values are bound parameters.
#pragma warning disable CA2100
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
#pragma warning restore CA2100
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

        var records = (await ReadJsonRowsAsync<BlobRecord>(command, cancellationToken).ConfigureAwait(false)).ToList();
        var hasMore = records.Count > maximum;
        if (hasMore)
            records.RemoveAt(records.Count - 1);
        return new TaggedBlobPage(records, hasMore);
    }

    private static List<string> BuildTagSearchPredicates(BlobTagFilter filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
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
        return predicates;
    }

    internal async Task<KeysetPage<BlobRecord>> ListBlobMaintenancePageAsync(
        string? afterGenerationId,
        int maximum,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximum);
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.CommandText = """
            SELECT data FROM blobs
            WHERE generation_id > $after
            ORDER BY generation_id
            LIMIT $limit;
            """;
        command.Parameters.AddWithValue("$after", afterGenerationId ?? string.Empty);
        command.Parameters.AddWithValue("$limit", checked(maximum + 1));
        var records = (await ReadJsonRowsAsync<BlobRecord>(command, cancellationToken).ConfigureAwait(false)).ToList();
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
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximum);
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
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
        var records = (await ReadJsonRowsAsync<ContainerRecord>(command, cancellationToken).ConfigureAwait(false)).ToList();
        var hasMore = records.Count > maximum;
        if (hasMore)
            records.RemoveAt(records.Count - 1);
        return new KeysetPage<ContainerRecord>(records, hasMore);
    }

    public Task<BlobRecord> PublishBlobAsync(
        BlobRecord proposed,
        string? expectedCurrentGeneration,
        string? expectedCurrentRevision,
        bool hierarchicalNamespace,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(proposed);
        return PublishBlobCoreAsync(
            proposed,
            expectedCurrentGeneration,
            expectedCurrentRevision,
            stagedBlockSnapshot: null,
            hierarchicalNamespace,
            cancellationToken);
    }

    public Task<BlobRecord> PublishBlockListAsync(
        BlobRecord proposed,
        string? expectedCurrentGeneration,
        string? expectedCurrentRevision,
        IReadOnlyList<StagedBlockRecord> stagedBlockSnapshot,
        bool hierarchicalNamespace,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(proposed);
        ArgumentNullException.ThrowIfNull(stagedBlockSnapshot);
        return PublishBlobCoreAsync(
            proposed,
            expectedCurrentGeneration,
            expectedCurrentRevision,
            stagedBlockSnapshot,
            hierarchicalNamespace,
            cancellationToken);
    }

    private async Task<BlobRecord> PublishBlobCoreAsync(
        BlobRecord proposed,
        string? expectedCurrentGeneration,
        string? expectedCurrentRevision,
        IReadOnlyList<StagedBlockRecord>? stagedBlockSnapshot,
        bool hierarchicalNamespace,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var (current, prepared) = await PrepareBlobPublicationAsync(
                connection, transaction, proposed, expectedCurrentGeneration,
                expectedCurrentRevision, stagedBlockSnapshot, hierarchicalNamespace,
                cancellationToken).ConfigureAwait(false);
            proposed = prepared;

            var serviceProperties = await GetServicePropertiesAsync(connection, transaction, proposed.Account, cancellationToken).ConfigureAwait(false);
            if (current is not null)
                await PreserveReplacedBlobAsync(
                    connection, transaction, current, proposed.Kind, serviceProperties,
                    hierarchicalNamespace, cancellationToken).ConfigureAwait(false);

            var published = proposed with
            {
                IsCurrent = true,
                IsDeleted = false,
                DeletionId = null,
                DeletedAt = null,
                DeleteRetentionUntil = null,
                VersionId = serviceProperties.VersioningEnabled && !hierarchicalNamespace
                    ? proposed.VersionId ?? CreateVersionId(proposed.LastModified)
                    : null,
                Snapshot = null
            };
            await InsertBlobRowAsync(connection, transaction, published, cancellationToken).ConfigureAwait(false);
            await DeleteStagedBlocksAsync(
                connection,
                transaction,
                proposed.Account,
                proposed.Container,
                proposed.Name,
                cancellationToken).ConfigureAwait(false);
            faultInjector.Inject(StorageFaultPoint.BeforeBlobMetadataCommit);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            faultInjector.Inject(StorageFaultPoint.AfterBlobMetadataCommit);
            return published;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    private static async Task<(BlobRecord? Current, BlobRecord Proposed)> PrepareBlobPublicationAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord proposed,
        string? expectedCurrentGeneration,
        string? expectedCurrentRevision,
        IReadOnlyList<StagedBlockRecord>? stagedBlockSnapshot,
        bool hierarchicalNamespace,
        CancellationToken cancellationToken)
    {
        var current = await GetCurrentBlobAsync(
            connection, transaction, proposed.Account, proposed.Container, proposed.Name,
            cancellationToken).ConfigureAwait(false);
        var activeCurrent = current is { IsDeleted: false } ? current : null;
        if (hierarchicalNamespace && activeCurrent is { IsDirectory: true } && !proposed.IsDirectory)
            throw new StoragePathConflictException();
        if (!string.Equals(activeCurrent?.GenerationId, expectedCurrentGeneration, StringComparison.Ordinal) ||
            !string.Equals(activeCurrent?.Revision, expectedCurrentRevision, StringComparison.Ordinal))
            throw new StorageConcurrencyException();
        if (activeCurrent is not null && activeCurrent.Kind != proposed.Kind)
            throw new StorageBlobTypeMismatchException();

        if (hierarchicalNamespace)
        {
            var (parentGroup, inheritedAcl) = await EnsureHierarchicalParentsAsync(
                connection, transaction, proposed, cancellationToken).ConfigureAwait(false);
            proposed = proposed with
            {
                Owner = activeCurrent?.Owner ?? proposed.Owner,
                Group = activeCurrent?.Group ?? parentGroup,
                AccessAcl = activeCurrent is null
                    ? proposed.AccessAcl ?? inheritedAcl
                    : activeCurrent.AccessAcl
            };
        }

        if (stagedBlockSnapshot is not null)
        {
            var actualBlocks = await ListStagedBlocksAsync(
                connection, transaction, proposed.Account, proposed.Container, proposed.Name,
                cancellationToken).ConfigureAwait(false);
            if (!EquivalentStagedBlocks(actualBlocks, stagedBlockSnapshot))
                throw new StorageConcurrencyException();
        }
        return (current, proposed);
    }

    private async Task PreserveReplacedBlobAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord current,
        BlobKind proposedKind,
        ServiceProperties serviceProperties,
        bool hierarchicalNamespace,
        CancellationToken cancellationToken)
    {
        if (!current.IsDeleted && string.Equals(current.Copy?.Status, "pending", StringComparison.Ordinal))
            throw new StoragePendingCopyException();
        var now = _timeProvider.GetUtcNow();
        if (current.IsDeleted && !hierarchicalNamespace)
            await PreserveDeletedBlobAsync(
                connection, transaction, current, proposedKind, now, cancellationToken).ConfigureAwait(false);
        else
            await PreserveActiveBlobAsync(
                connection, transaction, current, serviceProperties,
                hierarchicalNamespace, now, cancellationToken).ConfigureAwait(false);
    }

    private static async Task PreserveDeletedBlobAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord current,
        BlobKind proposedKind,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        if (current.Kind == proposedKind)
        {
            var historical = current with
            {
                IsCurrent = false,
                VersionId = null,
                Snapshot = current.Snapshot ?? await CreateUniqueSnapshotIdAsync(
                    connection, transaction, current.Account, current.Container, current.Name,
                    current.DeletedAt ?? now, cancellationToken).ConfigureAwait(false),
                Lease = LeaseRecord.Available,
                Revision = NewRevision()
            };
            await UpdateBlobRowAsync(connection, transaction, historical, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await DeleteSoftDeletedBlobRowsAsync(
                connection, transaction, current.Account, current.Container, current.Name,
                cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task PreserveActiveBlobAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord current,
        ServiceProperties serviceProperties,
        bool hierarchicalNamespace,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        var createsHistoricalVersion = serviceProperties.VersioningEnabled && !hierarchicalNamespace;
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
            await UpdateBlobRowAsync(connection, transaction, historical, cancellationToken).ConfigureAwait(false);
        }
        else if (serviceProperties.BlobSoftDeleteEnabled && !hierarchicalNamespace)
        {
            var historical = current with
            {
                IsCurrent = false,
                IsDeleted = true,
                DeletedAt = now,
                DeleteRetentionUntil = now.AddDays(serviceProperties.BlobSoftDeleteRetentionDays),
                VersionId = null,
                Snapshot = await CreateUniqueSnapshotIdAsync(
                    connection, transaction, current.Account, current.Container, current.Name,
                    now, cancellationToken).ConfigureAwait(false),
                Lease = LeaseRecord.Available,
                Revision = NewRevision()
            };
            await UpdateBlobRowAsync(connection, transaction, historical, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await DeleteBlobRowAsync(connection, transaction, current.GenerationId, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task PutBlobRecordAsync(
        BlobRecord record,
        string expectedRevision,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(record);
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var current = await GetBlobByGenerationAsync(connection, transaction, record.GenerationId, cancellationToken).ConfigureAwait(false);
            if (current is null || !string.Equals(current.Revision, expectedRevision, StringComparison.Ordinal))
                throw new StorageConcurrencyException();
            await UpdateBlobRowAsync(connection, transaction, record, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    internal async Task<BlobRecord?> GetBlobByGenerationAsync(
        string generationId,
        CancellationToken cancellationToken)
    {
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        return await GetBlobByGenerationAsync(connection, null, generationId, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<ObjectReplicationState?> GetObjectReplicationStateAsync(
        ObjectReplicationStateKey key,
        CancellationToken cancellationToken)
    {
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        return await GetObjectReplicationStateAsync(connection, null, key, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<ObjectReplicationStatePage> ListObjectReplicationStatesPageAsync(
        ObjectReplicationStateKey? cursor,
        int maximum,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximum);

        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.CommandText = """
            SELECT policy_id, rule_id, source_generation_id,
                   source_account, source_container, source_name,
                   destination_account, destination_container, destination_generation_id,
                   source_fingerprint, status, updated_ticks
            FROM object_replication_states
            WHERE $has_cursor = 0
               OR policy_id > $policy
               OR (policy_id = $policy AND rule_id > $rule)
               OR (policy_id = $policy AND rule_id = $rule AND source_generation_id > $generation)
            ORDER BY policy_id, rule_id, source_generation_id
            LIMIT $limit;
            """;
        command.Parameters.AddWithValue("$has_cursor", cursor.HasValue ? 1 : 0);
        command.Parameters.AddWithValue("$policy", cursor?.PolicyId ?? string.Empty);
        command.Parameters.AddWithValue("$rule", cursor?.RuleId ?? string.Empty);
        command.Parameters.AddWithValue("$generation", cursor?.SourceGenerationId ?? string.Empty);
        command.Parameters.AddWithValue("$limit", checked(maximum + 1));
        var states = new List<ObjectReplicationState>();
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            states.Add(ReadObjectReplicationState(reader));
        var hasMore = states.Count > maximum;
        if (hasMore)
            states.RemoveAt(states.Count - 1);
        return new ObjectReplicationStatePage(states, hasMore);
    }

    internal async Task<BlobRecord> ApplyObjectReplicationAsync(
        BlobRecord expectedSource,
        BlobRecord proposedDestination,
        ObjectReplicationState proposedState,
        string statusKey,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var (source, mapped, current) = await LoadObjectReplicationContextAsync(
                connection, transaction, expectedSource, proposedDestination,
                proposedState, cancellationToken).ConfigureAwait(false);
            var now = _timeProvider.GetUtcNow();
            var replicated = await WriteObjectReplicaAsync(
                connection, transaction, source, mapped, current, proposedDestination,
                now, cancellationToken).ConfigureAwait(false);
            await RecordObjectReplicationSuccessAsync(
                connection, transaction, source, proposedState, statusKey,
                replicated, now, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return replicated;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    internal async Task<bool> MarkObjectReplicationFailureAsync(
        BlobRecord expectedSource,
        ObjectReplicationState proposedState,
        string statusKey,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var source = await GetBlobByGenerationAsync(
                connection,
                transaction,
                expectedSource.GenerationId,
                cancellationToken).ConfigureAwait(false);
            if (source is null || !string.Equals(source.Revision, expectedSource.Revision, StringComparison.Ordinal))
                throw new StorageConcurrencyException();

            var key = new ObjectReplicationStateKey(
                proposedState.PolicyId,
                proposedState.RuleId,
                source.GenerationId);
            var existing = await GetObjectReplicationStateAsync(connection, transaction, key, cancellationToken).ConfigureAwait(false);
            if (source.ObjectReplicationStatuses.TryGetValue(statusKey, out var status) &&
                string.Equals(status.Status, "failed", StringComparison.Ordinal) &&
                string.Equals(status.SourceFingerprint, proposedState.SourceFingerprint, StringComparison.Ordinal) &&
                existing is { Status: "failed" } &&
                string.Equals(existing.SourceFingerprint, proposedState.SourceFingerprint, StringComparison.Ordinal))
            {
                await UpsertObjectReplicationStateAsync(
                    connection,
                    transaction,
                    existing with { UpdatedAt = _timeProvider.GetUtcNow() },
                    cancellationToken).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return false;
            }

            await PersistObjectReplicationFailureAsync(
                connection, transaction, source, proposedState, existing,
                statusKey, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    private async Task PersistObjectReplicationFailureAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord source,
        ObjectReplicationState proposedState,
        ObjectReplicationState? existing,
        string statusKey,
        CancellationToken cancellationToken)
    {
        var statuses = new Dictionary<string, ObjectReplicationStatusRecord>(
            source.ObjectReplicationStatuses,
            StringComparer.Ordinal)
        {
            [statusKey] = new ObjectReplicationStatusRecord
            {
                Status = "failed",
                SourceFingerprint = proposedState.SourceFingerprint
            }
        };
        await UpdateBlobRowAsync(
            connection,
            transaction,
            source with
            {
                Revision = NewRevision(),
                ObjectReplicationStatuses = statuses
            },
            cancellationToken).ConfigureAwait(false);
        await UpsertObjectReplicationStateAsync(
            connection,
            transaction,
            proposedState with
            {
                DestinationGenerationId = existing?.DestinationGenerationId,
                Status = "failed",
                UpdatedAt = _timeProvider.GetUtcNow()
            },
            cancellationToken).ConfigureAwait(false);
    }

    internal async Task<bool> RemoveObjectReplicaForMissingSourceAsync(
        ObjectReplicationState expectedState,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var key = new ObjectReplicationStateKey(
                expectedState.PolicyId,
                expectedState.RuleId,
                expectedState.SourceGenerationId);
            var state = await GetObjectReplicationStateAsync(connection, transaction, key, cancellationToken).ConfigureAwait(false);
            if (state is null ||
                !string.Equals(state.SourceFingerprint, expectedState.SourceFingerprint, StringComparison.Ordinal) ||
                await GetBlobByGenerationAsync(
                    connection,
                    transaction,
                    state.SourceGenerationId,
                    cancellationToken).ConfigureAwait(false) is not null)
            {
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return false;
            }

            if (state.DestinationGenerationId is { } destinationGeneration)
            {
                var destination = await GetBlobByGenerationAsync(
                    connection,
                    transaction,
                    destinationGeneration,
                    cancellationToken).ConfigureAwait(false);
                if (destination is not null)
                {
                    EnsureObjectReplicationTargetMutable(destination, _timeProvider.GetUtcNow());
                    await DeleteBlobRowAsync(connection, transaction, destination.GenerationId, cancellationToken).ConfigureAwait(false);
                }
            }

            await DeleteObjectReplicationStateAsync(connection, transaction, key, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    internal async Task<bool> ForgetObjectReplicationStateAsync(
        ObjectReplicationState expectedState,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var key = new ObjectReplicationStateKey(
                expectedState.PolicyId,
                expectedState.RuleId,
                expectedState.SourceGenerationId);
            var state = await GetObjectReplicationStateAsync(connection, transaction, key, cancellationToken).ConfigureAwait(false);
            if (state is null ||
                !string.Equals(state.SourceFingerprint, expectedState.SourceFingerprint, StringComparison.Ordinal) ||
                !string.Equals(
                    state.DestinationGenerationId,
                    expectedState.DestinationGenerationId,
                    StringComparison.Ordinal))
            {
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return false;
            }

            await DeleteObjectReplicationStateAsync(connection, transaction, key, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    internal async Task ApplyBlobRecordMutationsAsync(
        IReadOnlyList<BlobRecordMutation> mutations,
        CancellationToken cancellationToken,
        bool clearStagedBlocksForCurrentBlobs = false)
    {
        if (mutations.Count == 0)
            return;
        ValidateBlobRecordMutations(mutations);

        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var currentRecords = await ReadCurrentMutationRecordsAsync(
                connection, transaction, mutations, cancellationToken).ConfigureAwait(false);

            foreach (var mutation in mutations)
            {
                if (mutation.Replacement is null)
                {
                    await DeleteBlobRowAsync(
                        connection,
                        transaction,
                        mutation.GenerationId,
                        cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await UpdateBlobRowAsync(
                        connection,
                        transaction,
                        mutation.Replacement,
                        cancellationToken).ConfigureAwait(false);
                }
            }
            if (clearStagedBlocksForCurrentBlobs)
            {
                foreach (var current in currentRecords
                             .Where(record => record.IsCurrent && record.Snapshot is null)
                             .DistinctBy(record => (record.Account, record.Container, record.Name)))
                {
                    await DeleteStagedBlocksAsync(
                        connection,
                        transaction,
                        current.Account,
                        current.Container,
                        current.Name,
                        cancellationToken).ConfigureAwait(false);
                }
            }
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    private static void ValidateBlobRecordMutations(IReadOnlyList<BlobRecordMutation> mutations)
    {
        if (mutations.Select(item => item.GenerationId).Distinct(StringComparer.Ordinal).Count() != mutations.Count ||
            mutations.Any(item => item.Replacement is not null &&
                                  !string.Equals(
                                      item.GenerationId,
                                      item.Replacement.GenerationId,
                                      StringComparison.Ordinal)))
        {
            throw new ArgumentException("Blob record mutations must target unique, stable generation identities.", nameof(mutations));
        }
    }

    private static async Task<IReadOnlyList<BlobRecord>> ReadCurrentMutationRecordsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        IReadOnlyList<BlobRecordMutation> mutations,
        CancellationToken cancellationToken)
    {
        var currentRecords = new List<BlobRecord>(mutations.Count);
        foreach (var mutation in mutations)
        {
            var current = await GetBlobByGenerationAsync(
                connection,
                transaction,
                mutation.GenerationId,
                cancellationToken).ConfigureAwait(false);
            if (current is null ||
                !string.Equals(current.Revision, mutation.ExpectedRevision, StringComparison.Ordinal))
            {
                throw new StorageConcurrencyException();
            }
            currentRecords.Add(current);
        }
        return currentRecords;
    }

    internal async Task<bool> TryRestoreDeletedBlobAsync(
        BlobRecord restored,
        string expectedRevision,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var source = await GetBlobByGenerationAsync(
                connection,
                transaction,
                restored.GenerationId,
                cancellationToken).ConfigureAwait(false);
            if (source is null ||
                !source.IsDeleted ||
                !string.Equals(source.Revision, expectedRevision, StringComparison.Ordinal))
            {
                throw new StorageConcurrencyException();
            }

            await EnsureHierarchicalParentsAsync(connection, transaction, restored, cancellationToken).ConfigureAwait(false);

            var destination = await GetCurrentBlobAsync(
                connection,
                transaction,
                restored.Account,
                restored.Container,
                restored.Name,
                cancellationToken).ConfigureAwait(false);
            if (destination is not null &&
                !string.Equals(destination.GenerationId, restored.GenerationId, StringComparison.Ordinal))
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return false;
            }

            await UpdateBlobRowAsync(connection, transaction, restored, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    internal async Task EnsureHierarchicalDirectoriesAsync(
        string account,
        string container,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var command = connection.CreateCommand();
            await using var commandDisposal = command.ConfigureAwait(false);
            command.Transaction = transaction;
            command.CommandText = """
                SELECT data FROM blobs
                WHERE account = $account
                  AND container = $container
                  AND is_current = 1
                  AND is_deleted = 0
                ORDER BY name;
                """;
            command.Parameters.AddWithValue("$account", account);
            command.Parameters.AddWithValue("$container", container);
            var records = await ReadJsonRowsAsync<BlobRecord>(command, cancellationToken).ConfigureAwait(false);
            foreach (var record in records.Where(item => !item.IsDirectory))
                await EnsureHierarchicalParentsAsync(connection, transaction, record, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    internal async Task<bool> HasActiveBlobDescendantsAsync(
        string account,
        string container,
        string directoryName,
        CancellationToken cancellationToken)
    {
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.CommandText = """
            SELECT 1 FROM blobs
            WHERE account = $account
              AND container = $container
              AND is_current = 1
              AND is_deleted = 0
              AND name <> $directory
              AND substr(name, 1, length($prefix)) = $prefix
            LIMIT 1;
            """;
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", container);
        command.Parameters.AddWithValue("$directory", directoryName);
        command.Parameters.AddWithValue("$prefix", directoryName + "/");
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not null;
    }

    public async Task<BlobRecord> CompleteIncrementalCopyAsync(
        BlobRecord record,
        string expectedRevision,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(record);
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var current = await GetBlobByGenerationAsync(connection, transaction, record.GenerationId, cancellationToken).ConfigureAwait(false);
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
                cancellationToken).ConfigureAwait(false);
            var completed = record with { CopyDestinationSnapshot = snapshotId };
            await UpdateBlobRowAsync(connection, transaction, completed, cancellationToken).ConfigureAwait(false);
            var snapshot = completed with
            {
                GenerationId = Guid.NewGuid().ToString("N"),
                Revision = NewRevision(),
                VersionId = null,
                Snapshot = snapshotId,
                IsCurrent = false,
                Lease = LeaseRecord.Available
            };
            await InsertBlobRowAsync(connection, transaction, snapshot, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return completed;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task<BlobRecord> CreateSnapshotAsync(
        BlobRecord source,
        IReadOnlyDictionary<string, string>? snapshotMetadata,
        DateTimeOffset now,
        bool hierarchicalNamespace,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(source);
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var current = await GetCurrentBlobAsync(connection, transaction, source.Account, source.Container, source.Name, cancellationToken).ConfigureAwait(false);
            if (!string.Equals(current?.GenerationId, source.GenerationId, StringComparison.Ordinal) ||
                !string.Equals(current?.Revision, source.Revision, StringComparison.Ordinal))
                throw new StorageConcurrencyException();

            var snapshotId = await CreateUniqueSnapshotIdAsync(
                connection,
                transaction,
                source.Account,
                source.Container,
                source.Name,
                now,
                cancellationToken).ConfigureAwait(false);
            var snapshot = source with
            {
                GenerationId = Guid.NewGuid().ToString("N"),
                Revision = NewRevision(),
                IsCurrent = false,
                VersionId = null,
                Snapshot = snapshotId,
                Metadata = snapshotMetadata ?? source.Metadata,
                ETag = snapshotMetadata is null ? source.ETag : NewETag(),
                LastModified = snapshotMetadata is null ? source.LastModified : now,
                Lease = LeaseRecord.Available
            };

            var properties = await GetServicePropertiesAsync(connection, transaction, source.Account, cancellationToken).ConfigureAwait(false);
            string? newVersionId = null;
            if (properties.VersioningEnabled && !hierarchicalNamespace)
                newVersionId = await PreserveCurrentVersionForSnapshotAsync(
                    connection, transaction, source, now, cancellationToken).ConfigureAwait(false);
            await InsertBlobRowAsync(connection, transaction, snapshot, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return newVersionId is null ? snapshot : snapshot with { VersionId = newVersionId };
        }
        finally
        {
            _writeGate.Release();
        }
    }

    private static async Task<string> PreserveCurrentVersionForSnapshotAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord source,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        var historical = source with
        {
            IsCurrent = false,
            VersionId = source.VersionId ?? CreateVersionId(source.LastModified),
            Lease = LeaseRecord.Available,
            Revision = NewRevision()
        };
        await UpdateBlobRowAsync(connection, transaction, historical, cancellationToken).ConfigureAwait(false);
        var newVersionId = await CreateUniqueVersionIdAsync(
            connection,
            transaction,
            source.Account,
            source.Container,
            source.Name,
            now,
            cancellationToken).ConfigureAwait(false);
        var newCurrent = source with
        {
            GenerationId = Guid.NewGuid().ToString("N"),
            Revision = NewRevision(),
            VersionId = newVersionId,
            Snapshot = null,
            IsCurrent = true
        };
        await InsertBlobRowAsync(connection, transaction, newCurrent, cancellationToken).ConfigureAwait(false);
        return newVersionId;
    }

    public async Task<bool> DeleteBlobRecordAsync(
        string generationId,
        string expectedRevision,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var current = await GetBlobByGenerationAsync(connection, transaction, generationId, cancellationToken).ConfigureAwait(false);
            if (current is null)
                return false;
            if (!string.Equals(current.Revision, expectedRevision, StringComparison.Ordinal))
                throw new StorageConcurrencyException();
            var command = connection.CreateCommand();
            await using var commandDisposal = command.ConfigureAwait(false);
            command.Transaction = transaction;
            command.CommandText = "DELETE FROM blobs WHERE generation_id = $generation;";
            command.Parameters.AddWithValue("$generation", generationId);
            var deleted = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
            if (deleted && current.IsCurrent && current.Snapshot is null)
            {
                await DeleteStagedBlocksAsync(
                    connection,
                    transaction,
                    current.Account,
                    current.Container,
                    current.Name,
                    cancellationToken).ConfigureAwait(false);
            }
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return deleted;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task PutStagedBlockAsync(StagedBlockRecord block, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(block);
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var command = connection.CreateCommand();
            await using var commandDisposal = command.ConfigureAwait(false);
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
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            await ReplaceStagedBlockChunkReferencesAsync(connection, transaction, block, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
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
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        return await ListStagedBlocksAsync(connection, transaction: null, account, container, blobName, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<IReadOnlyList<StagedBlockRecord>> ListStagedBlocksAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string account,
        string container,
        string blobName,
        CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Transaction = transaction;
        command.CommandText = """
            SELECT data FROM staged_blocks
            WHERE account = $account AND container = $container AND blob_name = $blob
            ORDER BY created_ticks;
            """;
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", container);
        command.Parameters.AddWithValue("$blob", blobName);
        return await ReadJsonRowsAsync<StagedBlockRecord>(command, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<int> DeleteStagedBlocksAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string account,
        string container,
        string blobName,
        CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Transaction = transaction;
        command.CommandText = """
            DELETE FROM staged_blocks
            WHERE account = $account AND container = $container AND blob_name = $blob;
            """;
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", container);
        command.Parameters.AddWithValue("$blob", blobName);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> DeleteStagedBlocksAsync(
        string account,
        string container,
        string blobName,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var command = connection.CreateCommand();
            await using var commandDisposal = command.ConfigureAwait(false);
            command.CommandText = """
                DELETE FROM staged_blocks
                WHERE account = $account
                  AND container = $container
                  AND blob_name = $blob
                  AND NOT EXISTS (
                      SELECT 1
                      FROM blobs
                      WHERE blobs.account = staged_blocks.account
                        AND blobs.container = staged_blocks.container
                        AND blobs.name = staged_blocks.blob_name
                        AND blobs.is_current = 1
                        AND blobs.is_deleted = 0
                  );
                """;
            command.Parameters.AddWithValue("$account", account);
            command.Parameters.AddWithValue("$container", container);
            command.Parameters.AddWithValue("$blob", blobName);
            return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task CommitStagedBlocksAsync(
        string account,
        string container,
        string blobName,
        IReadOnlyCollection<string> committedIds,
        IReadOnlyCollection<string> removeIds,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            foreach (var blockId in committedIds.Concat(removeIds).Distinct(StringComparer.Ordinal))
            {
                var command = connection.CreateCommand();
                await using var commandDisposal = command.ConfigureAwait(false);
                command.Transaction = transaction;
                command.CommandText = """
                    DELETE FROM staged_blocks
                    WHERE account = $account AND container = $container AND blob_name = $blob AND block_id = $block;
                    """;
                command.Parameters.AddWithValue("$account", account);
                command.Parameters.AddWithValue("$container", container);
                command.Parameters.AddWithValue("$blob", blobName);
                command.Parameters.AddWithValue("$block", blockId);
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
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
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximum);
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var command = connection.CreateCommand();
            await using var commandDisposal = command.ConfigureAwait(false);
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
            return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task<ServiceProperties> GetServicePropertiesAsync(string account, CancellationToken cancellationToken)
    {
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        return await GetServicePropertiesAsync(connection, transaction: null, account, cancellationToken).ConfigureAwait(false);
    }

    public async Task PutServicePropertiesAsync(string account, ServiceProperties properties, CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var command = connection.CreateCommand();
            await using var commandDisposal = command.ConfigureAwait(false);
            command.CommandText = """
                INSERT INTO service_properties(account, data) VALUES ($account, $data)
                ON CONFLICT(account) DO UPDATE SET data = excluded.data;
                """;
            command.Parameters.AddWithValue("$account", account);
            command.Parameters.AddWithValue("$data", Serialize(properties));
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
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
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximum);
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
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
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        for (var offset = 0; offset < uniqueCandidates.Length; offset += maximumParametersPerQuery)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var count = Math.Min(maximumParametersPerQuery, uniqueCandidates.Length - offset);
            var command = connection.CreateCommand();
            await using var commandDisposal = command.ConfigureAwait(false);
            var parameterNames = new string[count];
            for (var index = 0; index < count; index++)
            {
                parameterNames[index] = $"$chunk{index}";
                command.Parameters.AddWithValue(parameterNames[index], uniqueCandidates[offset + index]);
            }
            var values = string.Join(',', parameterNames);
            // Parameter names are generated from bounded integer indexes, never from chunk IDs.
#pragma warning disable CA2100
            command.CommandText = $"""
                SELECT chunk_id FROM blob_chunk_references WHERE chunk_id IN ({values})
                UNION
                SELECT chunk_id FROM staged_block_chunk_references WHERE chunk_id IN ({values});
                """;
#pragma warning restore CA2100
            var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
            await using var readerDisposal = reader.ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                reachable.Add(reader.GetString(0));
        }
        return reachable;
    }

    public async Task<StorageInventorySummary> GetStorageInventorySummaryAsync(
        CancellationToken cancellationToken)
    {
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        long logicalBlobBytes;
        int blobRecordCount;
        var blobs = connection.CreateCommand();
        await using (blobs.ConfigureAwait(false))
        {
            blobs.CommandText = """
                SELECT COALESCE(SUM(logical_length + pending_copy_length), 0), COUNT(*)
                FROM blobs;
                """;
            var reader = (await blobs.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
            await using var readerDisposal = reader.ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                throw new InvalidDataException("The blob inventory query returned no aggregate row.");
            logicalBlobBytes = reader.GetInt64(0);
            blobRecordCount = checked((int)reader.GetInt64(1));
        }

        long logicalStagedBlockBytes;
        int stagedBlockCount;
        var blocks = connection.CreateCommand();
        await using (blocks.ConfigureAwait(false))
        {
            blocks.CommandText = "SELECT COALESCE(SUM(logical_length), 0), COUNT(*) FROM staged_blocks;";
            var reader = (await blocks.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
            await using var readerDisposal = reader.ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                throw new InvalidDataException("The staged-block inventory query returned no aggregate row.");
            logicalStagedBlockBytes = reader.GetInt64(0);
            stagedBlockCount = checked((int)reader.GetInt64(1));
        }

        int reachableChunkCount;
        var chunks = connection.CreateCommand();
        await using (chunks.ConfigureAwait(false))
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
                await chunks.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false),
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
        var connection = (await OpenAsync(cancellationToken).ConfigureAwait(false));
        await using var connectionDisposal = connection.ConfigureAwait(false);
        return await ReadStorageInventoryAsync(connection, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<MetadataBackupSnapshot> CreateBackupSnapshotAsync(
        string destinationDatabasePath,
        Func<IReadOnlySet<string>, IDisposable> acquireContentPins,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        IDisposable? pins = null;
        try
        {
            var source = (await OpenAsync(cancellationToken).ConfigureAwait(false));
            await using var sourceDisposal = source.ConfigureAwait(false);
            var inventory = await ReadVerifiedStorageInventoryAsync(source, cancellationToken).ConfigureAwait(false);
            pins = acquireContentPins(inventory.ReachableChunkIds);
            var destinationConnectionString = new SqliteConnectionStringBuilder
            {
                DataSource = destinationDatabasePath,
                Mode = SqliteOpenMode.ReadWriteCreate,
                Cache = SqliteCacheMode.Private,
                Pooling = false
            }.ToString();
            var destination = new SqliteConnection(destinationConnectionString);
            await using (destination.ConfigureAwait(false))
            {
                await destination.OpenAsync(cancellationToken).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();
                source.BackupDatabase(destination);
                cancellationToken.ThrowIfCancellationRequested();
                var journalMode = destination.CreateCommand();
                await using var journalModeDisposal = journalMode.ConfigureAwait(false);
                journalMode.CommandText = "PRAGMA journal_mode=DELETE;";
                var selectedMode = Convert.ToString(
                    await journalMode.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false),
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
        var connection = new SqliteConnection(connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        var integrity = connection.CreateCommand();
        await using (integrity.ConfigureAwait(false))
        {
            integrity.CommandText = "PRAGMA integrity_check;";
            var reader = (await integrity.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
            await using var readerDisposal = reader.ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var result = reader.GetString(0);
                if (!string.Equals(result, "ok", StringComparison.OrdinalIgnoreCase))
                    throw new InvalidDataException($"The metadata database failed SQLite integrity checking: {result}");
            }
        }
        await VerifyForeignKeysAsync(connection, cancellationToken).ConfigureAwait(false);

        var schemaVersion = await ReadSchemaVersionAsync(connection, cancellationToken).ConfigureAwait(false);
        var inventory = await ReadVerifiedStorageInventoryAsync(connection, cancellationToken).ConfigureAwait(false);
        var namespaceModes = new Dictionary<string, bool>(StringComparer.Ordinal);
        if (schemaVersion >= CurrentSchemaVersion)
        {
            var modes = connection.CreateCommand();
            await using var modesDisposal = modes.ConfigureAwait(false);
            modes.CommandText = "SELECT account, hierarchical_namespace_enabled FROM account_namespace_modes;";
            var reader = (await modes.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
            await using var readerDisposal = reader.ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                namespaceModes.Add(reader.GetString(0), reader.GetInt32(1) == 1);
        }
        return new MetadataDatabaseInspection(schemaVersion, inventory, namespaceModes);
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
        var connection = new SqliteConnection(connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
        await using (transaction.ConfigureAwait(false))
        {
            await ExecuteNonQueryAsync(connection, transaction, "DELETE FROM packed_chunks;", cancellationToken).ConfigureAwait(false);
            await ExecuteNonQueryAsync(connection, transaction, "DELETE FROM chunk_packs;", cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        await ExecuteNonQueryAsync(connection, "VACUUM;", cancellationToken).ConfigureAwait(false);
    }

    private static async Task<StorageMetadataInventory> ReadStorageInventoryAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        var schemaVersion = await ReadSchemaVersionAsync(connection, cancellationToken).ConfigureAwait(false);
        return schemaVersion >= 2
            ? await ReadIndexedStorageInventoryAsync(connection, cancellationToken).ConfigureAwait(false)
            : await ReadManifestStorageInventoryAsync(connection, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<StorageMetadataInventory> ReadVerifiedStorageInventoryAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        var authoritative = await ReadManifestStorageInventoryAsync(connection, cancellationToken).ConfigureAwait(false);
        var schemaVersion = await ReadSchemaVersionAsync(connection, cancellationToken).ConfigureAwait(false);
        if (schemaVersion < ChunkIndexSchemaVersion)
            return authoritative;

        var indexed = await ReadIndexedStorageInventoryAsync(connection, cancellationToken).ConfigureAwait(false);
        if (authoritative.LogicalBlobBytes != indexed.LogicalBlobBytes ||
            authoritative.LogicalStagedBlockBytes != indexed.LogicalStagedBlockBytes ||
            authoritative.BlobRecordCount != indexed.BlobRecordCount ||
            authoritative.StagedBlockCount != indexed.StagedBlockCount ||
            !authoritative.ReachableChunkIds.SetEquals(indexed.ReachableChunkIds))
        {
            throw new InvalidDataException("The metadata chunk-reference index does not match the authoritative manifests.");
        }
        if (schemaVersion >= 3)
            await VerifyBlobTagIndexAsync(connection, cancellationToken).ConfigureAwait(false);
        return authoritative;
    }

    private static async Task VerifyBlobTagIndexAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        var expected = new Dictionary<(string GenerationId, string Key), string>();
        var blobs = connection.CreateCommand();
        await using (blobs.ConfigureAwait(false))
        {
            blobs.CommandText = "SELECT data FROM blobs;";
            var reader = (await blobs.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
            await using var readerDisposal = reader.ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var blob = Deserialize<BlobRecord>(reader.GetString(0));
                foreach (var (key, value) in blob.Tags)
                    expected.Add((blob.GenerationId, key), value);
            }
        }

        var actual = new Dictionary<(string GenerationId, string Key), string>();
        var tags = connection.CreateCommand();
        await using (tags.ConfigureAwait(false))
        {
            tags.CommandText = "SELECT generation_id, tag_key, tag_value FROM blob_tags;";
            var reader = (await tags.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
            await using var readerDisposal = reader.ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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
        var chunks = connection.CreateCommand();
        await using (chunks.ConfigureAwait(false))
        {
            chunks.CommandText = """
                SELECT chunk_id FROM blob_chunk_references
                UNION
                SELECT chunk_id FROM staged_block_chunk_references;
                """;
            var reader = (await chunks.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
            await using var readerDisposal = reader.ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                reachable.Add(reader.GetString(0));
        }

        long logicalBlobBytes;
        int blobRecordCount;
        var blobs = connection.CreateCommand();
        await using (blobs.ConfigureAwait(false))
        {
            blobs.CommandText = """
                SELECT COALESCE(SUM(logical_length + pending_copy_length), 0), COUNT(*)
                FROM blobs;
                """;
            var reader = (await blobs.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
            await using var readerDisposal = reader.ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                throw new InvalidDataException("The blob inventory query returned no aggregate row.");
            logicalBlobBytes = reader.GetInt64(0);
            blobRecordCount = reader.GetInt32(1);
        }

        long logicalStagedBlockBytes;
        int stagedBlockCount;
        var blocks = connection.CreateCommand();
        await using (blocks.ConfigureAwait(false))
        {
            blocks.CommandText = "SELECT COALESCE(SUM(logical_length), 0), COUNT(*) FROM staged_blocks;";
            var reader = (await blocks.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
            await using var readerDisposal = reader.ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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

        var blobs = connection.CreateCommand();
        await using (blobs.ConfigureAwait(false))
        {
            blobs.CommandText = "SELECT data FROM blobs;";
            var reader = (await blobs.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
            await using var readerDisposal = reader.ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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

        var blocks = connection.CreateCommand();
        await using (blocks.ConfigureAwait(false))
        {
            blocks.CommandText = "SELECT data FROM staged_blocks;";
            var reader = (await blocks.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
            await using var readerDisposal = reader.ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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

    public static ulong NewDeletionId()
    {
        ulong value;
        do
        {
            value = BitConverter.ToUInt64(RandomNumberGenerator.GetBytes(sizeof(ulong)));
        }
        while (value == 0);
        return value;
    }

    public static string CreateVersionId(DateTimeOffset time) =>
        time.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ", CultureInfo.InvariantCulture);

    private static async Task MigrateVersion1ToVersion2Async(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<BlobRecord> blobs;
        IReadOnlyList<StagedBlockRecord> blocks;
        var command = connection.CreateCommand();
        await using (command.ConfigureAwait(false))
        {
            command.CommandText = "SELECT data FROM blobs;";
            blobs = await ReadJsonRowsAsync<BlobRecord>(command, cancellationToken).ConfigureAwait(false);
        }
        var blocksCommand = connection.CreateCommand();
        await using (blocksCommand.ConfigureAwait(false))
        {
            blocksCommand.CommandText = "SELECT data FROM staged_blocks;";
            blocks = await ReadJsonRowsAsync<StagedBlockRecord>(blocksCommand, cancellationToken).ConfigureAwait(false);
        }

        var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
        await using var transactionDisposal = transaction.ConfigureAwait(false);
        await ExecuteNonQueryAsync(
            connection,
            transaction,
            "ALTER TABLE blobs ADD COLUMN logical_length INTEGER NOT NULL DEFAULT 0;",
            cancellationToken).ConfigureAwait(false);
        await ExecuteNonQueryAsync(
            connection,
            transaction,
            "ALTER TABLE blobs ADD COLUMN pending_copy_length INTEGER NOT NULL DEFAULT 0;",
            cancellationToken).ConfigureAwait(false);
        await ExecuteNonQueryAsync(
            connection,
            transaction,
            "ALTER TABLE staged_blocks ADD COLUMN logical_length INTEGER NOT NULL DEFAULT 0;",
            cancellationToken).ConfigureAwait(false);

        foreach (var blob in blobs)
            await MigrateVersion1BlobAsync(connection, transaction, blob, cancellationToken).ConfigureAwait(false);

        foreach (var block in blocks)
            await MigrateVersion1StagedBlockAsync(connection, transaction, block, cancellationToken).ConfigureAwait(false);

        await ExecuteNonQueryAsync(
            connection,
            transaction,
            $"PRAGMA user_version={ChunkIndexSchemaVersion};",
            cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task MigrateVersion1BlobAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord blob,
        CancellationToken cancellationToken)
    {
        var update = connection.CreateCommand();
        await using var updateDisposal = update.ConfigureAwait(false);
        update.Transaction = transaction;
        update.CommandText = """
            UPDATE blobs
            SET logical_length = $logical, pending_copy_length = $pending
            WHERE generation_id = $generation;
            """;
        update.Parameters.AddWithValue("$logical", blob.Content.Length);
        update.Parameters.AddWithValue("$pending", blob.PendingCopyContent?.Length ?? 0);
        update.Parameters.AddWithValue("$generation", blob.GenerationId);
        if (await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
            throw new InvalidDataException("A blob changed while migrating the metadata schema.");
        await ReplaceBlobChunkReferencesAsync(connection, transaction, blob, cancellationToken).ConfigureAwait(false);
    }

    private static async Task MigrateVersion1StagedBlockAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        StagedBlockRecord block,
        CancellationToken cancellationToken)
    {
        var update = connection.CreateCommand();
        await using var updateDisposal = update.ConfigureAwait(false);
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
        if (await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
            throw new InvalidDataException("A staged block changed while migrating the metadata schema.");
        await ReplaceStagedBlockChunkReferencesAsync(connection, transaction, block, cancellationToken).ConfigureAwait(false);
    }

    private static async Task MigrateVersion2ToVersion3Async(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<BlobRecord> blobs;
        var command = connection.CreateCommand();
        await using (command.ConfigureAwait(false))
        {
            command.CommandText = "SELECT data FROM blobs;";
            blobs = await ReadJsonRowsAsync<BlobRecord>(command, cancellationToken).ConfigureAwait(false);
        }

        var transaction = ((SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
        await using var transactionDisposal = transaction.ConfigureAwait(false);
        await ExecuteNonQueryAsync(
            connection,
            transaction,
            "DELETE FROM blob_tags;",
            cancellationToken).ConfigureAwait(false);
        foreach (var blob in blobs)
            await ReplaceBlobTagsAsync(connection, transaction, blob, cancellationToken).ConfigureAwait(false);
        await ExecuteNonQueryAsync(
            connection,
            transaction,
            $"PRAGMA user_version={TagIndexSchemaVersion};",
            cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<SqliteConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await ExecuteNonQueryAsync(connection, "PRAGMA synchronous=FULL;", cancellationToken).ConfigureAwait(false);
        await ExecuteNonQueryAsync(
            connection,
            $"PRAGMA journal_size_limit={RetainedWalLimitBytes};",
            cancellationToken).ConfigureAwait(false);
        await ExecuteNonQueryAsync(connection, "PRAGMA foreign_keys=ON;", cancellationToken).ConfigureAwait(false);
        await ExecuteNonQueryAsync(connection, "PRAGMA busy_timeout=30000;", cancellationToken).ConfigureAwait(false);
        return connection;
    }

    private static async Task ExecuteNonQueryAsync(SqliteConnection connection, string text, CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        // Private callers supply only built-in schema and migration SQL, never request text.
#pragma warning disable CA2100
        command.CommandText = text;
#pragma warning restore CA2100
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task ExecuteNonQueryAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string text,
        CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Transaction = transaction;
        // Private callers supply only built-in migration SQL, never request text.
#pragma warning disable CA2100
        command.CommandText = text;
#pragma warning restore CA2100
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<int> ReadSchemaVersionAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.CommandText = "PRAGMA user_version;";
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture);
    }

    private static async Task VerifyForeignKeysAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.CommandText = "PRAGMA foreign_key_check;";
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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

    private static async Task<ObjectReplicationState?> GetObjectReplicationStateAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        ObjectReplicationStateKey key,
        CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Transaction = transaction;
        command.CommandText = """
            SELECT policy_id, rule_id, source_generation_id,
                   source_account, source_container, source_name,
                   destination_account, destination_container, destination_generation_id,
                   source_fingerprint, status, updated_ticks
            FROM object_replication_states
            WHERE policy_id = $policy AND rule_id = $rule AND source_generation_id = $generation;
            """;
        command.Parameters.AddWithValue("$policy", key.PolicyId);
        command.Parameters.AddWithValue("$rule", key.RuleId);
        command.Parameters.AddWithValue("$generation", key.SourceGenerationId);
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? ReadObjectReplicationState(reader) : null;
    }

    private static async Task UpsertObjectReplicationStateAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ObjectReplicationState state,
        CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO object_replication_states(
                policy_id, rule_id, source_generation_id,
                source_account, source_container, source_name,
                destination_account, destination_container, destination_generation_id,
                source_fingerprint, status, updated_ticks)
            VALUES (
                $policy, $rule, $source_generation,
                $source_account, $source_container, $source_name,
                $destination_account, $destination_container, $destination_generation,
                $fingerprint, $status, $updated)
            ON CONFLICT(policy_id, rule_id, source_generation_id) DO UPDATE SET
                source_account = excluded.source_account,
                source_container = excluded.source_container,
                source_name = excluded.source_name,
                destination_account = excluded.destination_account,
                destination_container = excluded.destination_container,
                destination_generation_id = excluded.destination_generation_id,
                source_fingerprint = excluded.source_fingerprint,
                status = excluded.status,
                updated_ticks = excluded.updated_ticks;
            """;
        command.Parameters.AddWithValue("$policy", state.PolicyId);
        command.Parameters.AddWithValue("$rule", state.RuleId);
        command.Parameters.AddWithValue("$source_generation", state.SourceGenerationId);
        command.Parameters.AddWithValue("$source_account", state.SourceAccount);
        command.Parameters.AddWithValue("$source_container", state.SourceContainer);
        command.Parameters.AddWithValue("$source_name", state.SourceName);
        command.Parameters.AddWithValue("$destination_account", state.DestinationAccount);
        command.Parameters.AddWithValue("$destination_container", state.DestinationContainer);
        command.Parameters.AddWithValue(
            "$destination_generation",
            (object?)state.DestinationGenerationId ?? DBNull.Value);
        command.Parameters.AddWithValue("$fingerprint", state.SourceFingerprint);
        command.Parameters.AddWithValue("$status", state.Status);
        command.Parameters.AddWithValue("$updated", state.UpdatedAt.UtcTicks);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task DeleteObjectReplicationStateAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ObjectReplicationStateKey key,
        CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Transaction = transaction;
        command.CommandText = """
            DELETE FROM object_replication_states
            WHERE policy_id = $policy AND rule_id = $rule AND source_generation_id = $generation;
            """;
        command.Parameters.AddWithValue("$policy", key.PolicyId);
        command.Parameters.AddWithValue("$rule", key.RuleId);
        command.Parameters.AddWithValue("$generation", key.SourceGenerationId);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static ObjectReplicationState ReadObjectReplicationState(SqliteDataReader reader) => new()
    {
        PolicyId = reader.GetString(0),
        RuleId = reader.GetString(1),
        SourceGenerationId = reader.GetString(2),
        SourceAccount = reader.GetString(3),
        SourceContainer = reader.GetString(4),
        SourceName = reader.GetString(5),
        DestinationAccount = reader.GetString(6),
        DestinationContainer = reader.GetString(7),
        DestinationGenerationId = reader.IsDBNull(8) ? null : reader.GetString(8),
        SourceFingerprint = reader.GetString(9),
        Status = reader.GetString(10),
        UpdatedAt = new DateTimeOffset(reader.GetInt64(11), TimeSpan.Zero)
    };

    private static void EnsureObjectReplicationTargetMutable(BlobRecord blob, DateTimeOffset now)
    {
        if (blob.HasLegalHold || blob.ImmutabilityUntil > now)
            throw new StorageImmutabilityException(blob.HasLegalHold);
    }

    private static async Task<BlobRecord?> GetCurrentBlobAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string account,
        string container,
        string name,
        CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Transaction = transaction;
        command.CommandText = """
            SELECT data FROM blobs
            WHERE account = $account AND container = $container AND name = $name AND is_current = 1;
            """;
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", container);
        command.Parameters.AddWithValue("$name", name);
        return await ReadSingleJsonAsync<BlobRecord>(command, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<BlobRecord?> GetBlobByGenerationAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string generationId,
        CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Transaction = transaction;
        command.CommandText = "SELECT data FROM blobs WHERE generation_id = $generation;";
        command.Parameters.AddWithValue("$generation", generationId);
        return await ReadSingleJsonAsync<BlobRecord>(command, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<(string Group, string? InheritedAcl)> EnsureHierarchicalParentsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord path,
        CancellationToken cancellationToken)
    {
        var root = await GetContainerAsync(
            connection,
            transaction,
            path.Account,
            path.Container,
            includeDeleted: false,
            cancellationToken).ConfigureAwait(false);
        var parentGroup = root?.Group ?? "$superuser";
        var parentAcl = root?.Acl;
        var separator = path.Name.IndexOf('/', StringComparison.Ordinal);
        while (separator > 0)
        {
            var directoryName = path.Name[..separator];
            var existing = await GetCurrentBlobAsync(
                connection,
                transaction,
                path.Account,
                path.Container,
                directoryName,
                cancellationToken).ConfigureAwait(false);
            if (existing is null)
            {
                var directory = CreateInheritedDirectoryRecord(path, directoryName, parentGroup, parentAcl);
                await InsertBlobRowAsync(connection, transaction, directory, cancellationToken).ConfigureAwait(false);
                parentAcl = directory.Acl;
            }
            else if (!existing.IsDirectory)
            {
                throw new StoragePathConflictException();
            }
            else
            {
                parentGroup = existing.Group;
                parentAcl = existing.Acl;
            }

            separator = path.Name.IndexOf('/', separator + 1);
        }

        return (parentGroup, parentAcl is null
            ? null
            : PosixAccessControl.InheritDefaultAcl(parentAcl, path.IsDirectory));
    }

    private static BlobRecord CreateInheritedDirectoryRecord(
        BlobRecord path,
        string directoryName,
        string parentGroup,
        string? parentAcl) => new()
        {
            Account = path.Account,
            Container = path.Container,
            Name = directoryName,
            GenerationId = Guid.NewGuid().ToString("N"),
            Revision = NewRevision(),
            IsCurrent = true,
            IsDirectory = true,
            Kind = BlobKind.BlockBlob,
            Content = ContentManifest.Empty(path.Content.Domain),
            ETag = NewETag(),
            CreatedAt = path.CreatedAt,
            LastModified = path.CreatedAt,
            Owner = path.Owner,
            Group = parentGroup,
            AccessAcl = parentAcl is null
            ? null
            : PosixAccessControl.InheritDefaultAcl(parentAcl, childIsDirectory: true),
            Http = new BlobHttpProperties(),
            Lease = LeaseRecord.Available,
            AccessTier = "Hot",
            AccessTierInferred = true
        };

    private static async Task DeleteSoftDeletedBlobRowsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string account,
        string container,
        string name,
        CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Transaction = transaction;
        command.CommandText = """
            DELETE FROM blobs
            WHERE account = $account AND container = $container AND name = $name AND is_deleted = 1;
            """;
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", container);
        command.Parameters.AddWithValue("$name", name);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
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
            var command = connection.CreateCommand();
            await using var commandDisposal = command.ConfigureAwait(false);
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
            if (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is null)
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
            var command = connection.CreateCommand();
            await using var commandDisposal = command.ConfigureAwait(false);
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
            if (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is null)
                return candidate;
        }
    }

    private static async Task InsertBlobRowAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord record,
        CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
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
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        await ReplaceBlobChunkReferencesAsync(connection, transaction, record, cancellationToken).ConfigureAwait(false);
        await ReplaceBlobTagsAsync(connection, transaction, record, cancellationToken).ConfigureAwait(false);
    }

    private static async Task UpdateBlobRowAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord record,
        CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
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
        if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
            throw new StorageConcurrencyException();
        await ReplaceBlobChunkReferencesAsync(connection, transaction, record, cancellationToken).ConfigureAwait(false);
        await ReplaceBlobTagsAsync(connection, transaction, record, cancellationToken).ConfigureAwait(false);
    }

    private static async Task DeleteBlobRowAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string generationId,
        CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Transaction = transaction;
        command.CommandText = "DELETE FROM blobs WHERE generation_id = $generation;";
        command.Parameters.AddWithValue("$generation", generationId);
        if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
            throw new StorageConcurrencyException();
    }

    private static async Task ReplaceBlobChunkReferencesAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord record,
        CancellationToken cancellationToken)
    {
        var clear = connection.CreateCommand();
        await using (clear.ConfigureAwait(false))
        {
            clear.Transaction = transaction;
            clear.CommandText = "DELETE FROM blob_chunk_references WHERE generation_id = $generation;";
            clear.Parameters.AddWithValue("$generation", record.GenerationId);
            await clear.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        foreach (var chunkId in EnumerateChunkIds(record).Distinct(StringComparer.Ordinal))
        {
            var insert = connection.CreateCommand();
            await using var insertDisposal = insert.ConfigureAwait(false);
            insert.Transaction = transaction;
            insert.CommandText = """
                INSERT INTO blob_chunk_references(generation_id, chunk_id)
                VALUES ($generation, $chunk);
                """;
            insert.Parameters.AddWithValue("$generation", record.GenerationId);
            insert.Parameters.AddWithValue("$chunk", chunkId);
            await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task ReplaceBlobTagsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        BlobRecord record,
        CancellationToken cancellationToken)
    {
        var clear = connection.CreateCommand();
        await using (clear.ConfigureAwait(false))
        {
            clear.Transaction = transaction;
            clear.CommandText = "DELETE FROM blob_tags WHERE generation_id = $generation;";
            clear.Parameters.AddWithValue("$generation", record.GenerationId);
            await clear.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        foreach (var (key, value) in record.Tags)
        {
            var insert = connection.CreateCommand();
            await using var insertDisposal = insert.ConfigureAwait(false);
            insert.Transaction = transaction;
            insert.CommandText = """
                INSERT INTO blob_tags(generation_id, tag_key, tag_value)
                VALUES ($generation, $key, $value);
                """;
            insert.Parameters.AddWithValue("$generation", record.GenerationId);
            insert.Parameters.AddWithValue("$key", key);
            insert.Parameters.AddWithValue("$value", value);
            await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task<IReadOnlyList<PackedChunkLocation>> ListPackedChunkLocationsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string packId,
        CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Transaction = transaction;
        command.CommandText = """
            SELECT chunk_id, pack_id, record_offset, record_length, payload_offset, payload_length
            FROM packed_chunks
            WHERE pack_id = $pack
            ORDER BY record_offset, chunk_id;
            """;
        command.Parameters.AddWithValue("$pack", packId);
        var locations = new List<PackedChunkLocation>();
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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
        var clear = connection.CreateCommand();
        await using (clear.ConfigureAwait(false))
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
            await clear.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        foreach (var chunkId in block.Content.Chunks.Select(chunk => chunk.Id).Distinct(StringComparer.Ordinal))
        {
            var insert = connection.CreateCommand();
            await using var insertDisposal = insert.ConfigureAwait(false);
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
            await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
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
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Transaction = transaction;
        command.CommandText = "SELECT data FROM service_properties WHERE account = $account;";
        command.Parameters.AddWithValue("$account", account);
        return await ReadSingleJsonAsync<ServiceProperties>(command, cancellationToken).ConfigureAwait(false) ?? new ServiceProperties();
    }

    private static async Task<T?> ReadSingleJsonAsync<T>(SqliteCommand command, CancellationToken cancellationToken)
        where T : class
    {
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is string json ? Deserialize<T>(json) : null;
    }

    private static async Task<IReadOnlyList<T>> ReadJsonRowsAsync<T>(SqliteCommand command, CancellationToken cancellationToken)
    {
        var values = new List<T>();
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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
