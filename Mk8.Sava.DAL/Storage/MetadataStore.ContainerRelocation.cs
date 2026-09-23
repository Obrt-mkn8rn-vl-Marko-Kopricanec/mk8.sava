using Microsoft.Data.Sqlite;

namespace Mk8.Sava.Storage;

public sealed partial class MetadataStore
{
    private enum ContainerRelocationStep
    {
        MoveBlobs,
        CloneStagedBlocks,
        CloneStagedBlockReferences,
        DeleteOldStagedBlocks,
        DeleteSourceContainer
    }

    private static async Task<bool> RelocateContainerNameAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string sourceName,
        ContainerRecord destination,
        CancellationToken cancellationToken)
    {
        var existingDestination = await GetContainerAsync(
            connection, transaction, destination.Account, destination.Name,
            includeDeleted: true, cancellationToken).ConfigureAwait(false);
        if (existingDestination is not null)
            return false;

        await InsertRelocatedContainerAsync(connection, transaction, destination, cancellationToken).ConfigureAwait(false);
        await ExecuteContainerRelocationStepAsync(
            connection, transaction, ContainerRelocationStep.MoveBlobs,
            destination.Account, sourceName, destination.Name, cancellationToken).ConfigureAwait(false);
        await ExecuteContainerRelocationStepAsync(
            connection, transaction, ContainerRelocationStep.CloneStagedBlocks,
            destination.Account, sourceName, destination.Name, cancellationToken).ConfigureAwait(false);
        await ExecuteContainerRelocationStepAsync(
            connection, transaction, ContainerRelocationStep.CloneStagedBlockReferences,
            destination.Account, sourceName, destination.Name, cancellationToken).ConfigureAwait(false);
        await ExecuteContainerRelocationStepAsync(
            connection, transaction, ContainerRelocationStep.DeleteOldStagedBlocks,
            destination.Account, sourceName, destination.Name, cancellationToken).ConfigureAwait(false);
        if (await ExecuteContainerRelocationStepAsync(
            connection, transaction, ContainerRelocationStep.DeleteSourceContainer,
            destination.Account, sourceName, destination.Name, cancellationToken).ConfigureAwait(false) != 1)
        {
            throw new StorageConcurrencyException();
        }
        return true;
    }

    private static async Task InsertRelocatedContainerAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ContainerRecord destination,
        CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO containers(account, name, deleted, modified_ticks, data)
            VALUES ($account, $name, $deleted, $modified, $data);
            """;
        AddContainerParameters(command, destination);
        if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
            throw new StorageConcurrencyException();
    }

    private static async Task<int> ExecuteContainerRelocationStepAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ContainerRelocationStep step,
        string account,
        string sourceName,
        string destinationName,
        CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Transaction = transaction;
        // Closed enum selects only built-in SQL; all three names are bound parameters.
#pragma warning disable CA2100
        command.CommandText = step switch
        {
            ContainerRelocationStep.MoveBlobs => """
                UPDATE blobs
                SET container = $destination,
                    data = json_set(data, '$.container', $destination)
                WHERE account = $account AND container = $container;
                """,
            ContainerRelocationStep.CloneStagedBlocks => """
                INSERT INTO staged_blocks(
                    account, container, blob_name, block_id, created_ticks, logical_length, data)
                SELECT account, $destination, blob_name, block_id, created_ticks, logical_length,
                       json_set(data, '$.container', $destination)
                FROM staged_blocks
                WHERE account = $account AND container = $container;
                """,
            ContainerRelocationStep.CloneStagedBlockReferences => """
                INSERT INTO staged_block_chunk_references(
                    account, container, blob_name, block_id, chunk_id)
                SELECT account, $destination, blob_name, block_id, chunk_id
                FROM staged_block_chunk_references
                WHERE account = $account AND container = $container;
                """,
            ContainerRelocationStep.DeleteOldStagedBlocks => """
                DELETE FROM staged_blocks
                WHERE account = $account AND container = $container;
                """,
            ContainerRelocationStep.DeleteSourceContainer => """
                DELETE FROM containers
                WHERE account = $account AND name = $container;
                """,
            _ => throw new ArgumentOutOfRangeException(nameof(step))
        };
#pragma warning restore CA2100
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", sourceName);
        if (step is ContainerRelocationStep.MoveBlobs or ContainerRelocationStep.CloneStagedBlocks or
            ContainerRelocationStep.CloneStagedBlockReferences)
        {
            command.Parameters.AddWithValue("$destination", destinationName);
        }
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
