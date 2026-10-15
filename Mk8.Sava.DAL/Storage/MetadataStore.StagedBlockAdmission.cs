using Microsoft.Data.Sqlite;

namespace Mk8.Sava.Storage;

public sealed partial class MetadataStore
{
    private static async Task EnsureStagedBlockAdmissionAsync(SqliteConnection connection, SqliteTransaction transaction,
        StagedBlockRecord block, CancellationToken cancellationToken)
    {
        var length = DecodedBlockIdLength(block.BlockId);
        var command = connection.CreateCommand();
        await using var commandLifetime = command.ConfigureAwait(false);
        command.Transaction = transaction;
        // Read only IDs, not every staged content manifest. The scan and upsert
        // share the same serialized transaction; earlier BLL checks are hints.
        command.CommandText = """
            SELECT block_id FROM staged_blocks
            WHERE account = $account AND container = $container AND blob_name = $blob;
            """;
        command.Parameters.AddWithValue("$account", block.Account);
        command.Parameters.AddWithValue("$container", block.Container);
        command.Parameters.AddWithValue("$blob", block.BlobName);
        var count = 0L;
        var replacement = false;
        var compatible = true;
        var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await using (reader.ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var id = reader.GetString(0);
                count++;
                replacement |= string.Equals(id, block.BlockId, StringComparison.Ordinal);
                compatible &= DecodedBlockIdLength(id) == length;
            }
        }
        if (!replacement && count >= BlobServiceLimits.MaximumUncommittedBlockCount)
            throw new StorageBlockAdmissionException(capacityExceeded: true);
        if (!compatible)
            throw new StorageBlockAdmissionException(capacityExceeded: false);
        var current = await GetCurrentBlobAsync(connection, transaction, block.Account, block.Container,
            block.BlobName, cancellationToken).ConfigureAwait(false);
        if (current is { IsDeleted: false })
        {
            if (current.Kind != BlobKind.BlockBlob)
                throw new StorageBlobTypeMismatchException();
            if (string.Equals(current.Copy?.Status, "pending", StringComparison.Ordinal))
                throw new StoragePendingCopyException();
            if (current.CommittedBlocks.Any(item => DecodedBlockIdLength(item.Id) != length))
                throw new StorageBlockAdmissionException(capacityExceeded: false);
        }
    }

    private static int DecodedBlockIdLength(string id)
    {
        Span<byte> bytes = stackalloc byte[BlobServiceLimits.MaximumBlockIdBytes];
        if (!Convert.TryFromBase64String(id, bytes, out var length) || length == 0)
            throw new InvalidDataException("A staged or committed block ID is invalid.");
        return length;
    }
}
