using System.Globalization;
using Microsoft.Data.Sqlite;

namespace Mk8.Sava.Storage;

public sealed partial class MetadataStore
{
    internal Task<BlobRecord> PublishAnalyticsBlobAsync(BlobRecord proposed, string prefix,
        string expectedContainerRevision, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(proposed);
        ArgumentException.ThrowIfNullOrEmpty(prefix);
        return PublishBlobCoreAsync(proposed, expectedCurrentGeneration: null, expectedCurrentRevision: null,
            expectedContainerRevision, stagedBlockSnapshot: null, hierarchicalNamespace: false,
            analyticsPrefix: prefix, cancellationToken);
    }

    private static async Task<string> AllocateAnalyticsNameAsync(SqliteConnection connection, SqliteTransaction transaction,
        string account, string container, string prefix, CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using var commandLifetime = command.ConfigureAwait(false);
        command.Transaction = transaction;
        // Numeric suffix order, not textual order: D6 remains compatible with
        // existing names, including the 999999 -> 1000000 boundary. Selection
        // and publication share the write gate and this same transaction.
        command.CommandText = """
            SELECT MAX(CAST(substr(name, length($prefix) + 1, length(name) - length($prefix) - 4) AS INTEGER))
            FROM blobs
            WHERE account = $account AND container = $container
              AND substr(name, 1, length($prefix)) = $prefix
              AND substr(name, -4) = '.log'
              AND length(name) > length($prefix) + 4
              AND substr(name, length($prefix) + 1, length(name) - length($prefix) - 4) NOT GLOB '*[^0-9]*';
            """;
        command.Parameters.AddWithValue("$account", account);
        command.Parameters.AddWithValue("$container", container);
        command.Parameters.AddWithValue("$prefix", prefix);
        var maximum = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        var next = maximum is long value ? checked(value + 1) : 0;
        return prefix + next.ToString("D6", CultureInfo.InvariantCulture) + ".log";
    }
}
