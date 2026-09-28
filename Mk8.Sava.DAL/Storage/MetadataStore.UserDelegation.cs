using System.Text.Json;

namespace Mk8.Sava.Storage;

public sealed partial class MetadataStore
{
    public async Task RecordUserDelegationRoleGrantsAsync(
        UserDelegationKeyIssue issue,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(issue);
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            var prune = connection.CreateCommand();
            await using (prune.ConfigureAwait(false))
            {
                prune.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)transaction;
                prune.CommandText = "DELETE FROM user_delegation_role_grants WHERE expires_ticks < $now;";
                prune.Parameters.AddWithValue("$now", _timeProvider.GetUtcNow().UtcTicks);
                await prune.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            var insert = connection.CreateCommand();
            await using (insert.ConfigureAwait(false))
            {
                insert.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)transaction;
                insert.CommandText = """
                    INSERT INTO user_delegation_role_grants
                        (key_fingerprint, account, object_id, tenant_id, start_text, expiry_text,
                         service, version, delegated_user_tenant_id, nonce, roles_json,
                         issued_permissions, expires_ticks)
                    VALUES ($fingerprint, $account, $object, $tenant, $start, $expiry,
                            $service, $version, $delegatedTenant, $nonce, $roles,
                            $permissions, $expires)
                    ON CONFLICT(key_fingerprint) DO UPDATE SET
                        roles_json = excluded.roles_json,
                        issued_permissions = excluded.issued_permissions,
                        expires_ticks = excluded.expires_ticks;
                    """;
                insert.Parameters.AddWithValue("$fingerprint", issue.KeyFingerprint);
                insert.Parameters.AddWithValue("$account", issue.Identity.Account);
                insert.Parameters.AddWithValue("$object", issue.Identity.ObjectId);
                insert.Parameters.AddWithValue("$tenant", issue.Identity.TenantId);
                insert.Parameters.AddWithValue("$start", issue.Identity.StartText);
                insert.Parameters.AddWithValue("$expiry", issue.Identity.ExpiryText);
                insert.Parameters.AddWithValue("$service", issue.Identity.Service);
                insert.Parameters.AddWithValue("$version", issue.Identity.Version);
                insert.Parameters.AddWithValue("$delegatedTenant", issue.Identity.DelegatedUserTenantId);
                insert.Parameters.AddWithValue("$nonce", issue.Nonce);
                insert.Parameters.AddWithValue("$roles", JsonSerializer.Serialize(issue.Roles.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal)));
                insert.Parameters.AddWithValue("$permissions", issue.IssuedPermissions);
                insert.Parameters.AddWithValue("$expires", issue.ExpiresAt.UtcTicks);
                await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task<IReadOnlyList<UserDelegationKeyCandidate>> ReadUserDelegationKeyCandidatesAsync(
        UserDelegationKeyIdentity identity,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(identity);
        var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.CommandText = """
            SELECT nonce, key_fingerprint FROM user_delegation_role_grants
            WHERE account = $account AND object_id = $object AND tenant_id = $tenant
              AND start_text = $start AND expiry_text = $expiry AND service = $service
              AND version = $version AND delegated_user_tenant_id = $delegatedTenant
              AND expires_ticks >= $now;
            """;
        command.Parameters.AddWithValue("$account", identity.Account);
        command.Parameters.AddWithValue("$object", identity.ObjectId);
        command.Parameters.AddWithValue("$tenant", identity.TenantId);
        command.Parameters.AddWithValue("$start", identity.StartText);
        command.Parameters.AddWithValue("$expiry", identity.ExpiryText);
        command.Parameters.AddWithValue("$service", identity.Service);
        command.Parameters.AddWithValue("$version", identity.Version);
        command.Parameters.AddWithValue("$delegatedTenant", identity.DelegatedUserTenantId);
        command.Parameters.AddWithValue("$now", _timeProvider.GetUtcNow().UtcTicks);
        var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await using var readerDisposal = reader.ConfigureAwait(false);
        var candidates = new List<UserDelegationKeyCandidate>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            candidates.Add(new UserDelegationKeyCandidate(reader.GetString(0), reader.GetString(1)));
        return candidates;
    }

    public async Task<(string[] Roles, string IssuedPermissions)?> ReadUserDelegationRoleGrantsAsync(
        string keyFingerprint,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(keyFingerprint);
        var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandDisposal = command.ConfigureAwait(false);
        command.CommandText = """
            SELECT roles_json, issued_permissions FROM user_delegation_role_grants
            WHERE key_fingerprint = $fingerprint AND expires_ticks >= $now;
            """;
        command.Parameters.AddWithValue("$fingerprint", keyFingerprint);
        command.Parameters.AddWithValue("$now", _timeProvider.GetUtcNow().UtcTicks);
        var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await using var readerDisposal = reader.ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? (JsonSerializer.Deserialize<string[]>(reader.GetString(0)) ?? [], reader.GetString(1))
            : null;
    }
}
