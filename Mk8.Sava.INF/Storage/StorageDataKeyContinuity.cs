using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Storage;

public sealed class StorageDataKeyContinuity(
    MetadataStore metadata,
    ChunkStore chunks,
    IOptions<SavaOptions> configuredOptions)
{
    private readonly SavaOptions _options = configuredOptions.Value;

    public async Task EnsureAsync(CancellationToken cancellationToken)
    {
        var inventory = await metadata.GetStorageInventoryAsync(cancellationToken);
        var requiredKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var id in inventory.ReachableChunkIds)
            AddRequiredKey(id);
        await foreach (var id in chunks.EnumeratePhysicalChunkIdsForStartupAsync(cancellationToken))
            AddRequiredKey(id);

        void AddRequiredKey(string id)
        {
            if (KeyIdForChunk(id) is { } keyId)
                requiredKeys.Add(keyId);
        }

        var fingerprints = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var keyId in requiredKeys)
        {
            var encoded = keyId == "cross-account"
                ? _options.CrossAccountEncryptionKey
                  ?? throw new InvalidDataException("The cross-account data encryption key is missing for stored content.")
                : _options.ResolveAccountDataEncryptionKey(keyId["account:".Length..]);
            fingerprints.Add(keyId, Convert.ToHexStringLower(SHA256.HashData(Convert.FromBase64String(encoded))));
        }

        await metadata.EnsureDataEncryptionKeyFingerprintsAsync(
            fingerprints,
            async (keyId, token) =>
            {
                foreach (var id in inventory.ReachableChunkIds)
                {
                    if (KeyIdForChunk(id) == keyId)
                        await VerifyAsync(id, keyId, token);
                }
                await foreach (var id in chunks.EnumeratePhysicalChunkIdsForStartupAsync(token))
                {
                    if (!inventory.ReachableChunkIds.Contains(id) && KeyIdForChunk(id) == keyId)
                        await VerifyAsync(id, keyId, token);
                }
            },
            cancellationToken);

        async Task VerifyAsync(string id, string keyId, CancellationToken token)
        {
            var status = await chunks.VerifyChunkAsync(id, token);
            if (status != ChunkIntegrityStatus.Verified)
            {
                throw new InvalidDataException(
                    $"Cannot establish data encryption key continuity for '{keyId}': " +
                    $"chunk '{id}' is {status}.");
            }
        }
    }

    private static string? KeyIdForChunk(string id)
    {
        if (id.EndsWith("/$zero", StringComparison.Ordinal))
            return null;
        var domain = ChunkStore.GetDomainFromChunkId(id);
        if (domain.Contains("/$cpk-", StringComparison.Ordinal))
            return null;
        return domain == "$global"
            ? "cross-account"
            : $"account:{domain.Split('/', 2)[0]}";
    }
}
