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
        var representatives = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);
        foreach (var id in inventory.ReachableChunkIds)
        {
            if (id.EndsWith("/$zero", StringComparison.Ordinal))
                continue;
            var domain = ChunkStore.GetDomainFromChunkId(id);
            if (domain.Contains("/$cpk-", StringComparison.Ordinal))
                continue;
            var keyId = domain == "$global"
                ? "cross-account"
                : $"account:{domain.Split('/', 2)[0]}";
            if (!representatives.TryGetValue(keyId, out var domains))
            {
                domains = new Dictionary<string, string>(StringComparer.Ordinal);
                representatives.Add(keyId, domains);
            }
            domains.TryAdd(domain, id);
        }

        var fingerprints = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var keyId in representatives.Keys)
        {
            var encoded = keyId == "cross-account"
                ? _options.CrossAccountEncryptionKey
                  ?? throw new InvalidDataException("The cross-account data encryption key is missing for reachable content.")
                : _options.ResolveAccountDataEncryptionKey(keyId["account:".Length..]);
            fingerprints.Add(keyId, Convert.ToHexStringLower(SHA256.HashData(Convert.FromBase64String(encoded))));
        }

        await metadata.EnsureDataEncryptionKeyFingerprintsAsync(
            fingerprints,
            async (keyId, token) =>
            {
                foreach (var id in representatives[keyId].Values)
                {
                    var status = await chunks.VerifyChunkAsync(id, token);
                    if (status != ChunkIntegrityStatus.Verified)
                    {
                        throw new InvalidDataException(
                            $"Cannot establish data encryption key continuity for '{keyId}': " +
                            $"representative chunk '{id}' is {status}.");
                    }
                }
            },
            cancellationToken);
    }
}
