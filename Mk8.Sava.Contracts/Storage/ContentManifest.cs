using System.Collections.ObjectModel;

namespace Mk8.Sava.Storage;

public sealed record ContentManifest(
    string Domain,
    long Length,
    string Sha256,
    IReadOnlyList<ChunkReference> Chunks)
{
    public const string SparseHash = "sparse";

    private readonly ReadOnlyCollection<ChunkReference> _chunks = Snapshot(Chunks);

    // Validation, pinning and later awaited reads must use the same owned references.
    public IReadOnlyList<ChunkReference> Chunks
    {
        get => _chunks;
        init => _chunks = Snapshot(value);
    }

    public static ContentManifest Empty(string domain) =>
        new(domain, 0, Convert.ToHexStringLower(SHA256.HashData([])), []);

    private static ReadOnlyCollection<ChunkReference> Snapshot(IReadOnlyList<ChunkReference> chunks)
    {
        ArgumentNullException.ThrowIfNull(chunks);
        return Array.AsReadOnly(chunks.ToArray());
    }
}
