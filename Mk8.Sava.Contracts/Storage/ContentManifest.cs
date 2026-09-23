namespace Mk8.Sava.Storage;

public sealed record ContentManifest(
    string Domain,
    long Length,
    string Sha256,
    IReadOnlyList<ChunkReference> Chunks)
{
    public const string SparseHash = "sparse";

    public static ContentManifest Empty(string domain) =>
        new(domain, 0, Convert.ToHexStringLower(SHA256.HashData([])), []);
}
