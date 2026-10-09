using System.Security.Cryptography;
using System.Text.Json;
using Mk8.Sava.Storage;
using Mk8.Sava.Transport;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class ContentManifestOwnershipTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void ConstructionAndWithReplacementDoNotRetainCallerAliases(bool list, bool replace)
    {
        var original = new ChunkReference("account/default/$zero", 0, 31);
        IReadOnlyList<ChunkReference> caller = list ? new List<ChunkReference> { original } : new[] { original };
        var manifest = CreateManifest(caller, replace);
        var clone = manifest with { Sha256 = new string('a', 64) };
        var (domain, length, digest, deconstructed) = manifest;

        ((IList<ChunkReference>)caller)[0] = original with { Id = "account/default/different", Length = 7 };
        if (caller is List<ChunkReference> mutable)
            mutable.Add(original);

        Assert.Equal("account/default", domain);
        Assert.Equal(31, length);
        Assert.Equal(ContentManifest.SparseHash, digest);
        Assert.Equal(new[] { original }, manifest.Chunks);
        Assert.Equal(new[] { original }, deconstructed);
        Assert.Equal(new[] { original }, clone.Chunks);
        Assert.Equal(manifest, manifest with { });
        Assert.Equal(new string('a', 64), clone.Sha256);
    }

    [Fact]
    public void PublishedChunkViewCannotMutateTheManifest()
    {
        var original = new ChunkReference("account/default/$zero", 0, 31);
        var manifest = CreateManifest(new List<ChunkReference> { original }, replace: false);
        if (manifest.Chunks is IList<ChunkReference> list)
        {
            Assert.Throws<NotSupportedException>(() => list[0] = original with { Length = 7 });
            Assert.Throws<NotSupportedException>(() => list.Clear());
            Assert.Throws<NotSupportedException>(() => list.Add(original));
        }
        Assert.Equal(new[] { original }, manifest.Chunks);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DurableJsonRetainsTheExistingShapeAndOwnsDeserializedReferences(bool web)
    {
        const string defaultJson = "{\"Domain\":\"account/default\",\"Length\":31,\"Sha256\":\"sparse\",\"Chunks\":[{\"Id\":\"account/default/$zero\",\"Offset\":0,\"Length\":31}]}";
        const string webJson = "{\"domain\":\"account/default\",\"length\":31,\"sha256\":\"sparse\",\"chunks\":[{\"id\":\"account/default/$zero\",\"offset\":0,\"length\":31}]}";
        var options = web ? JsonSerializerOptions.Web : JsonSerializerOptions.Default;
        var expected = web ? webJson : defaultJson;
        var decoded = JsonSerializer.Deserialize<ContentManifest>(expected, options);
        Assert.NotNull(decoded);
        Assert.Equal(expected, JsonSerializer.Serialize(decoded, options));
        Assert.Equal("account/default/$zero", Assert.Single(decoded.Chunks).Id);
        if (decoded.Chunks is IList<ChunkReference> list)
            Assert.Throws<NotSupportedException>(() => list.Clear());
        Assert.Equal(expected, JsonSerializer.Serialize(decoded, options));
    }

    [Fact]
    public void RpcDescriptorsStillExcludePhysicalReferencesAndAllowAuthoritativeResolution()
    {
        var caller = new[] { new ChunkReference("account/default/private-chunk", 0, 31) };
        var manifest = new ContentManifest("account/default", 31, new string('a', 64), caller);
        const string expected = "{\"Domain\":\"account/default\",\"Length\":31,\"Sha256\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"Chunks\":[]}";
        Assert.Equal(expected, JsonSerializer.Serialize(manifest, RpcJson.Options));
        var decoded = JsonSerializer.Deserialize<ContentManifest>(expected, RpcJson.Options);
        Assert.NotNull(decoded);
        Assert.Empty(decoded.Chunks);
        Assert.Equal(31, decoded.Length); // A Gateway descriptor is deliberately not a physical manifest.
        Assert.Equal(manifest.Sha256, decoded.Sha256);
        output.WriteLine("manifest_contract_fingerprint,{0}", RpcContracts.Fingerprint);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void MissingReferenceCollectionsAreRejectedAtTheOwnershipBoundary(bool replace)
    {
        Assert.Throws<ArgumentNullException>(() => CreateManifest(null!, replace));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(10000)]
    public void OwnedSnapshotHasLinearManagedAllocationCost(int count)
    {
        var references = Enumerable.Range(0, count)
            .Select(index => new ChunkReference("account/default/$zero", index, 1)).ToArray();
        var digest = Convert.ToHexStringLower(SHA256.HashData([]));
        for (var warmup = 0; warmup < 3; warmup++)
            GC.KeepAlive(new ContentManifest("account/default", count, digest, references));
        const int operations = 8;
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var iteration = 0; iteration < operations; iteration++)
            GC.KeepAlive(new ContentManifest("account/default", count, digest, references));
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        output.WriteLine("manifest_snapshot_allocation,entries={0},operations={1},managed_bytes={2}", count, operations, allocated);
        Assert.InRange(allocated, 0, operations * (count * (long)IntPtr.Size + 256));
    }

    private static ContentManifest CreateManifest(IReadOnlyList<ChunkReference> chunks, bool replace) => replace
        ? ContentManifest.Empty("account/default") with { Length = 31, Sha256 = ContentManifest.SparseHash, Chunks = chunks }
        : new ContentManifest("account/default", 31, ContentManifest.SparseHash, chunks);
}
