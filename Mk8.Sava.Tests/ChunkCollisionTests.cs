using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class ChunkCollisionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EqualDigestsNeverShareStorageWithoutEqualBytes(bool packed)
    {
        var application = new SavaWebApplicationFactory(
            Path.Combine(Path.GetTempPath(), $"mk8-sava-collision-{Guid.NewGuid():N}"),
            new NullStorageFaultInjector(),
            analyticsSink: null,
            configurationOverrides: new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sava:EnableSmallChunkPacking"] = packed.ToString(),
                ["Sava:MaintenanceScanInterval"] = "01:00:00"
            },
            deleteDataPath: true,
            disableMaintenance: true);
        await using var applicationDisposal1 = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var services = application.Services;
        using var chunks = CreateCollisionStore(services);
        var encryption = new BlobEncryption(Scope: null, CustomerProvidedKeySha256: null);
        var firstBytes = new byte[1024];
        var secondBytes = new byte[1024];
        DeterministicTestBytes.Fill(0x5100, firstBytes);
        DeterministicTestBytes.Fill(0x5200, secondBytes);

        using var first = await chunks.StorePinnedAsync(
            SavaWebApplicationFactory.AccountName,
            encryption,
            new MemoryStream(firstBytes, writable: false),
            CancellationToken.None);
        using var second = await chunks.StorePinnedAsync(
            SavaWebApplicationFactory.AccountName,
            encryption,
            new MemoryStream(secondBytes, writable: false),
            CancellationToken.None);
        var firstId = Assert.Single(first.Manifest.Chunks).Id;
        var secondId = Assert.Single(second.Manifest.Chunks).Id;
        Assert.NotEqual(firstId, secondId, StringComparer.Ordinal);
        Assert.EndsWith("-1024", firstId, StringComparison.Ordinal);
        Assert.EndsWith("-1024-1", secondId, StringComparison.Ordinal);
        await AssertPhysicalLocationsAsync(application, packed);

        using var repeatedFirst = await chunks.StorePinnedAsync(
            SavaWebApplicationFactory.AccountName,
            encryption,
            new MemoryStream(firstBytes, writable: false),
            CancellationToken.None).ConfigureAwait(true);
        using var repeatedSecond = await chunks.StorePinnedAsync(
            SavaWebApplicationFactory.AccountName,
            encryption,
            new MemoryStream(secondBytes, writable: false),
            CancellationToken.None).ConfigureAwait(true);
        Assert.Equal(firstId, Assert.Single(repeatedFirst.Manifest.Chunks).Id);
        Assert.Equal(secondId, Assert.Single(repeatedSecond.Manifest.Chunks).Id);

        await AssertRoundTripAsync(chunks, encryption, first.Manifest, firstBytes).ConfigureAwait(true);
        await AssertRoundTripAsync(chunks, encryption, second.Manifest, secondBytes).ConfigureAwait(true);
    }

    private static ChunkStore CreateCollisionStore(IServiceProvider services) => new(
        services.GetRequiredService<StoragePaths>(),
        services.GetRequiredService<MetadataStore>(),
        services.GetRequiredService<IStorageFaultInjector>(),
        services.GetRequiredService<IOptions<SavaOptions>>(),
        _ => Enumerable.Repeat((byte)0xa5, 32).ToArray());

    private static async Task AssertPhysicalLocationsAsync(SavaWebApplicationFactory application, bool packed)
    {
        Assert.Equal(
            packed ? 2 : 0,
            await application.Services.GetRequiredService<MetadataStore>()
                .CountPackedChunksAsync(CancellationToken.None).ConfigureAwait(false));
        Assert.Equal(
            packed ? 0 : 2,
            Directory.EnumerateFiles(
                Path.Combine(application.DataPath, "chunks"),
                "*.chunk",
                SearchOption.AllDirectories).Count());
    }

    private static async Task AssertRoundTripAsync(
        ChunkStore chunks,
        BlobEncryption encryption,
        ContentManifest manifest,
        byte[] expected)
    {
        using var output = new MemoryStream();
        await chunks.WriteRangeAsync(
            manifest,
            encryption,
            0,
            expected.Length,
            output,
            CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(expected, output.ToArray());
    }
}
