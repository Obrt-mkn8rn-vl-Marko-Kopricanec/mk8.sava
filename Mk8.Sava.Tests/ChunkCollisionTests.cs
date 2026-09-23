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
        var chunks = new ChunkStore(
            services.GetRequiredService<StoragePaths>(),
            services.GetRequiredService<MetadataStore>(),
            services.GetRequiredService<IStorageFaultInjector>(),
            services.GetRequiredService<IOptions<SavaOptions>>(),
            _ => Enumerable.Repeat((byte)0xa5, 32).ToArray());
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
        Assert.Equal(
            packed ? 2 : 0,
            services.GetRequiredService<MetadataStore>().CountPackedChunks());
        Assert.Equal(
            packed ? 0 : 2,
            Directory.EnumerateFiles(
                Path.Combine(application.DataPath, "chunks"),
                "*.chunk",
                SearchOption.AllDirectories).Count());

        using var repeatedFirst = await chunks.StorePinnedAsync(
            SavaWebApplicationFactory.AccountName,
            encryption,
            new MemoryStream(firstBytes, writable: false),
            CancellationToken.None);
        using var repeatedSecond = await chunks.StorePinnedAsync(
            SavaWebApplicationFactory.AccountName,
            encryption,
            new MemoryStream(secondBytes, writable: false),
            CancellationToken.None);
        Assert.Equal(firstId, Assert.Single(repeatedFirst.Manifest.Chunks).Id);
        Assert.Equal(secondId, Assert.Single(repeatedSecond.Manifest.Chunks).Id);

        using var firstOutput = new MemoryStream();
        using var secondOutput = new MemoryStream();
        await chunks.WriteRangeAsync(
            first.Manifest,
            encryption,
            0,
            firstBytes.Length,
            firstOutput,
            CancellationToken.None);
        await chunks.WriteRangeAsync(
            second.Manifest,
            encryption,
            0,
            secondBytes.Length,
            secondOutput,
            CancellationToken.None);
        Assert.Equal(firstBytes, firstOutput.ToArray());
        Assert.Equal(secondBytes, secondOutput.ToArray());
    }
}
