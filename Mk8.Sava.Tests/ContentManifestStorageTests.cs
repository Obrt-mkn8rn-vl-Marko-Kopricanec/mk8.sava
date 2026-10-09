using System.Security.Cryptography;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class ContentManifestStorageTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CallerMutationDuringPartialReadCannotChangeValidatedAndPinnedContent(bool replace)
    {
        var application = CreateApplication();
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync();
        Assert.DoesNotContain(application.Services.GetServices<IHostedService>(), service => service is StorageMaintenanceService);
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var encryption = new BlobEncryption(null, null);
        var bytes = new byte[32768];
        bytes.AsSpan(0, 16384).Fill(0x51);
        bytes.AsSpan(16384).Fill(0x62);
        using var input = new MemoryStream(bytes, writable: false);
        using var stored = await chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName, encryption, input, CancellationToken.None);
        Assert.Equal(2, stored.Manifest.Chunks.Count);
        var caller = stored.Manifest.Chunks.ToArray();
        var originalSecond = caller[1];
        var manifest = replace
            ? stored.Manifest with { Chunks = caller }
            : new ContentManifest(stored.Manifest.Domain, stored.Manifest.Length, stored.Manifest.Sha256, caller);
        using var destination = new MutatingDestination(() =>
            caller[1] = originalSecond with { Id = manifest.Domain + "/$zero" });

        await chunks.WriteRangeAsync(manifest, encryption, 7, bytes.Length - 11, destination, CancellationToken.None);

        Assert.Equal(2, destination.Writes);
        Assert.Equal(manifest.Domain + "/$zero", caller[1].Id);
        Assert.Equal(bytes.AsSpan(7, bytes.Length - 11).ToArray(), destination.ToArray());
        Assert.Equal(originalSecond, manifest.Chunks[1]);
        Assert.True(destination.CanWrite);
        Assert.True(input.CanRead);
        AssertReadsIdle(chunks);
        Assert.Equal(bytes, await chunks.ReadAllAsync(manifest, encryption, CancellationToken.None));
    }

    [Fact]
    public async Task AlteringASnapshotReplacementStillRunsStorageValidationBeforeAnyOutput()
    {
        var application = CreateApplication();
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var encryption = new BlobEncryption(null, null);
        var valid = chunks.Sparse(SavaWebApplicationFactory.AccountName, encryption, 31);
        var invalid = valid with { Chunks = [new ChunkReference(valid.Domain + "/$zero", 1, 31)] };
        using var destination = new MemoryStream();

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            chunks.WriteRangeAsync(invalid, encryption, 0, 31, destination, CancellationToken.None));

        Assert.Equal(0, destination.Length);
        AssertReadsIdle(chunks);
        await chunks.WriteRangeAsync(valid, encryption, 0, 31, destination, CancellationToken.None);
        Assert.Equal(new byte[31], destination.ToArray());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DomainCopyUsesOwnedReferencesAndRetainsHashAndKeyChecks(bool customerKey)
    {
        var application = CreateApplication();
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var key = new byte[32];
        DeterministicTestBytes.Fill(0x7310, key);
        try
        {
            var encryption = customerKey
                ? new BlobEncryption(null, Convert.ToBase64String(SHA256.HashData(key)), key)
                : new BlobEncryption(null, null);
            var bytes = new byte[4096];
            DeterministicTestBytes.Fill(0x7320, bytes);
            using var input = new MemoryStream(bytes, writable: false);
            using var stored = await chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName, encryption, input, CancellationToken.None);
            var caller = stored.Manifest.Chunks.ToArray();
            var manifest = stored.Manifest with { Chunks = caller };
            caller[0] = caller[0] with { Id = manifest.Domain + "/$zero" };

            using var copied = await chunks.CopyToDomainPinnedAsync(SavaWebApplicationFactory.SecondAccountName,
                encryption, encryption, manifest, CancellationToken.None);

            Assert.NotEqual(manifest.Domain, copied.Manifest.Domain, StringComparer.Ordinal);
            Assert.Equal(manifest.Sha256, copied.Manifest.Sha256);
            Assert.NotNull(copied.ContentMd5);
            Assert.Equal(stored.ContentMd5, copied.ContentMd5);
            Assert.Equal(bytes, await chunks.ReadAllAsync(copied.Manifest, encryption, CancellationToken.None));
            if (customerKey)
                await Assert.ThrowsAsync<InvalidDataException>(() => chunks.ReadAllAsync(
                    copied.Manifest, new BlobEncryption(null, Convert.ToBase64String(SHA256.HashData(new byte[32])), new byte[32]),
                    CancellationToken.None));
            AssertReadsIdle(chunks);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(key);
        }
    }

    private static SavaWebApplicationFactory CreateApplication() => new(TimeProvider.System,
        new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MinimumChunkBytes"] = "16384",
            ["Sava:TargetChunkBytes"] = "16384",
            ["Sava:MaximumChunkBytes"] = "16384",
            ["Sava:EnableSmallChunkPacking"] = "false"
        }, disableMaintenance: true);

    private static void AssertReadsIdle(ChunkStore chunks) => Assert.Contains(
        "mk8_sava_storage_work_active{lane=\"reads\"} 0\n", chunks.Admission.RenderPrometheus(), StringComparison.Ordinal);

    private sealed class MutatingDestination(Action mutate) : MemoryStream
    {
        internal int Writes { get; private set; }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await base.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
            if (++Writes == 1)
                mutate();
        }
    }
}
