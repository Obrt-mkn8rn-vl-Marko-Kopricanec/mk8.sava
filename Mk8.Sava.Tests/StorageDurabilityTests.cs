using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageDurabilityTests
{
    [Fact]
    public async Task DisposingServiceReleasesMetadataPoolBeforeDeletingItsRoot()
    {
        var root = Path.Combine(Path.GetTempPath(), $"mk8-sava-dispose-root-{Guid.NewGuid():N}");
        var application = new SavaWebApplicationFactory(root, deleteDataPath: false);
        await using (application.ConfigureAwait(true))
        {
            await application.InitializeAsync().ConfigureAwait(true);
            var metadata = application.Services.GetRequiredService<MetadataStore>();
            _ = await metadata.GetStorageInventoryAsync(CancellationToken.None).ConfigureAwait(true);
        }

        Directory.Delete(root, recursive: true);
        Assert.False(Directory.Exists(root));
    }

    [Fact]
    public async Task DisposingServiceClosesAnIncompletePhysicalInventoryScan()
    {
        var root = Path.Combine(Path.GetTempPath(), $"mk8-sava-dispose-scan-{Guid.NewGuid():N}");
        var application = new SavaWebApplicationFactory(
            root,
            new NullStorageFaultInjector(),
            analyticsSink: null,
            configurationOverrides: null,
            deleteDataPath: false,
            disableMaintenance: true);
        ChunkStore chunks;
        await using (application.ConfigureAwait(true))
        {
            await application.InitializeAsync().ConfigureAwait(true);
            chunks = application.Services.GetRequiredService<ChunkStore>();
            Assert.Null(chunks.ScanPhysicalUsageBatch(maximumEntries: 1));
            Assert.True(chunks.IsPhysicalUsageScanInProgress);
        }

        Assert.False(chunks.IsPhysicalUsageScanInProgress);
        Directory.Delete(root, recursive: true);
        Assert.False(Directory.Exists(root));
    }

    [Fact]
    public async Task ConcurrentPhysicalInventoryBatchesPublishOnlyCompleteSamples()
    {
        var root = Path.Combine(Path.GetTempPath(), $"mk8-sava-concurrent-scan-{Guid.NewGuid():N}");
        var application = new SavaWebApplicationFactory(
            root,
            new NullStorageFaultInjector(),
            analyticsSink: null,
            configurationOverrides: null,
            deleteDataPath: true,
            disableMaintenance: true);
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var paths = application.Services.GetRequiredService<StoragePaths>();
        for (var index = 0; index < 64; index++)
        {
            var path = Path.Combine(paths.Staging,
                $"inventory-{index:D3}.tmp");
            await File.WriteAllTextAsync(path, "x").ConfigureAwait(true);
        }

        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var completedSamples = 0;
        var workers = Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
        {
            for (var pass = 0; pass < 256; pass++)
            {
                var usage = chunks.ScanPhysicalUsageBatch(maximumEntries: 1);
                if (usage is null)
                    continue;
                Assert.Equal(64L, usage.StagingBytes);
                Interlocked.Increment(ref completedSamples);
            }
        })).ToArray();
        await Task.WhenAll(workers).ConfigureAwait(true);
        Assert.True(Volatile.Read(ref completedSamples) > 0);
    }

    [Fact]
    public async Task XunitFixtureDisposalRemovesItsTemporaryRoot()
    {
        var root = Path.Combine(Path.GetTempPath(), $"mk8-sava-fixture-dispose-{Guid.NewGuid():N}");
        var application = new SavaWebApplicationFactory(root, deleteDataPath: true);
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);

        await ((IAsyncLifetime)application).DisposeAsync().ConfigureAwait(true);
        Assert.False(Directory.Exists(root));
    }

    [Fact]
    public async Task PublishFilePreservesNoOverwriteAndAtomicReplacementSemantics()
    {
        var root = Path.Combine(Path.GetTempPath(), $"mk8-sava-publish-file-{Guid.NewGuid():N}");
        StorageDurability.EnsureDirectory(root);
        try
        {
            var source = Path.Combine(root, "source.tmp");
            var destination = Path.Combine(root, "destination.chunk");
            await File.WriteAllTextAsync(source, "first");
            StorageDurability.PublishFile(source, destination, overwrite: false);
            Assert.False(File.Exists(source));
            Assert.Equal("first", await File.ReadAllTextAsync(destination));

            await File.WriteAllTextAsync(source, "second");
            Assert.ThrowsAny<IOException>(() =>
                StorageDurability.PublishFile(source, destination, overwrite: false));
            Assert.Equal("first", await File.ReadAllTextAsync(destination));
            Assert.Equal("second", await File.ReadAllTextAsync(source));

            StorageDurability.PublishFile(source, destination, overwrite: true);
            Assert.False(File.Exists(source));
            Assert.Equal("second", await File.ReadAllTextAsync(destination));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task PublishDirectoryMovesCompleteTreeWithoutReplacingAnExistingTarget()
    {
        var root = Path.Combine(Path.GetTempPath(), $"mk8-sava-publish-directory-{Guid.NewGuid():N}");
        var source = Path.Combine(root, "staging");
        var destination = Path.Combine(root, "published");
        StorageDurability.EnsureDirectory(Path.Combine(source, "chunks"));
        try
        {
            await File.WriteAllTextAsync(Path.Combine(source, "chunks", "one.chunk"), "exact bytes");
            StorageDurability.PublishDirectory(source, destination);
            Assert.False(Directory.Exists(source));
            Assert.Equal("exact bytes", await File.ReadAllTextAsync(
                Path.Combine(destination, "chunks", "one.chunk")));

            StorageDurability.EnsureDirectory(source);
            Assert.ThrowsAny<IOException>(() => StorageDurability.PublishDirectory(source, destination));
            Assert.True(Directory.Exists(source));
            Assert.True(Directory.Exists(destination));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
