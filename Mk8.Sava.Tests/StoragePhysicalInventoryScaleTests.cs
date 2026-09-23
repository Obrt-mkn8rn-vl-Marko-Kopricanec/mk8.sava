using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StoragePhysicalInventoryScaleTests
{
    private const int EntriesPerPass = 1024;

    [Fact]
    public async Task FiftyThousandChunksScanWithinBoundedPassesAndAllocation()
    {
        var application = CreateApplication();
        try
        {
            await application.InitializeAsync();
            var paths = application.Services.GetRequiredService<StoragePaths>();
            CreateShardedChunks(paths.Chunks, shardCount: 50, filesPerShard: 1000);

            using var scanner = new StoragePhysicalInventoryScanner(paths);
            var passes = 0;
            bool complete;
            do
            {
                var allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
                complete = scanner.Advance(EntriesPerPass);
                var allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
                Assert.InRange(scanner.LastPassSteps, 1, EntriesPerPass);
                Assert.InRange(allocated, 0, 16 * 1024 * 1024);
                Assert.True(++passes <= 110, "The scan did not finish within the bounded pass count.");
            }
            while (!complete);

            var usage = scanner.ToPhysicalUsage(packedChunkCount: 0);
            Assert.Equal(50_000, usage.ChunkCount);
            Assert.Equal(50_000, usage.ChunkBytes);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task RemovedDirectoryDuringScanDoesNotPreventNextCompleteSample()
    {
        var application = CreateApplication();
        try
        {
            await application.InitializeAsync();
            var paths = application.Services.GetRequiredService<StoragePaths>();
            CreateShardedChunks(paths.Chunks, shardCount: 1, filesPerShard: 128);
            var churn = Path.Combine(paths.Root, "inventory-churn");
            Directory.CreateDirectory(churn);
            CreateFiles(churn, 128);

            using (var scanner = new StoragePhysicalInventoryScanner(paths))
            {
                Assert.False(scanner.Advance(maximumSteps: 8));
                Directory.Delete(churn, recursive: true);
                var passes = 1;
                while (!scanner.Advance(maximumSteps: 8))
                    Assert.True(++passes < 100);
                Assert.Equal(128, scanner.ToPhysicalUsage(packedChunkCount: 0).ChunkCount);
            }

            using var settled = new StoragePhysicalInventoryScanner(paths);
            while (!settled.Advance(EntriesPerPass))
                Assert.InRange(settled.LastPassSteps, 1, EntriesPerPass);
            Assert.Equal(128, settled.ToPhysicalUsage(packedChunkCount: 0).ChunkCount);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    private static SavaWebApplicationFactory CreateApplication() => new(
        Path.Combine(Path.GetTempPath(), $"mk8-sava-inventory-scale-{Guid.NewGuid():N}"),
        new NullStorageFaultInjector(),
        analyticsSink: null,
        configurationOverrides: null,
        deleteDataPath: true,
        disableMaintenance: true);

    private static void CreateShardedChunks(string root, int shardCount, int filesPerShard)
    {
        for (var shard = 0; shard < shardCount; shard++)
        {
            var directory = Path.Combine(root, shard.ToString("D3", System.Globalization.CultureInfo.InvariantCulture));
            Directory.CreateDirectory(directory);
            CreateFiles(directory, filesPerShard, ".chunk");
        }
    }

    private static void CreateFiles(string directory, int count, string extension = ".tmp")
    {
        ReadOnlySpan<byte> content = [0x5a];
        for (var index = 0; index < count; index++)
        {
            var name = index.ToString("D4", System.Globalization.CultureInfo.InvariantCulture) + extension;
            using var file = new FileStream(Path.Combine(directory, name), FileMode.CreateNew, FileAccess.Write);
            file.Write(content);
        }
    }
}
