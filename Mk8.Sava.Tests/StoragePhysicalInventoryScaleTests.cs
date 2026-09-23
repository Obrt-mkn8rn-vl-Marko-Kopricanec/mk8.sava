using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class StoragePhysicalInventoryScaleTests(ITestOutputHelper output)
{
    private const int EntriesPerPass = 1024;
    private const int ConcurrentWriteCount = 512;

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

    [Fact]
    public async Task FiftyThousandChunkScanKeepsConcurrentStagingMutationsResponsive()
    {
        var application = CreateApplication();
        try
        {
            await application.InitializeAsync();
            var paths = application.Services.GetRequiredService<StoragePaths>();
            CreateShardedChunks(paths.Chunks, shardCount: 50, filesPerShard: 1000);

            using var scanner = new StoragePhysicalInventoryScanner(paths);
            var measured = await ScanWithConcurrentStagingWritesAsync(scanner, paths.Staging);
            output.WriteLine(FormattableString.Invariant(
                $"inventory_concurrent_writes,passes={measured.Passes},overlapping_passes={measured.OverlappingPasses},max_pass_ms={measured.MaxPass.TotalMilliseconds:F3},p99_mutation_ms={measured.P99Write.TotalMilliseconds:F3}"));
            Assert.InRange(measured.Passes, 1, 110);
            Assert.True(measured.OverlappingPasses > 0, "The scan did not overlap any completed staging mutations.");
            Assert.True(measured.MaxPass <= TimeSpan.FromMilliseconds(500), "A bounded inventory pass exceeded 500 ms.");
            Assert.True(measured.P99Write <= TimeSpan.FromMilliseconds(250), "Staging mutation p99 exceeded 250 ms.");
            Assert.Equal(50_000, scanner.ToPhysicalUsage(packedChunkCount: 0).ChunkCount);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    private static async Task<ConcurrentScanMeasurements> ScanWithConcurrentStagingWritesAsync(
        StoragePhysicalInventoryScanner scanner, string stagingDirectory)
    {
        using var firstPass = new ManualResetEventSlim(false);
        var writesCompleted = new int[1];
        var writer = Task.Run(() => WriteStagingFiles(firstPass, stagingDirectory, writesCompleted));
        var scan = Task.Run(() => ScanWhileWriting(scanner, firstPass, writesCompleted));
        await Task.WhenAll(writer, scan).ConfigureAwait(false);
        var latencies = await writer.ConfigureAwait(false);
        var scanResult = await scan.ConfigureAwait(false);
        Array.Sort(latencies);
        return new ConcurrentScanMeasurements(
            scanResult.Passes,
            scanResult.OverlappingPasses,
            scanResult.MaxPass,
            latencies[(int)Math.Ceiling(latencies.Length * 0.99) - 1]);
    }

    private static (int Passes, int OverlappingPasses, TimeSpan MaxPass) ScanWhileWriting(
        StoragePhysicalInventoryScanner scanner, ManualResetEventSlim firstPass, int[] writesCompleted)
    {
        var passes = 0;
        var overlappingPasses = 0;
        var maximum = TimeSpan.Zero;
        try
        {
            bool complete;
            do
            {
                var started = Stopwatch.GetTimestamp();
                complete = scanner.Advance(EntriesPerPass);
                var elapsed = Stopwatch.GetElapsedTime(started);
                if (elapsed > maximum)
                    maximum = elapsed;
                if (++passes == 1)
                    firstPass.Set();
                var completed = Volatile.Read(ref writesCompleted[0]);
                if (completed is > 0 and < ConcurrentWriteCount)
                    overlappingPasses++;
                Assert.InRange(scanner.LastPassSteps, 1, EntriesPerPass);
                Assert.True(passes <= 110, "The concurrent scan did not finish within the pass budget.");
            }
            while (!complete);
            return (passes, overlappingPasses, maximum);
        }
        finally
        {
            firstPass.Set();
        }
    }

    private static TimeSpan[] WriteStagingFiles(
        ManualResetEventSlim firstPass, string stagingDirectory, int[] writesCompleted)
    {
        firstPass.Wait();
        var latencies = new TimeSpan[ConcurrentWriteCount];
        ReadOnlySpan<byte> content = [0x5a];
        for (var index = 0; index < ConcurrentWriteCount; index++)
        {
            var path = Path.Combine(stagingDirectory,
                $"inventory-writer-{index.ToString("D4", System.Globalization.CultureInfo.InvariantCulture)}.tmp");
            var started = Stopwatch.GetTimestamp();
            using (var file = new FileStream(path, FileMode.CreateNew, FileAccess.Write,
                       FileShare.ReadWrite | FileShare.Delete, bufferSize: 4096, FileOptions.WriteThrough))
            {
                file.Write(content);
                file.Flush(flushToDisk: true);
            }
            File.Delete(path);
            latencies[index] = Stopwatch.GetElapsedTime(started);
            Volatile.Write(ref writesCompleted[0], index + 1);
        }
        return latencies;
    }

    private sealed record ConcurrentScanMeasurements(
        int Passes, int OverlappingPasses, TimeSpan MaxPass, TimeSpan P99Write);

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
