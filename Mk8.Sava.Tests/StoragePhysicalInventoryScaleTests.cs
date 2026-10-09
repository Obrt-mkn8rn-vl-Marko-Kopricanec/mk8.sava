using System.Diagnostics;
using System.Runtime.InteropServices;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

[Collection("Physical inventory scale")]
public sealed class StoragePhysicalInventoryScaleTests(ITestOutputHelper output)
{
    private const int EntriesPerPass = 1024;
    private const int ConcurrentWriteCount = 512;
    private const int WritesPerPass = 8;

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
            var before = await ScanWithConcurrentStagingWritesAsync(scanner: null, paths.Staging);
            ReportControl("before", before);
            var measured = await ScanWithConcurrentStagingWritesAsync(scanner, paths.Staging);
            var after = await ScanWithConcurrentStagingWritesAsync(scanner: null, paths.Staging);
            ReportControl("after", after);
            output.WriteLine(FormattableString.Invariant(
                $"inventory_concurrent_writes,passes={measured.Passes},overlapping_passes={measured.OverlappingPasses},mutations={measured.WriteCount},max_pass_ms={measured.MaxPass.TotalMilliseconds:F3},p99_mutation_ms={measured.P99Write.TotalMilliseconds:F3}"));
            ReportMutationTail("concurrent", measured);
            Assert.InRange(measured.Passes, 1, 110);
            Assert.True(measured.OverlappingPasses > 0, "The scan did not overlap any completed staging mutations.");
            Assert.Equal(ConcurrentWriteCount, measured.WriteCount);
            Assert.True(measured.MaxPass <= TimeSpan.FromMilliseconds(500), "A bounded inventory pass exceeded 500 ms.");
            Assert.True(measured.P99Write <= TimeSpan.FromMilliseconds(250), "Staging mutation p99 exceeded 250 ms.");
            Assert.Equal(50_000, scanner.ToPhysicalUsage(packedChunkCount: 0).ChunkCount);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    private void ReportControl(string phase, ConcurrentScanMeasurements measured)
    {
        output.WriteLine(FormattableString.Invariant(
            $"inventory_control_writes,phase={phase},passes={measured.Passes},mutations={measured.WriteCount},p99_mutation_ms={measured.P99Write.TotalMilliseconds:F3}"));
        ReportMutationTail(phase, measured);
        Assert.Equal(ConcurrentWriteCount, measured.WriteCount);
    }

    private void ReportMutationTail(string phase, ConcurrentScanMeasurements measured)
    {
        output.WriteLine(FormattableString.Invariant(
            $"inventory_scan_timeline,phase={phase},timestamp_frequency={Stopwatch.Frequency},passes={measured.Timeline.Passes.Count}"));
        for (var index = 0; index < measured.Timeline.Passes.Count; index++)
        {
            var pass = measured.Timeline.Passes[index];
            output.WriteLine(FormattableString.Invariant(
                $"inventory_scan_pass,phase={phase},index={index},started_ticks={pass.Started},finished_ticks={pass.Finished}"));
        }
        // These are intersections of observed call intervals, not native execution,
        // scheduling or causality proof. Controls have no actual scanner intervals.
        // Correlated rows retain whole-operation latency, not sums of unrelated phase quantiles.
        for (var index = Math.Max(0, measured.Mutations.Length - 8); index < measured.Mutations.Length; index++)
        {
            var mutation = measured.Mutations[index];
            var total = measured.Timeline.Correlate(mutation.Started, mutation.Finished);
            var flush = measured.Timeline.Correlate(mutation.Written, mutation.Flushed);
            output.WriteLine(FormattableString.Invariant(
                $"inventory_mutation_tail,phase={phase},index={mutation.Index},total_ms={mutation.Total.TotalMilliseconds:F3},create_ms={Stopwatch.GetElapsedTime(mutation.Started, mutation.Opened).TotalMilliseconds:F3},write_ms={Stopwatch.GetElapsedTime(mutation.Opened, mutation.Written).TotalMilliseconds:F3},flush_ms={Stopwatch.GetElapsedTime(mutation.Written, mutation.Flushed).TotalMilliseconds:F3},close_ms={Stopwatch.GetElapsedTime(mutation.Flushed, mutation.Closed).TotalMilliseconds:F3},delete_ms={Stopwatch.GetElapsedTime(mutation.Closed, mutation.Finished).TotalMilliseconds:F3},started_ticks={mutation.Started},opened_ticks={mutation.Opened},written_ticks={mutation.Written},flushed_ticks={mutation.Flushed},closed_ticks={mutation.Closed},finished_ticks={mutation.Finished},scan_overlapping_passes={total.PassCount},scan_overlap_ms={total.Elapsed.TotalMilliseconds:F3},flush_scan_overlapping_passes={flush.PassCount},flush_scan_overlap_ms={flush.Elapsed.TotalMilliseconds:F3}"));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DirectoryLinksChangedBetweenPassesAreNotFollowedOnLinux(bool linkOnEvenStep)
    {
        if (!OperatingSystem.IsLinux())
            return;
        var application = CreateApplication();
        var externalRoot = Path.Combine(Path.GetTempPath(), $"mk8-sava-inventory-links-{Guid.NewGuid():N}");
        Directory.CreateDirectory(externalRoot);
        try
        {
            await application.InitializeAsync();
            var paths = application.Services.GetRequiredService<StoragePaths>();
            var target = Path.Combine(paths.Chunks, "replacement");
            var held = Path.Combine(externalRoot, "held");
            var outside = Path.Combine(externalRoot, "outside");
            Directory.CreateDirectory(target);
            Directory.CreateDirectory(outside);
            await File.WriteAllBytesAsync(Path.Combine(target, "original.chunk"), [0x5a]);
            await File.WriteAllBytesAsync(Path.Combine(outside, "foreign.chunk"), new byte[8192]);
            using (var scanner = new StoragePhysicalInventoryScanner(paths))
            {
                ScanWhileAlternatingDirectoryLink(scanner, target, held, outside, linkOnEvenStep);
                var usage = scanner.ToPhysicalUsage(packedChunkCount: 0);
                Assert.InRange(usage.ChunkBytes, 0, 1);
                Assert.InRange(usage.ChunkCount, 0, 1);
            }
            using var settled = new StoragePhysicalInventoryScanner(paths);
            var passes = 0;
            while (!settled.Advance(maximumSteps: 1))
                Assert.True(++passes < 100);
            var stable = settled.ToPhysicalUsage(packedChunkCount: 0);
            Assert.Equal(1, stable.ChunkBytes);
            Assert.Equal(1, stable.ChunkCount);
        }
        finally
        {
            await application.DisposeAsync();
            Directory.Delete(externalRoot, recursive: true);
        }
    }

    private static void ScanWhileAlternatingDirectoryLink(
        StoragePhysicalInventoryScanner scanner, string target, string held, string outside, bool linkOnEvenStep)
    {
        var linked = false;
        try
        {
            for (var step = 0; step < 100; step++)
            {
                var shouldLink = (step % 2 == 0) == linkOnEvenStep;
                if (shouldLink != linked)
                {
                    if (shouldLink)
                    {
                        Directory.Move(target, held);
                        Directory.CreateSymbolicLink(target, outside);
                    }
                    else
                    {
                        Directory.Delete(target);
                        Directory.Move(held, target);
                    }
                    linked = shouldLink;
                }
                if (scanner.Advance(maximumSteps: 1))
                    return;
            }
            Assert.Fail("Directory-link churn prevented the bounded inventory from completing.");
        }
        finally
        {
            if (linked)
            {
                Directory.Delete(target);
                Directory.Move(held, target);
            }
        }
    }

    private static async Task<ConcurrentScanMeasurements> ScanWithConcurrentStagingWritesAsync(
        StoragePhysicalInventoryScanner? scanner, string stagingDirectory)
    {
        using var rendezvous = new Barrier(2);
        using var abort = new CancellationTokenSource();
        var batchTimes = new long[2];
        var scanFinished = new int[1];
        // Dedicated workers avoid thread-pool starvation. Rendezvous waits are not timed;
        // each batch and inventory pass start in the same phase and must both finish.
        var writer = Task.Factory.StartNew(
            () => WriteStagingFiles(rendezvous, stagingDirectory, batchTimes, scanFinished, abort),
            CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        var scan = Task.Factory.StartNew(
            () => ScanWhileWriting(scanner, rendezvous, batchTimes, scanFinished, abort),
            CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        await Task.WhenAll(writer, scan).ConfigureAwait(false);
        var latencies = await writer.ConfigureAwait(false);
        var scanResult = await scan.ConfigureAwait(false);
        Assert.NotEmpty(latencies);
        Array.Sort(latencies, static (left, right) => left.Total.CompareTo(right.Total));
        return new ConcurrentScanMeasurements(
            scanResult.Passes,
            scanResult.OverlappingPasses,
            latencies.Length,
            scanResult.MaxPass,
            latencies[(int)Math.Ceiling(latencies.Length * 0.99) - 1].Total,
            latencies,
            scanResult.Timeline);
    }

    private static (int Passes, int OverlappingPasses, TimeSpan MaxPass, InventoryScanTimeline Timeline) ScanWhileWriting(
        StoragePhysicalInventoryScanner? scanner, Barrier rendezvous, long[] batchTimes,
        int[] scanFinished, CancellationTokenSource abort)
    {
        var passes = 0;
        var overlappingPasses = 0;
        var maximum = TimeSpan.Zero;
        var intervals = new InventoryScanTimeline.ScanInterval[InventoryScanTimeline.MaximumPasses];
        try
        {
            bool complete;
            do
            {
                WaitForPhase(rendezvous, abort.Token);
                var started = Stopwatch.GetTimestamp();
                complete = scanner?.Advance(EntriesPerPass) ?? passes + 1 >= ConcurrentWriteCount / WritesPerPass;
                var finished = Stopwatch.GetTimestamp();
                var elapsed = Stopwatch.GetElapsedTime(started, finished);
                if (elapsed > maximum)
                    maximum = elapsed;
                Volatile.Write(ref scanFinished[0], complete ? 1 : 0);
                WaitForPhase(rendezvous, abort.Token);
                if (batchTimes[0] < finished && batchTimes[1] > started)
                    overlappingPasses++;
                if (scanner is not null)
                    Assert.InRange(scanner.LastPassSteps, 1, EntriesPerPass);
                Assert.True(++passes <= 110, "The concurrent scan did not finish within the pass budget.");
                if (scanner is not null)
                    intervals[passes - 1] = new InventoryScanTimeline.ScanInterval(started, finished);
            }
            while (!complete);
            return (passes, overlappingPasses, maximum,
                new InventoryScanTimeline(scanner is null ? [] : intervals.AsSpan(0, passes)));
        }
        catch
        {
            abort.Cancel();
            throw;
        }
    }

    private static StagingMutation[] WriteStagingFiles(
        Barrier rendezvous, string stagingDirectory, long[] batchTimes,
        int[] scanFinished, CancellationTokenSource abort)
    {
        var latencies = new StagingMutation[ConcurrentWriteCount];
        var index = 0;
        try
        {
            do
            {
                WaitForPhase(rendezvous, abort.Token);
                batchTimes[0] = Stopwatch.GetTimestamp();
                var batchStart = index;
                var end = Math.Min(index + WritesPerPass, ConcurrentWriteCount);
                for (; index < end; index++)
                    latencies[index] = WriteStagingFile(stagingDirectory, index);
                batchTimes[1] = index > batchStart ? Stopwatch.GetTimestamp() : 0;
                WaitForPhase(rendezvous, abort.Token);
            }
            while (Volatile.Read(ref scanFinished[0]) == 0);
            return latencies[..index];
        }
        catch
        {
            abort.Cancel();
            throw;
        }
    }

    private static StagingMutation WriteStagingFile(string stagingDirectory, int index)
    {
        var path = Path.Combine(stagingDirectory,
            $"inventory-writer-{index.ToString("D4", System.Globalization.CultureInfo.InvariantCulture)}.tmp");
        var started = Stopwatch.GetTimestamp();
        long opened;
        long written;
        long flushed;
        using (var file = new FileStream(path, FileMode.CreateNew, FileAccess.Write,
                   FileShare.ReadWrite | FileShare.Delete, bufferSize: 4096, FileOptions.WriteThrough))
        {
            opened = Stopwatch.GetTimestamp();
            file.Write([0x5a]);
            written = Stopwatch.GetTimestamp();
            file.Flush(flushToDisk: true);
            flushed = Stopwatch.GetTimestamp();
        }
        var closed = Stopwatch.GetTimestamp();
        File.Delete(path);
        return new StagingMutation(index, started, opened, written, flushed, closed, Stopwatch.GetTimestamp());
    }

    private static void WaitForPhase(Barrier rendezvous, CancellationToken cancellationToken)
    {
        if (!rendezvous.SignalAndWait(TimeSpan.FromSeconds(30), cancellationToken))
            throw new TimeoutException("The inventory scan and staging writer did not rendezvous.");
    }

    private sealed record ConcurrentScanMeasurements(
        int Passes, int OverlappingPasses, int WriteCount, TimeSpan MaxPass, TimeSpan P99Write,
        StagingMutation[] Mutations, InventoryScanTimeline Timeline);

    [StructLayout(LayoutKind.Auto)]
    private readonly record struct StagingMutation(
        int Index, long Started, long Opened, long Written, long Flushed, long Closed, long Finished)
    {
        internal TimeSpan Total => Stopwatch.GetElapsedTime(Started, Finished);
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
