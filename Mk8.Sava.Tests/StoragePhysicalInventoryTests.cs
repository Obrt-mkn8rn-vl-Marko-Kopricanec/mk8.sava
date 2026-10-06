using System.Diagnostics;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class StoragePhysicalInventoryTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ClassificationPreservesBoundariesSuffixCaseAndTopLevelStaging(bool alternateRoot)
    {
        using var fixture = new InventoryFixture(alternateRoot);
        var paths = fixture.Paths;
        WriteFile(Path.Combine(paths.Chunks, "shard", "payload.chunk"), 3);
        WriteFile(Path.Combine(paths.Chunks, "direct.chunk"), 5);
        WriteFile(Path.Combine(paths.Packs, "shard", "payload.pack"), 7);
        WriteFile(Path.Combine(paths.Staging, "metadata.db"), 11);
        WriteFile(paths.Database, 23);
        WriteFile(paths.Database + "-wal", 29);
        WriteFile(paths.Database + "-shm", 31);

        WriteFile(Path.Combine(paths.Chunks, "wrong.CHUNK"), 101);
        WriteFile(Path.Combine(paths.Chunks, "suffix.chunk.tmp"), 103);
        WriteFile(Path.Combine(paths.Packs, "wrong.PACK"), 107);
        WriteFile(Path.Combine(paths.Staging, "nested", "hidden.tmp"), 109);
        WriteFile(Path.Combine(paths.Root, "chunks-lookalike", "payload.chunk"), 113);
        WriteFile(Path.Combine(paths.Root, "packs-lookalike", "payload.pack"), 127);
        WriteFile(Path.Combine(paths.Root, "staging-lookalike", "payload.tmp"), 131);
        WriteFile(paths.Database + "-wal-extra", 137);
        WriteFile(Path.Combine(paths.Root, "other", "metadata.db"), 139);
        WriteFile(Path.Combine(paths.Root, "other", "outside.chunk"), 149);
        Directory.CreateDirectory(Path.Combine(paths.Chunks, "directory.chunk"));

        using var scanner = new StoragePhysicalInventoryScanner(paths);
        Assert.Throws<InvalidOperationException>(() => scanner.ToPhysicalUsage(packedChunkCount: 7));
        var passes = 0;
        while (!scanner.Advance(maximumSteps: 1))
        {
            Assert.Equal(1, scanner.LastPassSteps);
            Assert.True(++passes < 128, "The classification fixture did not complete its one-step scan.");
        }
        var usage = scanner.ToPhysicalUsage(packedChunkCount: 7);
        Assert.Equal(15, usage.ChunkBytes);
        Assert.Equal(9, usage.ChunkCount);
        Assert.Equal(11, usage.StagingBytes);
        Assert.Equal(83, usage.MetadataBytes);
        Assert.True(scanner.Advance(maximumSteps: 1));
        Assert.Equal(0, scanner.LastPassSteps);
        Assert.Equal(usage, scanner.ToPhysicalUsage(packedChunkCount: 7));
        if (!OperatingSystem.IsLinux())
            Assert.Null(usage.AllocatedRootBytes);
    }

    [Fact]
    public void MixedInventoryHasABoundedMeasuredAllocationCost()
    {
        const int filesPerCategory = 1024;
        const int entriesPerPass = 1024;
        const long allocationBudget = 8 * 1024 * 1024;
        using var fixture = new InventoryFixture();
        var paths = fixture.Paths;
        CreateFiles(Path.Combine(paths.Chunks, "shard"), filesPerCategory, ".chunk");
        CreateFiles(Path.Combine(paths.Packs, "shard"), filesPerCategory, ".pack");
        CreateFiles(paths.Staging, filesPerCategory, ".tmp");
        CreateFiles(Path.Combine(paths.Root, "unclassified"), filesPerCategory, ".tmp");
        _ = Scan(paths, entriesPerPass, out _);

        for (var repetition = 0; repetition < 3; repetition++)
        {
            var thread = Environment.CurrentManagedThreadId;
            var before = GC.GetAllocatedBytesForCurrentThread();
            var started = Stopwatch.GetTimestamp();
            var usage = Scan(paths, entriesPerPass, out var passes);
            var elapsed = Stopwatch.GetElapsedTime(started);
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(thread, Environment.CurrentManagedThreadId);
            Assert.Equal(2 * filesPerCategory, usage.ChunkBytes);
            Assert.Equal(filesPerCategory, usage.ChunkCount);
            Assert.Equal(filesPerCategory, usage.StagingBytes);
            Assert.Equal(0, usage.MetadataBytes);
            Assert.InRange(allocated, 0, allocationBudget);
            output.WriteLine(FormattableString.Invariant(
                $"inventory_allocation,repetition={repetition},files={4 * filesPerCategory},passes={passes},managed_bytes={allocated},elapsed_ms={elapsed.TotalMilliseconds:F3}"));
        }
    }

    [Fact]
    public void LinuxNativeAllocationQueryPreservesIdentityNoFollowAndMissingPathResults()
    {
        if (!OperatingSystem.IsLinux())
            return;
        using var fixture = new InventoryFixture();
        var path = Path.Combine(fixture.Root, "native-\u5206\u985e.bin");
        WriteFile(path, 4097);
        Assert.True(StorageAllocationMeter.TryStat(path, out var file));
        Assert.True(file.Inode > 0);
        Assert.Equal(1U, file.LinkCount);
        Assert.True(file.AllocatedBytes >= 0);
        var link = Path.Combine(fixture.Root, "link");
        File.CreateSymbolicLink(link, path);
        Assert.True(StorageAllocationMeter.TryStat(link, out var linked));
        Assert.NotEqual(file.Inode, linked.Inode);
        Assert.Equal(file.DeviceMajor, linked.DeviceMajor);
        Assert.Equal(file.DeviceMinor, linked.DeviceMinor);
        Assert.False(StorageAllocationMeter.TryStat(Path.Combine(fixture.Root, "missing"), out var missing));
        Assert.Equal(default, missing);
        Assert.False(StorageAllocationMeter.TryStat(Path.Combine(path, "not-a-directory"), out var notDirectory));
        Assert.Equal(default, notDirectory);
        File.Delete(path);
        Assert.False(StorageAllocationMeter.TryStat(path, out var removed));
        Assert.Equal(default, removed);
        Assert.True(StorageAllocationMeter.TryStat(link, out var dangling));
        Assert.Equal(linked, dangling);
    }

    [Fact]
    public void LinuxNativeAllocationQueryHasABoundedMeasuredAllocationCost()
    {
        if (!OperatingSystem.IsLinux())
            return;
        const int queries = 1000;
        const long allocationBudget = 512 * 1024;
        using var fixture = new InventoryFixture();
        var path = Path.Combine(fixture.Root, "native-\u5206\u985e.bin");
        WriteFile(path, 4097);
        Assert.True(StorageAllocationMeter.TryStat(path, out var expected));
        for (var warmup = 0; warmup < 10; warmup++)
            Assert.True(StorageAllocationMeter.TryStat(path, out _));

        for (var repetition = 0; repetition < 3; repetition++)
        {
            var thread = Environment.CurrentManagedThreadId;
            var before = GC.GetAllocatedBytesForCurrentThread();
            var started = Stopwatch.GetTimestamp();
            for (var query = 0; query < queries; query++)
            {
                if (!StorageAllocationMeter.TryStat(path, out var actual) || actual != expected)
                    Assert.Fail("Native allocation query changed the stable fixture's measured identity or blocks.");
            }
            var elapsed = Stopwatch.GetElapsedTime(started);
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(thread, Environment.CurrentManagedThreadId);
            Assert.InRange(allocated, 0, allocationBudget);
            output.WriteLine(FormattableString.Invariant(
                $"native_stat_allocation,repetition={repetition},queries={queries},managed_bytes={allocated},elapsed_ms={elapsed.TotalMilliseconds:F3}"));
        }
    }

    private static StoragePhysicalUsage Scan(StoragePaths paths, int entriesPerPass, out int passes)
    {
        using var scanner = new StoragePhysicalInventoryScanner(paths);
        passes = 0;
        bool complete;
        do
        {
            complete = scanner.Advance(entriesPerPass);
            Assert.InRange(scanner.LastPassSteps, 1, entriesPerPass);
            Assert.True(++passes < 100, "The small bounded inventory did not complete.");
        }
        while (!complete);
        return scanner.ToPhysicalUsage(packedChunkCount: 0);
    }

    private static void CreateFiles(string directory, int count, string extension)
    {
        Directory.CreateDirectory(directory);
        for (var index = 0; index < count; index++)
        {
            var name = index.ToString("D4", System.Globalization.CultureInfo.InvariantCulture) + extension;
            using var file = new FileStream(Path.Combine(directory, name), FileMode.CreateNew, FileAccess.Write);
            file.Write([0x5a]);
        }
    }

    private static void WriteFile(string path, int length)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllBytes(path, new byte[length]);
    }

    private sealed class InventoryFixture : IDisposable
    {
        internal InventoryFixture(bool alternateRoot = false)
        {
            Root = Directory.CreateTempSubdirectory("sava-inventory-\u5206\u985e-").FullName;
            var configured = alternateRoot
                ? Path.Combine(Root, "..", Path.GetFileName(Root)) + Path.AltDirectorySeparatorChar
                : Root;
            if (alternateRoot && OperatingSystem.IsWindows())
                configured = configured.Replace(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            try
            {
                Paths = new StoragePaths(new TestEnvironment(Root), Options.Create(new SavaOptions { DataPath = configured }));
            }
            catch
            {
                Directory.Delete(Root, recursive: true);
                throw;
            }
        }

        internal string Root { get; }
        internal StoragePaths Paths { get; }

        public void Dispose()
        {
            Paths.Dispose();
            Directory.Delete(Root, recursive: true);
        }
    }

    private sealed class TestEnvironment(string root) : IHostEnvironment
    {
        public string EnvironmentName { get; set; } = Environments.Production;
        public string ApplicationName { get; set; } = nameof(StoragePhysicalInventoryTests);
        public string ContentRootPath { get; set; } = root;
        public IFileProvider ContentRootFileProvider { get; set; } = new NullFileProvider();
    }
}
