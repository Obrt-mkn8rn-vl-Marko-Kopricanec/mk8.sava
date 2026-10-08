using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StagingReclamationTests
{
    [Fact]
    public void EveryEntryConsumesBudgetAndBusyFilesDoNotStarveLaterAbandonedFiles()
    {
        using var fixture = new Fixture();
        for (var index = 0; index < 37; index++)
            fixture.Create($"fresh-{index:D2}.tmp", abandoned: false);
        for (var index = 0; index < 13; index++)
            fixture.Create($"old-{index:D2}.tmp", abandoned: true);
        using var reclaimer = new StagingReclaimer(fixture.Root);
        var examined = 0;
        var reclaimed = 0;
        StagingReclamationBatch batch;
        var passes = 0;
        do
        {
            batch = reclaimer.Advance(fixture.Cutoff, maximumEntries: 3);
            Assert.InRange(batch.ExaminedEntries, 0, 3);
            Assert.InRange(batch.ReclaimedFiles, 0, batch.ExaminedEntries);
            examined += batch.ExaminedEntries;
            reclaimed += batch.ReclaimedFiles;
            Assert.True(++passes < 30);
        }
        while (!batch.CycleCompleted);

        Assert.Equal(50, examined);
        Assert.Equal(13, reclaimed);
        Assert.Equal(37, Directory.GetFiles(fixture.Root).Length);
    }

    [Fact]
    public void LockedAndFreshFilesSurviveAndAreRevisitedAfterReleaseOrAgeChange()
    {
        using var fixture = new Fixture();
        var activePath = fixture.Create("active.tmp", abandoned: true);
        var freshPath = fixture.Create("fresh.tmp", abandoned: false);
        using var reclaimer = new StagingReclaimer(fixture.Root);
        using (var active = new FileStream(activePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        {
            Assert.Equal(0, CompleteCycle(reclaimer, fixture.Cutoff));
            Assert.True(File.Exists(activePath));
            Assert.True(File.Exists(freshPath));
        }
        Assert.Equal(1, CompleteCycle(reclaimer, fixture.Cutoff));
        Assert.False(File.Exists(activePath));
        Assert.True(File.Exists(freshPath));
        File.SetLastWriteTimeUtc(freshPath, fixture.Cutoff.UtcDateTime.AddDays(-1));
        Assert.Equal(1, CompleteCycle(reclaimer, fixture.Cutoff));
        Assert.False(File.Exists(freshPath));
    }

    [Fact]
    public void NonTemporaryEntriesAndDirectoriesConsumeBudgetWithoutRecursiveDeletion()
    {
        using var fixture = new Fixture();
        fixture.Create("keep.bin", abandoned: true);
        fixture.Create("suffix.tmp.more", abandoned: true);
        var nested = Path.Combine(fixture.Root, "directory.tmp");
        Directory.CreateDirectory(nested);
        var protectedPath = Path.Combine(nested, "old.tmp");
        File.WriteAllBytes(protectedPath, [0x5a]);
        File.SetLastWriteTimeUtc(protectedPath, fixture.Cutoff.UtcDateTime.AddDays(-1));
        using var reclaimer = new StagingReclaimer(fixture.Root);
        var examined = 0;
        StagingReclamationBatch batch;
        do
        {
            batch = reclaimer.Advance(fixture.Cutoff, maximumEntries: 1);
            Assert.InRange(batch.ExaminedEntries, 0, 1);
            Assert.Equal(0, batch.ReclaimedFiles);
            examined += batch.ExaminedEntries;
            Assert.InRange(examined, 0, 3);
        }
        while (!batch.CycleCompleted);
        Assert.Equal(3, examined);
        Assert.True(File.Exists(protectedPath));
        Assert.Equal(2, Directory.GetFiles(fixture.Root).Length);
    }

    [Fact]
    public void EndOfCycleRetiresTheEnumeratorAndFindsNewFilesInTheNextCycle()
    {
        using var fixture = new Fixture();
        using var reclaimer = new StagingReclaimer(fixture.Root);
        Assert.Equal(0, CompleteCycle(reclaimer, fixture.Cutoff));
        fixture.Create("new-old.tmp", abandoned: true);
        Assert.Equal(1, CompleteCycle(reclaimer, fixture.Cutoff));
        Assert.Empty(Directory.EnumerateFileSystemEntries(fixture.Root));
        Assert.Equal(0, CompleteCycle(reclaimer, fixture.Cutoff));
    }

    [Fact]
    public void TemporaryPatternRetainsThePlatformDirectoryApiMatchingSemantics()
    {
        using var fixture = new Fixture();
        foreach (var name in new[] { "plain.tmp", "upper.TMP", ".tmp", "space name.tmp", "unicode-分類.tmp", "suffix.tmp.more", "keep.bin" })
            fixture.Create(name, abandoned: true);
        var expected = Directory.EnumerateFiles(fixture.Root, "*.tmp", SearchOption.TopDirectoryOnly)
            .ToHashSet(StringComparer.Ordinal);
        using var reclaimer = new StagingReclaimer(fixture.Root);
        Assert.Equal(expected.Count, CompleteCycle(reclaimer, fixture.Cutoff));
        foreach (var path in expected)
            Assert.False(File.Exists(path));
        Assert.All(Directory.EnumerateFiles(fixture.Root), path => Assert.DoesNotContain(path, expected));
    }

    [Theory]
    [InlineData(MatchCasing.CaseSensitive, false)]
    [InlineData(MatchCasing.CaseInsensitive, true)]
    public void PerEntryMatchingAgreesWithBothDirectoryCasingContracts(MatchCasing casing, bool ignoreCase)
    {
        using var fixture = new Fixture();
        var names = new[] { "plain.tmp", "upper.TMP", "mixed.TmP", ".tmp", "space name.tmp", "unicode-分類.tmp", "suffix.tmp.more", "keep.bin" };
        foreach (var name in names)
            fixture.Create(name, abandoned: true);
        var expected = Directory.EnumerateFiles(fixture.Root, "*.tmp", new EnumerationOptions
        {
            MatchCasing = casing,
            MatchType = MatchType.Win32,
            AttributesToSkip = FileAttributes.None,
            IgnoreInaccessible = false,
        }).Select(Path.GetFileName).ToHashSet(StringComparer.Ordinal);

        // Exercise the production per-entry matcher against the BCL oracle on either host.
        foreach (var name in names)
            Assert.Equal(expected.Contains(name), StagingReclaimer.MatchesTemporaryFileName(name, ignoreCase));
        Assert.Equal(ignoreCase, expected.Contains("upper.TMP"));
        Assert.Equal(ignoreCase, expected.Contains("mixed.TmP"));
    }

    [Fact]
    public void DisposalRetiresAPartialCycleAndCannotResumeReclamation()
    {
        using var fixture = new Fixture();
        for (var index = 0; index < 3; index++)
            fixture.Create($"old-{index}.tmp", abandoned: true);
        var reclaimer = new StagingReclaimer(fixture.Root);
        using var lifetime = reclaimer;
        var first = reclaimer.Advance(fixture.Cutoff, maximumEntries: 1);
        Assert.Equal(1, first.ReclaimedFiles);
        Assert.False(first.CycleCompleted);
        reclaimer.Dispose();
        reclaimer.Dispose();
        Assert.Throws<ObjectDisposedException>(() => reclaimer.Advance(fixture.Cutoff, maximumEntries: 1));
        Assert.Equal(2, Directory.GetFiles(fixture.Root).Length);
    }

    [Fact]
    public void FailedEnumerationResetsBeforeARestoredDirectoryIsScanned()
    {
        using var fixture = new Fixture();
        var directory = Path.Combine(fixture.Root, "missing");
        using var reclaimer = new StagingReclaimer(directory);
        Assert.Throws<DirectoryNotFoundException>(() => reclaimer.Advance(fixture.Cutoff, maximumEntries: 1));
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "old.tmp");
        File.WriteAllBytes(path, [0x5a]);
        File.SetLastWriteTimeUtc(path, fixture.Cutoff.UtcDateTime.AddDays(-1));
        Assert.Equal(1, CompleteCycle(reclaimer, fixture.Cutoff));
        Assert.False(File.Exists(path));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void InvalidBudgetsCannotOpenAnEnumeratorOrDeleteFiles(int budget)
    {
        using var fixture = new Fixture();
        var path = fixture.Create("old.tmp", abandoned: true);
        using var reclaimer = new StagingReclaimer(fixture.Root);
        Assert.Throws<ArgumentOutOfRangeException>(() => reclaimer.Advance(fixture.Cutoff, budget));
        Assert.True(File.Exists(path));
        Assert.Equal(1, CompleteCycle(reclaimer, fixture.Cutoff));
    }

    [Fact]
    public void LinuxLinksAreCountedButNeitherTargetsNorLinksAreReclaimed()
    {
        if (!OperatingSystem.IsLinux())
            return;
        using var fixture = new Fixture();
        var external = Directory.CreateTempSubdirectory("sava-reclamation-outside-");
        try
        {
            var target = Path.Combine(external.FullName, "old.tmp");
            File.WriteAllBytes(target, [0x5a]);
            File.SetLastWriteTimeUtc(target, fixture.Cutoff.UtcDateTime.AddDays(-1));
            var fileLink = Path.Combine(fixture.Root, "file.tmp");
            var directoryLink = Path.Combine(fixture.Root, "directory.tmp");
            File.CreateSymbolicLink(fileLink, target);
            Directory.CreateSymbolicLink(directoryLink, external.FullName);
            using var reclaimer = new StagingReclaimer(fixture.Root);
            Assert.Equal(0, CompleteCycle(reclaimer, fixture.Cutoff));
            Assert.True(File.Exists(target));
            Assert.True(File.Exists(fileLink));
            Assert.True(Directory.Exists(directoryLink));
        }
        finally
        {
            external.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task ChunkStoreCallerKeepsTheBudgetAcrossPassesAndPreservesActiveFiles()
    {
        var application = new SavaWebApplicationFactory(
            Path.Combine(Path.GetTempPath(), $"sava-reclamation-caller-{Guid.NewGuid():N}"),
            new NullStorageFaultInjector(), analyticsSink: null, configurationOverrides: null,
            deleteDataPath: true, disableMaintenance: true);
        await using var lifetime = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(true);
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var staging = Path.Combine(application.DataPath, "staging");
        var cutoff = DateTimeOffset.UtcNow.AddHours(-1);
        for (var index = 0; index < 5; index++)
        {
            var path = Path.Combine(staging, $"old-{index}.tmp");
            await File.WriteAllBytesAsync(path, [0x5a]).ConfigureAwait(true);
            File.SetLastWriteTimeUtc(path, cutoff.UtcDateTime.AddDays(-1));
        }
        var activePath = Path.Combine(staging, "active.tmp");
        await using (var active = new FileStream(activePath, FileMode.CreateNew, FileAccess.ReadWrite,
            FileShare.None, bufferSize: 4096, FileOptions.Asynchronous).ConfigureAwait(false))
        {
            File.SetLastWriteTimeUtc(activePath, cutoff.UtcDateTime.AddDays(-1));
            var deleted = 0;
            for (var pass = 0; pass < 12; pass++)
            {
                var count = chunks.DeleteAbandonedStagingFiles(cutoff, maximumFiles: 1);
                Assert.InRange(count, 0, 1);
                deleted += count;
                Assert.True(File.Exists(activePath));
            }
            Assert.Equal(5, deleted);
        }
        var reclaimed = 0;
        for (var pass = 0; pass < 12; pass++)
            reclaimed += chunks.DeleteAbandonedStagingFiles(cutoff, maximumFiles: 1);
        Assert.Equal(1, reclaimed);
        Assert.False(File.Exists(activePath));
    }

    private static int CompleteCycle(StagingReclaimer reclaimer, DateTimeOffset cutoff)
    {
        var deleted = 0;
        var passes = 0;
        StagingReclamationBatch batch;
        do
        {
            batch = reclaimer.Advance(cutoff, maximumEntries: 1);
            Assert.InRange(batch.ExaminedEntries, 0, 1);
            Assert.InRange(batch.ReclaimedFiles, 0, batch.ExaminedEntries);
            deleted += batch.ReclaimedFiles;
            Assert.True(++passes < 100);
        }
        while (!batch.CycleCompleted);
        return deleted;
    }

    private sealed class Fixture : IDisposable
    {
        private readonly DirectoryInfo _directory = Directory.CreateTempSubdirectory("sava-reclamation-");
        internal string Root => _directory.FullName;
        internal DateTimeOffset Cutoff { get; } = DateTimeOffset.UtcNow.AddHours(-1);

        internal string Create(string name, bool abandoned)
        {
            var path = Path.Combine(Root, name);
            File.WriteAllBytes(path, [0x5a]);
            File.SetLastWriteTimeUtc(path, Cutoff.UtcDateTime.AddDays(abandoned ? -1 : 1));
            return path;
        }

        public void Dispose() => _directory.Delete(recursive: true);
    }
}
