using System.Runtime.ExceptionServices;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageCopyCheckpointProbeTests
{
    [Fact]
    public void UnarmedOrUnrelatedSeamsCannotInventCopyHistory()
    {
        var probe = new StorageCopyCheckpointProbe();
        probe.Inject(StorageFaultPoint.DuringChunkStagingWrite);
        Assert.Empty(probe.Checkpoints);
        Assert.Throws<InvalidOperationException>(probe.SourceStored);
        Assert.Throws<InvalidOperationException>(() => probe.RenderDiagnostics());
        probe.Start(CancellationToken.None);
        foreach (var point in new[]
        {
            StorageFaultPoint.BeforeBlobMetadataCommit, StorageFaultPoint.AfterBlobMetadataCommit,
            StorageFaultPoint.BeforePackMetadataCommit, StorageFaultPoint.AfterPackMetadataCommit,
            StorageFaultPoint.BeforeGarbageCollectionDelete
        })
            probe.Inject(point);
        Assert.Empty(probe.Checkpoints);
        Assert.Contains("No pre-cancellation storage seam was captured.", probe.RenderDiagnostics(), StringComparison.Ordinal);
        Assert.Throws<InvalidOperationException>(() => probe.Start(CancellationToken.None));
        probe.Inject(StorageFaultPoint.DuringChunkStagingWrite);
        Assert.Equal(1, Assert.Single(probe.Checkpoints).Sequence);
    }

    [Fact]
    public void RetainedHistoryIsOwnedAndBoundedWithoutResettingCumulativeReaches()
    {
        var probe = new StorageCopyCheckpointProbe();
        probe.Start(CancellationToken.None);
        probe.Inject(StorageFaultPoint.DuringChunkStagingWrite);
        var originalView = probe.Checkpoints;
        var original = Assert.Single(originalView);
        probe.SourceStored();
        for (var iteration = 0; iteration < StorageCopyCheckpointProbe.MaximumCheckpoints; iteration++)
        {
            probe.Inject(StorageFaultPoint.DuringChunkStagingWrite);
            probe.Inject(StorageFaultPoint.BeforeChunkPublication);
            probe.Inject(StorageFaultPoint.DuringPackRecordAppend);
        }
        Assert.Equal(StorageCopyCheckpointProbe.MaximumCheckpoints, probe.Checkpoints.Count);
        Assert.Equal(StorageCopyCheckpointProbe.Work.Source, original.Work);
        Assert.Equal(1, original.StagingReached);
        Assert.Equal(0, original.BeforePublicationReached);
        Assert.Equal(0, original.PackAppendReached);
        Assert.Single(originalView);
        Assert.Throws<NotSupportedException>(() => ((IList<StorageCopyCheckpointProbe.Checkpoint>)originalView).Clear());
        Assert.All(probe.Checkpoints, checkpoint =>
        {
            Assert.Equal(StorageCopyCheckpointProbe.Work.Copies, checkpoint.Work);
            Assert.True(checkpoint.Sequence > original.Sequence);
            Assert.True(checkpoint.End >= checkpoint.Start);
        });
        var last = probe.Checkpoints[^1];
        Assert.Equal(StorageCopyCheckpointProbe.MaximumCheckpoints + 1, last.StagingReached);
        Assert.Equal(StorageCopyCheckpointProbe.MaximumCheckpoints, last.BeforePublicationReached);
        Assert.Equal(StorageCopyCheckpointProbe.MaximumCheckpoints, last.PackAppendReached);
        Assert.Contains("not successful writes/publications or per-copy identity", probe.RenderDiagnostics(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task AlreadyCanceledOperationDoesNotCreateHistoricalCheckpoints()
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        var probe = new StorageCopyCheckpointProbe();
        probe.Start(cancellation.Token);
        probe.Inject(StorageFaultPoint.DuringChunkStagingWrite);
        probe.SourceStored();
        probe.Inject(StorageFaultPoint.BeforeChunkPublication);
        Assert.Empty(probe.Checkpoints);
    }

    [Fact]
    public async Task ConcurrentSeamReachesKeepTheirCountsWhileHistoryRemainsBounded()
    {
        var probe = new StorageCopyCheckpointProbe();
        probe.Start(CancellationToken.None);
        probe.SourceStored();
        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
        {
            for (var iteration = 0; iteration < 32; iteration++)
            {
                probe.Inject(StorageFaultPoint.DuringChunkStagingWrite);
                probe.Inject(StorageFaultPoint.BeforeChunkPublication);
            }
        })));
        var retained = probe.Checkpoints;
        Assert.Equal(StorageCopyCheckpointProbe.MaximumCheckpoints, retained.Count);
        Assert.Equal(256, retained[^1].StagingReached);
        Assert.Equal(256, retained[^1].BeforePublicationReached);
        Assert.Equal(512, retained[^1].Sequence);
        Assert.Equal(retained.Select(checkpoint => checkpoint.Sequence).Order(), retained.Select(checkpoint => checkpoint.Sequence));
    }

    [Theory]
    [InlineData(StorageFaultPoint.DuringChunkStagingWrite)]
    [InlineData(StorageFaultPoint.BeforeChunkPublication)]
    public async Task ActualCopyFailureRetainsReachedSeamsNotSuccessfulPublications(StorageFaultPoint failingPoint)
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var probe = new StorageCopyCheckpointProbe();
        probe.Start(cancellation.Token);
        var original = new IOException("controlled copy seam failure");
        var injector = new ObservingInterruption(probe, failingPoint, () => throw original);
        await using var application = CreateApplication(injector);
        await application.InitializeAsync();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var encryption = new BlobEncryption(null, null);
        using var input = new MemoryStream(CreateNonzeroBytes(4096), writable: false);
        using var source = await chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName, encryption, input, cancellation.Token);
        probe.SourceStored();
        injector.Arm();

        var failure = await Record.ExceptionAsync(async () =>
        {
            using var copied = await chunks.CopyToDomainPinnedAsync(
                SavaWebApplicationFactory.SecondAccountName, encryption, encryption, source.Manifest, cancellation.Token).ConfigureAwait(false);
        });

        if (failure is not null && CatastrophicExceptionPolicy.Contains(failure))
            ExceptionDispatchInfo.Throw(failure);
        Assert.Same(original, failure);
        var copyHistory = probe.Checkpoints.Where(checkpoint => checkpoint.Work == StorageCopyCheckpointProbe.Work.Copies).ToArray();
        Assert.Equal(failingPoint, copyHistory[^1].Point);
        Assert.Equal(failingPoint == StorageFaultPoint.DuringChunkStagingWrite ? 1 : 2, copyHistory.Length);
        Assert.Equal(2, copyHistory[^1].StagingReached);
        Assert.Equal(failingPoint == StorageFaultPoint.DuringChunkStagingWrite ? 1 : 2, copyHistory[^1].BeforePublicationReached);
        Assert.Empty(Directory.EnumerateFiles(Path.Combine(application.DataPath, "staging")));
        AssertNoDestinationChunks(application);
        Assert.Contains($"point={failingPoint}", probe.RenderDiagnostics(), StringComparison.Ordinal);
        await AssertHistorySurvivesCancellationAsync(probe, cancellation);
    }

    [Fact]
    public async Task ActualCopyCancellationCannotAddPostCancellationPublicationHistory()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var probe = new StorageCopyCheckpointProbe();
        probe.Start(cancellation.Token);
        var injector = new ObservingInterruption(probe, StorageFaultPoint.DuringChunkStagingWrite, cancellation.Cancel);
        await using var application = CreateApplication(injector);
        await application.InitializeAsync();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var encryption = new BlobEncryption(null, null);
        using var input = new MemoryStream(CreateNonzeroBytes(4096), writable: false);
        using var source = await chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName, encryption, input, cancellation.Token);
        probe.SourceStored();
        injector.Arm();

        var failure = await Record.ExceptionAsync(async () =>
        {
            using var copied = await chunks.CopyToDomainPinnedAsync(
                SavaWebApplicationFactory.SecondAccountName, encryption, encryption, source.Manifest, cancellation.Token).ConfigureAwait(false);
        });

        if (failure is not null && CatastrophicExceptionPolicy.Contains(failure))
            ExceptionDispatchInfo.Throw(failure);
        var canceled = Assert.IsAssignableFrom<OperationCanceledException>(failure);
        Assert.Equal(cancellation.Token, canceled.CancellationToken);
        Assert.Equal(StorageFaultPoint.DuringChunkStagingWrite, probe.Checkpoints[^1].Point);
        Assert.Equal(1, probe.Checkpoints[^1].BeforePublicationReached);
        Assert.Empty(Directory.EnumerateFiles(Path.Combine(application.DataPath, "staging")));
        AssertNoDestinationChunks(application);
        await AssertHistorySurvivesCancellationAsync(probe, cancellation);
    }

    [Fact]
    public async Task SuccessfulZeroCopiesCanHaveNoPhysicalStorageSeams()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var probe = new StorageCopyCheckpointProbe();
        probe.Start(cancellation.Token);
        await using var application = CreateApplication(probe);
        await application.InitializeAsync();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var encryption = new BlobEncryption(null, null);
        var bytes = new byte[4096];
        using var input = new MemoryStream(bytes, writable: false);
        using var source = await chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName, encryption, input, cancellation.Token);
        Assert.Equal(SavaWebApplicationFactory.AccountName + "/$zero", Assert.Single(source.Manifest.Chunks).Id);
        probe.SourceStored();
        using var copied = await chunks.CopyToDomainPinnedAsync(
            SavaWebApplicationFactory.SecondAccountName, encryption, encryption, source.Manifest, cancellation.Token);
        Assert.Equal(bytes, await chunks.ReadAllAsync(copied.Manifest, encryption, cancellation.Token));
        Assert.Empty(probe.Checkpoints);
        Assert.Contains("No pre-cancellation storage seam was captured.", probe.RenderDiagnostics(), StringComparison.Ordinal);
        AssertNoDestinationChunks(application);
    }

    [Fact]
    public async Task SuccessfulPackedStoreRecordsAppendReachSeparatelyFromPublication()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var probe = new StorageCopyCheckpointProbe();
        probe.Start(cancellation.Token);
        await using var application = CreateApplication(probe, packing: true);
        await application.InitializeAsync();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var bytes = CreateNonzeroBytes(1024);
        var encryption = new BlobEncryption(null, null);
        using var input = new MemoryStream(bytes, writable: false);
        using var source = await chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName, encryption, input, cancellation.Token);
        Assert.Equal(bytes, await chunks.ReadAllAsync(source.Manifest, encryption, cancellation.Token));
        Assert.Equal(new[]
        {
            StorageFaultPoint.DuringChunkStagingWrite,
            StorageFaultPoint.BeforeChunkPublication,
            StorageFaultPoint.DuringPackRecordAppend
        }, probe.Checkpoints.Select(checkpoint => checkpoint.Point));
        Assert.Equal(1, probe.Checkpoints[^1].StagingReached);
        Assert.Equal(1, probe.Checkpoints[^1].BeforePublicationReached);
        Assert.Equal(1, probe.Checkpoints[^1].PackAppendReached);
        Assert.Empty(Directory.EnumerateFiles(Path.Combine(application.DataPath, "staging")));
    }

    private static byte[] CreateNonzeroBytes(int length)
    {
        var bytes = new byte[length];
        bytes.AsSpan().Fill(0x6b);
        return bytes;
    }

    private static void AssertNoDestinationChunks(SavaWebApplicationFactory application)
    {
        var path = Path.Combine(application.DataPath, "chunks", SavaWebApplicationFactory.SecondAccountName);
        Assert.False(Directory.Exists(path) && Directory.EnumerateFiles(path, "*.chunk", SearchOption.AllDirectories).Any());
    }

    private static async Task AssertHistorySurvivesCancellationAsync(
        StorageCopyCheckpointProbe probe, CancellationTokenSource cancellation)
    {
        var before = probe.Checkpoints;
        await cancellation.CancelAsync().ConfigureAwait(false);
        probe.Inject(StorageFaultPoint.BeforeChunkPublication);
        probe.Inject(StorageFaultPoint.DuringPackRecordAppend);
        Assert.Equal(before, probe.Checkpoints);
    }

    private static SavaWebApplicationFactory CreateApplication(IStorageFaultInjector injector, bool packing = false) => new(
        Path.Combine(Path.GetTempPath(), $"mk8-sava-checkpoint-controls-{Guid.NewGuid():N}"), injector,
        analyticsSink: null,
        configurationOverrides: new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:EnableSmallChunkPacking"] = packing ? "true" : "false"
        }, deleteDataPath: true, disableMaintenance: true);

    private sealed class ObservingInterruption(
        StorageCopyCheckpointProbe probe, StorageFaultPoint failingPoint, Action interrupt) : IStorageFaultInjector
    {
        private int _armed;

        internal void Arm() => Volatile.Write(ref _armed, 1);

        public void Inject(StorageFaultPoint point)
        {
            probe.Inject(point);
            if (point == failingPoint && Volatile.Read(ref _armed) != 0)
                interrupt();
        }
    }
}
