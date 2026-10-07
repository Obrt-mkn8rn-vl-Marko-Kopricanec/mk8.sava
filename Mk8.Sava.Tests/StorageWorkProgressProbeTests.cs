using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageWorkProgressProbeTests
{
    [Fact]
    public async Task ProgressSnapshotsRemainOwnedAndBoundedAcrossIndependentCopyStages()
    {
        using var admission = new StorageWorkAdmission(new SavaOptions());
        var progress = new StorageWorkProgressProbe(admission, 2, TimeSpan.FromHours(1), CancellationToken.None);
        await using var disposal = progress.ConfigureAwait(true);
        progress.SetSourcePhase(StorageWorkProgressProbe.SourcePhase.Stored);
        progress.SetCopyPhase(0, StorageWorkProgressProbe.CopyPhase.Copying);
        var original = progress.Samples[^1];
        progress.SetCopyPhase(0, StorageWorkProgressProbe.CopyPhase.Reading);
        progress.SetCopyPhase(1, StorageWorkProgressProbe.CopyPhase.Verified);
        for (var capture = 0; capture < StorageWorkProgressProbe.MaximumSamples + 2; capture++)
            progress.Capture();
        Assert.Equal(StorageWorkProgressProbe.MaximumSamples, progress.Samples.Count);
        Assert.Equal(StorageWorkProgressProbe.CopyPhase.Copying, original.Copies[0]);
        Assert.Equal(StorageWorkProgressProbe.CopyPhase.Pending, original.Copies[1]);
        Assert.All(progress.Samples, sample =>
        {
            Assert.True(sample.Sequence > original.Sequence);
            Assert.True(sample.End >= sample.Start);
            Assert.Equal(StorageWorkProgressProbe.CopyPhase.Reading, sample.Copies[0]);
            Assert.Equal(StorageWorkProgressProbe.CopyPhase.Verified, sample.Copies[1]);
        });
        Assert.Throws<NotSupportedException>(() => ((IList<StorageWorkProgressProbe.CopyPhase>)original.Copies)[0] = StorageWorkProgressProbe.CopyPhase.Verified);
        Assert.Throws<NotSupportedException>(() => ((IList<StorageWorkProgressProbe.Snapshot>)progress.Samples).Clear());
    }

    [Fact]
    public async Task AlreadyCanceledOperationDoesNotInventHistoricalSamples()
    {
        using var admission = new StorageWorkAdmission(new SavaOptions());
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        var progress = new StorageWorkProgressProbe(admission, 1, TimeSpan.FromMilliseconds(50), cancellation.Token);
        await using var disposal = progress.ConfigureAwait(true);
        progress.SetSourcePhase(StorageWorkProgressProbe.SourcePhase.Storing);
        using var observation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await progress.WaitForFirstTickOrStopAsync(observation.Token);
        Assert.Empty(progress.Samples);
        Assert.Contains("No pre-cancellation sample was captured.", progress.RenderDiagnostics(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task DisposingSamplerJoinsObservationWithoutCancelingOrDisposingBorrowedWork()
    {
        using var admission = new StorageWorkAdmission(new SavaOptions());
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var progress = new StorageWorkProgressProbe(admission, 1, TimeSpan.FromHours(1), cancellation.Token);
        var count = progress.Samples.Count;
        await progress.DisposeAsync();
        await progress.WaitForFirstTickOrStopAsync(cancellation.Token);
        await progress.DisposeAsync();
        Assert.Equal(count, progress.Samples.Count);
        Assert.False(cancellation.IsCancellationRequested);
        using var lease = await admission.AcquireWriteAsync(cancellation.Token);
        Assert.True(lease.IsAcquired);
    }
}
