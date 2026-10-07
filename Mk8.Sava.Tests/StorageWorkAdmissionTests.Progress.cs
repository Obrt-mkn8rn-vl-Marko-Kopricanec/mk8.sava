using System.Security.Cryptography;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed partial class StorageWorkAdmissionTests
{
    [Fact]
    public async Task SuccessfulCopiesReportVerifiedOnlyAfterRealStorageReadsMatch()
    {
        var application = CreateApplication(8);
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var encryption = new BlobEncryption(null, null);
        var bytes = RandomNumberGenerator.GetBytes(4096);
        using var source = new MemoryStream(bytes, writable: false);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var stored = await chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName, encryption, source, cancellation.Token);
        var progress = new StorageWorkProgressProbe(chunks.Admission, 2, TimeSpan.FromSeconds(1), cancellation.Token);
        await using var progressDisposal = progress.ConfigureAwait(true);
        progress.SetSourcePhase(StorageWorkProgressProbe.SourcePhase.Stored);
        await Task.WhenAll(Enumerable.Range(0, 2).Select(copy =>
            CopyAndAssertAsync(chunks, stored.Manifest, encryption, bytes, progress, copy, cancellation.Token)));
        Assert.All(progress.Samples[^1].Copies, phase => Assert.Equal(StorageWorkProgressProbe.CopyPhase.Verified, phase));
        Assert.Contains("mk8_sava_storage_work_active{lane=\"writes\"} 0\n", chunks.Admission.RenderPrometheus(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task CopyByteMismatchRemainsAFailureAndIsNotReportedAsVerified()
    {
        var application = CreateApplication(8);
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var encryption = new BlobEncryption(null, null);
        var bytes = RandomNumberGenerator.GetBytes(4096);
        using var source = new MemoryStream(bytes, writable: false);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var stored = await chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName, encryption, source, cancellation.Token);
        var expected = (byte[])bytes.Clone();
        expected[0] ^= 1;
        var progress = new StorageWorkProgressProbe(chunks.Admission, 1, TimeSpan.FromSeconds(1), cancellation.Token);
        await using var progressDisposal = progress.ConfigureAwait(true);
        progress.SetSourcePhase(StorageWorkProgressProbe.SourcePhase.Stored);
        await Assert.ThrowsAnyAsync<Xunit.Sdk.XunitException>(() =>
            CopyAndAssertAsync(chunks, stored.Manifest, encryption, expected, progress, 0, cancellation.Token));
        Assert.Equal(StorageWorkProgressProbe.CopyPhase.Reading, progress.Samples[^1].Copies[0]);
        Assert.Contains("mk8_sava_storage_work_active{lane=\"writes\"} 0\n", chunks.Admission.RenderPrometheus(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task BlockedSourceRetainsActiveAdmissionBeforeCancellation()
    {
        var application = CreateApplication(8);
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var source = new ControlledReadStream(RandomNumberGenerator.GetBytes(4096));
        var progress = new StorageWorkProgressProbe(chunks.Admission, 6, TimeSpan.FromMilliseconds(50), cancellation.Token);
        await using var progressDisposal = progress.ConfigureAwait(true);
        progress.SetSourcePhase(StorageWorkProgressProbe.SourcePhase.Storing);
#pragma warning disable CA2025 // Finally cancels and joins this store before the source, probe, token and application are disposed.
        var storing = chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName, new BlobEncryption(null, null), source, cancellation.Token);
#pragma warning restore CA2025
        try
        {
            await source.Entered.WaitAsync(cancellation.Token);
            // Explicitly capture the established gate; also require a real periodic observation.
            progress.Capture();
            await progress.WaitForFirstTickOrStopAsync(cancellation.Token);
            var samples = progress.Samples;
            Assert.Contains(samples, sample => sample.Source == StorageWorkProgressProbe.SourcePhase.Storing &&
                sample.Admission.Contains("mk8_sava_storage_work_active{lane=\"writes\"} 1\n", StringComparison.Ordinal));
            await cancellation.CancelAsync();
            try
            {
                using var unused = await storing.ConfigureAwait(true);
                Assert.Fail("The blocked source store unexpectedly completed after cancellation.");
            }
            catch (OperationCanceledException exception) when (!CatastrophicExceptionPolicy.Contains(exception)) { }
            var retained = progress.Samples;
            progress.Capture();
            Assert.Equal(retained, progress.Samples);
            Assert.Contains(retained, sample => sample.Source == StorageWorkProgressProbe.SourcePhase.Storing &&
                sample.Admission.Contains("mk8_sava_storage_work_active{lane=\"writes\"} 1\n", StringComparison.Ordinal));
            Assert.Contains("source=Storing", progress.RenderDiagnostics(), StringComparison.Ordinal);
            Assert.Contains("mk8_sava_storage_work_active{lane=\"writes\"} 0\n", chunks.Admission.RenderPrometheus(), StringComparison.Ordinal);
        }
        finally
        {
            await cancellation.CancelAsync().ConfigureAwait(true);
            try { using var unused = await storing.ConfigureAwait(true); }
            catch (OperationCanceledException exception) when (!CatastrophicExceptionPolicy.Contains(exception)) { }
        }
    }

    [Fact]
    public async Task BlockedCopiesRetainReadAndWriteQueuesInsteadOfOnlyPostCancellationZeros()
    {
        var application = CreateApplication(8);
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var encryption = new BlobEncryption(null, null);
        var bytes = RandomNumberGenerator.GetBytes(4096);
        using var source = new MemoryStream(bytes, writable: false);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var stored = await chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName, encryption, source, cancellation.Token);
        using var heldRead = await chunks.Admission.AcquireReadAsync(cancellation.Token);
        var progress = new StorageWorkProgressProbe(chunks.Admission, 2, TimeSpan.FromMilliseconds(50), cancellation.Token);
        await using var progressDisposal = progress.ConfigureAwait(true);
        progress.SetSourcePhase(StorageWorkProgressProbe.SourcePhase.Stored);
#pragma warning disable CA2025 // Finally cancels and joins both copy/read operations before their borrowed inputs and probe are disposed.
        var copies = Task.WhenAll(Enumerable.Range(0, 2).Select(copy =>
            CopyAndAssertAsync(chunks, stored.Manifest, encryption, bytes, progress, copy, cancellation.Token)));
#pragma warning restore CA2025
        try
        {
            await progress.WaitForFirstTickOrStopAsync(cancellation.Token);
            progress.Capture();
            await cancellation.CancelAsync();
            // Select from the now-frozen ring, not a view that may legitimately be evicted
            // while this test thread is descheduled and periodic observation continues.
            var sample = progress.Samples[^1];
            Assert.Equal(StorageWorkProgressProbe.SourcePhase.Stored, sample.Source);
            Assert.All(sample.Copies, phase => Assert.Equal(StorageWorkProgressProbe.CopyPhase.Copying, phase));
            Assert.Contains("mk8_sava_storage_work_active{lane=\"writes\"} 1\n", sample.Admission, StringComparison.Ordinal);
            Assert.Contains("mk8_sava_storage_work_queued{lane=\"writes\"} 1\n", sample.Admission, StringComparison.Ordinal);
            Assert.Contains("mk8_sava_storage_work_queued{lane=\"reads\"} 1\n", sample.Admission, StringComparison.Ordinal);
            try
            {
                await copies.ConfigureAwait(true);
                Assert.Fail("The blocked cross-domain copies unexpectedly completed after cancellation.");
            }
            catch (OperationCanceledException exception) when (!CatastrophicExceptionPolicy.Contains(exception)) { }
            heldRead.Dispose();
            AssertCopyHistoryAndPostCancellationState(progress, sample, chunks);
        }
        finally
        {
            await cancellation.CancelAsync().ConfigureAwait(true);
            try { await copies.ConfigureAwait(true); }
            catch (OperationCanceledException exception) when (!CatastrophicExceptionPolicy.Contains(exception)) { }
        }
    }

    private static void AssertCopyHistoryAndPostCancellationState(
        StorageWorkProgressProbe progress, StorageWorkProgressProbe.Snapshot sample, ChunkStore chunks)
    {
        var retained = progress.Samples;
        progress.Capture();
        Assert.Equal(retained, progress.Samples);
        Assert.Contains(sample, retained);
        var diagnostic = progress.RenderDiagnostics();
        Assert.Contains("copy[0]=Copying, copy[1]=Copying", diagnostic, StringComparison.Ordinal);
        const string currentMarker = "Current admission state (possibly after cancellation/cleanup; not historical proof):";
        var currentOffset = diagnostic.IndexOf(currentMarker, StringComparison.Ordinal);
        Assert.True(currentOffset >= 0);
        Assert.Contains(sample.Admission, diagnostic[..currentOffset], StringComparison.Ordinal);
        Assert.Contains("mk8_sava_storage_work_queued{lane=\"writes\"} 0\n", diagnostic[currentOffset..], StringComparison.Ordinal);
        Assert.Contains("mk8_sava_storage_work_queued{lane=\"writes\"} 0\n", chunks.Admission.RenderPrometheus(), StringComparison.Ordinal);
        Assert.Contains("mk8_sava_storage_work_active{lane=\"reads\"} 0\n", chunks.Admission.RenderPrometheus(), StringComparison.Ordinal);
    }
}
