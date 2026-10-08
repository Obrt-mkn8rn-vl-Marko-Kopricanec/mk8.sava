using System.Security.Cryptography;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed partial class StorageWorkAdmissionTests
{
    [Fact]
    public async Task QueuedUploadDoesNotReadOrStageItsBodyAndCancellationReleasesTheQueue() =>
        await AssertQueuedUploadAsync().ConfigureAwait(true);

    [Fact]
    public async Task QueuedReadPinsItsContentAndActiveReadKeepsItsPermitUntilOutputCompletes() =>
        await AssertQueuedReadAsync().ConfigureAwait(true);

    [Fact]
    public async Task CrossDomainCopiesDoNotDeadlockWithOnePermitInEachLane() =>
        await AssertDomainCopiesAsync().ConfigureAwait(true);

    private static async Task AssertQueuedUploadAsync()
    {
        var application = CreateApplication(1);
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var encryption = new BlobEncryption(null, null);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var firstSource = new ControlledReadStream(RandomNumberGenerator.GetBytes(64 * 1024));
        var queuedSource = new ControlledReadStream(RandomNumberGenerator.GetBytes(64 * 1024));
#pragma warning disable CA2025 // The finally block cancels and joins both uploads before disposing streams or token sources.
        var first = chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName, encryption, firstSource, cancellation.Token);
        using var queueCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellation.Token);
        var queued = chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName, encryption, queuedSource, queueCancellation.Token);
#pragma warning restore CA2025
        try
        {
            await firstSource.Entered.WaitAsync(cancellation.Token).ConfigureAwait(false);
            Assert.False(queuedSource.HasRead, "A queued upload consumed bytes before receiving a storage permit.");
            Assert.Empty(Directory.EnumerateFiles(Path.Combine(application.DataPath, "staging")));
            await queueCancellation.CancelAsync().ConfigureAwait(false);
            try
            {
                await queued.ConfigureAwait(false);
                Assert.Fail("A canceled queued upload completed successfully.");
            }
            catch (OperationCanceledException)
            {
            }
            firstSource.Release();
            await first.ConfigureAwait(false);
            await AssertUploadRetryAsync(chunks, queuedSource, encryption, cancellation.Token).ConfigureAwait(false);
        }
        finally
        {
            await cancellation.CancelAsync().ConfigureAwait(false);
            try
            {
                await Task.WhenAll(first, queued).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            if (first.IsCompletedSuccessfully)
                (await first.ConfigureAwait(false)).Dispose();
            if (queued.IsCompletedSuccessfully)
                (await queued.ConfigureAwait(false)).Dispose();
            await firstSource.DisposeAsync().ConfigureAwait(false);
            await queuedSource.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task AssertUploadRetryAsync(
        ChunkStore chunks, ControlledReadStream source, BlobEncryption encryption, CancellationToken cancellationToken)
    {
        source.Release();
        using var retried = await chunks.StorePinnedAsync(
            SavaWebApplicationFactory.AccountName, encryption, source, cancellationToken).ConfigureAwait(false);
        using var read = new MemoryStream();
        await chunks.WriteRangeAsync(retried.Manifest, encryption, 0, retried.Manifest.Length, read, cancellationToken).ConfigureAwait(false);
        Assert.Equal(source.Bytes, read.ToArray());
    }

    private static async Task AssertQueuedReadAsync()
    {
        var application = CreateApplication(1);
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var service = application.Services.GetRequiredService<BlobService>();
        var encryption = new BlobEncryption(null, null);
        var bytes = RandomNumberGenerator.GetBytes(64 * 1024);
        using var source = new MemoryStream(bytes, writable: false);
        using var stored = await chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName, encryption, source, CancellationToken.None).ConfigureAwait(false);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var output = new ControlledWriteStream();
        var queuedOutput = new MemoryStream();
#pragma warning disable CA2025 // The finally block cancels and joins both reads before disposing streams or the token source.
        var first = chunks.WriteRangeAsync(stored.Manifest, encryption, 0, bytes.Length, output, cancellation.Token);
#pragma warning restore CA2025
        Task? queued = null;
        try
        {
            await output.Entered.WaitAsync(cancellation.Token).ConfigureAwait(false);
#pragma warning disable CA2025 // Both reads are canceled and joined in finally before either destination is disposed.
            queued = chunks.WriteRangeAsync(stored.Manifest, encryption, 0, bytes.Length, queuedOutput, cancellation.Token);
#pragma warning restore CA2025
            Assert.False(queued.IsCompleted);
            Assert.Equal(0, queuedOutput.Length);
            stored.Dispose();
            Assert.Equal(0, await service.CollectGarbageAsync(cancellation.Token).ConfigureAwait(false));
            var rejected = await Assert.ThrowsAsync<AzureStorageException>(() =>
                chunks.WriteRangeAsync(stored.Manifest, encryption, 0, bytes.Length, Stream.Null, cancellation.Token)).ConfigureAwait(false);
            Assert.Equal("ServerBusy", rejected.ErrorCode);
            output.Release();
            await Task.WhenAll(first, queued).ConfigureAwait(false);
            Assert.Equal(bytes, output.ToArray());
            Assert.Equal(bytes, queuedOutput.ToArray());
            Assert.Equal(stored.Manifest.Chunks.Count, await service.CollectGarbageAsync(cancellation.Token).ConfigureAwait(false));
        }
        finally
        {
            await cancellation.CancelAsync().ConfigureAwait(false);
            try
            {
                await Task.WhenAll(queued is null ? [first] : [first, queued]).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            await output.DisposeAsync().ConfigureAwait(false);
            await queuedOutput.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task AssertDomainCopiesAsync()
    {
        var checkpoints = new StorageCopyCheckpointProbe();
        var application = CreateApplication(8, checkpoints);
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var encryption = new BlobEncryption(null, null);
        var bytes = RandomNumberGenerator.GetBytes(2 * 1024 * 1024);
        using var source = new MemoryStream(bytes, writable: false);
        // This is a deadlock guard, not a disk-throughput assertion: Windows CI
        // must durably flush the source and six copies while the full suite runs.
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        checkpoints.Start(cancellation.Token);
        var progress = new StorageWorkProgressProbe(chunks.Admission, 6, TimeSpan.FromSeconds(1), cancellation.Token);
        await using var progressDisposal = progress.ConfigureAwait(false);
        try
        {
            progress.SetSourcePhase(StorageWorkProgressProbe.SourcePhase.Storing);
            using var stored = await chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName, encryption, source, cancellation.Token).ConfigureAwait(false);
            checkpoints.SourceStored();
            progress.SetSourcePhase(StorageWorkProgressProbe.SourcePhase.Stored);
            await Task.WhenAll(Enumerable.Range(0, 6).Select(copy =>
                CopyAndAssertAsync(chunks, stored.Manifest, encryption, bytes, progress, copy, cancellation.Token))).ConfigureAwait(false);
        }
        catch (OperationCanceledException exception) when (cancellation.IsCancellationRequested && !CatastrophicExceptionPolicy.Contains(exception))
        {
            Assert.Fail($"Cross-domain copies exceeded the unchanged 90-second deadlock guard.\n{progress.RenderDiagnostics()}\n{checkpoints.RenderDiagnostics()}");
        }
        Assert.Contains("mk8_sava_storage_work_active{lane=\"writes\"} 0\n", chunks.Admission.RenderPrometheus(), StringComparison.Ordinal);
    }

    private static async Task CopyAndAssertAsync(
        ChunkStore chunks, ContentManifest manifest, BlobEncryption encryption, byte[] bytes,
        StorageWorkProgressProbe progress, int copy, CancellationToken cancellationToken)
    {
        progress.SetCopyPhase(copy, StorageWorkProgressProbe.CopyPhase.Copying);
        using var copied = await chunks.CopyToDomainPinnedAsync(
            SavaWebApplicationFactory.SecondAccountName, encryption, encryption, manifest, cancellationToken).ConfigureAwait(false);
        Assert.Equal(SavaWebApplicationFactory.SecondAccountName, copied.Manifest.Domain);
        using var output = new MemoryStream();
        progress.SetCopyPhase(copy, StorageWorkProgressProbe.CopyPhase.Reading);
        await chunks.WriteRangeAsync(copied.Manifest, encryption, 0, bytes.Length, output, cancellationToken).ConfigureAwait(false);
        Assert.Equal(bytes, output.ToArray());
        progress.SetCopyPhase(copy, StorageWorkProgressProbe.CopyPhase.Verified);
    }

    private static SavaWebApplicationFactory CreateApplication(int queued, IStorageFaultInjector? injector = null)
    {
        var configuration = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaximumConcurrentStorageReads"] = "1",
            ["Sava:MaximumConcurrentStorageWrites"] = "1",
            ["Sava:MaximumConcurrentChunkCodecs"] = "1",
            ["Sava:MaximumQueuedStorageOperations"] = queued.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        };
        return injector is null ? new SavaWebApplicationFactory(configuration) : new SavaWebApplicationFactory(
            Path.Combine(Path.GetTempPath(), $"mk8-sava-tests-{Guid.NewGuid():N}"), injector,
            analyticsSink: null, configurationOverrides: configuration, deleteDataPath: true);
    }

    private sealed class ControlledWriteStream : MemoryStream
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Entered => _entered.Task;
        public void Release() => _release.TrySetResult();

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            _entered.TrySetResult();
            await _release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            await base.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class ControlledReadStream(byte[] bytes) : Stream
    {
        private readonly MemoryStream _content = new(bytes, writable: false);
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _hasRead;

        public byte[] Bytes => bytes;
        public Task Entered => _entered.Task;
        public bool HasRead => Volatile.Read(ref _hasRead) != 0;
        public void Release() => _release.TrySetResult();
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => bytes.Length;
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() => throw new NotSupportedException();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Interlocked.Exchange(ref _hasRead, 1);
            _entered.TrySetResult();
            await _release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return await _content.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _content.Dispose();
            base.Dispose(disposing);
        }
    }
}
