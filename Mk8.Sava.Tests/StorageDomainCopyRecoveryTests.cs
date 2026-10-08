using System.Security.Cryptography;
using System.Runtime.ExceptionServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageDomainCopyRecoveryTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StagingFailureReleasesCopyLanesAndUnpinsPartialDestination(bool customerProvidedKey)
    {
        using var keys = new CopyEncryptionPair(customerProvidedKey);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var injector = new CopyBoundaryInjector();
        await using var application = CreateApplication(injector);
        await application.InitializeAsync().ConfigureAwait(true);
        Assert.DoesNotContain(application.Services.GetServices<IHostedService>(), service => service is StorageMaintenanceService);
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var bytes = CreateMixedContent();
        using var input = new MemoryStream(bytes, writable: false);
        using var source = await chunks.StorePinnedAsync(
            SavaWebApplicationFactory.AccountName, keys.Source, input, cancellation.Token).ConfigureAwait(true);
        var original = new IOException("controlled destination staging failure");
        var boundary = injector.Arm(chunks.Admission.RenderPrometheus, () => throw original);

        var failure = await Record.ExceptionAsync(() => chunks.CopyToDomainPinnedAsync(
            SavaWebApplicationFactory.SecondAccountName, keys.Source, keys.Destination,
            source.Manifest, cancellation.Token)).ConfigureAwait(true);

        Assert.Same(original, RequireOrdinaryFailure(failure));
        AssertInterruptionAndReleasedWork(boundary, chunks);
        await AssertPartialDestinationRecoveryAsync(application, chunks, source, keys, bytes,
            cancellation.Token).ConfigureAwait(true);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CancellationAtStagingReleasesCopyLanesAndAllowsANewCopy(bool customerProvidedKey)
    {
        using var keys = new CopyEncryptionPair(customerProvidedKey);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var injector = new CopyBoundaryInjector();
        await using var application = CreateApplication(injector);
        await application.InitializeAsync().ConfigureAwait(true);
        Assert.DoesNotContain(application.Services.GetServices<IHostedService>(), service => service is StorageMaintenanceService);
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var bytes = CreateMixedContent();
        using var input = new MemoryStream(bytes, writable: false);
        using var source = await chunks.StorePinnedAsync(
            SavaWebApplicationFactory.AccountName, keys.Source, input, cancellation.Token).ConfigureAwait(true);
        var boundary = injector.Arm(chunks.Admission.RenderPrometheus, () => CancelAtStaging(cancellation));

        var failure = await Record.ExceptionAsync(() => chunks.CopyToDomainPinnedAsync(
            SavaWebApplicationFactory.SecondAccountName, keys.Source, keys.Destination,
            source.Manifest, cancellation.Token)).ConfigureAwait(true);

        var canceled = Assert.IsAssignableFrom<OperationCanceledException>(RequireOrdinaryFailure(failure));
        Assert.Equal(cancellation.Token, canceled.CancellationToken);
        AssertInterruptionAndReleasedWork(boundary, chunks);
        using var retryCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        await AssertPartialDestinationRecoveryAsync(application, chunks, source, keys, bytes,
            retryCancellation.Token).ConfigureAwait(true);
    }

    private static void CancelAtStaging(CancellationTokenSource cancellation)
    {
        // This is the synchronous storage-fault callback, not sync-over-async:
        // return only after its cancellation callbacks have been invoked.
        Assert.False(cancellation.IsCancellationRequested, "The guard expired before the controlled staging interruption.");
        cancellation.Cancel();
    }

    private static Exception RequireOrdinaryFailure(Exception? failure)
    {
        Assert.NotNull(failure);
        if (CatastrophicExceptionPolicy.Contains(failure))
            ExceptionDispatchInfo.Throw(failure);
        return failure;
    }

    private static SavaWebApplicationFactory CreateApplication(IStorageFaultInjector injector) => new(
        Path.Combine(Path.GetTempPath(), $"mk8-sava-copy-boundary-{Guid.NewGuid():N}"),
        injector,
        analyticsSink: null,
        configurationOverrides: new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaximumConcurrentStorageReads"] = "1",
            ["Sava:MaximumConcurrentStorageWrites"] = "1",
            ["Sava:MaximumConcurrentChunkCodecs"] = "1",
            ["Sava:MaximumQueuedStorageOperations"] = "8",
            ["Sava:MinimumChunkBytes"] = "16384",
            ["Sava:TargetChunkBytes"] = "16384",
            ["Sava:MaximumChunkBytes"] = "16384",
            ["Sava:EnableSmallChunkPacking"] = "false"
        },
        deleteDataPath: true,
        disableMaintenance: true);

    private static byte[] CreateMixedContent()
    {
        var bytes = new byte[128 * 1024];
        bytes.AsSpan(0, bytes.Length / 2).Fill(0x6b);
        RandomNumberGenerator.Fill(bytes.AsSpan(bytes.Length / 2));
        return bytes;
    }

    private static void AssertInterruptionAndReleasedWork(StagingInterruption boundary, ChunkStore chunks)
    {
        Assert.Equal(2, boundary.Attempts);
        Assert.Contains("mk8_sava_storage_work_active{lane=\"writes\"} 1\n", boundary.Admission, StringComparison.Ordinal);
        Assert.Contains("mk8_sava_storage_work_active{lane=\"codecs\"} 1\n", boundary.Admission, StringComparison.Ordinal);
        AssertAllLanesIdle(chunks);
    }

    private static void AssertAllLanesIdle(ChunkStore chunks)
    {
        var state = chunks.Admission.RenderPrometheus();
        foreach (var lane in new[] { "reads", "writes", "codecs", "queries" })
        {
            Assert.Contains($"mk8_sava_storage_work_active{{lane=\"{lane}\"}} 0\n", state, StringComparison.Ordinal);
            Assert.Contains($"mk8_sava_storage_work_queued{{lane=\"{lane}\"}} 0\n", state, StringComparison.Ordinal);
        }
    }

    private static async Task AssertPartialDestinationRecoveryAsync(
        SavaWebApplicationFactory application, ChunkStore chunks, StoredContent source,
        CopyEncryptionPair keys, byte[] bytes, CancellationToken cancellationToken)
    {
        Assert.Empty(Directory.EnumerateFiles(Path.Combine(application.DataPath, "staging")));
        var destinationPath = Path.Combine(application.DataPath, "chunks", SavaWebApplicationFactory.SecondAccountName);
        Assert.Single(Directory.EnumerateFiles(destinationPath, "*.chunk", SearchOption.AllDirectories));
        Assert.Equal(1, await application.Services.GetRequiredService<BlobService>()
            .CollectGarbageAsync(cancellationToken).ConfigureAwait(false));
        Assert.False(Directory.Exists(destinationPath) &&
            Directory.EnumerateFiles(destinationPath, "*.chunk", SearchOption.AllDirectories).Any());
        Assert.Equal(bytes, await chunks.ReadAllAsync(source.Manifest, keys.Source, cancellationToken).ConfigureAwait(false));

        using var copied = await chunks.CopyToDomainPinnedAsync(
            SavaWebApplicationFactory.SecondAccountName, keys.Source, keys.Destination,
            source.Manifest, cancellationToken).ConfigureAwait(false);
        Assert.NotEqual(source.Manifest.Domain, copied.Manifest.Domain, StringComparer.Ordinal);
        Assert.Equal(source.Manifest.Length, copied.Manifest.Length);
        Assert.Equal(source.Manifest.Sha256, copied.Manifest.Sha256);
        Assert.NotNull(source.ContentMd5);
        Assert.Equal(source.ContentMd5, copied.ContentMd5);
        Assert.Equal(bytes, await chunks.ReadAllAsync(copied.Manifest, keys.Destination, cancellationToken).ConfigureAwait(false));
        if (keys.Destination.CustomerProvidedKey is not null)
        {
            await Assert.ThrowsAsync<InvalidDataException>(() => chunks.ReadAllAsync(
                copied.Manifest, keys.Source, cancellationToken)).ConfigureAwait(false);
        }
        AssertAllLanesIdle(chunks);
    }

    private sealed class CopyEncryptionPair : IDisposable
    {
        private readonly byte[] _sourceKey = RandomNumberGenerator.GetBytes(32);
        private readonly byte[] _destinationKey = RandomNumberGenerator.GetBytes(32);

        public CopyEncryptionPair(bool customerProvidedKey)
        {
            Source = CreateEncryption(customerProvidedKey, _sourceKey);
            Destination = CreateEncryption(customerProvidedKey, _destinationKey);
        }

        public BlobEncryption Source { get; }
        public BlobEncryption Destination { get; }

        private static BlobEncryption CreateEncryption(bool customerProvidedKey, byte[] key) => customerProvidedKey
            ? new BlobEncryption(null, Convert.ToBase64String(SHA256.HashData(key)), key)
            : new BlobEncryption(null, null);

        public void Dispose()
        {
            CryptographicOperations.ZeroMemory(_sourceKey);
            CryptographicOperations.ZeroMemory(_destinationKey);
        }
    }

    private sealed class CopyBoundaryInjector : IStorageFaultInjector
    {
        private StagingInterruption? _interruption;

        internal StagingInterruption Arm(Func<string> admission, Action interrupt)
        {
            var next = new StagingInterruption(admission, interrupt);
            if (Interlocked.CompareExchange(ref _interruption, next, null) is not null)
                throw new InvalidOperationException("The copy boundary has already been armed.");
            return next;
        }

        public void Inject(StorageFaultPoint point)
        {
            if (point == StorageFaultPoint.DuringChunkStagingWrite)
                Volatile.Read(ref _interruption)?.Observe();
        }
    }

    private sealed class StagingInterruption(Func<string> admission, Action interrupt)
    {
        private int _attempts;
        private string _admission = string.Empty;

        public int Attempts => Volatile.Read(ref _attempts);
        public string Admission => Volatile.Read(ref _admission);

        internal void Observe()
        {
            if (Interlocked.Increment(ref _attempts) != 2)
                return;
            Volatile.Write(ref _admission, admission());
            interrupt();
        }
    }
}
