using System.Threading.RateLimiting;
using Mk8.Sava.Configuration;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageAdmissionLimiterTests
{
    [Theory]
    [InlineData("reads")]
    [InlineData("writes")]
    [InlineData("codecs")]
    [InlineData("queries")]
    public async Task EachLaneIsBoundedAndServesItsQueueInOrder(string lane) =>
        await AssertFifoAsync(lane).ConfigureAwait(true);

    [Theory]
    [InlineData("reads")]
    [InlineData("writes")]
    [InlineData("codecs")]
    [InlineData("queries")]
    public async Task CancellationAndShutdownReleaseQueuedWork(string lane) =>
        await AssertCancellationAndShutdownAsync(lane).ConfigureAwait(true);

    [Fact]
    public async Task RepeatedCancellationUnlinksWaitersWithoutReleasingTheActivePermit() =>
        await AssertCancellationChurnAsync().ConfigureAwait(true);

    [Fact]
    public async Task CancellationRacingPermitHandoffDoesNotLeakOrDoubleReleasePermits() =>
        await AssertHandoffRaceAsync().ConfigureAwait(true);

    private static async Task AssertCancellationChurnAsync()
    {
        using var limiter = new StorageWorkLimiter(1, 1);
        using var first = await limiter.AcquireAsync(CancellationToken.None).ConfigureAwait(false);
        for (var index = 0; index < 1024; index++)
        {
            using var cancellation = new CancellationTokenSource();
            var queued = limiter.AcquireAsync(cancellation.Token).AsTask();
            Assert.Equal((1, 1, 0L), limiter.GetStatistics());
            await cancellation.CancelAsync().ConfigureAwait(false);
            try
            {
                using var unexpected = await queued.ConfigureAwait(false);
                Assert.Fail("Canceled work received a permit.");
            }
            catch (OperationCanceledException)
            {
            }
            Assert.Equal((1, 0, 0L), limiter.GetStatistics());
        }
        first.Dispose();
        using var retried = await limiter.AcquireAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.Equal((1, 0, 0L), limiter.GetStatistics());
    }

    private static async Task AssertHandoffRaceAsync()
    {
        using var limiter = new StorageWorkLimiter(1, 1);
        for (var index = 0; index < 256; index++)
        {
            using var first = await limiter.AcquireAsync(CancellationToken.None).ConfigureAwait(false);
            using var cancellation = new CancellationTokenSource();
            var queued = limiter.AcquireAsync(cancellation.Token).AsTask();
            await Task.WhenAll(Task.Run(first.Dispose), cancellation.CancelAsync()).ConfigureAwait(false);
            try
            {
                using var granted = await queued.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            Assert.Equal((0, 0, 0L), limiter.GetStatistics());
        }
    }

    private static async Task AssertFifoAsync(string lane)
    {
        var admission = CreateAdmission(2);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var first = await AcquireAsync(admission, lane, cancellation.Token).ConfigureAwait(false);
        var second = AcquireAsync(admission, lane, cancellation.Token).AsTask();
        var third = AcquireAsync(admission, lane, cancellation.Token).AsTask();
        try
        {
            Assert.False(second.IsCompleted);
            Assert.False(third.IsCompleted);
            AssertMetric(admission, "queued", lane, 2);
            var rejected = await Assert.ThrowsAsync<AzureStorageException>(async () =>
                await AcquireAsync(admission, lane, cancellation.Token).ConfigureAwait(false)).ConfigureAwait(false);
            AssertBusy(rejected);
            AssertMetric(admission, "rejected_total", lane, 1);
            first.Dispose();
            using var secondLease = await second.ConfigureAwait(false);
            Assert.False(third.IsCompleted);
            AssertMetric(admission, "active", lane, 1);
            AssertMetric(admission, "queued", lane, 1);
            secondLease.Dispose();
            using var thirdLease = await third.ConfigureAwait(false);
            AssertMetric(admission, "active", lane, 1);
            AssertMetric(admission, "queued", lane, 0);
        }
        finally
        {
            admission.Dispose();
            try
            {
                var leases = await Task.WhenAll(second, third).ConfigureAwait(false);
                foreach (var lease in leases)
                    lease.Dispose();
            }
            catch (AzureStorageException)
            {
            }
        }
    }

    private static async Task AssertCancellationAndShutdownAsync(string lane)
    {
        var admission = CreateAdmission(1);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var first = await AcquireAsync(admission, lane, cancellation.Token).ConfigureAwait(false);
        using var queueCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellation.Token);
        var canceled = AcquireAsync(admission, lane, queueCancellation.Token).AsTask();
        try
        {
            AssertMetric(admission, "queued", lane, 1);
            await queueCancellation.CancelAsync().ConfigureAwait(false);
            try
            {
                using var unexpected = await canceled.ConfigureAwait(false);
                Assert.Fail("Canceled work received a permit.");
            }
            catch (OperationCanceledException)
            {
            }
            AssertMetric(admission, "queued", lane, 0);
            var closed = AcquireAsync(admission, lane, cancellation.Token).AsTask();
            admission.Dispose();
            try
            {
                using var unexpected = await closed.ConfigureAwait(false);
                Assert.Fail("Closed work received a permit.");
            }
            catch (AzureStorageException exception)
            {
                AssertBusy(exception);
            }
        }
        finally
        {
            admission.Dispose();
            try
            {
                using var lease = await canceled.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    private static StorageWorkAdmission CreateAdmission(int queued) => new(new SavaOptions
    {
        MaximumConcurrentStorageReads = 1,
        MaximumConcurrentStorageWrites = 1,
        MaximumConcurrentChunkCodecs = 1,
        MaximumConcurrentBlobQueries = 1,
        MaximumQueuedStorageOperations = queued
    });

    private static ValueTask<RateLimitLease> AcquireAsync(StorageWorkAdmission admission, string lane, CancellationToken cancellationToken) => lane switch
    {
        "reads" => admission.AcquireReadAsync(cancellationToken),
        "writes" => admission.AcquireWriteAsync(cancellationToken),
        "codecs" => admission.AcquireCodecAsync(cancellationToken),
        "queries" => admission.AcquireQueryAsync(cancellationToken),
        _ => throw new ArgumentOutOfRangeException(nameof(lane))
    };

    private static void AssertBusy(AzureStorageException exception)
    {
        Assert.Equal(503, exception.StatusCode);
        Assert.Equal("ServerBusy", exception.ErrorCode);
        Assert.Equal("1", exception.ResponseHeaders["Retry-After"]);
    }

    private static void AssertMetric(StorageWorkAdmission admission, string metric, string lane, long value) =>
        Assert.Contains($"mk8_sava_storage_work_{metric}{{lane=\"{lane}\"}} {value}\n", admission.RenderPrometheus(), StringComparison.Ordinal);
}
