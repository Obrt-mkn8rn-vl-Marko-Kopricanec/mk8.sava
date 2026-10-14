using System.Runtime.ExceptionServices;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Mk8.Sava.Tests;

public sealed partial class AzureSdkCompatibilityTests
{
    [Theory]
    [InlineData(0, false, false)]
    [InlineData(1, false, false)]
    [InlineData(2, false, false)]
    [InlineData(3, false, false)]
    [InlineData(2, true, false)]
    [InlineData(2, false, true)]
    [InlineData(2, true, true)]
    public async Task LoopbackSourceStartupFailureJoinsStopAndDisposal(
        int phase, bool failStop, bool failDispose)
    {
        var probe = new SourceLifetimeProbe(failStop, failDispose);
        await using var probeLifetime = probe.ConfigureAwait(false);
        var entered = new TaskCompletionSource<WebApplication>(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var startupFailure = new IOException("controlled source startup failure");
#pragma warning disable CA2000 // The asynchronous acquisition result is owned by the guaranteed actual join below; every unexpectedly successful result is immediately async-disposed there.
        var startup = LoopbackSource.StartAsync([1, 2, 3],
            builder => builder.Services.AddSingleton<IHostedService>(_ => probe),
            async (current, application) =>
            {
                if ((int)current != phase)
                    return;
                _ = application.Services.GetServices<IHostedService>().ToArray();
                entered.TrySetResult(application);
#pragma warning disable VSTHRD003 // This is the test's controlled startup gate; finally releases it before joining the actual startup and retiring its owners.
                await release.Task.ConfigureAwait(false);
#pragma warning restore VSTHRD003
                throw startupFailure;
            });
#pragma warning restore CA2000
        WebApplication? acquired = null;
        Exception? verification = null;
        Exception? fallback = null;
        Exception? joined = null;
        try
        {
            verification = await Record.ExceptionAsync(async () =>
            {
                acquired = await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), CancellationToken.None).ConfigureAwait(false);
#pragma warning disable VSTHRD003 // This verification joins the test-owned startup passed below; the enclosing finally independently joins the same operation before any fallback disposal.
                await VerifySourceStartupAsync(acquired, probe, phase, failStop, failDispose,
                    startup, release, startupFailure).ConfigureAwait(false);
#pragma warning restore VSTHRD003
            }).ConfigureAwait(true);
        }
        finally
        {
            release.TrySetResult();
            joined = await JoinSourceStartupAsync(startup).ConfigureAwait(true);
            if (verification is null && joined is null)
                verification = new InvalidOperationException("The failing source startup unexpectedly transferred ownership.");
            if (acquired is null && entered.Task.IsCompletedSuccessfully)
                acquired = await entered.Task.ConfigureAwait(true);
            if (acquired is not null)
                fallback = await Record.ExceptionAsync(() => acquired.DisposeAsync().AsTask()).ConfigureAwait(true);
        }
        ThrowSourceVerificationFailures(verification, joined, fallback, probe);
    }

    private static async Task<Exception?> JoinSourceStartupAsync(Task<LoopbackSource> startup)
    {
        try
        {
#pragma warning disable VSTHRD003 // The calling test owns this actual startup task; this join runs before its fallback owners can retire.
            var unexpected = await startup.ConfigureAwait(false);
#pragma warning restore VSTHRD003
            await using var lifetime = unexpected.ConfigureAwait(false);
            return null;
        }
#pragma warning disable CA1031 // Return the complete ordinary/fatal startup or cleanup graph to the caller's verification inventory; no success is inferred from a fault.
        catch (Exception failure)
#pragma warning restore CA1031
        {
            return failure;
        }
    }

    private static async Task VerifySourceStartupAsync(WebApplication acquired, SourceLifetimeProbe probe,
        int phase, bool failStop, bool failDispose, Task<LoopbackSource> startup,
        TaskCompletionSource release, IOException startupFailure)
    {
        Assert.False(startup.IsCompleted);
        Assert.Equal(phase >= 2 ? 1 : 0, probe.StartCount);
        var stopped = acquired.Lifetime.ApplicationStopped;
        release.TrySetResult();
#pragma warning disable VSTHRD003 // The caller owns this actual startup and joins it in finally before releasing the captured application.
        var failure = await Record.ExceptionAsync(() => startup).ConfigureAwait(false);
#pragma warning restore VSTHRD003
        if (!failStop && !failDispose)
        {
            Assert.Same(startupFailure, failure);
        }
        else
        {
            var failures = Assert.IsType<AggregateException>(failure).Flatten().InnerExceptions;
            Assert.Contains(startupFailure, failures);
            if (failStop)
                Assert.Contains(probe.StopFailure, failures);
            if (failDispose)
                Assert.Contains(probe.DisposalFailure, failures);
            Assert.Equal(1 + (failStop ? 1 : 0) + (failDispose ? 1 : 0), failures.Count);
        }
        Assert.Equal(1, probe.DisposalCount);
        Assert.Equal(1, probe.DisposalAttemptCount);
        Assert.Throws<ObjectDisposedException>(() => acquired.Services.GetRequiredService<IHostedService>());
        if (phase >= 2)
        {
            Assert.True(stopped.IsCancellationRequested);
            Assert.Equal(1, probe.StopCount);
        }
    }

    private static void ThrowSourceVerificationFailures(Exception? verification, Exception? joined,
        Exception? fallback, SourceLifetimeProbe probe)
    {
        var failures = new List<Exception>();
        if (verification is not null)
        {
            failures.Add(verification);
            if (joined is not null)
                failures.Add(joined);
        }
        if (fallback is not null && (verification is not null || !ReferenceEquals(fallback, probe.DisposalFailure)))
            failures.Add(fallback);
        if (failures.Count == 1)
            ExceptionDispatchInfo.Throw(failures[0]);
        if (failures.Count > 1)
            throw new AggregateException("Verification and owned source cleanup failed.", failures);
    }

    [Fact]
    public async Task LoopbackSourceSuccessfulAcquisitionTransfersItsLiveOwner()
    {
        var probe = new SourceLifetimeProbe(failStop: false, failDispose: false);
        await using var probeLifetime = probe.ConfigureAwait(false);
        var source = await LoopbackSource.StartAsync([1, 2, 3],
            builder => builder.Services.AddSingleton<IHostedService>(_ => probe)).ConfigureAwait(true);
        Exception? verification = null;
        Exception? cleanup = null;
        try
        {
            await using (source.ConfigureAwait(false))
            {
                verification = await Record.ExceptionAsync(async () =>
                {
                    using var client = new HttpClient();
                    using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                    Assert.Equal(new byte[] { 1, 2, 3 },
                        await client.GetByteArrayAsync(source.Uri, cancellation.Token).ConfigureAwait(true));
                    Assert.Equal(1, probe.StartCount);
                    Assert.Equal(0, probe.DisposalCount);
                }).ConfigureAwait(true);
            }
        }
#pragma warning disable CA1031 // Keep the independent owned disposal outcome alongside any verification failure; both are rethrown below, including fatal graphs.
        catch (Exception failure)
#pragma warning restore CA1031
        {
            cleanup = failure;
        }
        if (verification is not null && cleanup is not null)
            throw new AggregateException("Verification and source disposal failed.", verification, cleanup);
        if (cleanup is not null)
            ExceptionDispatchInfo.Throw(cleanup);
        if (verification is not null)
            ExceptionDispatchInfo.Throw(verification);
        Assert.Equal(1, probe.DisposalCount);
        Assert.Equal(1, probe.DisposalAttemptCount);
        Assert.Equal(1, probe.StopCount);
    }

    private sealed class SourceLifetimeProbe(bool failStop, bool failDispose) : IHostedService, IAsyncDisposable
    {
        public IOException StopFailure { get; } = new("controlled source stop failure");
        public IOException DisposalFailure { get; } = new("controlled source disposal failure");
        public int StartCount { get; private set; }
        public int StopCount { get; private set; }
        public int DisposalCount { get; private set; }
        public int DisposalAttemptCount { get; private set; }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            StartCount++;
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            StopCount++;
            return failStop ? Task.FromException(StopFailure) : Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            DisposalAttemptCount++;
            if (DisposalCount != 0)
                return ValueTask.CompletedTask;
            DisposalCount++;
            return failDispose ? ValueTask.FromException(DisposalFailure) : ValueTask.CompletedTask;
        }
    }
}
