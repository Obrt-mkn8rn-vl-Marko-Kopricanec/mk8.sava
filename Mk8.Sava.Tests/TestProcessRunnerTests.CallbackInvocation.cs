using Mk8.Sava.Protocol;
using Xunit.Sdk;

namespace Mk8.Sava.Tests;

public sealed partial class TestProcessRunnerTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PendingCallbackChainReportsWhetherTheThrowingBodyHasAlreadyExited(bool throwBeforeHold)
    {
        using var cancellation = new CancellationTokenSource();
        var fixture = await ProcessFixture.StartAsync("ordinary").ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var observation = TestProcessRunner.ObserveAsync(fixture.Root,
            TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(2), cancellation.Token);
        var original = new IOException("controlled callback-chain failure");
        var throwing = new CancellationCallbackProbe(() => throw original);
        var held = new CancellationCallbackProbe(() =>
        {
            entered.TrySetResult();
#pragma warning disable VSTHRD002 // Hold one real synchronous callback; guaranteed cleanup releases it and joins the original callback-chain and observer tasks.
            if (!release.Wait(TimeSpan.FromSeconds(10)))
                throw new TimeoutException("Controlled callback-chain gate was not released.");
#pragma warning restore VSTHRD002
        });
        // The pinned BCL processes registrations in LIFO order. Both orders distinguish
        // a callback not yet invoked from an already-thrown body while the chain is pending.
        var first = cancellation.Token.Register(throwBeforeHold ? held.Invoke : throwing.Invoke);
        await using var firstLifetime = first.ConfigureAwait(false);
        var second = cancellation.Token.Register(throwBeforeHold ? throwing.Invoke : held.Invoke);
        await using var secondLifetime = second.ConfigureAwait(false);
        var work = new CancellationWork(cancellation.CancelAsync(), observation, throwing);
        string? snapshot = null;
        await RunWithCleanupAsync(async () =>
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            var failure = await Record.ExceptionAsync(() => AssertCallbackFaultAsync(
                work, fixture, cancellation, value => snapshot = value)).ConfigureAwait(false);

            var assertion = Assert.IsType<ThrowsException>(failure);
            Assert.IsType<TimeoutException>(assertion.InnerException);
            Assert.NotNull(snapshot);
            var progress = throwing.Progress;
            AssertPendingCallbackSnapshot(snapshot, throwBeforeHold, progress);
            Assert.Equal(new CancellationCallbackProbe.CallbackProgress(true, false, false), held.Progress);
            Assert.False(work.CallbacksCompleted);
            output.WriteLine($"throw_before_hold={throwBeforeHold}; throwing=[{progress.ToDiagnostic()}]; held=[{held.Progress.ToDiagnostic()}].");
        }, async () =>
        {
            release.Set();
            var cleanupFailure = await Assert.ThrowsAsync<AggregateException>(() => work.RetireAsync(fixture)).ConfigureAwait(false);
            Assert.Contains(original, cleanupFailure.InnerExceptions);
            Assert.False(CatastrophicExceptionPolicy.Contains(cleanupFailure));
        }).ConfigureAwait(true);
        Assert.Equal(new CancellationCallbackProbe.CallbackProgress(true, true, false), throwing.Progress);
        Assert.Equal(new CancellationCallbackProbe.CallbackProgress(true, true, true), held.Progress);
        Assert.True(work.CallbacksFaulted);
        Assert.True(work.ObservationCanceled);
    }

    private static void AssertPendingCallbackSnapshot(
        string snapshot, bool hasThrown, CancellationCallbackProbe.CallbackProgress progress)
    {
        Assert.Equal(hasThrown, progress.Entered);
        Assert.Equal(hasThrown, progress.BodyExited);
        Assert.False(progress.ReturnedNormally);
        Assert.Contains($"ControlledCallback=[{progress.ToDiagnostic()}]", snapshot, StringComparison.Ordinal);
        Assert.Contains("CallbacksCompleted=False", snapshot, StringComparison.Ordinal);
        Assert.Contains("ObservationCompleted=False", snapshot, StringComparison.Ordinal);
        Assert.Contains("RootExited=False", snapshot, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReturningCallbackProgressDoesNotReplaceTheOriginalCancellationTask()
    {
        using var cancellation = new CancellationTokenSource();
        var invocations = 0;
        var probe = new CancellationCallbackProbe(() => invocations++);
        var initial = probe.Progress;
        Assert.Equal(new CancellationCallbackProbe.CallbackProgress(false, false, false), initial);
        var registration = cancellation.Token.Register(probe.Invoke);
        await using var registrationLifetime = registration.ConfigureAwait(false);
        var callbacks = cancellation.CancelAsync();
        await callbacks.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(true);

        Assert.True(cancellation.IsCancellationRequested);
        Assert.True(callbacks.IsCompletedSuccessfully);
        Assert.Equal(1, invocations);
        Assert.Equal(new CancellationCallbackProbe.CallbackProgress(true, true, true), probe.Progress);
        Assert.Equal(new CancellationCallbackProbe.CallbackProgress(false, false, false), initial);
        Assert.Throws<InvalidOperationException>(probe.Invoke);
        Assert.Equal(1, invocations);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ThrowingCallbackProgressPreservesOrdinaryAndWrappedFatalPayloads(bool fatal)
    {
        using var cancellation = new CancellationTokenSource();
#pragma warning disable CA2201 // Synthetic wrapped fault tests callback identity/graph preservation, not real memory exhaustion.
        var original = new IOException("controlled invocation failure", fatal ? new OutOfMemoryException("synthetic callback fatal") : null);
#pragma warning restore CA2201
        var probe = new CancellationCallbackProbe(() => throw original);
        var registration = cancellation.Token.Register(probe.Invoke);
        await using var registrationLifetime = registration.ConfigureAwait(false);
        var callbacks = cancellation.CancelAsync();
        var failure = await Assert.ThrowsAsync<AggregateException>(() => callbacks.WaitAsync(TimeSpan.FromSeconds(5)))
            .ConfigureAwait(true);

        Assert.Same(original, Assert.Single(failure.InnerExceptions));
        Assert.Equal(fatal, CatastrophicExceptionPolicy.Contains(failure));
        Assert.True(callbacks.IsFaulted);
        Assert.Equal(new CancellationCallbackProbe.CallbackProgress(true, true, false), probe.Progress);
    }
}
