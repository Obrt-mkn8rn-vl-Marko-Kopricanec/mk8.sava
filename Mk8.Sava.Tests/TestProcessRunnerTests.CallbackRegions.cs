using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed partial class TestProcessRunnerTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task HeldCallbackRetainsObserverRegistrationBoundariesWithoutCompletingTheChain(int heldRegion)
    {
        using var cancellation = new CancellationTokenSource();
        var fixture = await ProcessFixture.StartAsync("ordinary").ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var held = CreateHeldRegistration(release, entered);
        // The three controlled positions use the pinned LIFO registration order. Only one invokes held;
        // the other two are no-op controls, not unobserved native work or a competing reader.
        var outerHold = RegisterHeldRegion(heldRegion, 2, held, cancellation.Token);
        await using var outerHoldLifetime = outerHold.ConfigureAwait(false);
        var beforeObserver = new CancellationCallbackProbe(static () => { });
        var beforeRegistration = cancellation.Token.Register(beforeObserver.Invoke);
        await using var beforeLifetime = beforeRegistration.ConfigureAwait(false);
        var middleHold = RegisterHeldRegion(heldRegion, 1, held, cancellation.Token);
        await using var middleHoldLifetime = middleHold.ConfigureAwait(false);
        var observation = TestProcessRunner.ObserveAsync(fixture.Root,
            TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(2), cancellation.Token);
        var afterObserver = new CancellationCallbackProbe(static () => { });
        var afterRegistration = cancellation.Token.Register(afterObserver.Invoke);
        await using var afterLifetime = afterRegistration.ConfigureAwait(false);
        var innerHold = RegisterHeldRegion(heldRegion, 0, held, cancellation.Token);
        await using var innerHoldLifetime = innerHold.ConfigureAwait(false);
        var original = new IOException("controlled registration-region failure");
        var throwing = new CancellationCallbackProbe(() => throw original);
        var throwingRegistration = cancellation.Token.Register(throwing.Invoke);
        await using var throwingLifetime = throwingRegistration.ConfigureAwait(false);
        var work = new CancellationWork(cancellation.CancelAsync(), observation, throwing, beforeObserver, afterObserver);
        await RunWithCleanupAsync(async () =>
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            await Assert.ThrowsAsync<TimeoutException>(() => work.ObserveCallbacksAsync(
                fixture, cancellation, value => AssertRegistrationRegion(value, heldRegion, throwing, beforeObserver, afterObserver)))
                .ConfigureAwait(false);
            Assert.False(work.CallbacksCompleted);
            Assert.Equal(new CancellationCallbackProbe.CallbackProgress(true, false, false), held.Progress);
        }, async () =>
        {
            release.Set();
            var failure = await Assert.ThrowsAsync<AggregateException>(() => work.RetireAsync(fixture)).ConfigureAwait(false);
            Assert.Contains(original, failure.InnerExceptions);
            Assert.False(CatastrophicExceptionPolicy.Contains(failure));
        }).ConfigureAwait(true);
        Assert.True(work.CallbacksFaulted);
        Assert.True(work.ObservationCanceled);
        Assert.Equal(new CancellationCallbackProbe.CallbackProgress(true, true, true), held.Progress);
        Assert.Equal(new CancellationCallbackProbe.CallbackProgress(true, true, true), beforeObserver.Progress);
        Assert.Equal(new CancellationCallbackProbe.CallbackProgress(true, true, true), afterObserver.Progress);
    }

    private void AssertRegistrationRegion(string snapshot, int heldRegion, CancellationCallbackProbe throwing,
        CancellationCallbackProbe beforeObserver, CancellationCallbackProbe afterObserver)
    {
        // Report before assertions so an unexpected state remains available without admitting success.
        output.WriteLine($"held_region={heldRegion}. {snapshot}");
        Assert.Equal(new CancellationCallbackProbe.CallbackProgress(true, true, false), throwing.Progress);
        var before = beforeObserver.Progress;
        var after = afterObserver.Progress;
        Assert.Equal(new CancellationCallbackProbe.CallbackProgress(heldRegion == 2, heldRegion == 2, heldRegion == 2), before);
        Assert.Equal(new CancellationCallbackProbe.CallbackProgress(heldRegion != 0, heldRegion != 0, heldRegion != 0), after);
        Assert.Contains($"ObserverRegistrations=[BeforeObserve={before.ToDiagnostic()}; AfterObserve={after.ToDiagnostic()}]",
            snapshot, StringComparison.Ordinal);
        Assert.Contains("CallbacksCompleted=False", snapshot, StringComparison.Ordinal);
        Assert.Contains("ControlledCallback=[Entered=True, BodyExited=True, ReturnedNormally=False]", snapshot, StringComparison.Ordinal);
        // In the later regions the real observer may already settle/retire its root; no pending/root-live assertion is invented.
    }

    private static CancellationTokenRegistration RegisterHeldRegion(
        int selected, int position, CancellationCallbackProbe held, CancellationToken token)
        => token.Register(() =>
        {
            if (selected == position)
                held.Invoke();
        });

    private static CancellationCallbackProbe CreateHeldRegistration(ManualResetEventSlim release, TaskCompletionSource entered)
        => new(() =>
        {
            entered.TrySetResult();
#pragma warning disable VSTHRD002 // This real synchronous callback is deliberately held; guaranteed cleanup releases it and joins the first callback-chain and observer tasks before dependency disposal.
            if (!release.Wait(TimeSpan.FromSeconds(10)))
                throw new TimeoutException("Controlled registration-region callback was not released.");
#pragma warning restore VSTHRD002
        });
}
