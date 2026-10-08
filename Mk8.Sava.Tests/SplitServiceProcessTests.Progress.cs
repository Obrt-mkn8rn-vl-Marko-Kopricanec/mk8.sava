using System.Runtime.ExceptionServices;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed partial class SplitServiceProcessTests
{
    [Fact]
    public async Task CleanupDeadlineIdentifiesAHeldActualCaptureCallback()
    {
        var fixture = await ServiceFixture.StartAsync("ordinary").ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);
        await fixture.ReleaseRootAsync().ConfigureAwait(true);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        using var registration = fixture.Service.CaptureCancellation.Register(() =>
        {
            entered.TrySetResult();
#pragma warning disable VSTHRD002 // Deliberately hold the real synchronous cancellation callback; finally releases it and joins the actual CancelAsync task.
            release.Wait();
#pragma warning restore VSTHRD002
        });
        var disposal = fixture.Service.DisposeAsync().AsTask();
#pragma warning disable VSTHRD003 // This task was started immediately above in this context; guaranteed cleanup joins its owned observation rather than unrelated work.
        var observedDisposal = Record.ExceptionAsync(() => disposal);
#pragma warning restore VSTHRD003
        await ServiceFixture.RetireAndDisposeAsync(ExerciseAsync, JoinAsync).ConfigureAwait(true);

        async Task ExerciseAsync()
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(true);
            var observed = await observedDisposal.WaitAsync(TimeSpan.FromSeconds(6)).ConfigureAwait(true);
            ThrowUnexpectedFailure(observed);
            var failure = Assert.IsType<TimeoutException>(observed);

            Assert.False(fixture.Service.CaptureCallbackCompletion.IsCompleted);
            Assert.Contains("CaptureCancellationRequested=True", failure.Message, StringComparison.Ordinal);
            Assert.Contains("CaptureCallbacks=Running", failure.Message, StringComparison.Ordinal);
            Assert.Contains("Task states (sequential observations): Stdout=", failure.Message, StringComparison.Ordinal);
            Assert.Contains("RootObserver=", failure.Message, StringComparison.Ordinal);
            Assert.Contains("RootWait=", failure.Message, StringComparison.Ordinal);
            Assert.Contains("Cleanup elapsed=", failure.Message, StringComparison.Ordinal);
            Assert.IsAssignableFrom<OperationCanceledException>(failure.InnerException);
            output.WriteLine(failure.Message);
        }

        async ValueTask JoinAsync()
        {
            release.Set();
            // Attempt both owned joins even if one fails; do not erase the exercise failure in finally.
            await ServiceFixture.RetireAndDisposeAsync(JoinDisposalAsync,
                () => new ValueTask(fixture.Service.CaptureCallbackCompletion.WaitAsync(TimeSpan.FromSeconds(5))))
                .ConfigureAwait(true);
        }

        async Task JoinDisposalAsync()
        {
            var failure = await observedDisposal.WaitAsync(TimeSpan.FromSeconds(6)).ConfigureAwait(true);
            ThrowUnexpectedFailure(failure);
        }
    }

    private static void ThrowUnexpectedFailure(Exception? failure)
    {
        // Only the actual service's expected ordinary timeout is retained as control data.
        // WaitAsync deadlines themselves are outside Record.ExceptionAsync and still fail either join.
        if (failure is not null && (failure is not TimeoutException || CatastrophicExceptionPolicy.Contains(failure)))
            ExceptionDispatchInfo.Throw(failure);
    }

    [Fact]
    public async Task CleanupFaultIdentifiesTheActualCallbackAndRetainsItsOriginalGraph()
    {
        var fixture = await ServiceFixture.StartAsync("ordinary").ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);
        await fixture.ReleaseRootAsync().ConfigureAwait(true);
        var callbackFailure = new IOException("controlled capture callback failure");
        using var registration = fixture.Service.CaptureCancellation.Register(() => throw callbackFailure);

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Service.DisposeAsync().AsTask()
            .WaitAsync(TimeSpan.FromSeconds(6))).ConfigureAwait(true);

        Assert.True(fixture.Service.CaptureCallbackCompletion.IsFaulted);
        Assert.Contains("CaptureCallbacks=Faulted", failure.Message, StringComparison.Ordinal);
        Assert.Contains("CaptureCancellationRequested=True", failure.Message, StringComparison.Ordinal);
        var callbacks = Assert.IsType<AggregateException>(failure.InnerException);
        Assert.Contains(callbackFailure, callbacks.Flatten().InnerExceptions);
        output.WriteLine(failure.Message);
    }
}
