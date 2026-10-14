using System.Runtime.ExceptionServices;

namespace Mk8.Sava.Tests;

public sealed partial class SplitServiceProcessTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RepeatedDisposalJoinsTheHeldCaptureCallbackAndRetainsItsOutcome(bool failCallback)
    {
        var fixture = await ServiceFixture.StartAsync("ordinary").ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);
        await fixture.ReleaseRootAsync().ConfigureAwait(true);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var callbackFailure = new IOException("controlled repeated-disposal callback fault");
        var registration = fixture.Service.CaptureCancellation.Register(() =>
        {
            entered.TrySetResult();
#pragma warning disable VSTHRD002 // Hold this actual synchronous callback; guaranteed cleanup releases it and joins the first cancellation operation.
            release.Wait(CancellationToken.None);
#pragma warning restore VSTHRD002
            if (failCallback)
                throw callbackFailure;
        });
        await using var registrationLifetime = registration.ConfigureAwait(false);
        var first = fixture.Service.DisposeAsync().AsTask();
#pragma warning disable VSTHRD003 // The test starts and owns this actual disposal; finally independently joins it before retiring the fixture.
        var observedFirst = Record.ExceptionAsync(() => first);
#pragma warning restore VSTHRD003
        try
        {
            await VerifyRepeatedDisposalAsync(fixture, entered, release, first, observedFirst, failCallback, callbackFailure)
                .ConfigureAwait(true);
        }
        finally
        {
            release.Set();
            await ServiceFixture.RetireAndDisposeAsync(async () =>
            {
#pragma warning disable VSTHRD003 // observedFirst is this test's owned first disposal observation; release and actual callback join precede fixture retirement.
                var failure = await observedFirst.ConfigureAwait(false);
#pragma warning restore VSTHRD003
                if (failCallback && failure is InvalidOperationException { InnerException: AggregateException callbacks } &&
                    callbacks.Flatten().InnerExceptions.Contains(callbackFailure))
                {
                    fixture.VerifiedCleanupFailure ??= failure;
                }
                else if (failure is not null)
                {
                    ExceptionDispatchInfo.Throw(failure);
                }
            }, () => new ValueTask(JoinCallbackAsync(fixture.Service, callbackFailure, failCallback)))
                .ConfigureAwait(true);
        }
    }

    private static async Task JoinCallbackAsync(SplitServiceProcess service, IOException callbackFailure, bool expectedFault)
    {
        var failure = await Record.ExceptionAsync(() => service.CaptureCallbackCompletion
            .WaitAsync(TimeSpan.FromSeconds(5), CancellationToken.None)).ConfigureAwait(false);
        if (expectedFault && failure is AggregateException aggregate &&
            aggregate.Flatten().InnerExceptions is [var only] && ReferenceEquals(only, callbackFailure))
        {
            return;
        }
        if (failure is not null)
            ExceptionDispatchInfo.Throw(failure);
    }

    private static async Task VerifyRepeatedDisposalAsync(ServiceFixture fixture, TaskCompletionSource entered,
        ManualResetEventSlim release, Task first, Task<Exception?> observedFirst, bool failCallback, IOException callbackFailure)
    {
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), CancellationToken.None).ConfigureAwait(false);
        Assert.False(first.IsCompleted);
        var second = fixture.Service.DisposeAsync().AsTask();
        Assert.False(second.IsCompleted);
        release.Set();
        var failure = await observedFirst.WaitAsync(TimeSpan.FromSeconds(6), CancellationToken.None).ConfigureAwait(false);
        if (failCallback)
        {
            var actual = Assert.IsType<InvalidOperationException>(failure);
            var callbacks = Assert.IsType<AggregateException>(actual.InnerException);
            Assert.Contains(callbackFailure, callbacks.Flatten().InnerExceptions);
            fixture.VerifiedCleanupFailure = actual;
        }
        else
        {
            Assert.Null(failure);
        }
        Assert.Same(failure, await Record.ExceptionAsync(() => second.WaitAsync(TimeSpan.FromSeconds(6), CancellationToken.None))
            .ConfigureAwait(false));
        Assert.Same(failure, await Record.ExceptionAsync(() => fixture.Service.DisposeAsync().AsTask()).ConfigureAwait(false));
        Assert.True(fixture.Root.HasExited);
        Assert.True(fixture.Service.CaptureCallbackCompletion.IsCompleted);
    }
}
