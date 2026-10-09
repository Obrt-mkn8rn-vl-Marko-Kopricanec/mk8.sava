using System.Runtime.ExceptionServices;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed partial class TestProcessRunnerTests
{
    [Fact]
    public async Task HeldCallbackRetainsRequestedCancellationAndPreCleanupTaskProgress()
    {
        using var cancellation = new CancellationTokenSource();
        var fixture = await ProcessFixture.StartAsync("ordinary").ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var observation = TestProcessRunner.ObserveAsync(fixture.Root,
            TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(2), cancellation.Token);
        var registration = cancellation.Token.Register(() =>
        {
            entered.TrySetResult();
#pragma warning disable VSTHRD002 // Cancellation callbacks are synchronous; this controlled ten-second gate models a held callback and is released in guaranteed cleanup.
            if (!release.Wait(TimeSpan.FromSeconds(10)))
                throw new TimeoutException("Controlled cancellation callback was not released.");
#pragma warning restore VSTHRD002
        });
        await using var registrationLifetime = registration.ConfigureAwait(false);
        var work = new CancellationWork(cancellation.CancelAsync(), observation);
        string? snapshot = null;
        await RunWithCleanupAsync(async () =>
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            await Assert.ThrowsAsync<TimeoutException>(() => work.ObserveCallbacksAsync(
                fixture, cancellation, value =>
                {
                    snapshot = value;
                    output.WriteLine(value);
                })).ConfigureAwait(false);

            Assert.NotNull(snapshot);
            Assert.Contains("CancellationRequested=True", snapshot, StringComparison.Ordinal);
            Assert.Contains("CallbacksCompleted=False", snapshot, StringComparison.Ordinal);
            Assert.Contains("ObservationCompleted=False", snapshot, StringComparison.Ordinal);
            Assert.Contains("RootExited=False", snapshot, StringComparison.Ordinal);
            Assert.False(work.CallbacksCompleted);
            Assert.False(work.ObservationCompleted);
        }, async () =>
        {
            release.Set();
            await work.RetireAsync(fixture).ConfigureAwait(false);
        }).ConfigureAwait(true);
        Assert.True(work.CallbacksSucceeded);
        Assert.True(work.ObservationCanceled);
    }

    [Fact]
    public async Task CallbackFaultRemainsDistinctFromObserverCancellationAndRetainsProgress()
    {
        using var cancellation = new CancellationTokenSource();
        var fixture = await ProcessFixture.StartAsync("ordinary").ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);
        var beforeObserver = new CancellationCallbackProbe(static () => { });
        var beforeRegistration = cancellation.Token.Register(beforeObserver.Invoke);
        await using var beforeLifetime = beforeRegistration.ConfigureAwait(false);
        var observation = TestProcessRunner.ObserveAsync(fixture.Root,
            TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(2), cancellation.Token);
        var afterObserver = new CancellationCallbackProbe(static () => { });
        var afterRegistration = cancellation.Token.Register(afterObserver.Invoke);
        await using var afterLifetime = afterRegistration.ConfigureAwait(false);
        var original = new IOException("controlled cancellation callback failure");
        var callbackProgress = new CancellationCallbackProbe(() => throw original);
        var registration = cancellation.Token.Register(callbackProgress.Invoke);
        await using var registrationLifetime = registration.ConfigureAwait(false);
        var work = new CancellationWork(cancellation.CancelAsync(), observation, callbackProgress, beforeObserver, afterObserver);
        string? snapshot = null;
        await RunWithCleanupAsync(async () =>
        {
            var failure = await AssertCallbackFaultAsync(work, fixture, cancellation,
                value => snapshot = value).ConfigureAwait(false);

            Assert.Contains(original, failure.InnerExceptions);
            Assert.NotNull(snapshot);
            Assert.Contains("CancellationRequested=True", snapshot, StringComparison.Ordinal);
            Assert.Contains("CallbacksStatus=Faulted", snapshot, StringComparison.Ordinal);
            Assert.Contains("ControlledCallback=[Entered=True, BodyExited=True, ReturnedNormally=False]", snapshot, StringComparison.Ordinal);
            Assert.Contains("ObserverRegistrations=[BeforeObserve=Entered=True, BodyExited=True, ReturnedNormally=True; " +
                "AfterObserve=Entered=True, BodyExited=True, ReturnedNormally=True]", snapshot, StringComparison.Ordinal);
        }, async () =>
        {
            // Join both actual tasks; retain, rather than replace, the deliberately faulted callback operation.
            var cleanupFailure = await Assert.ThrowsAsync<AggregateException>(() => work.RetireAsync(fixture)).ConfigureAwait(false);
            Assert.Contains(original, cleanupFailure.InnerExceptions);
            Assert.False(CatastrophicExceptionPolicy.Contains(cleanupFailure));
        }).ConfigureAwait(true);
        Assert.True(work.CallbacksFaulted);
        Assert.True(work.ObservationCanceled);
    }

    private Task<AggregateException> AssertCallbackFaultAsync(
        CancellationWork work, ProcessFixture fixture, CancellationTokenSource cancellation, Action<string> retain)
        => Assert.ThrowsAsync<AggregateException>(() => work.ObserveCallbacksAsync(
            fixture, cancellation, value =>
            {
                retain(value);
                // Report before the expected-type assertion can reject an unexpected timeout.
                output.WriteLine(value);
            }));

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PrimaryAndIndependentCleanupFailuresRemainInTheFailureGraph(bool fatal)
    {
#pragma warning disable CA2201 // Synthetic failure tests graph preservation, not actual memory exhaustion.
        Exception primary = fatal ? new OutOfMemoryException("synthetic primary fatal") : new IOException("primary failure");
#pragma warning restore CA2201
        var cleanup = new IOException("independent cleanup failure");
        var attempted = false;
        var failure = await Assert.ThrowsAsync<AggregateException>(() => RunWithCleanupAsync(
            () => Task.FromException(primary), () =>
            {
                attempted = true;
                return Task.FromException(cleanup);
            })).ConfigureAwait(true);

        Assert.True(attempted);
        Assert.Collection(failure.InnerExceptions,
            first => Assert.Same(primary, first), second => Assert.Same(cleanup, second));
        Assert.Equal(fatal, CatastrophicExceptionPolicy.Contains(failure));
    }

    [Fact]
    public async Task KnownFatalCallbackFaultEscapesWhenObserverRemainsPendingPastCleanup()
    {
#pragma warning disable CA2201 // Synthetic wrapped failure tests known-task fault propagation, not actual exhaustion.
        var original = new IOException("wrapped callback fault", new OutOfMemoryException("synthetic pending-join fatal"));
#pragma warning restore CA2201
        var observation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completion = new CancellationWork(Task.FromException(original), observation.Task).JoinAsync();
        try
        {
            var failure = await Record.ExceptionAsync(() => completion.WaitAsync(TimeSpan.FromSeconds(7))).ConfigureAwait(true);

            Assert.Same(original, failure);
            Assert.False(observation.Task.IsCompleted);
        }
        finally
        {
            observation.TrySetResult();
            ObserveEventualCancellationFault(completion);
        }
    }

    private sealed class CancellationWork(Task callbacks, Task observation, CancellationCallbackProbe? callbackProgress = null,
        CancellationCallbackProbe? beforeObserver = null, CancellationCallbackProbe? afterObserver = null)
    {
        internal bool CallbacksCompleted => callbacks.IsCompleted;
        internal bool ObservationCompleted => observation.IsCompleted;
        internal bool CallbacksSucceeded => callbacks.IsCompletedSuccessfully;
        internal bool CallbacksFaulted => callbacks.IsFaulted;
        internal bool ObservationCanceled => observation.IsCanceled;

        internal async Task ObserveCallbacksAsync(ProcessFixture fixture, CancellationTokenSource cancellation, Action<string> report)
        {
            try
            {
                await callbacks.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false);
            }
            catch (Exception failure) when (!CatastrophicExceptionPolicy.Contains(failure))
            {
                report($"Caller cancellation callback observation failed: {failure.GetType().Name}. " +
                    $"CancellationRequested={cancellation.IsCancellationRequested}, CallbacksStatus={callbacks.Status}, " +
                    $"CallbacksCompleted={callbacks.IsCompleted}, ObservationStatus={observation.Status}, " +
                    $"ObservationCompleted={observation.IsCompleted}. " +
                    $"ControlledCallback=[{callbackProgress?.Progress.ToDiagnostic() ?? "NotInstrumented"}]. " +
                    // Registration-time markers surround observer creation, not native work or an atomic callback-chain snapshot.
                    $"ObserverRegistrations=[BeforeObserve={beforeObserver?.Progress.ToDiagnostic() ?? "NotInstrumented"}; " +
                    $"AfterObserve={afterObserver?.Progress.ToDiagnostic() ?? "NotInstrumented"}]. " +
                    $"{fixture.StartupDiagnostic}");
                throw;
            }
        }

        internal Task<OperationCanceledException> ObserveCancellationAsync()
            => Assert.ThrowsAnyAsync<OperationCanceledException>(() => observation.WaitAsync(TimeSpan.FromSeconds(6)));

        internal Task RetireAsync(ProcessFixture fixture) => RunWithCleanupAsync(fixture.ReleaseRootAsync, JoinAsync);

        internal async Task JoinAsync()
        {
            var completion = Task.WhenAll(callbacks, ObserveTestCompletionAsync(observation));
            try
            {
                await completion.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            }
            catch (Exception failure) when (!CatastrophicExceptionPolicy.Contains(failure))
            {
                ThrowKnownCancellationFault(callbacks);
                ThrowKnownCancellationFault(observation);
                throw;
            }
            finally
            {
                ObserveEventualCancellationFault(callbacks);
                ObserveEventualCancellationFault(observation);
                ObserveEventualCancellationFault(completion);
            }
        }
    }

    private static void ThrowKnownCancellationFault(Task task)
    {
        if (task.Exception is not { } failures)
            return;
        foreach (var failure in failures.InnerExceptions)
            if (CatastrophicExceptionPolicy.Contains(failure))
                ExceptionDispatchInfo.Throw(failure);
    }

    private static void ObserveEventualCancellationFault(Task task)
        => _ = task.ContinueWith(static completed => _ = completed.Exception,
            CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnFaulted,
            TaskScheduler.Default);

    private static async Task RunWithCleanupAsync(Func<Task> action, Func<Task> cleanup)
    {
        Exception? primary = null;
        var cleanupCompleted = false;
        try
        {
            try
            {
                await action().ConfigureAwait(false);
            }
            catch (Exception failure)
            {
                primary = failure;
                throw;
            }
            finally
            {
                await cleanup().ConfigureAwait(false);
                cleanupCompleted = true;
            }
        }
        catch (Exception failure)
        {
            if (primary is not null && !cleanupCompleted)
                throw new AggregateException("Primary operation and owned cancellation cleanup both failed.", primary, failure);
            throw;
        }
    }
}
