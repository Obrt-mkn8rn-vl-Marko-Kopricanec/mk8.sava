namespace Mk8.Sava.Tests;

public sealed partial class SplitServiceProcessTests
{
    [Theory]
    [InlineData("ordinary")]
    [InlineData("inherited")]
    public async Task NaturalRootExitAndCleanupShareOneActualWait(string mode)
    {
        var fixture = await ServiceFixture.StartAsync(mode).ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);
        var completion = fixture.Service.RootExitCompletion;
        var exitToken = fixture.Service.RootExitCancellation;
        Assert.False(completion.IsCompleted);
        Assert.Throws<InvalidOperationException>(() => { _ = fixture.Service.CleanupRootExitCompletion; });
        await fixture.ReleaseRootAsync().ConfigureAwait(true);

        await fixture.Service.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(6)).ConfigureAwait(true);

        Assert.Same(completion, fixture.Service.RootExitCompletion);
        Assert.Same(completion, fixture.Service.CleanupRootExitCompletion);
        Assert.True(completion.IsCompletedSuccessfully);
        Assert.True(exitToken.IsCancellationRequested);
        AssertExitLifetimeDisposed(fixture.Service);
        Assert.True(fixture.Root.HasExited);
        Assert.Contains("root-output-without-newline", fixture.Service.Logs, StringComparison.Ordinal);
        if (fixture.Descendant is not null)
            Assert.False(fixture.Descendant.HasExited);
    }

    [Fact]
    public async Task LiveRootCleanupReusesOwnedTaskAndDisposesItsExitLifetime()
    {
        var fixture = await ServiceFixture.StartAsync("ordinary").ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);
        var completion = fixture.Service.RootExitCompletion;
        var exitToken = fixture.Service.RootExitCancellation;
        await fixture.Service.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(6)).ConfigureAwait(true);

        Assert.Same(completion, fixture.Service.RootExitCompletion);
        Assert.Same(completion, fixture.Service.CleanupRootExitCompletion);
        Assert.True(completion.IsCompletedSuccessfully);
        Assert.True(exitToken.IsCancellationRequested);
        AssertExitLifetimeDisposed(fixture.Service);
        Assert.True(fixture.Root.HasExited);
    }

    [Fact]
    public async Task HeldCaptureCallbackCannotCancelACompletedOwnedRootWait()
    {
        var fixture = await ServiceFixture.StartAsync("ordinary").ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);
        var completion = fixture.Service.RootExitCompletion;
        var exitToken = fixture.Service.RootExitCancellation;
        await fixture.ReleaseRootAsync().ConfigureAwait(true);
        await completion.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(true);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        using var registration = fixture.Service.CaptureCancellation.Register(() =>
        {
            entered.TrySetResult();
#pragma warning disable VSTHRD002 // Hold the actual synchronous capture callback; guaranteed cleanup releases it and joins its first cancellation task.
            release.Wait();
#pragma warning restore VSTHRD002
        });
        var disposal = fixture.Service.DisposeAsync().AsTask();
#pragma warning disable VSTHRD003 // This task is started immediately above; guaranteed cleanup joins this owned observation.
        var observedDisposal = Record.ExceptionAsync(() => disposal);
#pragma warning restore VSTHRD003
        await ServiceFixture.RetireAndDisposeAsync(ExerciseAsync, JoinAsync).ConfigureAwait(true);

        async Task ExerciseAsync()
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(true);
            var failure = await observedDisposal.WaitAsync(TimeSpan.FromSeconds(6)).ConfigureAwait(true);
            ThrowUnexpectedFailure(failure);
            var timeout = Assert.IsType<TimeoutException>(failure);
            Assert.Same(completion, fixture.Service.RootExitCompletion);
            Assert.Same(completion, fixture.Service.CleanupRootExitCompletion);
            Assert.True(completion.IsCompletedSuccessfully);
            Assert.Contains("OwnedRootExit=RanToCompletion", timeout.Message, StringComparison.Ordinal);
            Assert.Contains("ExitCancellationRequested=False", timeout.Message, StringComparison.Ordinal);
            Assert.Contains("CaptureCallbacks=Running", timeout.Message, StringComparison.Ordinal);
            Assert.True(exitToken.IsCancellationRequested);
            AssertExitLifetimeDisposed(fixture.Service);
            output.WriteLine(timeout.Message);
        }

        async ValueTask JoinAsync()
        {
            release.Set();
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

    private static void AssertExitLifetimeDisposed(SplitServiceProcess service) =>
        Assert.Throws<ObjectDisposedException>(() => { _ = service.RootExitCancellation; });
}
