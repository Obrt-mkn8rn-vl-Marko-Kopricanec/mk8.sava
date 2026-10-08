using System.Globalization;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Mk8.Sava.Tests;

public sealed partial class TestProcessRunnerTests
{
    [Fact]
    public async Task UnexpectedCallbackTimeoutReportsProgressBeforeTheFaultAssertionFails()
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
#pragma warning disable VSTHRD002 // Deliberately hold the real synchronous callback; guaranteed cleanup releases it and joins both actual operations before resource disposal.
            if (!release.Wait(TimeSpan.FromSeconds(10)))
                throw new TimeoutException("Controlled reporting callback was not released.");
#pragma warning restore VSTHRD002
        });
        await using var registrationLifetime = registration.ConfigureAwait(false);
        var work = new CancellationWork(cancellation.CancelAsync(), observation);
        var recordedOutput = new CallbackTestOutput();
        var assertionOwner = new TestProcessRunnerTests(recordedOutput);
        string? snapshot = null;
        await RunWithCleanupAsync(async () =>
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            var failure = await Record.ExceptionAsync(() => assertionOwner.AssertCallbackFaultAsync(
                work, fixture, cancellation, value => snapshot = value)).ConfigureAwait(false);

            var assertion = Assert.IsType<ThrowsException>(failure);
            Assert.IsType<TimeoutException>(assertion.InnerException);
            Assert.NotNull(snapshot);
            Assert.Equal(snapshot, Assert.Single(recordedOutput.Messages));
            Assert.Contains("CancellationRequested=True", snapshot, StringComparison.Ordinal);
            Assert.Contains("CallbacksCompleted=False", snapshot, StringComparison.Ordinal);
            Assert.Contains("ObservationCompleted=False", snapshot, StringComparison.Ordinal);
            Assert.Contains("RootExited=False", snapshot, StringComparison.Ordinal);
            Assert.False(work.CallbacksCompleted);
            Assert.False(work.ObservationCompleted);
            output.WriteLine($"The expected-fault assertion still rejected the real timeout: {assertion.Message}");
            output.WriteLine(snapshot);
        }, async () =>
        {
            release.Set();
            await work.RetireAsync(fixture).ConfigureAwait(false);
        }).ConfigureAwait(true);
        Assert.True(work.CallbacksSucceeded);
        Assert.True(work.ObservationCanceled);
    }

    private sealed class CallbackTestOutput : ITestOutputHelper
    {
        private readonly List<string> messages = [];

        internal IReadOnlyList<string> Messages => messages;

        public void WriteLine(string message) => messages.Add(message);

        public void WriteLine(string format, params object[] args)
            => WriteLine(string.Format(CultureInfo.InvariantCulture, format, args));
    }
}
