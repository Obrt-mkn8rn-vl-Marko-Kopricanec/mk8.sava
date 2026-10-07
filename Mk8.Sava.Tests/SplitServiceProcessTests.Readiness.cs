namespace Mk8.Sava.Tests;

public sealed partial class SplitServiceProcessTests
{
    [Theory]
    [InlineData("stdout")]
    [InlineData("stderr")]
    public async Task WriterReadinessCannotCompleteFixtureBeforeBothCapturesAcknowledgeOutput(string delayed)
    {
        var writerReady = new TaskCompletionSource<ServiceFixture>(TaskCreationOptions.RunContinuationsAsynchronously);
#pragma warning disable CA2000 // The finally's guaranteed cleanup joins this construction and disposes its returned fixture; StartAsync owns failure cleanup.
        var starting = ServiceFixture.StartAsync("delayed-" + delayed, fixture => writerReady.TrySetResult(fixture));
#pragma warning restore CA2000
        ServiceFixture? bound = null;
        try
        {
            bound = await writerReady.Task.WaitAsync(TimeSpan.FromSeconds(20)).ConfigureAwait(true);
            Assert.False(bound.Root.HasExited);
            Assert.DoesNotContain("root-" + (string.Equals(delayed, "stdout", StringComparison.Ordinal) ? "output" : "error") +
                "-without-newline", bound.Service.Logs, StringComparison.Ordinal);
            output.WriteLine($"Writer readiness with delayed {delayed}:\n{bound.Service.Logs}");

            // Observe a pending construction without cancelling it or relaxing the listener's existing one-second deadline.
            await Assert.ThrowsAsync<TimeoutException>(() => starting.WaitAsync(TimeSpan.FromSeconds(1))).ConfigureAwait(true);
            Assert.False(starting.IsCompleted);
            await bound.ReleaseOutputAsync().ConfigureAwait(true);
            var ready = await starting.WaitAsync(TimeSpan.FromSeconds(20)).ConfigureAwait(true);
            Assert.Same(bound, ready);
            Assert.Contains("root-output-without-newline", ready.Service.Logs, StringComparison.Ordinal);
            Assert.Contains("root-error-without-newline", ready.Service.Logs, StringComparison.Ordinal);

            var failure = await Assert.ThrowsAsync<TimeoutException>(() => ready.Service.WaitForAddressAsync(
                "Application", TimeSpan.FromSeconds(1))).ConfigureAwait(true);
            Assert.Contains("root-output-without-newline", failure.Message, StringComparison.Ordinal);
            Assert.Contains("root-error-without-newline", failure.Message, StringComparison.Ordinal);
            Assert.False(ready.Root.HasExited);
            output.WriteLine(failure.Message);
        }
        finally
        {
            // Release the controlled producer and join construction before disposing its returned owned graph.
            await ServiceFixture.RetireAndDisposeAsync(
                () => bound is not null && !starting.IsCompleted ? bound.ReleaseOutputAsync() : Task.CompletedTask,
                async () =>
                {
                    // Construction owns its existing setup deadline and failure cleanup; do not abandon it at another timer.
#pragma warning disable VSTHRD003 // This test owns starting; its context-independent construction must be joined before fixture disposal.
                    var ready = await starting.ConfigureAwait(false);
#pragma warning restore VSTHRD003
                    await ready.DisposeAsync().ConfigureAwait(false);
                }).ConfigureAwait(true);
        }
    }
}
