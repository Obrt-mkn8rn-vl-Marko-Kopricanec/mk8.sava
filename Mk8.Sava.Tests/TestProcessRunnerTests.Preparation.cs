namespace Mk8.Sava.Tests;

public sealed partial class TestProcessRunnerTests
{
    [Fact]
    public async Task RetainingBothPipesSkipsUnusedNativePreparationAndStillRequiresBothEofs()
    {
        var fixture = await ProcessFixture.StartAsync("both", fault: FixtureFault.RejectedNativePreparation).ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);
        Assert.Contains("DescendantPreparation=[NativeTypeStarted=False, NativeTypeCompleted=False, NativeTypeSkipped=True, PipesPrepared=True]",
            fixture.StartupDiagnostic, StringComparison.Ordinal);
        Assert.NotNull(fixture.Descendant);
        Assert.False(fixture.Descendant.HasExited);
        output.WriteLine(fixture.StartupDiagnostic);
        await fixture.ReleaseRootAsync().ConfigureAwait(true);
        var observation = TestProcessRunner.ObserveAsync(fixture.Root, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        try
        {
            var failure = await Assert.ThrowsAsync<TimeoutException>(() => observation.WaitAsync(TimeSpan.FromSeconds(6)))
                .ConfigureAwait(true);

            Assert.Contains("RootExited=True, StdoutEof=False, StderrEof=False", failure.Message, StringComparison.Ordinal);
            Assert.Contains("root-output-without-newline", failure.Message, StringComparison.Ordinal);
            Assert.Contains("root-error-without-newline", failure.Message, StringComparison.Ordinal);
            Assert.False(fixture.Descendant.HasExited);
            output.WriteLine(failure.Message);
        }
        finally
        {
            await fixture.ReleaseDescendantAsync().ConfigureAwait(true);
            await ObserveTestCompletionAsync(observation).ConfigureAwait(true);
        }
    }

    [Fact]
    public async Task ExitAfterRequiredPipePreparationRetainsPhaseAndPartialOutputsBeforeCleanup()
    {
        var failure = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            var fixture = await ProcessFixture.StartAsync("stderr", fault: FixtureFault.ExitedDescendantAfterPreparation)
                .ConfigureAwait(false);
            await fixture.DisposeAsync().ConfigureAwait(false);
        }).ConfigureAwait(true);

        var preparation = OperatingSystem.IsWindows()
            ? "DescendantPreparation=[NativeTypeStarted=True, NativeTypeCompleted=True, NativeTypeSkipped=False, PipesPrepared=True]"
            : "DescendantPreparation=[NativeTypeStarted=False, NativeTypeCompleted=False, NativeTypeSkipped=True, PipesPrepared=True]";
        Assert.Contains(preparation, failure.Message, StringComparison.Ordinal);
        Assert.Contains("RootReady=False", failure.Message, StringComparison.Ordinal);
        Assert.Contains("DescendantReady=False", failure.Message, StringComparison.Ordinal);
        Assert.Contains("injected-after-preparation-error", failure.Message, StringComparison.Ordinal);
        Assert.Contains("root-output-without-newline", failure.Message, StringComparison.Ordinal);
        Assert.Contains("root-error-without-newline", failure.Message, StringComparison.Ordinal);
        output.WriteLine(failure.Message);
    }
}
