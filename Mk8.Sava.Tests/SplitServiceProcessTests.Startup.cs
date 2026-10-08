using System.Runtime.ExceptionServices;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed partial class SplitServiceProcessTests
{
    [Fact]
    public async Task WithheldReadyProofRetainsActualStartupCancellationAndPreCleanupCapture()
    {
        var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ServiceFixture.StartAsync("withheld-ready")).ConfigureAwait(true);

        var original = Assert.IsAssignableFrom<OperationCanceledException>(failure.InnerException);
        Assert.Equal(original.CancellationToken, failure.CancellationToken);
        Assert.True(failure.CancellationToken.IsCancellationRequested);
        Assert.Contains("Phase=AwaitingReadyProof", failure.Message, StringComparison.Ordinal);
        Assert.Contains("StartupCancellationRequested=True", failure.Message, StringComparison.Ordinal);
        Assert.Contains("PipePreparationStarted=True, PipePreparationCompleted=True, ReadyWithheld=True", failure.Message, StringComparison.Ordinal);
        Assert.Contains("ReadyPublishStarted=False, ReadyPublishCompleted=False, ReadyProof=False", failure.Message, StringComparison.Ordinal);
        Assert.Contains("RootExited=False, StdoutEof=False, StderrEof=False", failure.Message, StringComparison.Ordinal);
        Assert.Contains("Stdout=[ReadRequests=", failure.Message, StringComparison.Ordinal);
        Assert.Contains("Stderr=[ReadRequests=", failure.Message, StringComparison.Ordinal);
        Assert.Contains("root-output-without-newline", failure.Message, StringComparison.Ordinal);
        Assert.Contains("root-error-without-newline", failure.Message, StringComparison.Ordinal);
        output.WriteLine(failure.Message);
    }

    [Theory]
    [InlineData("stdout")]
    [InlineData("stderr")]
    public async Task ReadyProofWithoutOneCaptureRetainsTheDistinctAcknowledgementPhase(string withheld)
    {
        var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ServiceFixture.StartAsync("delayed-" + withheld)).ConfigureAwait(true);

        var original = Assert.IsAssignableFrom<OperationCanceledException>(failure.InnerException);
        Assert.Equal(original.CancellationToken, failure.CancellationToken);
        Assert.True(failure.CancellationToken.IsCancellationRequested);
        Assert.Contains("Phase=AwaitingCapturedOutput", failure.Message, StringComparison.Ordinal);
        Assert.Contains("StartupCancellationRequested=True", failure.Message, StringComparison.Ordinal);
        Assert.Contains("ReadyPublishStarted=True, ReadyPublishCompleted=True, ReadyProof=True", failure.Message, StringComparison.Ordinal);
        Assert.Contains("RootExited=False, StdoutEof=False, StderrEof=False", failure.Message, StringComparison.Ordinal);
        var missing = string.Equals(withheld, "stdout", StringComparison.Ordinal) ? "output" : "error";
        var present = string.Equals(withheld, "stdout", StringComparison.Ordinal) ? "error" : "output";
        Assert.DoesNotContain("root-" + missing + "-without-newline", failure.Message, StringComparison.Ordinal);
        Assert.Contains("root-" + present + "-without-newline", failure.Message, StringComparison.Ordinal);
        output.WriteLine(failure.Message);
    }

    [Fact]
    public async Task ProducerExitBeforeReadyProofRetainsOriginalFailureAndObservedPhase()
    {
        var failure = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ServiceFixture.StartAsync("exit-before-ready")).ConfigureAwait(true);

        var original = Assert.IsType<InvalidOperationException>(failure.InnerException);
        Assert.Equal("Service fixture exited before its readiness proof.", original.Message);
        Assert.Contains("Phase=AwaitingReadyProof", failure.Message, StringComparison.Ordinal);
        Assert.Contains("StartupCancellationRequested=False", failure.Message, StringComparison.Ordinal);
        Assert.Contains("PipePreparationStarted=True, PipePreparationCompleted=True", failure.Message, StringComparison.Ordinal);
        Assert.Contains("ReadyPublishStarted=False, ReadyPublishCompleted=False, ReadyProof=False", failure.Message, StringComparison.Ordinal);
        Assert.Contains("RootExited=True", failure.Message, StringComparison.Ordinal);
        output.WriteLine(failure.Message);
    }

    [Fact]
    public async Task FailedWriterReadyCallbackIsObservedBeforeSuccessfulOwnedRetirement()
    {
        var original = new IOException("controlled startup callback failure");
        ServiceFixture? observed = null;
        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => ServiceFixture.StartAsync("ordinary", fixture =>
        {
            observed = fixture;
            throw original;
        })).ConfigureAwait(true);

        Assert.Same(original, failure.InnerException);
        Assert.Contains("Phase=WriterReadyCallback", failure.Message, StringComparison.Ordinal);
        Assert.Contains("ReadyProof=True", failure.Message, StringComparison.Ordinal);
        Assert.Contains("RootExited=False", failure.Message, StringComparison.Ordinal);
        Assert.NotNull(observed);
        Assert.True(observed.Service.RootExitCompletion.IsCompletedSuccessfully);
        Assert.True(observed.Service.CaptureCallbackCompletion.IsCompletedSuccessfully);
        AssertExitLifetimeDisposed(observed.Service);
        output.WriteLine(failure.Message);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StartupAndIndependentRetirementFailuresBothSurviveOwnedServiceCleanup(bool fatal)
    {
        var original = fatal
            ? new IOException("controlled wrapped fatal startup", CreateFatal("oom"))
            : new IOException("controlled ordinary startup");
        ServiceFixture? observed = null;
        var recorded = await Record.ExceptionAsync(() => ServiceFixture.StartAsync("ordinary", fixture =>
        {
            observed = fixture;
            fixture.BlockRetirementSignal("release-root");
            throw original;
        })).ConfigureAwait(true);

        // Only this controlled original fatal graph is expected; dispatch any independent
        // catastrophic cleanup failure rather than normalize it as an ordinary assertion.
        var failure = Assert.IsType<AggregateException>(recorded);
        Assert.Equal(2, failure.InnerExceptions.Count);
        if (fatal)
            Assert.Same(original, failure.InnerExceptions[0]);
        else
        {
            var startup = Assert.IsType<InvalidOperationException>(failure.InnerExceptions[0]);
            Assert.Same(original, startup.InnerException);
            Assert.Contains("Phase=WriterReadyCallback", startup.Message, StringComparison.Ordinal);
            Assert.Contains("RootExited=False", startup.Message, StringComparison.Ordinal);
        }
        if (CatastrophicExceptionPolicy.Contains(failure.InnerExceptions[1]))
            ExceptionDispatchInfo.Throw(failure.InnerExceptions[1]);
        var retirementFailures = failure.InnerExceptions[1] is AggregateException cleanup
            ? cleanup.Flatten().InnerExceptions
            : [failure.InnerExceptions[1]];
        Assert.Contains(retirementFailures, static error => error is IOException or UnauthorizedAccessException);
        Assert.NotNull(observed);
        // Access to the retained FIRST callback task proves cleanup was attempted despite
        // retirement failing; it does not certify arbitrary descendants or future faults.
        Assert.NotNull(observed.Service.CaptureCallbackCompletion);
        AssertExitLifetimeDisposed(observed.Service);
        output.WriteLine(failure.ToString());
    }
}
