using System.Diagnostics;

namespace Mk8.Sava.Tests;

public sealed partial class SplitServiceProcessTests
{
    [Theory]
    [InlineData("uncooperative", "")]
    [InlineData("ordinary", "release-root")]
    [InlineData("inherited", "release-descendant")]
    public async Task FailedCooperativeRetirementStillDisposesOwnedService(string mode, string blockedSignal)
    {
        ArgumentNullException.ThrowIfNull(blockedSignal);
        var fixture = await ServiceFixture.StartAsync(mode).ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);
        using var root = Process.GetProcessById(fixture.Service.Id);
        _ = root.SafeHandle;
        root.EnableRaisingEvents = true;
        using var descendant = fixture.Descendant is null ? null : Process.GetProcessById(fixture.Descendant.Id);
        if (descendant is not null)
        {
            _ = descendant.SafeHandle;
            descendant.EnableRaisingEvents = true;
        }
        try
        {
            if (blockedSignal.Length > 0)
                fixture.BlockRetirementSignal(blockedSignal);

            var failure = await Record.ExceptionAsync(() => fixture.DisposeAsync().AsTask()
                .WaitAsync(TimeSpan.FromSeconds(10))).ConfigureAwait(true);

            Assert.NotNull(failure);
            output.WriteLine(failure.ToString());
            if (blockedSignal.Length == 0)
                Assert.IsAssignableFrom<OperationCanceledException>(failure);
            else
                Assert.True(failure is IOException or UnauthorizedAccessException, failure.ToString());
            // The failure is retirement-only: successful owned disposal has observed its root/drain/callback completion.
            Assert.True(root.HasExited);
            if (descendant is not null)
                Assert.True(descendant.HasExited);
            Assert.Contains("root-output-without-newline", fixture.Service.Logs, StringComparison.Ordinal);
            await fixture.Service.DisposeAsync().ConfigureAwait(true);
        }
        finally
        {
            // The rejected workflow skips disposal, so the red control must still retire its independently bound roots.
            try
            {
                await fixture.Service.DisposeAsync().ConfigureAwait(true);
            }
            finally
            {
                if (!root.HasExited)
                {
                    root.Kill(entireProcessTree: true);
                    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                    await root.WaitForExitAsync(timeout.Token).ConfigureAwait(true);
                }
            }
        }
    }

    [Fact]
    public async Task RetirementFailureKeepsIdentityAfterSuccessfulCleanup()
    {
        var retirementFailure = new IOException("controlled retirement failure");
        var cleanupAttempted = false;

        var failure = await Record.ExceptionAsync(() => ServiceFixture.RetireAndDisposeAsync(
            () => Task.FromException(retirementFailure), () =>
            {
                cleanupAttempted = true;
                return ValueTask.CompletedTask;
            }).AsTask()).ConfigureAwait(true);

        Assert.True(cleanupAttempted);
        Assert.Same(retirementFailure, failure);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task IndependentRetirementAndCleanupFailuresAreBothRetained(bool fatalRetirement)
    {
        var retirementFailure = fatalRetirement
            ? new IOException("controlled wrapped fatal retirement", CreateFatal("oom"))
            : new IOException("controlled retirement failure");
        var cleanupFailure = new IOException("independent cleanup failure");
        var cleanupAttempted = false;

        var failure = await Record.ExceptionAsync(() => ServiceFixture.RetireAndDisposeAsync(
            () => Task.FromException(retirementFailure), () =>
            {
                cleanupAttempted = true;
                return ValueTask.FromException(cleanupFailure);
            }).AsTask()).ConfigureAwait(true);

        Assert.True(cleanupAttempted);
        var aggregate = Assert.IsType<AggregateException>(failure);
        Assert.Collection(aggregate.InnerExceptions,
            first => Assert.Same(retirementFailure, first), second => Assert.Same(cleanupFailure, second));
    }

    [Fact]
    public async Task CleanupFailureKeepsIdentityAfterSuccessfulRetirement()
    {
        var cleanupFailure = new IOException("controlled cleanup failure");
        var retired = false;

        var failure = await Record.ExceptionAsync(() => ServiceFixture.RetireAndDisposeAsync(() =>
            {
                retired = true;
                return Task.CompletedTask;
            }, () => ValueTask.FromException(cleanupFailure)).AsTask()).ConfigureAwait(true);

        Assert.True(retired);
        Assert.Same(cleanupFailure, failure);
    }
}
