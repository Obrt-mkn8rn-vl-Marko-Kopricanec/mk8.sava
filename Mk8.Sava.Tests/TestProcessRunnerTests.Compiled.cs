namespace Mk8.Sava.Tests;

public sealed partial class TestProcessRunnerTests
{
    [Theory]
    [InlineData(1, "--not-a-fixture")]
    [InlineData(4, "not-a-pipe-mode")]
    public async Task CompiledFixtureRejectsInvalidInvocationBeforePublishingReadiness(int argument, string value)
    {
        var directory = Directory.CreateTempSubdirectory("sava-compiled-invalid-");
        try
        {
            var start = TestProcessFixtureProgram.CreateStartInfo("root", directory.FullName, "ordinary", 0, "None");
            start.ArgumentList[argument] = value;
            start.RedirectStandardOutput = true;
            start.RedirectStandardError = true;
            using var process = System.Diagnostics.Process.Start(start)
                ?? throw new InvalidOperationException("The invalid-invocation control did not start.");
            var result = await TestProcessRunner.ObserveAsync(process, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2))
                .ConfigureAwait(true);
            Assert.Equal(64, result.ExitCode);
            Assert.Empty(result.StandardOutput);
            Assert.Contains("Invalid controlled process-fixture invocation.", result.StandardError, StringComparison.Ordinal);
            Assert.Empty(directory.EnumerateFileSystemInfos());
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(23)]
    public async Task CompiledFixturePreservesExitCodeAndUnterminatedOutputs(int exitCode)
    {
        var fixture = await ProcessFixture.StartAsync("ordinary", exitCode, compiled: true).ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);
        var observation = TestProcessRunner.ObserveAsync(fixture.Root, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(2));
        await fixture.ReleaseRootAsync().ConfigureAwait(true);
        var result = await observation.ConfigureAwait(true);

        Assert.Equal(exitCode, result.ExitCode);
        Assert.Equal("root-output-without-newline", result.StandardOutput);
        Assert.Equal("root-error-without-newline", result.StandardError);
        Assert.Equal("dotnet", fixture.Root.StartInfo.FileName);
    }

    [Fact]
    public async Task CompiledFixtureRetainsBothRealInheritedPipesWhileDescendantIsLive()
    {
        var fixture = await ProcessFixture.StartAsync("both", fault: FixtureFault.RejectedNativePreparation, compiled: true)
            .ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);
        Assert.Contains("DescendantPreparation=[NativeTypeStarted=False, NativeTypeCompleted=False, NativeTypeSkipped=True, PipesPrepared=True]",
            fixture.StartupDiagnostic, StringComparison.Ordinal);
        await fixture.ReleaseRootAsync().ConfigureAwait(true);
        Assert.NotNull(fixture.Descendant);
        Assert.False(fixture.Descendant.HasExited);
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
}
