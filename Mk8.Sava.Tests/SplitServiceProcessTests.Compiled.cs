using System.Diagnostics;

namespace Mk8.Sava.Tests;

public sealed partial class SplitServiceProcessTests
{
    [Theory]
    [InlineData(2, "relative-directory")]
    [InlineData(3, "UnknownFault")]
    public async Task ClosedServiceCommandRejectsInvalidAdmissionBeforePublishingMarkers(int argument, string value)
    {
        var directory = Directory.CreateTempSubdirectory("sava-closed-service-invalid-");
        try
        {
            var start = SplitServiceClosedFixtureProgram.CreateStartInfo(directory.FullName);
            start.ArgumentList[argument] = value;
            using var process = Process.Start(start)
                ?? throw new InvalidOperationException("The closed-service admission control did not start.");
            var result = await TestProcessRunner.ObserveAsync(process, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2))
                .ConfigureAwait(true);

            Assert.Equal(64, result.ExitCode);
            Assert.Empty(result.StandardOutput);
            Assert.Contains("Invalid controlled closed-service invocation.", result.StandardError, StringComparison.Ordinal);
            Assert.Empty(directory.EnumerateFileSystemInfos());
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task ClosedServicePreparationRejectsBeforeReadinessAndRetainsOwnedStartupFailure()
    {
        var failure = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ServiceFixture.StartAsync("closed-preparation-rejected")).ConfigureAwait(true);

        var original = Assert.IsType<InvalidOperationException>(failure.InnerException);
        Assert.Equal("Service fixture exited before its readiness proof.", original.Message);
        Assert.Contains("Phase=AwaitingReadyProof", failure.Message, StringComparison.Ordinal);
        Assert.Contains("StartupCancellationRequested=False", failure.Message, StringComparison.Ordinal);
        Assert.Contains("PipePreparationStarted=True, PipePreparationCompleted=False", failure.Message, StringComparison.Ordinal);
        Assert.Contains("ReadyPublishStarted=False, ReadyPublishCompleted=False, ReadyProof=False", failure.Message, StringComparison.Ordinal);
        Assert.Contains("RootExited=True", failure.Message, StringComparison.Ordinal);
        output.WriteLine(failure.Message);
    }

    [Fact]
    public async Task ClosedServiceNativePlatformGuardAndLiveRootEofRemainDistinct()
    {
        var directory = Directory.CreateTempSubdirectory("sava-closed-service-native-");
        try
        {
            var start = SplitServiceClosedFixtureProgram.CreateStartInfo(directory.FullName);
            Assert.Equal("dotnet", start.FileName);
            if (!OperatingSystem.IsWindows())
            {
                using var process = Process.Start(start)
                    ?? throw new InvalidOperationException("The closed-service platform control did not start.");
                var result = await TestProcessRunner.ObserveAsync(process, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2))
                    .ConfigureAwait(true);
                Assert.Equal(65, result.ExitCode);
                Assert.Empty(result.StandardOutput);
                Assert.Contains("requires Windows", result.StandardError, StringComparison.Ordinal);
                Assert.Empty(directory.EnumerateFileSystemInfos());
                return;
            }

            var service = new SplitServiceProcess(start, TimeSpan.FromSeconds(1));
            await using var lifetime = service.ConfigureAwait(false);
            using var readiness = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            await ServiceFixture.RetireAndDisposeAsync(async () =>
            {
                while (!File.Exists(Path.Combine(directory.FullName, "ready-publish-completed")) ||
                    !service.DiagnosticSnapshot.Contains("StdoutEof=True, StderrEof=True", StringComparison.Ordinal))
                {
                    Assert.False(service.RootExitCompletion.IsCompleted);
                    await Task.Delay(20, readiness.Token).ConfigureAwait(false);
                }
                Assert.True(File.Exists(Path.Combine(directory.FullName, "pipe-preparation-completed")));
                var failure = await Assert.ThrowsAsync<InvalidOperationException>(
                    () => service.WaitForAddressAsync("Application", TimeSpan.FromSeconds(5))).ConfigureAwait(false);
                Assert.Contains("RootExited=False, StdoutEof=True, StderrEof=True", failure.Message, StringComparison.Ordinal);
                Assert.Contains("root-output-without-newline", failure.Message, StringComparison.Ordinal);
                Assert.Contains("root-error-without-newline", failure.Message, StringComparison.Ordinal);
                Assert.False(service.RootExitCompletion.IsCompleted);
                output.WriteLine(failure.Message);
            }, () => ServiceFixture.RetireAndDisposeAsync(async () =>
            {
                await File.WriteAllTextAsync(Path.Combine(directory.FullName, "release-root"), string.Empty).ConfigureAwait(false);
                await service.RootExitCompletion.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            }, service.DisposeAsync)).ConfigureAwait(true);
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }
}
