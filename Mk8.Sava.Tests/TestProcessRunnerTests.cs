using System.Diagnostics;
using System.Globalization;
using Mk8.Sava.Protocol;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class TestProcessRunnerTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData("stdout")]
    [InlineData("stderr")]
    [InlineData("both")]
    public async Task ExitedRootWithInheritedPipesHasAWholeObservationDeadline(string inherited)
    {
        await using var fixture = await ProcessFixture.StartAsync(inherited).ConfigureAwait(false);
        await fixture.ReleaseRootAsync().ConfigureAwait(false);
        Assert.True(fixture.Root.HasExited);
        Assert.NotNull(fixture.Descendant);
        Assert.False(fixture.Descendant.HasExited);
        var watch = Stopwatch.StartNew();
        var observation = TestProcessRunner.ObserveAsync(fixture.Root, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        try
        {
            var failure = await Assert.ThrowsAsync<TimeoutException>(() => observation.WaitAsync(TimeSpan.FromSeconds(6)))
                .ConfigureAwait(false);

            Assert.Contains("root-output-without-newline", failure.Message, StringComparison.Ordinal);
            Assert.Contains("root-error-without-newline", failure.Message, StringComparison.Ordinal);
            Assert.Contains("RootExited=True", failure.Message, StringComparison.Ordinal);
            Assert.Contains($"StdoutEof={inherited == "stderr"}", failure.Message, StringComparison.Ordinal);
            Assert.Contains($"StderrEof={inherited == "stdout"}", failure.Message, StringComparison.Ordinal);
            Assert.Contains("descendant exit is not established", failure.Message, StringComparison.Ordinal);
            Assert.False(fixture.Descendant.HasExited);
            output.WriteLine($"{inherited}: bounded observation returned after {watch.Elapsed.TotalMilliseconds:F3} ms; " +
                "root exited, owned descendant still alive.");
        }
        finally
        {
            await fixture.ReleaseDescendantAsync().ConfigureAwait(false);
            await ObserveTestCompletionAsync(observation).ConfigureAwait(false);
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(23)]
    public async Task OrdinaryExitPreservesExitCodeAndBothUnterminatedOutputs(int exitCode)
    {
        await using var fixture = await ProcessFixture.StartAsync("ordinary", exitCode).ConfigureAwait(false);
        var observation = TestProcessRunner.ObserveAsync(fixture.Root, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(2));
        await fixture.ReleaseRootAsync().ConfigureAwait(false);

        var result = await observation.ConfigureAwait(false);

        Assert.Equal(exitCode, result.ExitCode);
        Assert.Equal("root-output-without-newline", result.StandardOutput);
        Assert.Equal("root-error-without-newline", result.StandardError);
    }

    [Fact]
    public async Task RootExitAndBothEofsDoNotProveDescendantExit()
    {
        await using var fixture = await ProcessFixture.StartAsync("neither").ConfigureAwait(false);
        var observation = TestProcessRunner.ObserveAsync(fixture.Root, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(2));
        await fixture.ReleaseRootAsync().ConfigureAwait(false);

        var result = await observation.ConfigureAwait(false);

        Assert.Equal(0, result.ExitCode);
        Assert.NotNull(fixture.Descendant);
        Assert.False(fixture.Descendant.HasExited);
        output.WriteLine("Root exit and both EOFs observed while the independently owned descendant remains alive.");
    }

    [Fact]
    public async Task TimeoutKillsAnOwnedLiveRootAndRetainsPartialOutput()
    {
        await using var fixture = await ProcessFixture.StartAsync("ordinary").ConfigureAwait(false);

        var failure = await Assert.ThrowsAsync<TimeoutException>(() => TestProcessRunner.ObserveAsync(
            fixture.Root, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2)).WaitAsync(TimeSpan.FromSeconds(6)))
            .ConfigureAwait(false);

        Assert.True(fixture.Root.HasExited);
        Assert.Contains("root-output-without-newline", failure.Message, StringComparison.Ordinal);
        Assert.Contains("root-error-without-newline", failure.Message, StringComparison.Ordinal);
        Assert.Contains("RootExited=True", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CallerCancellationUsesIndependentCleanupAndPreservesItsToken()
    {
        await using var fixture = await ProcessFixture.StartAsync("ordinary").ConfigureAwait(false);
        using var cancellation = new CancellationTokenSource();
        var observation = TestProcessRunner.ObserveAsync(fixture.Root,
            TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(2), cancellation.Token);
        cancellation.Cancel();

        var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => observation.WaitAsync(TimeSpan.FromSeconds(6)))
            .ConfigureAwait(false);

        Assert.Equal(cancellation.Token, failure.CancellationToken);
        Assert.True(fixture.Root.HasExited);
    }

    [Fact]
    public async Task BothEofsWithoutRootExitStillExpireTheAggregateDeadline()
    {
        await using var fixture = await ProcessFixture.StartAsync("closed").ConfigureAwait(false);

        var failure = await Assert.ThrowsAsync<TimeoutException>(() => TestProcessRunner.ObserveAsync(
            fixture.Root, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2)).WaitAsync(TimeSpan.FromSeconds(6)))
            .ConfigureAwait(false);

        Assert.Contains("RootExited=False, StdoutEof=True, StderrEof=True", failure.Message, StringComparison.Ordinal);
        Assert.Contains("Cleanup RootExited=True", failure.Message, StringComparison.Ordinal);
        Assert.True(fixture.Root.HasExited);
    }

    [Fact]
    public async Task LiveRootTimeoutAttemptsOwnedTreeCleanupWithoutInferringAllDescendantExit()
    {
        await using var fixture = await ProcessFixture.StartAsync("both").ConfigureAwait(false);

        var failure = await Assert.ThrowsAsync<TimeoutException>(() => TestProcessRunner.ObserveAsync(
            fixture.Root, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2)).WaitAsync(TimeSpan.FromSeconds(6)))
            .ConfigureAwait(false);

        Assert.True(fixture.Root.HasExited);
        Assert.Contains("descendant exit is not established", failure.Message, StringComparison.Ordinal);
        await fixture.ReleaseDescendantAsync().ConfigureAwait(false);
        Assert.NotNull(fixture.Descendant);
        Assert.True(fixture.Descendant.HasExited);
    }

    [Fact]
    public async Task ConcurrentDrainsPreserveOutputsLargerThanEitherNativePipeBuffer()
    {
        await using var fixture = await ProcessFixture.StartAsync("flood").ConfigureAwait(false);
        var observation = TestProcessRunner.ObserveAsync(fixture.Root, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(2));
        await fixture.ReleaseRootAsync().ConfigureAwait(false);

        var result = await observation.ConfigureAwait(false);

        Assert.Equal(0, result.ExitCode);
        Assert.Equal("root-output-without-newline" + new string('o', 262144), result.StandardOutput);
        Assert.Equal("root-error-without-newline" + new string('e', 262144), result.StandardError);
    }

    private static async Task ObserveTestCompletionAsync(Task observation)
    {
        try
        {
            await observation.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        }
        catch (TimeoutException exception) when (observation.IsCompleted && !CatastrophicExceptionPolicy.Contains(exception)) { }
        catch (OperationCanceledException exception) when (observation.IsCompleted && !CatastrophicExceptionPolicy.Contains(exception)) { }
    }

    private sealed class ProcessFixture : IAsyncDisposable
    {
        private readonly DirectoryInfo directory;
        private readonly string releaseRoot;
        private readonly string releaseDescendant;

        private ProcessFixture(DirectoryInfo directory, Process root)
        {
            this.directory = directory;
            Root = root;
            releaseRoot = Path.Combine(directory.FullName, "release-root");
            releaseDescendant = Path.Combine(directory.FullName, "release-descendant");
        }

        internal Process Root { get; }
        internal Process? Descendant { get; private set; }

        internal static async Task<ProcessFixture> StartAsync(string inherited, int exitCode = 0)
        {
            var directory = Directory.CreateTempSubdirectory("sava-process-bound-");
            var script = Path.Combine(directory.FullName, OperatingSystem.IsWindows() ? "fixture.ps1" : "fixture.sh");
            await File.WriteAllTextAsync(script, OperatingSystem.IsWindows() ? WindowsScript : UnixScript)
                .ConfigureAwait(false);
            var start = new ProcessStartInfo(OperatingSystem.IsWindows() ? "pwsh" : "bash")
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            foreach (var argument in OperatingSystem.IsWindows()
                ? new[] { "-NoLogo", "-NoProfile", "-NonInteractive", "-File", script }
                : new[] { "--noprofile", "--norc", script })
                start.ArgumentList.Add(argument);
            start.ArgumentList.Add("root");
            start.ArgumentList.Add(directory.FullName);
            start.ArgumentList.Add(inherited);
            start.ArgumentList.Add(exitCode.ToString(CultureInfo.InvariantCulture));
            var root = Process.Start(start) ?? throw new InvalidOperationException("The process fixture did not start.");
            var fixture = new ProcessFixture(directory, root);
            try
            {
                using var startup = new CancellationTokenSource(TimeSpan.FromSeconds(20));
                while (!File.Exists(Path.Combine(directory.FullName, "ready")))
                {
                    if (root.HasExited)
                    {
                        var failure = await TestProcessRunner.ObserveAsync(root, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1))
                            .ConfigureAwait(false);
                        throw new InvalidOperationException($"Fixture exited before readiness ({failure.ExitCode}): " +
                            failure.StandardOutput + failure.StandardError);
                    }
                    await Task.Delay(20, startup.Token).ConfigureAwait(false);
                }
                var childPath = Path.Combine(directory.FullName, "descendant-pid");
                if (File.Exists(childPath))
                {
                    var childId = int.Parse(await File.ReadAllTextAsync(childPath, startup.Token).ConfigureAwait(false),
                        CultureInfo.InvariantCulture);
                    fixture.Descendant = Process.GetProcessById(childId);
                    // Bind the known live child before its parent exits; do not rediscover it by PID during cleanup.
                    _ = fixture.Descendant.SafeHandle;
                    fixture.Descendant.EnableRaisingEvents = true;
                }
                return fixture;
            }
            catch
            {
                await fixture.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }

        internal async Task ReleaseRootAsync()
        {
            await File.WriteAllTextAsync(releaseRoot, string.Empty).ConfigureAwait(false);
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await Root.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
        }

        internal async Task ReleaseDescendantAsync()
        {
            await File.WriteAllTextAsync(releaseDescendant, string.Empty).ConfigureAwait(false);
            if (Descendant is not null)
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await Descendant.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
                Assert.True(Descendant.HasExited);
            }
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await File.WriteAllTextAsync(releaseDescendant, string.Empty).ConfigureAwait(false);
                await StopOwnedProcessAsync(Descendant).ConfigureAwait(false);
                await StopOwnedProcessAsync(Root).ConfigureAwait(false);
            }
            finally
            {
                Descendant?.Dispose();
                Root.Dispose();
                directory.Delete(recursive: true);
            }
        }

        private static async Task StopOwnedProcessAsync(Process? process)
        {
            if (process is null)
                return;
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await process.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
        }

        private const string UnixScript = """
            role=$1
            directory=$2
            inherited=$3
            exit_code=$4
            if [ "$role" = descendant ]; then
                printf ready > "$directory/descendant-ready"
                for ((attempt=0; attempt<1200; attempt++)); do
                    [ -f "$directory/release-descendant" ] && exit 0
                    sleep 0.05
                done
                exit 91
            fi
            printf root-output-without-newline
            printf root-error-without-newline >&2
            if [ "$inherited" = stdout ] || [ "$inherited" = stderr ] || [ "$inherited" = both ] || [ "$inherited" = neither ]; then
                case "$inherited" in
                    stdout) bash --noprofile --norc "$0" descendant "$directory" "$inherited" 0 2>/dev/null & ;;
                    stderr) bash --noprofile --norc "$0" descendant "$directory" "$inherited" 0 >/dev/null & ;;
                    both) bash --noprofile --norc "$0" descendant "$directory" "$inherited" 0 & ;;
                    neither) bash --noprofile --norc "$0" descendant "$directory" "$inherited" 0 >/dev/null 2>&1 & ;;
                esac
                printf %s "$!" > "$directory/descendant-pid"
                for ((attempt=0; attempt<400; attempt++)); do
                    [ -f "$directory/descendant-ready" ] && break
                    sleep 0.05
                done
            fi
            if [ "$inherited" = closed ]; then
                exec 1>&- 2>&-
            fi
            printf ready > "$directory/ready"
            for ((attempt=0; attempt<1200; attempt++)); do
                if [ -f "$directory/release-root" ]; then
                    if [ "$inherited" = flood ]; then
                        printf '%262144s' '' | tr ' ' o
                        printf '%262144s' '' | tr ' ' e >&2
                    fi
                    exit "$exit_code"
                fi
                sleep 0.05
            done
            exit 92
            """;

        private const string WindowsScript = """
            param($role, $directory, $inherited, [int]$exitCode)
            $ErrorActionPreference = 'Stop'
            if ($role -eq 'descendant' -or $inherited -eq 'closed') {
                Add-Type -TypeDefinition '
                    using System;
                    using System.Runtime.InteropServices;
                    public static class FixturePipeHandles {
                        [DllImport("kernel32.dll")] public static extern IntPtr GetStdHandle(int kind);
                        [DllImport("kernel32.dll")] public static extern bool CloseHandle(IntPtr handle);
                    }'
            }
            if ($role -eq 'descendant') {
                if ($inherited -notin @('stdout', 'both')) {
                    [void][FixturePipeHandles]::CloseHandle([FixturePipeHandles]::GetStdHandle(-11))
                }
                if ($inherited -notin @('stderr', 'both')) {
                    [void][FixturePipeHandles]::CloseHandle([FixturePipeHandles]::GetStdHandle(-12))
                }
                [IO.File]::WriteAllText((Join-Path $directory 'descendant-ready'), 'ready')
                for ($attempt = 0; $attempt -lt 1200; $attempt++) {
                    if (Test-Path (Join-Path $directory 'release-descendant')) { exit 0 }
                    Start-Sleep -Milliseconds 50
                }
                exit 91
            }
            [Console]::Out.Write('root-output-without-newline')
            [Console]::Out.Flush()
            [Console]::Error.Write('root-error-without-newline')
            [Console]::Error.Flush()
            if ($inherited -in @('stdout', 'stderr', 'both', 'neither')) {
                $start = [Diagnostics.ProcessStartInfo]::new('pwsh')
                $start.UseShellExecute = $false
                foreach ($argument in @('-NoLogo', '-NoProfile', '-NonInteractive', '-File', $PSCommandPath,
                    'descendant', $directory, $inherited, '0')) { $start.ArgumentList.Add($argument) }
                $child = [Diagnostics.Process]::Start($start)
                [IO.File]::WriteAllText((Join-Path $directory 'descendant-pid'), [string]$child.Id)
                for ($attempt = 0; $attempt -lt 400; $attempt++) {
                    if (Test-Path (Join-Path $directory 'descendant-ready')) { break }
                    Start-Sleep -Milliseconds 50
                }
            }
            if ($inherited -eq 'closed') {
                [void][FixturePipeHandles]::CloseHandle([FixturePipeHandles]::GetStdHandle(-11))
                [void][FixturePipeHandles]::CloseHandle([FixturePipeHandles]::GetStdHandle(-12))
            }
            [IO.File]::WriteAllText((Join-Path $directory 'ready'), 'ready')
            for ($attempt = 0; $attempt -lt 1200; $attempt++) {
                if (Test-Path (Join-Path $directory 'release-root')) {
                    if ($inherited -eq 'flood') {
                        [Console]::Out.Write([string]::new([char]'o', 262144))
                        [Console]::Out.Flush()
                        [Console]::Error.Write([string]::new([char]'e', 262144))
                        [Console]::Error.Flush()
                    }
                    exit $exitCode
                }
                Start-Sleep -Milliseconds 50
            }
            exit 92
            """;
    }
}
