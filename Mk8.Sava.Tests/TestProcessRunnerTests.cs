using System.Diagnostics;
using System.Globalization;
using Mk8.Sava.Protocol;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class TestProcessRunnerTests(ITestOutputHelper output)
{
    [Fact]
    public async Task RootReadinessAloneCannotCertifyAnUnreadyDescendant()
    {
        ProcessFixture? returnedFixture = null;
        try
        {
            var failure = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                returnedFixture = await ProcessFixture.StartAsync("both",
                    fault: FixtureFault.WithheldDescendantReadiness).ConfigureAwait(false);
            }).ConfigureAwait(true);
            Assert.True(failure is TimeoutException or InvalidOperationException,
                $"Unexpected startup failure kind: {failure.GetType().Name}");
            Assert.Contains("RootReady=True", failure.Message, StringComparison.Ordinal);
            Assert.Contains("DescendantReady=False", failure.Message, StringComparison.Ordinal);
            Assert.Contains("root-output-without-newline", failure.Message, StringComparison.Ordinal);
            Assert.Contains("root-error-without-newline", failure.Message, StringComparison.Ordinal);
            output.WriteLine($"Controlled startup rejection: {failure.GetType().Name}: {failure.Message}");
        }
        finally
        {
            if (returnedFixture is not null)
                await returnedFixture.DisposeAsync().ConfigureAwait(true);
        }
    }

    [Fact]
    public async Task DescendantExitBeforeReadinessReportsStartupFailureAndPartialOutputs()
    {
        var failure = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            var fixture = await ProcessFixture.StartAsync("both",
                fault: FixtureFault.ExitedDescendantBeforeReadiness).ConfigureAwait(false);
            await fixture.DisposeAsync().ConfigureAwait(false);
        }).ConfigureAwait(true);
        Assert.Contains("RootReady=False", failure.Message, StringComparison.Ordinal);
        Assert.Contains("DescendantReady=False", failure.Message, StringComparison.Ordinal);
        Assert.Contains("injected-descendant-startup-error", failure.Message, StringComparison.Ordinal);
        Assert.Contains("root-output-without-newline", failure.Message, StringComparison.Ordinal);
        output.WriteLine($"Controlled startup rejection: {failure.GetType().Name}: {failure.Message}");
    }

    [Theory]
    [InlineData("stdout")]
    [InlineData("stderr")]
    [InlineData("both")]
    public async Task ExitedRootWithInheritedPipesHasAWholeObservationDeadline(string inherited)
    {
        var fixture = await ProcessFixture.StartAsync(inherited).ConfigureAwait(true);
        await using var fixtureLifetime = fixture.ConfigureAwait(false);
        await fixture.ReleaseRootAsync().ConfigureAwait(true);
        Assert.True(fixture.Root.HasExited);
        Assert.NotNull(fixture.Descendant);
        Assert.False(fixture.Descendant.HasExited);
        var watch = Stopwatch.StartNew();
        var observation = TestProcessRunner.ObserveAsync(fixture.Root, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        try
        {
            var failure = await Assert.ThrowsAsync<TimeoutException>(() => observation.WaitAsync(TimeSpan.FromSeconds(6)))
                .ConfigureAwait(true);

            Assert.Contains("root-output-without-newline", failure.Message, StringComparison.Ordinal);
            Assert.Contains("root-error-without-newline", failure.Message, StringComparison.Ordinal);
            Assert.Contains("RootExited=True", failure.Message, StringComparison.Ordinal);
            Assert.Contains($"StdoutEof={string.Equals(inherited, "stderr", StringComparison.Ordinal)}", failure.Message, StringComparison.Ordinal);
            Assert.Contains($"StderrEof={string.Equals(inherited, "stdout", StringComparison.Ordinal)}", failure.Message, StringComparison.Ordinal);
            Assert.Contains("descendant exit is not established", failure.Message, StringComparison.Ordinal);
            Assert.False(fixture.Descendant.HasExited);
            output.WriteLine($"{inherited}: bounded observation returned after {watch.Elapsed.TotalMilliseconds:F3} ms; " +
                "root exited, owned descendant still alive.");
        }
        finally
        {
            await fixture.ReleaseDescendantAsync().ConfigureAwait(true);
            await ObserveTestCompletionAsync(observation).ConfigureAwait(true);
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(23)]
    public async Task OrdinaryExitPreservesExitCodeAndBothUnterminatedOutputs(int exitCode)
    {
        var fixture = await ProcessFixture.StartAsync("ordinary", exitCode).ConfigureAwait(true);
        await using var fixtureLifetime = fixture.ConfigureAwait(false);
        var observation = TestProcessRunner.ObserveAsync(fixture.Root, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(2));
        await fixture.ReleaseRootAsync().ConfigureAwait(true);

        var result = await observation.ConfigureAwait(true);

        Assert.Equal(exitCode, result.ExitCode);
        Assert.Equal("root-output-without-newline", result.StandardOutput);
        Assert.Equal("root-error-without-newline", result.StandardError);
    }

    [Fact]
    public async Task RootExitAndBothEofsDoNotProveDescendantExit()
    {
        var fixture = await ProcessFixture.StartAsync("neither").ConfigureAwait(true);
        await using var fixtureLifetime = fixture.ConfigureAwait(false);
        var observation = TestProcessRunner.ObserveAsync(fixture.Root, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(2));
        await fixture.ReleaseRootAsync().ConfigureAwait(true);

        var result = await observation.ConfigureAwait(true);

        Assert.Equal(0, result.ExitCode);
        Assert.NotNull(fixture.Descendant);
        Assert.False(fixture.Descendant.HasExited);
        output.WriteLine("Root exit and both EOFs observed while the independently owned descendant remains alive.");
    }

    [Fact]
    public async Task TimeoutKillsAnOwnedLiveRootAndRetainsPartialOutput()
    {
        var fixture = await ProcessFixture.StartAsync("ordinary").ConfigureAwait(true);
        await using var fixtureLifetime = fixture.ConfigureAwait(false);

        var failure = await Assert.ThrowsAsync<TimeoutException>(() => TestProcessRunner.ObserveAsync(
            fixture.Root, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2)).WaitAsync(TimeSpan.FromSeconds(6)))
            .ConfigureAwait(true);

        Assert.True(fixture.Root.HasExited);
        Assert.Contains("root-output-without-newline", failure.Message, StringComparison.Ordinal);
        Assert.Contains("root-error-without-newline", failure.Message, StringComparison.Ordinal);
        Assert.Contains("RootExited=True", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CallerCancellationUsesIndependentCleanupAndPreservesItsToken()
    {
        var fixture = await ProcessFixture.StartAsync("ordinary").ConfigureAwait(true);
        await using var fixtureLifetime = fixture.ConfigureAwait(false);
        using var cancellation = new CancellationTokenSource();
        var observation = TestProcessRunner.ObserveAsync(fixture.Root,
            TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(2), cancellation.Token);
        await cancellation.CancelAsync().WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(true);

        var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => observation.WaitAsync(TimeSpan.FromSeconds(6)))
            .ConfigureAwait(true);

        Assert.Equal(cancellation.Token, failure.CancellationToken);
        Assert.True(fixture.Root.HasExited);
    }

    [Fact]
    public async Task BothEofsWithoutRootExitStillExpireTheAggregateDeadline()
    {
        var fixture = await ProcessFixture.StartAsync("closed").ConfigureAwait(true);
        await using var fixtureLifetime = fixture.ConfigureAwait(false);

        var failure = await Assert.ThrowsAsync<TimeoutException>(() => TestProcessRunner.ObserveAsync(
            fixture.Root, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2)).WaitAsync(TimeSpan.FromSeconds(6)))
            .ConfigureAwait(true);

        Assert.Contains("RootExited=False, StdoutEof=True, StderrEof=True", failure.Message, StringComparison.Ordinal);
        Assert.Contains("Cleanup RootExited=True", failure.Message, StringComparison.Ordinal);
        Assert.True(fixture.Root.HasExited);
    }

    [Fact]
    public async Task LiveRootTimeoutAttemptsOwnedTreeCleanupWithoutInferringAllDescendantExit()
    {
        var fixture = await ProcessFixture.StartAsync("both").ConfigureAwait(true);
        await using var fixtureLifetime = fixture.ConfigureAwait(false);

        var failure = await Assert.ThrowsAsync<TimeoutException>(() => TestProcessRunner.ObserveAsync(
            fixture.Root, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2)).WaitAsync(TimeSpan.FromSeconds(6)))
            .ConfigureAwait(true);

        Assert.True(fixture.Root.HasExited);
        Assert.Contains("descendant exit is not established", failure.Message, StringComparison.Ordinal);
        await fixture.ReleaseDescendantAsync().ConfigureAwait(true);
        Assert.NotNull(fixture.Descendant);
        Assert.True(fixture.Descendant.HasExited);
    }

    [Fact]
    public async Task ConcurrentDrainsPreserveOutputsLargerThanEitherNativePipeBuffer()
    {
        var fixture = await ProcessFixture.StartAsync("flood").ConfigureAwait(true);
        await using var fixtureLifetime = fixture.ConfigureAwait(false);
        var observation = TestProcessRunner.ObserveAsync(fixture.Root, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(2));
        await fixture.ReleaseRootAsync().ConfigureAwait(true);

        var result = await observation.ConfigureAwait(true);

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

    private enum FixtureFault
    {
        None,
        WithheldDescendantReadiness,
        ExitedDescendantBeforeReadiness,
    }

    private sealed class ProcessFixture : IAsyncDisposable
    {
        private readonly DirectoryInfo temporaryDirectory;
        private readonly string releaseRoot;
        private readonly string releaseDescendant;
        private Process? startedRoot;

        private ProcessFixture(DirectoryInfo directory)
        {
            temporaryDirectory = directory;
            releaseRoot = Path.Combine(directory.FullName, "release-root");
            releaseDescendant = Path.Combine(directory.FullName, "release-descendant");
        }

        internal Process Root => startedRoot ?? throw new InvalidOperationException("The fixture root has not started.");
        internal Process? Descendant { get; private set; }

        internal static async Task<ProcessFixture> StartAsync(
            string inherited, int exitCode = 0, FixtureFault fault = FixtureFault.None)
        {
            var fixture = new ProcessFixture(Directory.CreateTempSubdirectory("sava-process-bound-"));
            try
            {
                await fixture.InitializeAsync(inherited, exitCode, fault).ConfigureAwait(false);
                return fixture;
            }
            catch
            {
                await fixture.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }

        private async Task InitializeAsync(string inherited, int exitCode, FixtureFault fault)
        {
            var script = Path.Combine(temporaryDirectory.FullName, OperatingSystem.IsWindows() ? "fixture.ps1" : "fixture.sh");
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
            start.ArgumentList.Add(temporaryDirectory.FullName);
            start.ArgumentList.Add(inherited);
            start.ArgumentList.Add(exitCode.ToString(CultureInfo.InvariantCulture));
            start.ArgumentList.Add(fault.ToString());
            startedRoot = Process.Start(start) ?? throw new InvalidOperationException("The process fixture did not start.");
            using var startup = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var requiresDescendant = inherited is "stdout" or "stderr" or "both" or "neither";
            try
            {
                var state = ReadStartupState();
                while (!state.IsReady(requiresDescendant))
                {
                    if (state.RootExited)
                        throw new InvalidOperationException($"Fixture root exited before readiness ({Root.ExitCode}).");
                    await Task.Delay(20, startup.Token).ConfigureAwait(false);
                    state = ReadStartupState();
                }
                startup.Token.ThrowIfCancellationRequested();
                if (requiresDescendant)
                {
                    var childPath = Path.Combine(temporaryDirectory.FullName, "descendant-pid");
                    var childId = int.Parse(await File.ReadAllTextAsync(childPath, startup.Token).ConfigureAwait(false),
                        CultureInfo.InvariantCulture);
                    Descendant = Process.GetProcessById(childId);
                    // Bind the known live child before its parent exits; never rediscover it during cleanup.
                    _ = Descendant.SafeHandle;
                    Descendant.EnableRaisingEvents = true;
                    if (Descendant.HasExited)
                        throw new InvalidOperationException("The fixture descendant exited before binding completed.");
                }
            }
            catch (Exception failure) when (!CatastrophicExceptionPolicy.Contains(failure))
            {
                // Capture handshake state before diagnostic observation attempts owned-root cleanup.
                var state = ReadStartupState().ToDiagnostic();
                var captured = await CaptureStartupFailureAsync().ConfigureAwait(false);
                if (failure is OperationCanceledException && startup.IsCancellationRequested)
                    throw new TimeoutException($"Fixture startup exceeded 20 seconds. {state}. {captured}", failure);
                throw new InvalidOperationException($"Fixture startup failed: {failure.Message}. {state}. {captured}", failure);
            }
        }

        private StartupState ReadStartupState() => new(
            Root.HasExited,
            File.Exists(Path.Combine(temporaryDirectory.FullName, "ready")),
            File.Exists(Path.Combine(temporaryDirectory.FullName, "descendant-starting")),
            File.Exists(Path.Combine(temporaryDirectory.FullName, "descendant-ready")),
            File.Exists(Path.Combine(temporaryDirectory.FullName, "descendant-pid")));

        private async Task<string> CaptureStartupFailureAsync()
        {
            try
            {
                // Let an unready child consume its release signal before observing pipes or deleting the fixture directory.
                await File.WriteAllTextAsync(releaseDescendant, string.Empty).ConfigureAwait(false);
                // No reader is started on success; the helper under test still owns its drains.
                var captured = await TestProcessRunner.ObserveAsync(Root, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1))
                    .ConfigureAwait(false);
                return $"Diagnostic root exit code={captured.ExitCode}. " +
                    "Root exit and EOF do not establish arbitrary descendant exit.\n" +
                    $"stdout:\n{captured.StandardOutput}\nstderr:\n{captured.StandardError}";
            }
            catch (Exception failure) when (!CatastrophicExceptionPolicy.Contains(failure))
            {
                return $"Startup diagnostic observation: {failure.GetType().Name}: {failure.Message}";
            }
        }

        private readonly record struct StartupState(
            bool RootExited, bool RootReady, bool DescendantStarting, bool DescendantReady, bool DescendantPidPublished)
        {
            internal bool IsReady(bool requiresDescendant) => !RootExited && RootReady &&
                (!requiresDescendant || (DescendantReady && DescendantPidPublished));

            internal string ToDiagnostic() => $"RootExited={RootExited}, RootReady={RootReady}, " +
                $"DescendantStarting={DescendantStarting}, DescendantReady={DescendantReady}, " +
                $"DescendantPidPublished={DescendantPidPublished}";
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
                await StopOwnedProcessAsync(startedRoot).ConfigureAwait(false);
            }
            finally
            {
                Descendant?.Dispose();
                startedRoot?.Dispose();
                temporaryDirectory.Delete(recursive: true);
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
            fault=$5
            if [ "$role" = descendant ]; then
                printf starting > "$directory/descendant-starting"
                if [ "$fault" = ExitedDescendantBeforeReadiness ]; then
                    printf injected-descendant-startup-error >&2
                    exit 97
                fi
                if [ "$fault" != WithheldDescendantReadiness ]; then
                    printf ready > "$directory/descendant-ready"
                fi
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
                    stdout) bash --noprofile --norc "$0" descendant "$directory" "$inherited" 0 "$fault" 2>/dev/null & ;;
                    stderr) bash --noprofile --norc "$0" descendant "$directory" "$inherited" 0 "$fault" >/dev/null & ;;
                    both) bash --noprofile --norc "$0" descendant "$directory" "$inherited" 0 "$fault" & ;;
                    neither) bash --noprofile --norc "$0" descendant "$directory" "$inherited" 0 "$fault" >/dev/null 2>&1 & ;;
                esac
                descendant_pid=$!
                printf %s "$descendant_pid" > "$directory/descendant-pid"
                if [ "$fault" = WithheldDescendantReadiness ]; then
                    printf ready > "$directory/ready"
                fi
                for ((attempt=0; attempt<400; attempt++)); do
                    [ -f "$directory/descendant-ready" ] && break
                    if ! kill -0 "$descendant_pid" 2>/dev/null; then
                        if wait "$descendant_pid"; then descendant_exit=0; else descendant_exit=$?; fi
                        printf 'Descendant fixture exited before readiness (%s).' "$descendant_exit" >&2
                        exit 93
                    fi
                    sleep 0.05
                done
                if [ ! -f "$directory/descendant-ready" ]; then
                    printf 'Descendant fixture never became ready.' >&2
                    exit 94
                fi
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
            param($role, $directory, $inherited, [int]$exitCode, $fault)
            $ErrorActionPreference = 'Stop'
            if ($role -eq 'descendant') {
                [IO.File]::WriteAllText((Join-Path $directory 'descendant-starting'), 'starting')
                if ($fault -eq 'ExitedDescendantBeforeReadiness') {
                    [Console]::Error.Write('injected-descendant-startup-error')
                    [Console]::Error.Flush()
                    exit 97
                }
            }
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
                if ($fault -ne 'WithheldDescendantReadiness') {
                    [IO.File]::WriteAllText((Join-Path $directory 'descendant-ready'), 'ready')
                }
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
                    'descendant', $directory, $inherited, '0', $fault)) { $start.ArgumentList.Add($argument) }
                $child = [Diagnostics.Process]::Start($start)
                [IO.File]::WriteAllText((Join-Path $directory 'descendant-pid'), [string]$child.Id)
                if ($fault -eq 'WithheldDescendantReadiness') {
                    [IO.File]::WriteAllText((Join-Path $directory 'ready'), 'ready')
                }
                for ($attempt = 0; $attempt -lt 400; $attempt++) {
                    if (Test-Path (Join-Path $directory 'descendant-ready')) { break }
                    if ($child.HasExited) {
                        [Console]::Error.Write("Descendant fixture exited before readiness ($($child.ExitCode)).")
                        [Console]::Error.Flush()
                        exit 93
                    }
                    Start-Sleep -Milliseconds 50
                }
                if (-not (Test-Path (Join-Path $directory 'descendant-ready'))) {
                    [Console]::Error.Write('Descendant fixture never became ready.')
                    [Console]::Error.Flush()
                    exit 94
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
