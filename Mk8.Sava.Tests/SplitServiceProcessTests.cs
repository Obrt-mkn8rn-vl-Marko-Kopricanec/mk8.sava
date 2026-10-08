using System.Diagnostics;
using System.Globalization;
using Mk8.Sava.Protocol;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed partial class SplitServiceProcessTests(ITestOutputHelper output)
{
    [Fact]
    public async Task ListenerTimeoutIncludesUnterminatedOutputsAndPreCleanupState()
    {
        var fixture = await ServiceFixture.StartAsync("ordinary").ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);

        var failure = await Assert.ThrowsAsync<TimeoutException>(() => fixture.Service.WaitForAddressAsync(
            "Application", TimeSpan.FromSeconds(1))).ConfigureAwait(true);

        Assert.IsType<TimeoutException>(failure.InnerException);
        Assert.Contains("Application listener observation", failure.Message, StringComparison.Ordinal);
        Assert.Contains("RootExited=False, StdoutEof=False, StderrEof=False", failure.Message, StringComparison.Ordinal);
        Assert.Contains("Logical capture progress (sequential stream/task observations, not native I/O)", failure.Message, StringComparison.Ordinal);
        Assert.Contains("Stdout=[ReadRequests=", failure.Message, StringComparison.Ordinal);
        Assert.Contains("Stderr=[ReadRequests=", failure.Message, StringComparison.Ordinal);
        Assert.Contains("root-output-without-newline", failure.Message, StringComparison.Ordinal);
        Assert.Contains("root-error-without-newline", failure.Message, StringComparison.Ordinal);
        Assert.False(fixture.Root.HasExited);
        output.WriteLine(failure.Message);
    }

    [Fact]
    public async Task RootExitRejectsStartupEvenWhenDescendantRetainsBothPipes()
    {
        var fixture = await ServiceFixture.StartAsync("inherited").ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);
        await fixture.ReleaseRootAsync().ConfigureAwait(true);
        Assert.NotNull(fixture.Descendant);
        Assert.False(fixture.Descendant.HasExited);

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Service.WaitForAddressAsync(
            "Application", TimeSpan.FromSeconds(5)).WaitAsync(TimeSpan.FromSeconds(8))).ConfigureAwait(true);

        Assert.Contains("RootExited=True, StdoutEof=False, StderrEof=False", failure.Message, StringComparison.Ordinal);
        Assert.Contains("root-output-without-newline", failure.Message, StringComparison.Ordinal);
        Assert.Contains("descendant exit is not established", failure.Message, StringComparison.Ordinal);
        Assert.IsType<InvalidOperationException>(failure.InnerException);
        Assert.False(fixture.Descendant.HasExited);
        output.WriteLine(failure.Message);
    }

    [Fact]
    public async Task ExitedRootCleanupClosesOwnedReadEndsWithoutCertifyingDescendantExit()
    {
        var fixture = await ServiceFixture.StartAsync("inherited").ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);
        await fixture.ReleaseRootAsync().ConfigureAwait(true);

        await fixture.Service.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(6)).ConfigureAwait(true);

        Assert.NotNull(fixture.Descendant);
        Assert.False(fixture.Descendant.HasExited);
        Assert.Contains("root-error-without-newline", fixture.Service.Logs, StringComparison.Ordinal);
        output.WriteLine("Owned service disposal completed while the independently bound descendant remained alive.");
    }

    [Fact]
    public async Task CallerCancellationRetainsTokenInnerFailureAndLiveRootSnapshot()
    {
        var fixture = await ServiceFixture.StartAsync("ordinary").ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync().ConfigureAwait(true);

        var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fixture.Service.WaitForAddressAsync(
            "Gateway", TimeSpan.FromSeconds(10), cancellation.Token)).ConfigureAwait(true);

        Assert.Equal(cancellation.Token, failure.CancellationToken);
        Assert.IsAssignableFrom<OperationCanceledException>(failure.InnerException);
        Assert.Contains("Gateway listener observation", failure.Message, StringComparison.Ordinal);
        Assert.Contains("RootExited=False", failure.Message, StringComparison.Ordinal);
        Assert.False(fixture.Root.HasExited);
    }

    [Fact]
    public async Task PipeEofBeforeListenerDoesNotMisreportLiveRootAsExited()
    {
        var fixture = await ServiceFixture.StartAsync("closed").ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Service.WaitForAddressAsync(
            "Application", TimeSpan.FromSeconds(5))).ConfigureAwait(true);

        Assert.Contains("RootExited=False, StdoutEof=True", failure.Message, StringComparison.Ordinal);
        Assert.Contains("root-output-without-newline", failure.Message, StringComparison.Ordinal);
        Assert.False(fixture.Root.HasExited);
        output.WriteLine(failure.Message);
    }

    [Fact]
    public async Task StderrCannotPublishTheStdoutListener()
    {
        var fixture = await ServiceFixture.StartAsync("stderr").ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);

        var failure = await Assert.ThrowsAsync<TimeoutException>(() => fixture.Service.WaitForAddressAsync(
            "Gateway", TimeSpan.FromSeconds(1))).ConfigureAwait(true);

        Assert.Contains("Now listening on: http://127.0.0.1:12345/", failure.Message, StringComparison.Ordinal);
        Assert.False(fixture.Root.HasExited);
    }

    [Fact]
    public async Task FragmentedListenerRequiresCompleteLineAndCanSucceedAfterAnObservationTimeout()
    {
        var fixture = await ServiceFixture.StartAsync("fragmented").ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);

        var failure = await Assert.ThrowsAsync<TimeoutException>(() => fixture.Service.WaitForAddressAsync(
            "Gateway", TimeSpan.FromSeconds(1))).ConfigureAwait(true);
        Assert.Contains("Now listening on: http://127.0.0.1:", failure.Message, StringComparison.Ordinal);
        await fixture.CompleteListenerAsync().ConfigureAwait(true);

        var address = await fixture.Service.WaitForAddressAsync("Gateway", TimeSpan.FromSeconds(5)).ConfigureAwait(true);

        Assert.Equal(new Uri("http://127.0.0.1:12345/"), address);
        Assert.False(fixture.Root.HasExited);
    }

    [Fact]
    public async Task LiveRootCleanupPreservesLogsAndRepeatedDisposalIsHarmless()
    {
        var fixture = await ServiceFixture.StartAsync("ordinary").ConfigureAwait(true);
        await using var lifetime = fixture.ConfigureAwait(false);

        await fixture.Service.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(6)).ConfigureAwait(true);
        await fixture.Service.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(6)).ConfigureAwait(true);

        Assert.True(fixture.Root.HasExited);
        Assert.Contains("root-output-without-newline", fixture.Service.Logs, StringComparison.Ordinal);
        Assert.Contains("root-error-without-newline", fixture.Service.Logs, StringComparison.Ordinal);
    }

    private sealed class ServiceFixture : IAsyncDisposable
    {
        private readonly DirectoryInfo directory;
        private int disposalStarted;

        private ServiceFixture(DirectoryInfo directory, SplitServiceProcess service)
        {
            this.directory = directory;
            Service = service;
            Root = Process.GetProcessById(service.Id);
            _ = Root.SafeHandle;
            Root.EnableRaisingEvents = true;
        }

        internal SplitServiceProcess Service { get; }
        internal Process Root { get; }
        internal Process? Descendant { get; private set; }

        internal static async Task<ServiceFixture> StartAsync(string mode, Action<ServiceFixture>? writerReady = null)
        {
            var fixtureDirectory = Directory.CreateTempSubdirectory("sava-service-observation-");
            var script = Path.Combine(fixtureDirectory.FullName, OperatingSystem.IsWindows() ? "fixture.ps1" : "fixture.sh");
            await File.WriteAllTextAsync(script, OperatingSystem.IsWindows() ? WindowsScript : UnixScript).ConfigureAwait(false);
            var start = CreateStart(script, fixtureDirectory.FullName, mode);
#pragma warning disable CA2000 // Returned fixture owns this service in its readonly field; the catch disposes it if constructor transfer fails.
            var service = new SplitServiceProcess(start, TimeSpan.FromSeconds(1));
#pragma warning restore CA2000
            ServiceFixture? fixture = null;
            try
            {
                fixture = new ServiceFixture(fixtureDirectory, service);
                await fixture.BindReadyProcessesAsync(mode, writerReady).ConfigureAwait(false);
                return fixture;
            }
            catch (Exception failure)
            {
                // Retain the startup graph AND independently failed cleanup. The owned
                // service must be attempted before wrappers/signaling state are released.
                await RetireAndDisposeAsync(() => Task.FromException(failure), async () =>
                {
                    if (fixture is not null)
                        await fixture.DisposeAsync().ConfigureAwait(false);
                    else
                        await RetireAndDisposeAsync(() => service.DisposeAsync().AsTask(), () =>
                        {
                            fixtureDirectory.Delete(recursive: true);
                            return ValueTask.CompletedTask;
                        }).ConfigureAwait(false);
                }).ConfigureAwait(false);
                throw;
            }
        }

        private static ProcessStartInfo CreateStart(string script, string directory, string mode)
        {
            if (string.Equals(mode, "closed-preparation-rejected", StringComparison.Ordinal))
                return SplitServiceClosedFixtureProgram.CreateStartInfo(directory, "RejectedPreparation");
            if (OperatingSystem.IsWindows() && string.Equals(mode, "closed", StringComparison.Ordinal))
                return SplitServiceClosedFixtureProgram.CreateStartInfo(directory);

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
            start.ArgumentList.Add(directory);
            start.ArgumentList.Add(mode);
            return start;
        }

        private async Task BindReadyProcessesAsync(string mode, Action<ServiceFixture>? writerReady)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var observation = Stopwatch.StartNew();
            var phase = "AwaitingReadyProof";
            try
            {
                while (!File.Exists(Path.Combine(directory.FullName, "ready")))
                {
                    if (Root.HasExited)
                        throw new InvalidOperationException("Service fixture exited before its readiness proof.");
                    await Task.Delay(20, timeout.Token).ConfigureAwait(false);
                }
                phase = "BindingDescendant";
                if (string.Equals(mode, "inherited", StringComparison.Ordinal))
                {
                    var pid = int.Parse(await File.ReadAllTextAsync(Path.Combine(directory.FullName, "descendant-pid"),
                        timeout.Token).ConfigureAwait(false), CultureInfo.InvariantCulture);
                    Descendant = Process.GetProcessById(pid);
                    _ = Descendant.SafeHandle;
                    Descendant.EnableRaisingEvents = true;
                    Assert.False(Descendant.HasExited);
                }
                phase = "WriterReadyCallback";
                writerReady?.Invoke(this);
                phase = "AwaitingCapturedOutput";
                while (!CapturedExpectedOutput(mode))
                {
                    if (Root.HasExited)
                        throw new InvalidOperationException($"Service fixture exited before capture acknowledgement.\n{Service.Logs}");
                    await Task.Delay(20, timeout.Token).ConfigureAwait(false);
                }
                phase = "CheckingRootLiveness";
                if (Root.HasExited)
                    throw new InvalidOperationException("Service fixture exited before capture readiness completed.");
            }
            catch (Exception failure) when (!CatastrophicExceptionPolicy.Contains(failure))
            {
                throw CaptureStartupFailure(failure, mode, phase, observation.Elapsed, timeout.IsCancellationRequested);
            }
        }

        private Exception CaptureStartupFailure(Exception failure, string mode, string phase, TimeSpan elapsed, bool cancellationRequested)
        {
            string diagnostic;
            try
            {
                // Failure-only sequential observations, before retirement. Marker existence
                // is not readiness admission, atomic progress, or a native-operation proof.
                diagnostic = $"Service fixture startup failed. Mode={mode}, Phase={phase}, Elapsed={elapsed}, " +
                    $"StartupCancellationRequested={cancellationRequested}. Pre-cleanup producer markers: " +
                    $"Started={Marker("producer-started")}, PipePreparationStarted={Marker("pipe-preparation-started")}, " +
                    $"PipePreparationCompleted={Marker("pipe-preparation-completed")}, ReadyWithheld={Marker("ready-withheld")}, " +
                    $"ReadyPublishStarted={Marker("ready-publish-started")}, ReadyPublishCompleted={Marker("ready-publish-completed")}, " +
                    $"ReadyProof={Marker("ready")}, DescendantPidPublished={Marker("descendant-pid")}, " +
                    $"DescendantReady={Marker("descendant-ready")}, DescendantBound={Descendant is not null}.\n" +
                    $"{Service.DiagnosticSnapshot}\n{Service.Logs}";
            }
#pragma warning disable CA1031 // Preserve the independent diagnostic failure graph, including fatal graphs, rather than erase the startup failure.
            catch (Exception diagnosticFailure)
#pragma warning restore CA1031
            {
                return new AggregateException("Startup and pre-cleanup diagnostic capture both failed.", failure, diagnosticFailure);
            }
            return failure is OperationCanceledException canceled
                ? new OperationCanceledException(diagnostic, failure, canceled.CancellationToken)
                : new InvalidOperationException(diagnostic, failure);

            bool Marker(string name) => File.Exists(Path.Combine(directory.FullName, name));
        }

        private bool CapturedExpectedOutput(string mode)
        {
            // A writer's ready file proves neither asynchronous drain has received its bytes.
            var logs = Service.Logs;
            if (!logs.Contains("root-output-without-newline", StringComparison.Ordinal) ||
                !logs.Contains("root-error-without-newline", StringComparison.Ordinal))
                return false;
            if (string.Equals(mode, "stderr", StringComparison.Ordinal))
                return logs.Contains("Now listening on: http://127.0.0.1:12345/", StringComparison.Ordinal);
            if (string.Equals(mode, "fragmented", StringComparison.Ordinal))
                return logs.Contains("Now listening on: http://127.0.0.1:", StringComparison.Ordinal);
            return true;
        }

        internal Task ReleaseOutputAsync() => File.WriteAllTextAsync(Path.Combine(directory.FullName, "release-output"), string.Empty);

        internal Task CompleteListenerAsync() => File.WriteAllTextAsync(Path.Combine(directory.FullName, "complete-listener"), string.Empty);

        internal void BlockRetirementSignal(string signal) => Directory.CreateDirectory(Path.Combine(directory.FullName, signal));

        internal async Task ReleaseRootAsync()
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await File.WriteAllTextAsync(Path.Combine(directory.FullName, "flush-output"), string.Empty, timeout.Token)
                .ConfigureAwait(false);
            // Root/child file readiness is not proof that our two independent drains received their known outputs.
            while (!Service.Logs.Contains("root-output-without-newline", StringComparison.Ordinal) ||
                !Service.Logs.Contains("root-error-without-newline", StringComparison.Ordinal))
                await Task.Delay(20, timeout.Token).ConfigureAwait(false);
            await File.WriteAllTextAsync(Path.Combine(directory.FullName, "release-root"), string.Empty, timeout.Token)
                .ConfigureAwait(false);
            await Root.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref disposalStarted, 1) != 0)
                return;
            Exception? workflowFailure = null;
            var resourcesReleased = false;
            try
            {
                try
                {
                    await RetireAndDisposeAsync(RetireAsync, Service.DisposeAsync).ConfigureAwait(false);
                }
                catch (Exception failure)
                {
                    workflowFailure = failure;
                    throw;
                }
                finally
                {
                    Descendant?.Dispose();
                    Root.Dispose();
                    directory.Delete(recursive: true);
                    resourcesReleased = true;
                }
            }
            catch (Exception releaseFailure)
            {
                // Catch bodies run after the nested finally; filters would inspect resourcesReleased before unwinding.
                if (workflowFailure is not null && !resourcesReleased)
                    throw new AggregateException("Fixture resource release also failed.", workflowFailure, releaseFailure);
                throw;
            }
        }

        private async Task RetireAsync()
        {
            await File.WriteAllTextAsync(Path.Combine(directory.FullName, "release-descendant"), string.Empty).ConfigureAwait(false);
            if (Descendant is not null)
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await Descendant.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
            }
            if (!Root.HasExited)
            {
                // Unrelated listener/cancellation tests do not require forceful live-root cleanup.
                // Retire the controlled root cooperatively; the dedicated cleanup test still calls Dispose while live.
                using var rootRetirement = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await File.WriteAllTextAsync(Path.Combine(directory.FullName, "release-root"), string.Empty,
                    rootRetirement.Token).ConfigureAwait(false);
                await Root.WaitForExitAsync(rootRetirement.Token).ConfigureAwait(false);
            }
        }

        internal static async ValueTask RetireAndDisposeAsync(Func<Task> retirement, Func<ValueTask> cleanup)
        {
            Exception? retirementFailure = null;
            var cleanupCompleted = false;
            try
            {
                try
                {
                    await retirement().ConfigureAwait(false);
                }
                catch (Exception failure)
                {
                    retirementFailure = failure;
                    throw;
                }
                finally
                {
                    await cleanup().ConfigureAwait(false);
                    cleanupCompleted = true;
                }
            }
            catch (Exception cleanupFailure)
            {
                if (retirementFailure is not null && !cleanupCompleted)
                    throw new AggregateException("Retirement and owned service cleanup both failed.", retirementFailure, cleanupFailure);
                throw;
            }
        }

        private const string UnixScript = """
            directory=$1
            mode=$2
            if [ "$mode" = descendant ]; then
                printf ready > "$directory/descendant-ready"
                for ((i=0; i<1200; i++)); do
                    [ -f "$directory/release-descendant" ] && exit 0
                    sleep 0.05
                done
                exit 91
            fi
            printf started > "$directory/producer-started"
            [ "$mode" != delayed-stdout ] && printf root-output-without-newline
            [ "$mode" != delayed-stderr ] && printf root-error-without-newline >&2
            if [ "$mode" = inherited ]; then
                bash --noprofile --norc "$0" "$directory" descendant &
                printf %s "$!" > "$directory/descendant-pid"
                for ((i=0; i<400; i++)); do
                    [ -f "$directory/descendant-ready" ] && break
                    sleep 0.05
                done
                [ -f "$directory/descendant-ready" ] || exit 93
            fi
            [ "$mode" = stderr ] && printf '\nNow listening on: http://127.0.0.1:12345/\n' >&2
            [ "$mode" = fragmented ] && printf '\nNow listening on: http://127.0.0.1:'
            printf started > "$directory/pipe-preparation-started"
            if [ "$mode" = closed ]; then exec 1>&- 2>&-; fi
            printf completed > "$directory/pipe-preparation-completed"
            [ "$mode" = exit-before-ready ] && exit 23
            if [ "$mode" = withheld-ready ]; then
                printf withheld > "$directory/ready-withheld"
            else
                printf started > "$directory/ready-publish-started"
                printf ready > "$directory/ready"
                printf completed > "$directory/ready-publish-completed"
            fi
            output_flushed=0
            output_released=0
            for ((i=0; i<1200; i++)); do
                if [ "$output_released" = 0 ] && [ -f "$directory/release-output" ]; then
                    [ "$mode" = delayed-stdout ] && printf root-output-without-newline
                    [ "$mode" = delayed-stderr ] && printf root-error-without-newline >&2
                    output_released=1
                fi
                if [ "$output_flushed" = 0 ] && [ -f "$directory/flush-output" ]; then
                    printf '\n'
                    printf '\n' >&2
                    output_flushed=1
                fi
                if [ "$mode" = fragmented ] && [ -f "$directory/complete-listener" ]; then
                    printf '12345/\r\n'
                    mode=ordinary
                fi
                [ "$mode" != uncooperative ] && [ -f "$directory/release-root" ] && exit 23
                sleep 0.05
            done
            exit 92
            """;

        private const string WindowsScript = """
            param($directory, $mode)
            $ErrorActionPreference = 'Stop'
            if ($mode -eq 'closed') { throw 'Closed service fixtures must use the compiled command.' }
            if ($mode -eq 'descendant') {
                [IO.File]::WriteAllText((Join-Path $directory 'descendant-ready'), 'ready')
                for ($i = 0; $i -lt 1200; $i++) {
                    if (Test-Path (Join-Path $directory 'release-descendant') -PathType Leaf) { exit 0 }
                    Start-Sleep -Milliseconds 50
                }
                exit 91
            }
            [IO.File]::WriteAllText((Join-Path $directory 'producer-started'), 'started')
            if ($mode -ne 'delayed-stdout') {
                [Console]::Out.Write('root-output-without-newline')
                [Console]::Out.Flush()
            }
            if ($mode -ne 'delayed-stderr') {
                [Console]::Error.Write('root-error-without-newline')
                [Console]::Error.Flush()
            }
            if ($mode -eq 'inherited') {
                $start = [Diagnostics.ProcessStartInfo]::new('pwsh')
                $start.UseShellExecute = $false
                foreach ($argument in @('-NoLogo', '-NoProfile', '-NonInteractive', '-File', $PSCommandPath, $directory, 'descendant')) {
                    $start.ArgumentList.Add($argument)
                }
                $child = [Diagnostics.Process]::Start($start)
                [IO.File]::WriteAllText((Join-Path $directory 'descendant-pid'), [string]$child.Id)
                for ($i = 0; $i -lt 400; $i++) {
                    if (Test-Path (Join-Path $directory 'descendant-ready')) { break }
                    Start-Sleep -Milliseconds 50
                }
                if (-not (Test-Path (Join-Path $directory 'descendant-ready'))) { exit 93 }
            }
            if ($mode -eq 'stderr') {
                [Console]::Error.Write("`nNow listening on: http://127.0.0.1:12345/`n")
                [Console]::Error.Flush()
            }
            if ($mode -eq 'fragmented') {
                [Console]::Out.Write("`nNow listening on: http://127.0.0.1:")
                [Console]::Out.Flush()
            }
            [IO.File]::WriteAllText((Join-Path $directory 'pipe-preparation-started'), 'started')
            [IO.File]::WriteAllText((Join-Path $directory 'pipe-preparation-completed'), 'completed')
            if ($mode -eq 'exit-before-ready') { exit 23 }
            if ($mode -eq 'withheld-ready') {
                [IO.File]::WriteAllText((Join-Path $directory 'ready-withheld'), 'withheld')
            } else {
                [IO.File]::WriteAllText((Join-Path $directory 'ready-publish-started'), 'started')
                [IO.File]::WriteAllText((Join-Path $directory 'ready'), 'ready')
                [IO.File]::WriteAllText((Join-Path $directory 'ready-publish-completed'), 'completed')
            }
            $outputFlushed = $false
            $outputReleased = $false
            for ($i = 0; $i -lt 1200; $i++) {
                if (-not $outputReleased -and (Test-Path (Join-Path $directory 'release-output') -PathType Leaf)) {
                    if ($mode -eq 'delayed-stdout') {
                        [Console]::Out.Write('root-output-without-newline')
                        [Console]::Out.Flush()
                    }
                    if ($mode -eq 'delayed-stderr') {
                        [Console]::Error.Write('root-error-without-newline')
                        [Console]::Error.Flush()
                    }
                    $outputReleased = $true
                }
                if (-not $outputFlushed -and (Test-Path (Join-Path $directory 'flush-output'))) {
                    [Console]::Out.Write("`n")
                    [Console]::Out.Flush()
                    [Console]::Error.Write("`n")
                    [Console]::Error.Flush()
                    $outputFlushed = $true
                }
                if ($mode -eq 'fragmented' -and (Test-Path (Join-Path $directory 'complete-listener'))) {
                    [Console]::Out.Write("12345/`r`n")
                    [Console]::Out.Flush()
                    $mode = 'ordinary'
                }
                if ($mode -ne 'uncooperative' -and (Test-Path (Join-Path $directory 'release-root') -PathType Leaf)) { exit 23 }
                Start-Sleep -Milliseconds 50
            }
            exit 92
            """;
    }
}
