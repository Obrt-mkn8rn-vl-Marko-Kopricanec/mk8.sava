using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Text;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

internal sealed class SplitServiceProcess : IAsyncDisposable
{
    private readonly Process _process;
    private readonly Lock _processGate = new();
    private readonly CancellationTokenSource _captureLifetime = new();
    private readonly CapturedStream _stdout = new();
    private readonly CapturedStream _stderr = new();
    private readonly Task _output;
    private readonly Task _error;
    private readonly Task _rootExit;
    private readonly TimeSpan _cleanupTimeout;
    private readonly TaskCompletionSource<Uri> _address = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private bool _processDisposed;
    private bool _exitedAtDisposal;
    private int _stopping;

    public SplitServiceProcess(ProcessStartInfo start, TimeSpan? cleanupTimeout = null)
    {
        _cleanupTimeout = cleanupTimeout ?? TimeSpan.FromSeconds(30);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(_cleanupTimeout, TimeSpan.Zero, nameof(cleanupTimeout));
        _process = Process.Start(start) ?? throw new InvalidOperationException("A split service could not start.");
        Id = _process.Id;
        _output = CaptureAsync(_stdout, _process.StandardOutput, publishAddress: true);
        _error = CaptureAsync(_stderr, _process.StandardError, publishAddress: false);
        _rootExit = ObserveRootExitAsync();
        ObserveEventualFault(_address.Task);
    }

    public int Id { get; }
    public string Logs => $"stdout:\n{_stdout.Text}\nstderr:\n{_stderr.Text}";

    public async Task<Uri> WaitForAddressAsync(string role, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(role);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);
        var observation = Stopwatch.StartNew();
        try
        {
            var address = await _address.Task.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
            if (RootExited())
                throw new InvalidOperationException("Service root exited after publishing its listener.");
            return address;
        }
        catch (Exception failure) when (!CatastrophicExceptionPolicy.Contains(failure))
        {
            // Snapshot before the host's subsequent cleanup; output arrival and root/EOF observations are independent.
            var diagnostic = $"{role} listener observation failed after {observation.Elapsed}. {Snapshot()}\n{Logs}";
            if (failure is OperationCanceledException && cancellationToken.IsCancellationRequested)
                throw new OperationCanceledException(diagnostic, failure, cancellationToken);
            if (failure is TimeoutException)
                throw new TimeoutException(diagnostic, failure);
            throw new InvalidOperationException(diagnostic, failure);
        }
    }

    private bool RootExited()
    {
        lock (_processGate)
            return _processDisposed ? _exitedAtDisposal : _process.HasExited;
    }

    private string Snapshot() => $"PID={Id}, RootExited={RootExited()}, StdoutEof={_stdout.ReachedEof}, " +
        $"StderrEof={_stderr.ReachedEof}, ListenerPublished={_address.Task.IsCompletedSuccessfully}. " +
        "Root exit and pipe EOFs describe only the observed root/streams; descendant exit is not established.";

    private async Task ObserveRootExitAsync()
    {
        try
        {
            await _process.WaitForExitAsync(_captureLifetime.Token).ConfigureAwait(false);
            _address.TrySetException(new InvalidOperationException("Service root exited before listener publication."));
        }
        catch (OperationCanceledException failure) when (_captureLifetime.IsCancellationRequested &&
            !CatastrophicExceptionPolicy.Contains(failure))
        { }
        catch (Exception failure)
        {
            _address.TrySetException(failure);
            throw;
        }
    }

    private async Task CaptureAsync(CapturedStream capture, StreamReader reader, bool publishAddress)
    {
        try
        {
            await capture.ReadAsync(reader, publishAddress ? PublishLine : null, _captureLifetime.Token).ConfigureAwait(false);
            if (publishAddress)
                _address.TrySetException(new InvalidOperationException("Standard output ended before listener publication."));
        }
        catch (Exception failure) when (_captureLifetime.IsCancellationRequested &&
            (failure is OperationCanceledException or ObjectDisposedException) && !CatastrophicExceptionPolicy.Contains(failure))
        { }
        catch (Exception failure)
        {
            _address.TrySetException(failure);
            throw;
        }
    }

    private void PublishLine(string line)
    {
        const string prefix = "Now listening on: ";
        var offset = line.IndexOf(prefix, StringComparison.Ordinal);
        if (offset >= 0 && Uri.TryCreate(line[(offset + prefix.Length)..].Trim(), UriKind.Absolute, out var address))
            _address.TrySetResult(address);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _stopping, 1) != 0)
            return;
        var beforeCleanup = Snapshot();
        try
        {
            await CleanUpAsync(beforeCleanup).ConfigureAwait(false);
        }
        finally
        {
            lock (_processGate)
            {
                _exitedAtDisposal = _process.HasExited;
                _process.Dispose();
                _processDisposed = true;
            }
            _captureLifetime.Dispose();
        }
    }

    private async Task CleanUpAsync(string beforeCleanup)
    {
        using var cleanup = new CancellationTokenSource(_cleanupTimeout);
        var killDiagnostic = TryKillRoot();
        _address.TrySetException(new InvalidOperationException("Service stopped before listener publication."));
        var callbacks = _captureLifetime.CancelAsync();
        ObserveEventualFault(callbacks);
        // Own read-end closure also covers a descendant retaining the write ends after the root exited.
        _process.StandardOutput.Dispose();
        _process.StandardError.Dispose();
        var rootWait = _process.WaitForExitAsync(cleanup.Token);
        await ObserveCleanupAsync([_output, _error, _rootExit, callbacks, rootWait],
            () => $"Split service cleanup observation failed. Before cleanup: {beforeCleanup} " +
                $"After cleanup: {Snapshot()} {killDiagnostic}\n{Logs}", cleanup.Token).ConfigureAwait(false);
    }

    internal static async Task ObserveCleanupAsync(IEnumerable<Task> operations, Func<string> diagnostic,
        CancellationToken cancellationToken)
    {
        var tasks = operations.ToArray();
        var completion = Task.WhenAll(tasks);
        try
        {
            await completion.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception failure) when (!CatastrophicExceptionPolicy.Contains(failure))
        {
            // WhenAll remains pending until every task settles; its Exception cannot reveal earlier constituent faults.
            ThrowKnownFatalFaults(tasks);
            var message = diagnostic();
            // Diagnostic capture can overlap task completion. Recheck before normalizing the ordinary/deadline failure.
            ThrowKnownFatalFaults(tasks);
            if (failure is OperationCanceledException && cancellationToken.IsCancellationRequested)
                throw new TimeoutException(message, failure);
            throw new InvalidOperationException(message, failure);
        }
        finally
        {
            // Bounded observation is not a guarantee that native reads/callbacks or arbitrary descendants have ended.
            ObserveEventualFault(completion);
        }
    }

    private static void ThrowKnownFatalFaults(Task[] tasks)
    {
        foreach (var task in tasks)
        {
            if (task.Exception is not { } failures)
                continue;
            foreach (var failure in failures.InnerExceptions)
            {
                if (CatastrophicExceptionPolicy.Contains(failure))
                    ExceptionDispatchInfo.Throw(failure);
            }
        }
    }

    private string TryKillRoot()
    {
        try
        {
            if (!_process.HasExited)
                _process.Kill(entireProcessTree: true);
            return string.Empty;
        }
        catch (Exception failure) when ((failure is Win32Exception or InvalidOperationException) &&
            !CatastrophicExceptionPolicy.Contains(failure))
        {
            return $"Root kill attempt: {failure.GetType().Name}: {failure.Message}";
        }
    }

    private static void ObserveEventualFault(Task task)
        => _ = task.ContinueWith(static completed => _ = completed.Exception,
            CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnFaulted,
            TaskScheduler.Default);

    private sealed class CapturedStream
    {
        private readonly Lock gate = new();
        private readonly StringBuilder text = new();
        private bool reachedEof;

        internal string Text { get { lock (gate) return text.ToString(); } }
        internal bool ReachedEof { get { lock (gate) return reachedEof; } }

        internal async Task ReadAsync(StreamReader reader, Action<string>? onLine, CancellationToken cancellationToken)
        {
            var buffer = new char[4096];
            var line = new StringBuilder();
            while (true)
            {
                var count = await reader.ReadAsync(buffer.AsMemory(), cancellationToken).ConfigureAwait(false);
                lock (gate)
                {
                    text.Append(buffer, 0, count);
                    reachedEof = count == 0;
                }
                if (count == 0)
                {
                    if (line.Length > 0)
                        onLine?.Invoke(line.ToString());
                    return;
                }
                if (onLine is not null)
                    PublishCompleteLines(buffer.AsSpan(0, count), line, onLine);
            }
        }

        private static void PublishCompleteLines(ReadOnlySpan<char> buffer, StringBuilder line, Action<string> onLine)
        {
            foreach (var character in buffer)
            {
                if (character == '\n')
                {
                    onLine(line.ToString());
                    line.Clear();
                }
                else
                {
                    line.Append(character);
                }
            }
        }
    }
}
