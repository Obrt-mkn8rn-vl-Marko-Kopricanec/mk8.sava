using System.ComponentModel;
using System.Diagnostics;
using System.Text;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

internal static class TestProcessRunner
{
    internal static async Task<(int ExitCode, string StandardOutput, string StandardError)> ObserveAsync(
        Process process, TimeSpan completionTimeout, TimeSpan cleanupTimeout, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(completionTimeout, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(cleanupTimeout, TimeSpan.Zero);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(completionTimeout);
        var output = new ProcessOutputCapture();
        var error = new ProcessOutputCapture();
        var stdout = output.ReadAsync(process.StandardOutput, onLine: null, timeout.Token);
        var stderr = error.ReadAsync(process.StandardError, onLine: null, timeout.Token);
        var completion = Task.WhenAll(process.WaitForExitAsync(timeout.Token), stdout, stderr);
        OperationCanceledException? interruption = null;
        var interruptedState = string.Empty;
        var cleanupState = string.Empty;
        try
        {
            // This deadline covers root exit AND both EOFs, including an already-exited root.
            await completion.WaitAsync(timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException exception) when (timeout.IsCancellationRequested &&
            !CatastrophicExceptionPolicy.Contains(exception))
        {
            interruption = exception;
            var outputProgress = output.Progress;
            var errorProgress = error.Progress;
            interruptedState = $"RootExited={process.HasExited}, StdoutEof={outputProgress.ReachedEof}, StderrEof={errorProgress.ReachedEof}. " +
                "Logical capture progress (sequential stream/task observations, not native I/O): " +
                $"Stdout=[{outputProgress.ToDiagnostic()}, Task={stdout.Status}], Stderr=[{errorProgress.ToDiagnostic()}, Task={stderr.Status}]";
        }
        finally
        {
            if (!completion.IsCompletedSuccessfully)
            {
                cleanupState = await CleanUpAsync(process, completion, timeout, cleanupTimeout).ConfigureAwait(false);
            }
        }

        if (interruption is null)
            return (process.ExitCode, output.Text, error.Text);

        var diagnostic = $"Process observation interrupted. {interruptedState}. {cleanupState}. " +
            "Root exit and pipe EOFs describe only the observed root/streams; descendant exit is not established.\n" +
            $"stdout (partial):\n{output.Text}\nstderr (partial):\n{error.Text}";
        if (cancellationToken.IsCancellationRequested)
            throw new OperationCanceledException(diagnostic, interruption, cancellationToken);
        throw new TimeoutException(diagnostic, interruption);
    }

    private static async Task<string> CleanUpAsync(
        Process process, Task completion, CancellationTokenSource observationTimeout, TimeSpan cleanupTimeout)
    {
        using var cleanup = new CancellationTokenSource(cleanupTimeout);
        var cleanupCompletion = Task.WhenAll(completion, observationTimeout.CancelAsync());
        var errors = new StringBuilder();
        try
        {
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
        }
        catch (Exception exception) when ((exception is Win32Exception or InvalidOperationException or AggregateException) &&
            !CatastrophicExceptionPolicy.Contains(exception))
        {
            errors.Append(" Kill attempt: ").Append(exception.Message);
        }

        // Cancellation need not promptly stop an in-flight native pipe read. Close our read ends as well.
        process.StandardOutput.Dispose();
        process.StandardError.Dispose();
        try
        {
            await process.WaitForExitAsync(cleanup.Token).ConfigureAwait(false);
            await cleanupCompletion.WaitAsync(cleanup.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (!CatastrophicExceptionPolicy.Contains(exception))
        {
            errors.Append(" Cleanup observation: ").Append(exception.GetType().Name).Append(": ").Append(exception.Message);
        }
        finally
        {
            // Never wait without a deadline. Observe an eventual fault if native I/O outlives cleanup.
            _ = cleanupCompletion.ContinueWith(static finished => _ = finished.Exception,
                CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
        return $"Cleanup RootExited={process.HasExited}, ObservationSettled={cleanupCompletion.IsCompleted}" + errors;
    }

}
