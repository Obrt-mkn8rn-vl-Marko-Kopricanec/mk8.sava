using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
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
        var rootExit = process.WaitForExitAsync(timeout.Token);
        Task[] observations = [rootExit, stdout, stderr];
        var operations = observations;
        var completion = Task.WhenAll(observations);
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
                var cleanup = await CleanUpAsync(process, completion, observations, timeout, cleanupTimeout).ConfigureAwait(false);
                cleanupState = cleanup.Diagnostic;
                operations = cleanup.Operations;
            }
        }

        ThrowKnownFatalFaults(operations);
        if (interruption is null)
            return (process.ExitCode, output.Text, error.Text);

        var diagnostic = $"Process observation interrupted. {interruptedState}. {cleanupState}. " +
            "Root exit and pipe EOFs describe only the observed root/streams; descendant exit is not established.\n" +
            $"stdout (partial):\n{output.Text}\nstderr (partial):\n{error.Text}";
        // Snapshot construction can overlap a constituent fault; do not normalize one already available here.
        ThrowKnownFatalFaults(operations);
        if (cancellationToken.IsCancellationRequested)
            throw new OperationCanceledException(diagnostic, interruption, cancellationToken);
        throw new TimeoutException(diagnostic, interruption);
    }

    private static async Task<(string Diagnostic, Task[] Operations)> CleanUpAsync(
        Process process, Task completion, Task[] observations, CancellationTokenSource observationTimeout, TimeSpan cleanupTimeout)
    {
        using var cleanup = new CancellationTokenSource(cleanupTimeout);
        var callbacks = observationTimeout.CancelAsync();
        var cleanupCompletion = Task.WhenAll(completion, callbacks);
        Task[] operations = [.. observations, callbacks];
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
            var rootWait = process.WaitForExitAsync(cleanup.Token);
            operations = [.. operations, rootWait];
            errors.Append(await ObserveCleanupAsync(rootWait, cleanupCompletion, operations, cleanup.Token).ConfigureAwait(false));
        }
        catch (Exception exception) when (!CatastrophicExceptionPolicy.Contains(exception))
        {
            errors.Append(DescribeCleanupFailure(exception, operations));
        }
        finally
        {
            // Never wait without a deadline. Observe an eventual fault if native I/O outlives cleanup.
            ObserveEventualFault(cleanupCompletion);
            foreach (var operation in operations)
                ObserveEventualFault(operation);
        }
        var diagnostic = $"Cleanup RootExited={process.HasExited}, ObservationSettled={cleanupCompletion.IsCompleted}" + errors;
        ThrowKnownFatalFaults(operations);
        return (diagnostic, operations);
    }

    internal static async Task<string> ObserveCleanupAsync(
        Task rootWait, Task completion, Task[] operations, CancellationToken cancellationToken)
    {
        try
        {
            await rootWait.WaitAsync(cancellationToken).ConfigureAwait(false);
            await completion.WaitAsync(cancellationToken).ConfigureAwait(false);
            return string.Empty;
        }
        catch (Exception failure) when (!CatastrophicExceptionPolicy.Contains(failure))
        {
            return DescribeCleanupFailure(failure, operations);
        }
    }

    private static string DescribeCleanupFailure(Exception failure, Task[] operations)
    {
        // WhenAll has no Exception until every constituent settles; inspect the actual available fault graphs.
        ThrowKnownFatalFaults(operations);
        var diagnostic = $" Cleanup observation: {failure.GetType().Name}: {failure.Message}";
        ThrowKnownFatalFaults(operations);
        return diagnostic;
    }

    private static void ThrowKnownFatalFaults(Task[] operations)
    {
        foreach (var operation in operations)
        {
            if (operation.Exception is not { } failures)
                continue;
            foreach (var failure in failures.InnerExceptions)
                if (CatastrophicExceptionPolicy.Contains(failure))
                    ExceptionDispatchInfo.Throw(failure);
        }
    }

    private static void ObserveEventualFault(Task task)
        => _ = task.ContinueWith(static finished => _ = finished.Exception,
            CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

}
