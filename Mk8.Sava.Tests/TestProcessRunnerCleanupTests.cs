using Mk8.Sava.Protocol;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class TestProcessRunnerCleanupTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public async Task AvailableFatalConstituentEscapesWithoutWaitingForAnUnrelatedPendingTask(int faultIndex)
    {
        var original = WrappedFatal(accessViolation: false);
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task[] operations = [Task.CompletedTask, Task.CompletedTask, Task.CompletedTask, Task.CompletedTask, Task.CompletedTask];
        operations[faultIndex] = Task.FromException(original);
        operations[faultIndex == 1 ? 2 : 1] = pending.Task;
        var completion = Task.WhenAll(operations.AsSpan(0, 4));
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync().ConfigureAwait(true);
        string? diagnostic = null;
        try
        {
            Assert.False(completion.IsCompleted);
            Assert.Null(completion.Exception);
            var failure = await Record.ExceptionAsync(async () =>
            {
                diagnostic = await TestProcessRunner.ObserveCleanupAsync(operations[4], completion,
                    operations, cancellation.Token).WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
            }).ConfigureAwait(true);
            output.WriteLine($"Controlled cleanup: FaultIndex={faultIndex}, Failure={failure?.GetType().Name ?? "none"}, " +
                $"AggregateCompleted={completion.IsCompleted}, PendingCompleted={pending.Task.IsCompleted}, Diagnostic={diagnostic ?? "none"}.");

            Assert.Same(original, failure);
            Assert.False(pending.Task.IsCompleted);
        }
        finally
        {
            pending.TrySetResult();
            await JoinControlledOperationsAsync(operations).ConfigureAwait(true);
        }
    }

    [Fact]
    public async Task ACompletedOrdinaryFirstAggregateStillDispatchesItsAvailableFatalPayload()
    {
        var original = WrappedFatal(accessViolation: true);
        Task[] operations = [Task.FromException(new IOException("ordinary first fault")),
            Task.FromException(original), Task.CompletedTask, Task.CompletedTask, Task.CompletedTask];
        var completion = Task.WhenAll(operations.AsSpan(0, 4));
        var failure = await Record.ExceptionAsync(() => TestProcessRunner.ObserveCleanupAsync(
            operations[4], completion, operations, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(1))).ConfigureAwait(true);

        output.WriteLine($"Completed mixed cleanup: Failure={failure?.GetType().Name ?? "none"}, AggregateFaulted={completion.IsFaulted}.");
        Assert.Same(original, failure);
        await JoinControlledOperationsAsync(operations).ConfigureAwait(true);
    }

    [Fact]
    public async Task DiagnosticConstructionCannotHideANewlyAvailableFatalFault()
    {
        var original = WrappedFatal(accessViolation: false);
        var late = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var diagnosticFailure = new PublishingDiagnosticFailure("controlled diagnostic construction")
        {
            Publish = () => late.TrySetException(original)
        };
        Task[] operations = [late.Task, Task.CompletedTask, Task.CompletedTask, Task.CompletedTask,
            Task.FromException(diagnosticFailure)];
        var completion = Task.WhenAll(operations.AsSpan(0, 4));
        try
        {
            var failure = await Record.ExceptionAsync(() => TestProcessRunner.ObserveCleanupAsync(
                operations[4], completion, operations, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(1))).ConfigureAwait(true);
            output.WriteLine($"Diagnostic overlap: Failure={failure?.GetType().Name ?? "none"}, LateFaultAvailable={late.Task.IsFaulted}.");

            Assert.True(late.Task.IsFaulted);
            Assert.Same(original, failure);
        }
        finally
        {
            late.TrySetResult();
            await JoinControlledOperationsAsync(operations).ConfigureAwait(true);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SuccessfulAndCanceledConstituentsKeepTheirOrdinaryCleanupResult(bool canceled)
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync().ConfigureAwait(true);
        Task[] operations = [canceled ? Task.FromCanceled(cancellation.Token) : Task.CompletedTask,
            Task.CompletedTask, Task.CompletedTask, Task.CompletedTask, Task.CompletedTask];
        var completion = Task.WhenAll(operations.AsSpan(0, 4));
        var diagnostic = await TestProcessRunner.ObserveCleanupAsync(operations[4], completion,
            operations, CancellationToken.None).ConfigureAwait(true);

        if (canceled)
            Assert.Contains(" Cleanup observation: TaskCanceledException:", diagnostic, StringComparison.Ordinal);
        else
            Assert.Empty(diagnostic);
        await JoinControlledOperationsAsync(operations).ConfigureAwait(true);
    }

    [Fact]
    public async Task OrdinaryFaultAndPendingTaskRetainDeadlineDiagnosticsRatherThanManufacturingCompletion()
    {
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task[] operations = [Task.FromException(new IOException("ordinary known fault")), pending.Task,
            Task.CompletedTask, Task.CompletedTask, Task.CompletedTask];
        var completion = Task.WhenAll(operations.AsSpan(0, 4));
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync().ConfigureAwait(true);
        try
        {
            var diagnostic = await TestProcessRunner.ObserveCleanupAsync(operations[4], completion,
                operations, cancellation.Token).ConfigureAwait(true);

            Assert.Contains(" Cleanup observation: TaskCanceledException:", diagnostic, StringComparison.Ordinal);
            Assert.False(pending.Task.IsCompleted);
            Assert.False(completion.IsCompleted);
            Assert.Null(completion.Exception);
        }
        finally
        {
            pending.TrySetResult();
            await JoinControlledOperationsAsync(operations).ConfigureAwait(true);
        }
    }

    private static IOException WrappedFatal(bool accessViolation)
    {
#pragma warning disable CA2201 // Synthetic payloads test original fault-graph dispatch; this is not actual exhaustion or native corruption.
        Exception fatal = accessViolation ? new AccessViolationException("synthetic native fault") : new OutOfMemoryException("synthetic allocation fault");
#pragma warning restore CA2201
        return new IOException("controlled wrapped cleanup fault", fatal);
    }

    private static async Task JoinControlledOperationsAsync(Task[] operations)
    {
        try
        {
            await Task.WhenAll(operations).WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
        }
        catch (Exception failure) when (failure is IOException or OperationCanceledException)
        {
            // These are the exact deliberately completed model faults, not a native/process cleanup success.
            Assert.All(operations, operation => Assert.True(operation.IsCompleted));
        }
    }

    private sealed class PublishingDiagnosticFailure : IOException
    {
        public PublishingDiagnosticFailure() { }
        public PublishingDiagnosticFailure(string? message) : base(message) { }
        public PublishingDiagnosticFailure(string? message, int hresult) : base(message, hresult) { }
        public PublishingDiagnosticFailure(string? message, Exception? innerException) : base(message, innerException) { }

        internal Action? Publish { private get; init; }

        public override string Message
        {
            get
            {
                Publish?.Invoke();
                return base.Message;
            }
        }
    }
}
