using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed partial class SplitServiceProcessTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public async Task KnownFatalInAnyCleanupOperationWinsOverPendingDeadline(int faultIndex)
    {
        var fatal = CreateFatal("oom");
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task[] operations = [Task.CompletedTask, Task.CompletedTask, Task.CompletedTask, Task.CompletedTask, Task.CompletedTask];
        operations[faultIndex] = Task.FromException(fatal);
        operations[(faultIndex + 1) % operations.Length] = pending.Task;
        using var deadline = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        var diagnosticRequested = false;
        try
        {
            var failure = await Record.ExceptionAsync(() => SplitServiceProcess.ObserveCleanupAsync(operations, () =>
            {
                diagnosticRequested = true;
                return "ordinary cleanup diagnostic";
            }, deadline.Token).WaitAsync(TimeSpan.FromSeconds(5))).ConfigureAwait(true);

            Assert.Same(fatal, failure);
            Assert.False(diagnosticRequested);
            Assert.False(pending.Task.IsCompleted);
        }
        finally
        {
            pending.TrySetResult();
        }
    }

    [Theory]
    [InlineData("wrapped")]
    [InlineData("aggregate")]
    public async Task NestedFatalGraphKeepsItsIdentityWhileAnotherOperationIsPending(string kind)
    {
        var fatal = CreateFatal(kind);
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var deadline = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        try
        {
            var failure = await Record.ExceptionAsync(() => SplitServiceProcess.ObserveCleanupAsync(
                [Task.FromException(fatal), pending.Task], () => "ordinary diagnostic", deadline.Token)
                .WaitAsync(TimeSpan.FromSeconds(5))).ConfigureAwait(true);

            Assert.Same(fatal, failure);
            Assert.False(pending.Task.IsCompleted);
        }
        finally
        {
            pending.TrySetResult();
        }
    }

    [Fact]
    public async Task FatalPublishedDuringCancellationIsNotConvertedToTimeout()
    {
        var fatal = CreateFatal("access");
        var fault = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        var observation = SplitServiceProcess.ObserveCleanupAsync([fault.Task, pending.Task],
            () => "ordinary cancellation diagnostic", cancellation.Token);
        // Register after the observation: the fault publication precedes its cancellation wake-up.
        using var publication = cancellation.Token.Register(() => fault.TrySetException(fatal));
        try
        {
            await cancellation.CancelAsync().ConfigureAwait(true);
            var failure = await Record.ExceptionAsync(() => observation.WaitAsync(TimeSpan.FromSeconds(5))).ConfigureAwait(true);

            Assert.Same(fatal, failure);
            Assert.False(pending.Task.IsCompleted);
        }
        finally
        {
            pending.TrySetResult();
        }
    }

    [Fact]
    public async Task FatalAvailableDuringDiagnosticConstructionWinsOverOrdinaryFailure()
    {
        var fatal = CreateFatal("oom");
        var fault = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var deadline = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        try
        {
            var failure = await Record.ExceptionAsync(() => SplitServiceProcess.ObserveCleanupAsync(
                [fault.Task, pending.Task], () =>
                {
                    fault.SetException(fatal);
                    return "diagnostic constructed after fatal publication";
                }, deadline.Token).WaitAsync(TimeSpan.FromSeconds(5))).ConfigureAwait(true);

            Assert.Same(fatal, failure);
            Assert.False(pending.Task.IsCompleted);
        }
        finally
        {
            pending.TrySetResult();
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CompletedMixedFaultsRetainFatalIdentityRegardlessOfOrder(bool ordinaryFirst)
    {
        var fatal = CreateFatal("access");
        var ordinary = new IOException("ordinary completed fault");
        Task[] operations = ordinaryFirst
            ? [Task.FromException(ordinary), Task.FromException(fatal)]
            : [Task.FromException(fatal), Task.FromException(ordinary)];

        var failure = await Record.ExceptionAsync(() => SplitServiceProcess.ObserveCleanupAsync(operations,
            () => "ordinary mixed diagnostic", CancellationToken.None)).ConfigureAwait(true);

        Assert.NotNull(failure);
        Assert.True(CatastrophicExceptionPolicy.Contains(failure));
        if (failure is AggregateException aggregate)
            Assert.Contains(fatal, aggregate.Flatten().InnerExceptions);
        else
            Assert.Same(fatal, failure);
    }

    [Fact]
    public async Task OrdinaryCompletedFaultKeepsItsOriginalInnerFailure()
    {
        var ordinary = new IOException("controlled ordinary fault");

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => SplitServiceProcess.ObserveCleanupAsync(
            [Task.FromException(ordinary), Task.CompletedTask], () => "ordinary diagnostic", CancellationToken.None))
            .ConfigureAwait(true);

        Assert.Same(ordinary, failure.InnerException);
        Assert.Equal("ordinary diagnostic", failure.Message);
    }

    [Fact]
    public async Task PendingOrdinaryOperationKeepsDeadlineAndDoesNotCertifyCompletion()
    {
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var deadline = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        try
        {
            var failure = await Assert.ThrowsAsync<TimeoutException>(() => SplitServiceProcess.ObserveCleanupAsync(
                [pending.Task], () => "bounded deadline diagnostic", deadline.Token).WaitAsync(TimeSpan.FromSeconds(5)))
                .ConfigureAwait(true);

            var canceled = Assert.IsAssignableFrom<OperationCanceledException>(failure.InnerException);
            Assert.Equal(deadline.Token, canceled.CancellationToken);
            Assert.Equal("bounded deadline diagnostic", failure.Message);
            Assert.False(pending.Task.IsCompleted);
        }
        finally
        {
            pending.TrySetResult();
        }
    }

    [Fact]
    public async Task IndependentlyCanceledOperationIsNotMistakenForOwnerDeadline()
    {
        using var independent = new CancellationTokenSource();
        await independent.CancelAsync().ConfigureAwait(true);

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => SplitServiceProcess.ObserveCleanupAsync(
            [Task.FromCanceled(independent.Token)], () => "independent cancellation", CancellationToken.None))
            .ConfigureAwait(true);

        var canceled = Assert.IsAssignableFrom<OperationCanceledException>(failure.InnerException);
        Assert.Equal(independent.Token, canceled.CancellationToken);
    }

    [Fact]
    public async Task CompletedSuccessfulOperationsDoNotRequestFailureDiagnostic()
    {
        var diagnosticRequested = false;

        await SplitServiceProcess.ObserveCleanupAsync([Task.CompletedTask, Task.CompletedTask], () =>
        {
            diagnosticRequested = true;
            return "unexpected success diagnostic";
        }, CancellationToken.None).ConfigureAwait(true);

        Assert.False(diagnosticRequested);
    }

    private static Exception CreateFatal(string kind)
    {
#pragma warning disable CA2201 // Synthetic reserved failures test bounded task-fault identity, not actual OOM/corruption.
        return kind switch
        {
            "oom" => new OutOfMemoryException("synthetic cleanup OOM"),
            "access" => new AccessViolationException("synthetic cleanup access violation"),
            "wrapped" => new IOException("wrapped synthetic fatal", new OutOfMemoryException("synthetic nested OOM")),
            "aggregate" => new AggregateException(new IOException("ordinary graph member"), new AccessViolationException("synthetic graph fatal")),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown synthetic failure kind.")
        };
#pragma warning restore CA2201
    }
}
