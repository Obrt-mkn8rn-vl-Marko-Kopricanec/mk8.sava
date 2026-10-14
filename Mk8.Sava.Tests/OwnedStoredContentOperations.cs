using System.Runtime.ExceptionServices;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

// Test-only single owner. Results remain borrowed by verification; this owner
// cancels and joins every actual store and releases every successful pin.
internal static class OwnedStoredContentOperations
{
    internal static async Task RunAsync(
        Func<CancellationToken, Task<StoredContent>> first,
        Func<CancellationToken, Task<StoredContent>> second,
        Func<StoredContent[], Task> verify,
        TimeSpan observationTimeout)
    {
        using var cancellation = new CancellationTokenSource();
        var pending = new List<Task<StoredContent>>(2);
        Task<StoredContent[]>? completion = null;
        var failures = new List<Exception>();
        try
        {
            var primary = await Record.ExceptionAsync(async () =>
            {
                pending.Add(first(cancellation.Token));
                pending.Add(second(cancellation.Token));
                completion = Task.WhenAll(pending);
                var stored = await completion.WaitAsync(observationTimeout, CancellationToken.None).ConfigureAwait(false);
                await verify(stored).ConfigureAwait(false);
            }).ConfigureAwait(false);
            AddFailure(failures, primary);
        }
        finally
        {
            AddFailure(failures, await Record.ExceptionAsync(() => cancellation.CancelAsync()).ConfigureAwait(false));
            foreach (var operation in pending)
                AddFailure(failures, await JoinAndReleaseAsync(operation).ConfigureAwait(false));
            // Observe the actual aggregate too; an observation timeout never
            // transfers ownership or makes its later failure disappear.
            if (completion is not null)
            {
#pragma warning disable VSTHRD003 // This helper creates/owns completion above and joins it after every constituent actual task; all stores are context-free.
                AddFailure(failures, await Record.ExceptionAsync(() => completion).ConfigureAwait(false));
#pragma warning restore VSTHRD003
            }
        }
        if (failures.Count == 1)
            ExceptionDispatchInfo.Throw(failures[0]);
        if (failures.Count > 1)
            throw new AggregateException("Verification and independently observed owned-store failures.", failures);
    }

    private static async Task<Exception?> JoinAndReleaseAsync(Task<StoredContent> operation) =>
        await Record.ExceptionAsync(async () =>
        {
#pragma warning disable VSTHRD003 // The caller registered this actual store at acquisition and owns its guaranteed join before dependency retirement.
            var stored = await operation.ConfigureAwait(false);
#pragma warning restore VSTHRD003
            stored.Dispose();
        }).ConfigureAwait(false);

    private static void AddFailure(List<Exception> failures, Exception? failure)
    {
        if (failure is not null && !failures.Exists(existing => ReferenceEquals(existing, failure)))
            failures.Add(failure);
    }
}
