using System.Runtime.ExceptionServices;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StoredContentOperationOwnershipTests
{
    [Fact]
    public async Task SiblingFaultStillReleasesEverySuccessfulResultAndKeepsTheFault()
    {
        using var pin = new PinProbe();
        using var stored = new StoredContent(ContentManifest.Empty("fixture"), pin);
        var failure = new IOException("controlled sibling store failure");
        var verified = false;
        var actual = await Record.ExceptionAsync(() => OwnedStoredContentOperations.RunAsync(
            _ => Task.FromResult(stored), _ => Task.FromException<StoredContent>(failure), _ =>
            {
                verified = true;
                return Task.CompletedTask;
            }, TimeSpan.FromSeconds(5))).ConfigureAwait(true);
        Assert.Same(failure, actual);
        Assert.False(verified);
        Assert.Equal(1, pin.Disposals);
    }

    [Fact]
    public async Task TimeoutCancelsButDoesNotRetireBeforeTheActualStoreSettles()
    {
        using var pin = new PinProbe();
        using var stored = new StoredContent(ContentManifest.Empty("fixture"), pin);
        var cancellationObserved = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var exited = false;
#pragma warning disable CA2025 // finally releases the controlled store and joins this ACTUAL owner via Record.ExceptionAsync before either using fallback can dispose its resources; interprocedural analysis misses that join.
        var owner = OwnedStoredContentOperations.RunAsync(_ => Task.FromResult(stored), StoreAsync,
            _ => Task.CompletedTask, TimeSpan.FromMilliseconds(100));
#pragma warning restore CA2025
        Exception? verification;
        Exception? failure;
        try
        {
            verification = await Record.ExceptionAsync(async () =>
            {
                await cancellationObserved.Task.WaitAsync(TimeSpan.FromSeconds(5), CancellationToken.None).ConfigureAwait(true);
                await pin.Disposed.Task.WaitAsync(TimeSpan.FromSeconds(5), CancellationToken.None).ConfigureAwait(true);
                Assert.False(exited);
                Assert.False(owner.IsCompleted);
                Assert.Equal(1, pin.Disposals);
            }).ConfigureAwait(true);
        }
        finally
        {
            release.TrySetResult();
#pragma warning disable VSTHRD003 // owner is this test's actual operation; join after controlled release even when assertions fail, before fallback disposal.
            failure = await Record.ExceptionAsync(() => owner).ConfigureAwait(true);
#pragma warning restore VSTHRD003
        }
        if (verification is not null)
            ExceptionDispatchInfo.Throw(verification);
        var aggregate = Assert.IsType<AggregateException>(failure);
        Assert.Contains(aggregate.Flatten().InnerExceptions, static error => error is TimeoutException);
        Assert.Contains(aggregate.Flatten().InnerExceptions, static error => error is OperationCanceledException);
        Assert.True(exited);
        Assert.Equal(1, pin.Disposals);

        async Task<StoredContent> StoreAsync(CancellationToken cancellationToken)
        {
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                throw new InvalidOperationException("The controlled wait cannot complete without cancellation.");
            }
            catch (OperationCanceledException)
            {
                cancellationObserved.TrySetResult();
#pragma warning disable VSTHRD003 // release is this test's asynchronous signal, not ambient context-affine work; finally always releases it.
                await release.Task.ConfigureAwait(false);
#pragma warning restore VSTHRD003
                exited = true;
                throw;
            }
        }
    }

    private sealed class PinProbe : IDisposable
    {
        internal int Disposals { get; private set; }
        internal TaskCompletionSource Disposed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public void Dispose()
        {
            Disposals++;
            Disposed.TrySetResult();
        }
    }
}
