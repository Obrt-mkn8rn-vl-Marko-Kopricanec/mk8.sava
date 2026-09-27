using System.Threading.RateLimiting;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Storage;

internal sealed class StorageWorkLimiter : IDisposable
{
    private readonly Lock _gate = new();
    private readonly LinkedList<QueuedWork> _queue = new();
    private int _active;
    private long _rejected;
    private bool _closed;
    private readonly int _permits;
    private readonly int _queueLimit;

    public StorageWorkLimiter(int permits, int queueLimit)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(permits);
        ArgumentOutOfRangeException.ThrowIfNegative(queueLimit);
        _permits = permits;
        _queueLimit = queueLimit;
    }

    public (int Active, int Queued, long Rejected) GetStatistics()
    {
        lock (_gate)
            return (_active, _queue.Count, _rejected);
    }

    public async ValueTask<RateLimitLease> AcquireAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        QueuedWork work;
        TaskCompletionSource<RateLimitLease> completion;
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_closed, this);
            if (_active < _permits)
            {
                _active++;
                return new WorkLease(this);
            }
            if (_queue.Count >= _queueLimit)
            {
                _rejected++;
                throw CreateBusyError();
            }
            completion = new TaskCompletionSource<RateLimitLease>(TaskCreationOptions.RunContinuationsAsynchronously);
            work = new QueuedWork(this, completion, cancellationToken);
            work.Node = _queue.AddLast(work);
        }
        var registration = cancellationToken.UnsafeRegister(static state =>
        {
            var queued = (QueuedWork)state!;
            queued.Owner.Cancel(queued);
        }, work);
        await using var registrationDisposal = registration.ConfigureAwait(false);
        return await completion.Task.ConfigureAwait(false);
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_closed)
                return;
            _closed = true;
            while (_queue.First is { } first)
            {
                _queue.Remove(first);
                _rejected++;
                first.Value.Completion.TrySetException(CreateBusyError());
            }
        }
        GC.SuppressFinalize(this);
    }

    private void Cancel(QueuedWork work)
    {
        lock (_gate)
        {
            if (work.Node?.List is null)
                return;
            _queue.Remove(work.Node);
            work.Completion.TrySetCanceled(work.CancellationToken);
        }
    }

    private void Release()
    {
        lock (_gate)
        {
            if (_queue.First is { } first)
            {
                _queue.Remove(first);
                WorkLease? lease = new(this);
                try
                {
                    if (first.Value.Completion.TrySetResult(lease))
                        lease = null;
                }
                finally
                {
                    lease?.Dispose();
                }
            }
            else
            {
                _active--;
            }
        }
    }

    private static AzureStorageException CreateBusyError() => new(
        503, "ServerBusy", "The server is currently unable to receive requests. Please retry your request.",
        responseHeaders: new Dictionary<string, string>(StringComparer.Ordinal) { ["Retry-After"] = "1" });

    private sealed class QueuedWork(StorageWorkLimiter owner, TaskCompletionSource<RateLimitLease> completion, CancellationToken cancellationToken)
    {
        public StorageWorkLimiter Owner { get; } = owner;
        public CancellationToken CancellationToken { get; } = cancellationToken;
        public TaskCompletionSource<RateLimitLease> Completion { get; } = completion;
        public LinkedListNode<QueuedWork>? Node { get; set; }
    }

    private sealed class WorkLease(StorageWorkLimiter owner) : RateLimitLease
    {
#pragma warning disable CA2213 // A lease borrows the limiter; disposing it must release only this permit, not close every other operation.
        private StorageWorkLimiter? _owner = owner;
#pragma warning restore CA2213
        public override bool IsAcquired => true;
        public override IEnumerable<string> MetadataNames => [];

        public override bool TryGetMetadata(string metadataName, out object? metadata)
        {
            metadata = null;
            return false;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                Interlocked.Exchange(ref _owner, null)?.Release();
            base.Dispose(disposing);
        }
    }
}
