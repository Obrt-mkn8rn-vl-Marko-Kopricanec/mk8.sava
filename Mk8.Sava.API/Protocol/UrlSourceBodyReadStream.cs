namespace Mk8.Sava.Protocol;

// Borrows the response's stream. Only an outstanding asynchronous source read is
// timed; destination work between reads is not charged to the source idle budget.
internal sealed class UrlSourceBodyReadStream(
    Stream inner,
    TimeSpan idleTimeout,
    CancellationToken requestCancellation) : Stream
{
    private bool _disposed;

    public override bool CanRead => !_disposed && inner.CanRead;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        requestCancellation.ThrowIfCancellationRequested();
        cancellationToken.ThrowIfCancellationRequested();
        if (buffer.IsEmpty)
            return 0;

        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(requestCancellation, cancellationToken);
        deadline.CancelAfter(idleTimeout);
        try
        {
            // Join the actual I/O. Cancellation is cooperative, not permission to
            // abandon a read while its response/buffer owner is being retired.
            var read = await inner.ReadAsync(buffer, deadline.Token).ConfigureAwait(false);
            deadline.Token.ThrowIfCancellationRequested();
            return read;
        }
        catch (OperationCanceledException exception) when (deadline.IsCancellationRequested &&
            !requestCancellation.IsCancellationRequested && !cancellationToken.IsCancellationRequested &&
            !CatastrophicExceptionPolicy.Contains(exception))
        {
            throw new AzureStorageException(StatusCodes.Status500InternalServerError, "CannotVerifyCopySource",
                "The source response body did not make progress before the read idle timeout.");
        }
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    // All URL-source consumers use asynchronous reads. Do not offer a synchronous
    // path that silently bypasses the cancellation/deadline contract.
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override int Read(Span<byte> buffer) => throw new NotSupportedException();
    public override void Flush() => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        _disposed = true;
        // The enclosing response/await-using scope owns inner; this adapter does not.
        base.Dispose(disposing);
    }
}
