using System.Buffers.Binary;
using System.Security.Cryptography;

namespace Mk8.Sava.Transport;

internal sealed class FramedWriteStream(
    Stream destination, long maximumBytes, Func<CancellationToken, Task>? beforeFirstWrite = null) : Stream
{
    private readonly IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
    private readonly byte[] header = new byte[4];
    private long writtenBytes;
    private WriteState state;

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => state is WriteState.Idle or WriteState.Started;
    public override long Length => writtenBytes;
    public override long Position { get => writtenBytes; set => throw new NotSupportedException(); }

    internal async Task CompleteAsync(CancellationToken cancellationToken)
    {
        EnsureActive();
        try
        {
            await StartAsync(cancellationToken).ConfigureAwait(false);
            Array.Clear(header);
            await destination.WriteAsync(header, cancellationToken).ConfigureAwait(false);
            await destination.WriteAsync(hash.GetHashAndReset(), cancellationToken).ConfigureAwait(false);
            await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
            state = WriteState.Completed;
        }
        catch
        {
            state = WriteState.Failed;
            throw;
        }
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        EnsureActive();
        try
        {
            if (buffer.Length > maximumBytes - writtenBytes)
                throw new InvalidDataException("The application transport output exceeds its configured bound.");
            await StartAsync(cancellationToken).ConfigureAwait(false);
            while (buffer.Length > 0)
            {
                var count = Math.Min(buffer.Length, RpcFrames.MaximumDataFrameBytes);
                BinaryPrimitives.WriteInt32BigEndian(header, count);
                await destination.WriteAsync(header, cancellationToken).ConfigureAwait(false);
                await destination.WriteAsync(buffer[..count], cancellationToken).ConfigureAwait(false);
                hash.AppendData(buffer.Span[..count]);
                writtenBytes += count;
                buffer = buffer[count..];
            }
        }
        catch
        {
            // A destination may have advanced before failing; never resume framing or publish a later success proof.
            state = WriteState.Failed;
            throw;
        }
    }

    private async Task StartAsync(CancellationToken cancellationToken)
    {
        if (state == WriteState.Started)
            return;
        if (beforeFirstWrite is not null)
            await beforeFirstWrite(cancellationToken).ConfigureAwait(false);
        state = WriteState.Started;
    }

    private void EnsureActive()
    {
        ThrowIfFailed();
        if (state == WriteState.Completed)
            throw new InvalidOperationException("The application transport output is already complete.");
    }

    private void ThrowIfFailed()
    {
        if (state == WriteState.Failed)
            throw new InvalidDataException("The application transport output is permanently failed.");
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        ThrowIfFailed();
        try
        {
            await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            state = WriteState.Failed;
            throw;
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            hash.Dispose();
        base.Dispose(disposing);
    }

    public override void Flush() => throw new NotSupportedException();
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    private enum WriteState
    {
        Idle,
        Started,
        Completed,
        Failed,
    }
}
