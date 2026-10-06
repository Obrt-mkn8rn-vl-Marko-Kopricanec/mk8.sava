using System.Buffers.Binary;
using System.Security.Cryptography;

namespace Mk8.Sava.Transport;

internal sealed class FramedWriteStream(
    Stream destination, long maximumBytes, Func<CancellationToken, Task>? beforeFirstWrite = null) : Stream
{
    private readonly IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
    private readonly byte[] header = new byte[4];
    private long writtenBytes;
    private bool started;
    private bool completed;

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => !completed;
    public override long Length => writtenBytes;
    public override long Position { get => writtenBytes; set => throw new NotSupportedException(); }

    internal async Task CompleteAsync(CancellationToken cancellationToken)
    {
        if (completed)
            throw new InvalidOperationException("The application transport output is already complete.");
        await StartAsync(cancellationToken).ConfigureAwait(false);
        Array.Clear(header);
        await destination.WriteAsync(header, cancellationToken).ConfigureAwait(false);
        await destination.WriteAsync(hash.GetHashAndReset(), cancellationToken).ConfigureAwait(false);
        await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
        completed = true;
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (completed)
            throw new InvalidOperationException("The application transport output is already complete.");
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

    private async Task StartAsync(CancellationToken cancellationToken)
    {
        if (started)
            return;
        if (beforeFirstWrite is not null)
            await beforeFirstWrite(cancellationToken).ConfigureAwait(false);
        started = true;
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override Task FlushAsync(CancellationToken cancellationToken) => destination.FlushAsync(cancellationToken);

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
}
