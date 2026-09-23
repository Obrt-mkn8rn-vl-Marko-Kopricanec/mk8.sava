using System.Buffers;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

internal sealed class BlobSeekableReadStream(
    BlobService service,
    BlobRecord blob,
    BlobEncryption encryption) : Stream
{
    private long _position;

    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;
    public override long Length => blob.Content.Length;
    public override long Position
    {
        get => _position;
        set => Seek(value, SeekOrigin.Begin);
    }

    public override int Read(byte[] buffer, int offset, int count) =>
        ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();

    public override int Read(Span<byte> buffer)
    {
        if (buffer.IsEmpty)
            return 0;
        var rented = ArrayPool<byte>.Shared.Rent(buffer.Length);
        try
        {
            var read = ReadAsync(rented.AsMemory(0, buffer.Length)).AsTask().GetAwaiter().GetResult();
            rented.AsSpan(0, read).CopyTo(buffer);
            return read;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented, clearArray: true);
        }
    }

    public override Task<int> ReadAsync(
        byte[] buffer,
        int offset,
        int count,
        CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override async ValueTask<int> ReadAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        if (buffer.IsEmpty || _position >= Length)
            return 0;
        var count = checked((int)Math.Min(buffer.Length, Length - _position));
        using var destination = new FixedMemoryWriteStream(buffer[..count]);
        await service.WriteContentAsync(
            blob,
            encryption,
            _position,
            count,
            destination,
            cancellationToken).ConfigureAwait(false);
        if (destination.Position != count)
            throw new EndOfStreamException("The blob content ended before the requested query range.");
        _position += count;
        return count;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        var next = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => checked(_position + offset),
            SeekOrigin.End => checked(Length + offset),
            _ => throw new ArgumentOutOfRangeException(nameof(origin))
        };
        if (next < 0)
            throw new IOException("Cannot seek before the beginning of the blob.");
        _position = next;
        return next;
    }

    public override void Flush()
    {
    }

    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    private sealed class FixedMemoryWriteStream(Memory<byte> destination) : Stream
    {
        private int _written;

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => _written;
        public override long Position
        {
            get => _written;
            set => throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count) =>
            Write(buffer.AsSpan(offset, count));

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            if (buffer.Length > destination.Length - _written)
                throw new IOException("The blob range exceeded the destination buffer.");
            buffer.CopyTo(destination.Span[_written..]);
            _written += buffer.Length;
        }

        public override Task WriteAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken)
        {
            Write(buffer.AsSpan(offset, count));
            return Task.CompletedTask;
        }

        public override ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            Write(buffer.Span);
            return ValueTask.CompletedTask;
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
    }
}
