using System.Security.Cryptography;

namespace Mk8.Sava.Protocol;

internal sealed class TransactionalChecksumReadStream(Stream inner) : Stream
{
    private readonly IncrementalHash _md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
    private readonly StorageCrc64 _crc64 = new();
    private TransactionalChecksums? _completed;
    private bool _disposed;

    public override bool CanRead => !_disposed;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public TransactionalChecksums Complete()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _completed ??= new TransactionalChecksums(_md5.GetHashAndReset(), _crc64.GetHash());
        return _completed;
    }

    public override int Read(byte[] buffer, int offset, int count) =>
        Read(buffer.AsSpan(offset, count));

    public override int Read(Span<byte> buffer)
    {
        EnsureReadable();
        var read = inner.Read(buffer);
        Append(buffer[..read]);
        return read;
    }

    public override async ValueTask<int> ReadAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        EnsureReadable();
        var read = await inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        Append(buffer.Span[..read]);
        return read;
    }

    public override Task<int> ReadAsync(
        byte[] buffer,
        int offset,
        int count,
        CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override void Flush() => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_disposed)
        {
            _disposed = true;
            _md5.Dispose();
        }
        base.Dispose(disposing);
    }

    private void EnsureReadable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completed is not null)
            throw new InvalidOperationException("The transactional checksum has already been completed.");
    }

    private void Append(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty)
            return;
        if (_completed is not null)
            throw new InvalidOperationException("The transactional checksum has already been completed.");
        _md5.AppendData(bytes);
        _crc64.Append(bytes);
    }
}
