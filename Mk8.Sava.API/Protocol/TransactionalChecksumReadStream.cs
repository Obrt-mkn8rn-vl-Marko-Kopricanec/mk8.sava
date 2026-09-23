using System.Security.Cryptography;

namespace Mk8.Sava.Protocol;

internal sealed record TransactionalChecksums(byte[] Md5, byte[] Crc64)
{
    public static TransactionalChecksums Empty { get; } =
        new(MD5.HashData([]), new StorageCrc64().GetHash());

    public string Md5Base64 => Convert.ToBase64String(Md5);
    public string Crc64Base64 => Convert.ToBase64String(Crc64);
}

internal sealed class TransactionalChecksumReadStream(Stream inner) : Stream
{
    private readonly IncrementalHash _md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
    private readonly StorageCrc64 _crc64 = new();
    private TransactionalChecksums? _completed;

    public override bool CanRead => true;
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
        _completed ??= new TransactionalChecksums(_md5.GetHashAndReset(), _crc64.GetHash());
        return _completed;
    }

    public override int Read(byte[] buffer, int offset, int count) =>
        Read(buffer.AsSpan(offset, count));

    public override int Read(Span<byte> buffer)
    {
        var read = inner.Read(buffer);
        Append(buffer[..read]);
        return read;
    }

    public override async ValueTask<int> ReadAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
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
        if (disposing)
            _md5.Dispose();
        base.Dispose(disposing);
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
