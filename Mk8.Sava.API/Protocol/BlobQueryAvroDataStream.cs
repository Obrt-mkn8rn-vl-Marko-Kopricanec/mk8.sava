namespace Mk8.Sava.Protocol;

internal sealed class BlobQueryAvroDataStream(
    BlobQueryAvroWriter writer,
    CancellationToken requestCancellationToken) : Stream
{
    private long _position;

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => _position;
    public override long Position
    {
        get => _position;
        set => throw new NotSupportedException();
    }

    public override void Flush()
    {
    }

    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public override void Write(byte[] buffer, int offset, int count) =>
        Write(buffer.AsSpan(offset, count));

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        // Stream's synchronous fallback is required by the Arrow writer; the async path is used for async writes.
#pragma warning disable VSTHRD002
        writer.AppendDataAsync(buffer.ToArray(), requestCancellationToken).GetAwaiter().GetResult();
#pragma warning restore VSTHRD002
        _position += buffer.Length;
    }

    public override async Task WriteAsync(
        byte[] buffer,
        int offset,
        int count,
        CancellationToken cancellationToken)
    {
        await WriteAsync(buffer.AsMemory(offset, count), cancellationToken).ConfigureAwait(false);
    }

    public override async ValueTask WriteAsync(
        ReadOnlyMemory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        if (!cancellationToken.CanBeCanceled || cancellationToken == requestCancellationToken)
        {
            await writer.AppendDataAsync(buffer, requestCancellationToken).ConfigureAwait(false);
            _position += buffer.Length;
            return;
        }

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            requestCancellationToken,
            cancellationToken);
        await writer.AppendDataAsync(buffer, linked.Token).ConfigureAwait(false);
        _position += buffer.Length;
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
}
