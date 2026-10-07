using System.Buffers.Binary;
using System.Security.Cryptography;

namespace Mk8.Sava.Transport;

internal sealed class FramedReadStream(Stream source, long maximumBytes) : Stream
{
    private readonly IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
    private readonly byte[] header = new byte[4];
    private byte[] frame = [];
    private int frameOffset;
    private int frameLength;
    private long receivedBytes;
    private ReadState state;

    public override bool CanRead => state != ReadState.Disposed;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

    internal async Task EnsureCompletedAsync(CancellationToken cancellationToken)
    {
        var probe = new byte[1];
        if (await ReadAsync(probe, cancellationToken).ConfigureAwait(false) != 0)
            throw new InvalidDataException("The application operation did not consume its complete input.");
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(state == ReadState.Disposed, this);
        if (state == ReadState.Failed)
            throw new InvalidDataException("The application transport input previously failed and cannot be resumed.");
        if (buffer.Length == 0 || state == ReadState.Verified)
            return 0;
        try
        {
            if (frameOffset == frameLength)
            {
                await source.ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);
                var length = BinaryPrimitives.ReadInt32BigEndian(header);
                if (length == 0)
                {
                    await VerifyTerminalAsync(cancellationToken).ConfigureAwait(false);
                    return 0;
                }
                if (length < 0 || length > RpcFrames.MaximumDataFrameBytes || length > maximumBytes - receivedBytes)
                    throw new InvalidDataException("The application transport data frame exceeds its configured bound.");
                // Empty/control RPCs require proof, not a 64 KiB data reservation.
                // Validate an untrusted header before creating the bounded buffer.
                if (frame.Length == 0)
                    // A valid positive header proves the total is positive; keep
                    // capacity for later frames, not merely the first short frame.
                    frame = new byte[(int)Math.Min(maximumBytes, RpcFrames.MaximumDataFrameBytes)];
                await source.ReadExactlyAsync(frame.AsMemory(0, length), cancellationToken).ConfigureAwait(false);
                hash.AppendData(frame.AsSpan(0, length));
                receivedBytes += length;
                frameOffset = 0;
                frameLength = length;
            }
            var count = Math.Min(buffer.Length, frameLength - frameOffset);
            frame.AsMemory(frameOffset, count).CopyTo(buffer);
            frameOffset += count;
            return count;
        }
        catch
        {
            // Reads can consume bytes or reset the hash before failing. Never
            // reinterpret the remaining suffix as a different successful input.
            state = ReadState.Failed;
            throw;
        }
    }

    private async Task VerifyTerminalAsync(CancellationToken cancellationToken)
    {
        var expected = new byte[SHA256.HashSizeInBytes];
        await source.ReadExactlyAsync(expected, cancellationToken).ConfigureAwait(false);
        if (!CryptographicOperations.FixedTimeEquals(expected, hash.GetHashAndReset()))
            throw new InvalidDataException("The application transport input proof does not match its content.");
        var trailing = new byte[1];
        if (await source.ReadAsync(trailing, cancellationToken).ConfigureAwait(false) != 0)
            throw new InvalidDataException("The application transport has data after its successful terminal frame.");
        state = ReadState.Verified;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            state = ReadState.Disposed;
            hash.Dispose();
        }
        base.Dispose(disposing);
    }

    public override void Flush() => throw new NotSupportedException();
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    private enum ReadState { Active, Verified, Failed, Disposed }
}
