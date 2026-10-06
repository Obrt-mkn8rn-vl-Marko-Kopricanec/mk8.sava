using System.Buffers.Binary;
using System.Text.Json;

namespace Mk8.Sava.Transport;

internal static class RpcFrames
{
    internal const int MaximumDataFrameBytes = 64 * 1024;

    internal static async Task WriteControlAsync<T>(
        Stream stream, T value, int maximumBytes, CancellationToken cancellationToken)
    {
        using var buffer = new BoundedControlBuffer(maximumBytes);
        await JsonSerializer.SerializeAsync(buffer, value, RpcJson.Options, cancellationToken).ConfigureAwait(false);
        var header = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(header, checked((int)buffer.Length));
        await stream.WriteAsync(header, cancellationToken).ConfigureAwait(false);
        await stream.WriteAsync(buffer.GetBuffer().AsMemory(0, checked((int)buffer.Length)), cancellationToken).ConfigureAwait(false);
    }

    internal static async Task<T> ReadControlAsync<T>(
        Stream stream, int maximumBytes, CancellationToken cancellationToken)
    {
        var header = new byte[4];
        await stream.ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);
        var length = BinaryPrimitives.ReadInt32BigEndian(header);
        if (length <= 0 || length > maximumBytes)
            throw new InvalidDataException("The application transport control frame exceeds its configured bound.");
        var bytes = GC.AllocateUninitializedArray<byte>(length);
        await stream.ReadExactlyAsync(bytes, cancellationToken).ConfigureAwait(false);
        return JsonSerializer.Deserialize<T>(bytes, RpcJson.Options) ??
            throw new InvalidDataException("The application transport control frame is invalid.");
    }

    private sealed class BoundedControlBuffer(int maximumBytes) : MemoryStream
    {
        public override void Write(byte[] buffer, int offset, int count)
        {
            CheckBound(count);
            base.Write(buffer, offset, count);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            CheckBound(buffer.Length);
            base.Write(buffer);
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            CheckBound(buffer.Length);
            return base.WriteAsync(buffer, cancellationToken);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            CheckBound(count);
            return base.WriteAsync(buffer, offset, count, cancellationToken);
        }

        private void CheckBound(int count)
        {
            if (count > maximumBytes - Position)
                throw new InvalidDataException("The application transport control frame exceeds its configured bound.");
        }
    }
}
