using System.Buffers.Binary;

namespace Mk8.Sava.Protocol;

internal static class StructuredBodyEncoder
{
    private const int HeaderLength = 13;
    private const int SegmentHeaderLength = 10;
    private const int ChecksumLength = 8;
    private const long DefaultSegmentLength = 4L * 1024 * 1024;
    private const ushort IncludeCrc64 = 0x0001;

    public static long GetEncodedLength(long contentLength)
    {
        var (_, segmentCount) = GetSegmentation(contentLength);
        return checked(
            HeaderLength +
            contentLength +
            (long)segmentCount * (SegmentHeaderLength + ChecksumLength) +
            ChecksumLength);
    }

    public static async Task WriteAsync(
        long contentLength,
        Func<long, long, Stream, CancellationToken, Task> writeRange,
        Stream destination,
        CancellationToken cancellationToken)
    {
        var (segmentLength, segmentCount) = GetSegmentation(contentLength);
        var encodedLength = GetEncodedLength(contentLength);
        var header = new byte[HeaderLength];
        header[0] = 1;
        BinaryPrimitives.WriteUInt64LittleEndian(header.AsSpan(1), (ulong)encodedLength);
        BinaryPrimitives.WriteUInt16LittleEndian(header.AsSpan(9), IncludeCrc64);
        BinaryPrimitives.WriteUInt16LittleEndian(header.AsSpan(11), segmentCount);
        await destination.WriteAsync(header, cancellationToken).ConfigureAwait(false);

        var messageCrc64 = new StorageCrc64();
        var segmentHeader = new byte[SegmentHeaderLength];
        long offset = 0;
        for (var index = 1; index <= segmentCount; index++)
        {
            var currentLength = Math.Min(segmentLength, contentLength - offset);
            BinaryPrimitives.WriteUInt16LittleEndian(segmentHeader, (ushort)index);
            BinaryPrimitives.WriteUInt64LittleEndian(segmentHeader.AsSpan(sizeof(ushort)), (ulong)currentLength);
            await destination.WriteAsync(segmentHeader, cancellationToken).ConfigureAwait(false);

            var segmentCrc64 = new StorageCrc64();
            var hashingDestination = new Crc64WriteStream(
                destination,
                segmentCrc64,
                messageCrc64,
                currentLength);
            if (currentLength > 0)
                await writeRange(offset, currentLength, hashingDestination, cancellationToken).ConfigureAwait(false);
            if (hashingDestination.BytesWritten != currentLength)
                throw new InvalidDataException("The structured response producer wrote an unexpected number of bytes.");

            await destination.WriteAsync(segmentCrc64.GetHash(), cancellationToken).ConfigureAwait(false);
            offset = checked(offset + currentLength);
        }

        if (offset != contentLength)
            throw new InvalidDataException("The structured response producer did not write the complete content.");
        await destination.WriteAsync(messageCrc64.GetHash(), cancellationToken).ConfigureAwait(false);
    }

    private static (long SegmentLength, ushort SegmentCount) GetSegmentation(long contentLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(contentLength);
        if (contentLength == 0)
            return (DefaultSegmentLength, 1);

        var minimumSegmentLength = contentLength / ushort.MaxValue;
        if (contentLength % ushort.MaxValue != 0)
            minimumSegmentLength++;
        var segmentLength = Math.Max(DefaultSegmentLength, minimumSegmentLength);
        var segmentCount = contentLength / segmentLength;
        if (contentLength % segmentLength != 0)
            segmentCount++;
        if (segmentCount > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(contentLength));
        return (segmentLength, (ushort)segmentCount);
    }

    private sealed class Crc64WriteStream(
        Stream destination,
        StorageCrc64 segmentCrc64,
        StorageCrc64 messageCrc64,
        long expectedLength) : Stream
    {
        public long BytesWritten { get; private set; }
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => BytesWritten;

        public override long Position
        {
            get => BytesWritten;
            set => throw new NotSupportedException();
        }

        public override void Flush() => destination.Flush();

        public override Task FlushAsync(CancellationToken cancellationToken) =>
            destination.FlushAsync(cancellationToken);

        public override void Write(byte[] buffer, int offset, int count) =>
            Write(buffer.AsSpan(offset, count));

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            ValidateLength(buffer.Length);
            segmentCrc64.Append(buffer);
            messageCrc64.Append(buffer);
            destination.Write(buffer);
            BytesWritten += buffer.Length;
        }

        public override Task WriteAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken) =>
            WriteArrayAsync(buffer, offset, count, cancellationToken);

        public override async ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            ValidateLength(buffer.Length);
            segmentCrc64.Append(buffer.Span);
            messageCrc64.Append(buffer.Span);
            await destination.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
            BytesWritten += buffer.Length;
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();

        private async Task WriteArrayAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken) =>
            await WriteAsync(buffer.AsMemory(offset, count), cancellationToken).ConfigureAwait(false);

        private void ValidateLength(int count)
        {
            if (count < 0 || BytesWritten > expectedLength - count)
                throw new InvalidDataException("The structured response producer wrote too many bytes.");
        }
    }
}
