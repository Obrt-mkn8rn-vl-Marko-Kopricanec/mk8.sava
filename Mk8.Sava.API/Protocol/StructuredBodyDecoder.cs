using System.Buffers.Binary;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

internal static class StructuredBodyDecoder
{
    internal const string ContentType = "XSM/1.0; properties=crc64";

    private const int HeaderLength = 13;
    private const int SegmentHeaderLength = 10;
    private const int ChecksumLength = 8;
    private const ushort IncludeCrc64 = 0x0001;

    public static async Task DecodeAsync(
        Stream source,
        Stream destination,
        long encodedLength,
        long expectedContentLength,
        long maximumContentLength,
        CancellationToken cancellationToken)
    {
        if (expectedContentLength < 0 || encodedLength < 0)
            throw InvalidBody();
        if (expectedContentLength > maximumContentLength)
            throw new RequestBodyTooLargeException(maximumContentLength);

        var header = new byte[HeaderLength];
        await ReadExactlyAsync(source, header, cancellationToken).ConfigureAwait(false);
        if (header[0] != 1)
            throw InvalidBody();

        var messageLength = BinaryPrimitives.ReadUInt64LittleEndian(header.AsSpan(1, sizeof(ulong)));
        var flags = BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(9, sizeof(ushort)));
        var segmentCount = BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(11, sizeof(ushort)));
        if (messageLength != (ulong)encodedLength || flags != IncludeCrc64 || segmentCount == 0)
            throw InvalidBody();

        ulong calculatedLength;
        try
        {
            calculatedLength = checked(
                (ulong)HeaderLength +
                (ulong)expectedContentLength +
                (ulong)segmentCount * (SegmentHeaderLength + ChecksumLength) +
                ChecksumLength);
        }
        catch (OverflowException)
        {
            throw InvalidBody();
        }
        if (calculatedLength != messageLength)
            throw InvalidBody();

        var messageCrc64 = new StorageCrc64();
        var segmentHeader = new byte[SegmentHeaderLength];
        var expectedChecksum = new byte[ChecksumLength];
        var buffer = new byte[128 * 1024];
        long decodedLength = 0;

        for (var index = 1; index <= segmentCount; index++)
        {
            await ReadExactlyAsync(source, segmentHeader, cancellationToken).ConfigureAwait(false);
            if (BinaryPrimitives.ReadUInt16LittleEndian(segmentHeader) != index)
                throw InvalidBody();

            var unsignedSegmentLength = BinaryPrimitives.ReadUInt64LittleEndian(segmentHeader.AsSpan(sizeof(ushort)));
            if (unsignedSegmentLength > long.MaxValue)
                throw InvalidBody();
            var segmentLength = (long)unsignedSegmentLength;
            if (segmentLength > expectedContentLength - decodedLength)
                throw InvalidBody();

            var segmentCrc64 = new StorageCrc64();
            var remaining = segmentLength;
            while (remaining > 0)
            {
                var requested = (int)Math.Min(buffer.Length, remaining);
                var read = await source.ReadAsync(buffer.AsMemory(0, requested), cancellationToken).ConfigureAwait(false);
                if (read == 0)
                    throw InvalidBody();
                segmentCrc64.Append(buffer.AsSpan(0, read));
                messageCrc64.Append(buffer.AsSpan(0, read));
                await destination.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
                decodedLength = checked(decodedLength + read);
                remaining -= read;
            }

            await ReadExactlyAsync(source, expectedChecksum, cancellationToken).ConfigureAwait(false);
            if (!CryptographicOperations.FixedTimeEquals(expectedChecksum, segmentCrc64.GetHash()))
                throw Crc64Mismatch();
        }

        await ReadExactlyAsync(source, expectedChecksum, cancellationToken).ConfigureAwait(false);
        if (!CryptographicOperations.FixedTimeEquals(expectedChecksum, messageCrc64.GetHash()))
            throw Crc64Mismatch();
        if (decodedLength != expectedContentLength)
            throw InvalidBody();
    }

    private static async Task ReadExactlyAsync(
        Stream source,
        Memory<byte> destination,
        CancellationToken cancellationToken)
    {
        var offset = 0;
        while (offset < destination.Length)
        {
            var read = await source.ReadAsync(destination[offset..], cancellationToken).ConfigureAwait(false);
            if (read == 0)
                throw InvalidBody();
            offset += read;
        }
    }

    private static AzureStorageException InvalidBody() => new(
        StatusCodes.Status400BadRequest,
        "InvalidRequestBody",
        "The structured request body is invalid.");

    private static AzureStorageException Crc64Mismatch() => new(
        StatusCodes.Status400BadRequest,
        "Crc64Mismatch",
        "The CRC64 checksum in the structured request body did not match the calculated value.");
}
