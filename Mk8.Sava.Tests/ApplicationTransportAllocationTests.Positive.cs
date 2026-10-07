using System.Buffers.Binary;
using System.Diagnostics;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Tests;

public sealed partial class ApplicationTransportAllocationTests
{
    [Theory]
    [InlineData(31, 256)]
    [InlineData(4096, 768)]
    [InlineData(65536, 7168)]
    public async Task DeclaredPositiveInputsHaveBoundedManagedAllocation(int length, int budgetKibibytes)
    {
        var content = Enumerable.Range(0, length).Select(index => (byte)(index % 251)).ToArray();
        using var wire = new MemoryStream();
        using (var writer = new FramedWriteStream(wire, length))
        {
            await writer.WriteAsync(content);
            await writer.CompleteAsync(CancellationToken.None);
        }
        var destination = new byte[length];
        for (var iteration = 0; iteration < 10; iteration++)
            await ReadBoundedInputAsync(wire, destination);
        var thread = Environment.CurrentManagedThreadId;
        var watch = Stopwatch.StartNew();
        var before = GC.GetAllocatedBytesForCurrentThread();

        for (var iteration = 0; iteration < 100; iteration++)
            await ReadBoundedInputAsync(wire, destination);

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        watch.Stop();
        Assert.Equal(thread, Environment.CurrentManagedThreadId);
        Assert.Equal(content, destination);
        output.WriteLine("positive_frame_allocation,length={0},operations=100,managed_bytes={1},budget_bytes={2},elapsed_ms={3:F3}",
            length, allocated, budgetKibibytes * 1024L, watch.Elapsed.TotalMilliseconds);
        Assert.InRange(allocated, 1L, budgetKibibytes * 1024L);
    }

    [Theory]
    [InlineData(1, 1L)]
    [InlineData(31, 31L)]
    [InlineData(65537, 65537L)]
    [InlineData(31, long.MaxValue)]
    public async Task DeclaredBoundsPreserveSplitFramesFragmentationProofAndBorrowedSource(int length, long maximumBytes)
    {
        var content = Enumerable.Range(0, length).Select(index => (byte)(index % 251)).ToArray();
        using var wire = new MemoryStream();
        using (var writer = new FramedWriteStream(wire, length))
        {
            var first = Math.Min(13, length);
            await writer.WriteAsync(content.AsMemory(0, first));
            await writer.WriteAsync(content.AsMemory(first));
            await writer.CompleteAsync(CancellationToken.None);
        }
        using var fragmented = new ShortReadStream(wire.ToArray());
        var received = new byte[length];
        using (var reader = new FramedReadStream(fragmented, maximumBytes))
        {
            // Both the wire and caller split frames; no large caller buffer is required.
            var position = 0;
            while (position < length)
            {
                var count = await reader.ReadAsync(received.AsMemory(position, Math.Min(11, length - position)));
                Assert.True(count > 0);
                position += count;
            }
            Assert.Equal(content, received);
            await reader.EnsureCompletedAsync(CancellationToken.None);
            Assert.Equal(fragmented.Length, fragmented.Position);
        }
        Assert.True(fragmented.CanRead);
    }

    [Theory]
    [InlineData(31L, 32)]
    [InlineData(4096L, 4097)]
    [InlineData(long.MaxValue, 65537)]
    public async Task InvalidPositiveHeaderIsRejectedBeforeReadingItsMissingBody(long maximumBytes, int announcedLength)
    {
        var header = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(header, announcedLength);
        using var wire = new MemoryStream(header);
        using var reader = new FramedReadStream(wire, maximumBytes);

        await Assert.ThrowsAsync<InvalidDataException>(() => reader.EnsureCompletedAsync(CancellationToken.None));

        Assert.Equal(4, wire.Position);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SmallPositiveInputStillRequiresAnIntactTerminalProof(bool truncate)
    {
        var content = new byte[31];
        using var wire = new MemoryStream();
        using (var writer = new FramedWriteStream(wire, content.Length))
        {
            await writer.WriteAsync(content);
            await writer.CompleteAsync(CancellationToken.None);
        }
        if (truncate)
            wire.SetLength(wire.Length - 1);
        else
            wire.GetBuffer()[checked((int)wire.Length) - 1] ^= 1;
        wire.Position = 0;
        using var reader = new FramedReadStream(wire, content.Length);
        var received = new byte[content.Length];
        await reader.ReadExactlyAsync(received);
        Assert.Equal(content, received);

        if (truncate)
            await Assert.ThrowsAsync<EndOfStreamException>(() => reader.EnsureCompletedAsync(CancellationToken.None));
        else
            await Assert.ThrowsAsync<InvalidDataException>(() => reader.EnsureCompletedAsync(CancellationToken.None));
    }

    private static async Task ReadBoundedInputAsync(MemoryStream wire, byte[] destination)
    {
        wire.Position = 0;
        using var reader = new FramedReadStream(wire, destination.LongLength);
        await reader.ReadExactlyAsync(destination).ConfigureAwait(false);
        await reader.EnsureCompletedAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(wire.Length, wire.Position);
    }
}
