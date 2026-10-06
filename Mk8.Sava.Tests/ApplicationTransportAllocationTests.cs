using System.Diagnostics;
using System.Security.Cryptography;
using Mk8.Sava.Transport;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed partial class ApplicationTransportAllocationTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(0L)]
    [InlineData(long.MaxValue)]
    public async Task EmptyVerifiedInputsDoNotReserveADataFrame(long maximumBytes)
    {
        using var wire = new MemoryStream();
        using (var writer = new FramedWriteStream(wire, 0))
            await writer.CompleteAsync(CancellationToken.None);
        for (var iteration = 0; iteration < 10; iteration++)
            await ReadEmptyInputAsync(wire, maximumBytes);
        var thread = Environment.CurrentManagedThreadId;
        var watch = Stopwatch.StartNew();
        var before = GC.GetAllocatedBytesForCurrentThread();

        for (var iteration = 0; iteration < 100; iteration++)
            await ReadEmptyInputAsync(wire, maximumBytes);

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        watch.Stop();
        // MemoryStream completes every read synchronously; a thread switch would
        // invalidate this scoped allocation measurement rather than be a pass.
        Assert.Equal(thread, Environment.CurrentManagedThreadId);
        output.WriteLine("100 verified empty inputs / maximum {0}: {1} managed bytes; {2:F3} ms",
            maximumBytes, allocated, watch.Elapsed.TotalMilliseconds);
        Assert.InRange(allocated, 1L, 256L * 1024);
    }

    [Fact]
    public async Task DeferredBufferHandlesShortThenMaximumFramesAndFragmentedReads()
    {
        var bytes = RandomNumberGenerator.GetBytes(65584);
        using var wire = new MemoryStream();
        using (var writer = new FramedWriteStream(wire, bytes.Length))
        {
            await writer.WriteAsync(bytes.AsMemory(0, 31));
            await writer.WriteAsync(bytes.AsMemory(31, 65536));
            await writer.WriteAsync(bytes.AsMemory(65567));
            await writer.CompleteAsync(CancellationToken.None);
        }
        using var fragmented = new ShortReadStream(wire.ToArray());
        using var reader = new FramedReadStream(fragmented, bytes.Length);
        using var received = new MemoryStream();

        await reader.CopyToAsync(received);
        await reader.EnsureCompletedAsync(CancellationToken.None);

        Assert.Equal(bytes, received.ToArray());
        Assert.Equal(fragmented.Length, fragmented.Position);
    }

    [Fact]
    public async Task ZeroLengthReadCannotConsumeOrAcceptACorruptEmptyProof()
    {
        using var wire = new MemoryStream();
        using (var writer = new FramedWriteStream(wire, 0))
            await writer.CompleteAsync(CancellationToken.None);
        wire.GetBuffer()[checked((int)wire.Length) - 1] ^= 1;
        wire.Position = 0;
        using var reader = new FramedReadStream(wire, 0);

        Assert.Equal(0, await reader.ReadAsync(Memory<byte>.Empty));
        Assert.Equal(0, wire.Position);
        await Assert.ThrowsAsync<InvalidDataException>(() => reader.EnsureCompletedAsync(CancellationToken.None));
    }

    private static async Task ReadEmptyInputAsync(MemoryStream wire, long maximumBytes)
    {
        wire.Position = 0;
        using var reader = new FramedReadStream(wire, maximumBytes);
        await reader.EnsureCompletedAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(wire.Length, wire.Position);
    }

    private sealed class ShortReadStream(byte[] bytes) : MemoryStream(bytes)
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            base.ReadAsync(buffer[..Math.Min(buffer.Length, 7)], cancellationToken);
    }
}
