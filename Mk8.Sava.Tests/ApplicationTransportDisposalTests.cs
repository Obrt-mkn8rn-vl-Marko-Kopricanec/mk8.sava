using Mk8.Sava.Transport;

namespace Mk8.Sava.Tests;

public sealed class ApplicationTransportDisposalTests
{
    [Theory]
    [InlineData("idle", false)]
    [InlineData("idle", true)]
    [InlineData("buffered", false)]
    [InlineData("buffered", true)]
    [InlineData("verified", false)]
    [InlineData("verified", true)]
    [InlineData("failed", false)]
    [InlineData("failed", true)]
    public async Task DisposedReaderCannotReturnBufferedBytesOrAccessItsBorrowedSource(string phase, bool asynchronous)
    {
        using var wire = await CreateWireAsync(new byte[4]).ConfigureAwait(true);
        if (phase is "failed")
            wire.GetBuffer()[checked((int)wire.Length) - 1] ^= 1;
        using var reader = new FramedReadStream(wire, 4);
        if (phase is "buffered")
            Assert.Equal(1, await reader.ReadAsync(new byte[1]).ConfigureAwait(true));
        else if (phase is "verified")
            await reader.CopyToAsync(Stream.Null).ConfigureAwait(true);
        else if (phase is "failed")
            await Assert.ThrowsAsync<InvalidDataException>(() => reader.CopyToAsync(Stream.Null)).ConfigureAwait(true);
        await ReleaseAsync(reader, asynchronous).ConfigureAwait(true);
        var position = wire.Position;
        var reads = wire.Reads;

        await Assert.ThrowsAsync<ObjectDisposedException>(() => reader.ReadAsync(new byte[1]).AsTask()).ConfigureAwait(true);
        Assert.Equal(position, wire.Position);
        Assert.Equal(reads, wire.Reads);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => reader.ReadAsync(Memory<byte>.Empty).AsTask()).ConfigureAwait(true);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => reader.ReadAsync(new byte[1], 0, 1, CancellationToken.None)).ConfigureAwait(true);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => reader.EnsureCompletedAsync(CancellationToken.None)).ConfigureAwait(true);
        await ReleaseAsync(reader, asynchronous: false).ConfigureAwait(true);
        await reader.DisposeAsync().ConfigureAwait(true);
        Assert.False(reader.CanRead);
        Assert.Equal(position, wire.Position);
        Assert.Equal(reads, wire.Reads);
        Assert.True(wire.CanRead);
        Assert.True(wire.CanWrite);
    }

    [Theory]
    [InlineData("idle", false)]
    [InlineData("idle", true)]
    [InlineData("started", false)]
    [InlineData("started", true)]
    [InlineData("completed", false)]
    [InlineData("completed", true)]
    [InlineData("failed", false)]
    [InlineData("failed", true)]
    public async Task DisposedWriterCannotInvokeCallbackOrTouchItsBorrowedDestination(string phase, bool asynchronous)
    {
        using var wire = new ObservedWire();
        var callbacks = 0;
        using var writer = new FramedWriteStream(wire, 4, _ =>
        {
            callbacks++;
            return Task.CompletedTask;
        });
        if (phase is "started")
            await writer.WriteAsync(new byte[1]).ConfigureAwait(true);
        else if (phase is "completed")
        {
            await writer.WriteAsync(new byte[4]).ConfigureAwait(true);
            await writer.CompleteAsync(CancellationToken.None).ConfigureAwait(true);
        }
        else if (phase is "failed")
            await Assert.ThrowsAsync<InvalidDataException>(() => writer.WriteAsync(new byte[5]).AsTask()).ConfigureAwait(true);
        await ReleaseAsync(writer, asynchronous).ConfigureAwait(true);
        var position = wire.Position;
        var writes = wire.Writes;
        var flushes = wire.Flushes;
        var callbackCount = callbacks;

        await Assert.ThrowsAsync<ObjectDisposedException>(() => writer.WriteAsync(new byte[1]).AsTask()).ConfigureAwait(true);
        // A late disposed-hash error is insufficient if bytes or the response descriptor escaped first.
        Assert.Equal(position, wire.Position);
        Assert.Equal(writes, wire.Writes);
        Assert.Equal(callbackCount, callbacks);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => writer.WriteAsync(ReadOnlyMemory<byte>.Empty).AsTask()).ConfigureAwait(true);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => writer.WriteAsync(new byte[1], 0, 1, CancellationToken.None)).ConfigureAwait(true);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => writer.CompleteAsync(CancellationToken.None)).ConfigureAwait(true);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => writer.FlushAsync(CancellationToken.None)).ConfigureAwait(true);
        await ReleaseAsync(writer, asynchronous: false).ConfigureAwait(true);
        await writer.DisposeAsync().ConfigureAwait(true);
        Assert.False(writer.CanWrite);
        Assert.Equal(position, wire.Position);
        Assert.Equal(writes, wire.Writes);
        Assert.Equal(flushes, wire.Flushes);
        Assert.Equal(callbackCount, callbacks);
        Assert.True(wire.CanRead);
        Assert.True(wire.CanWrite);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(65541)]
    public async Task ActiveFramesStillRoundTripBeforeTheirOwnedWrappersAreDisposed(int length)
    {
        var bytes = Enumerable.Range(0, length).Select(index => (byte)(index % 251)).ToArray();
        using var wire = await CreateWireAsync(bytes).ConfigureAwait(true);
        var reader = new FramedReadStream(wire, length);
        await using (reader.ConfigureAwait(false))
        {
            using var received = new MemoryStream();
            await reader.CopyToAsync(received).ConfigureAwait(true);
            await reader.EnsureCompletedAsync(CancellationToken.None).ConfigureAwait(true);
            Assert.Equal(bytes, received.ToArray());
        }
        Assert.True(wire.CanRead);
        Assert.True(wire.CanWrite);
        Assert.Equal(wire.Length, wire.Position);
    }

    private static async Task ReleaseAsync(Stream stream, bool asynchronous)
    {
        if (asynchronous)
            await stream.DisposeAsync().ConfigureAwait(false);
        else
        {
#pragma warning disable CA1849, VSTHRD103 // The fixture deliberately exercises the synchronous Stream.Dispose contract as well as DisposeAsync.
            stream.Dispose();
#pragma warning restore CA1849, VSTHRD103
        }
    }

    private static async Task<ObservedWire> CreateWireAsync(byte[] bytes)
    {
        var wire = new ObservedWire();
        try
        {
            using var writer = new FramedWriteStream(wire, bytes.Length);
            await writer.WriteAsync(bytes).ConfigureAwait(false);
            await writer.CompleteAsync(CancellationToken.None).ConfigureAwait(false);
            wire.Position = 0;
            return wire;
        }
        catch
        {
            await wire.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private sealed class ObservedWire : MemoryStream
    {
        internal int Reads { get; private set; }
        internal int Writes { get; private set; }
        internal int Flushes { get; private set; }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Reads++;
            return base.ReadAsync(buffer, cancellationToken);
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Writes++;
            return base.WriteAsync(buffer, cancellationToken);
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            Flushes++;
            return base.FlushAsync(cancellationToken);
        }
    }
}
