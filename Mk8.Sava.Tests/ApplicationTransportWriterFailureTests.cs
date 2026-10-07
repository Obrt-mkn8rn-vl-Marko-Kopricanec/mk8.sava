using Mk8.Sava.Transport;

namespace Mk8.Sava.Tests;

public sealed class ApplicationTransportWriterFailureTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(4)]
    public async Task ExceedingTheBoundCannotBeSwallowedAndFollowedBySuccess(int length)
    {
        using var wire = new ObservedDestination();
        using var writer = new FramedWriteStream(wire, length);
        await writer.WriteAsync(new byte[length]).ConfigureAwait(true);

        await Assert.ThrowsAsync<InvalidDataException>(() => writer.WriteAsync(new byte[1]).AsTask()).ConfigureAwait(true);

        await AssertIrreversibleAsync(writer, wire).ConfigureAwait(true);
    }

    [Theory]
    [InlineData("callback")]
    [InlineData("header")]
    [InlineData("body")]
    [InlineData("later-header")]
    [InlineData("later-body")]
    [InlineData("terminal-header")]
    [InlineData("proof")]
    [InlineData("complete-flush")]
    [InlineData("explicit-flush")]
    public async Task AnyFailedOutputPhasePreventsFurtherDestinationAccess(string phase)
    {
        var injected = new IOException("controlled output failure");
        var failedWrite = phase switch
        {
            "header" => 1,
            "body" => 2,
            "later-header" => 3,
            "later-body" => 4,
            "terminal-header" => 3,
            "proof" => 4,
            _ => 0,
        };
        var failedFlush = phase is "complete-flush" or "explicit-flush";
        using var wire = new ObservedDestination(failedWrite, failedFlush, injected);
        var callbacks = 0;
        using var writer = new FramedWriteStream(wire, 65541, _ =>
        {
            callbacks++;
            return phase is "callback" ? Task.FromException(injected) : Task.CompletedTask;
        });
        var content = new byte[phase is "later-header" or "later-body" ? 65541 : 4];
        var failure = await Record.ExceptionAsync(async () =>
        {
            await writer.WriteAsync(content).ConfigureAwait(false);
            if (phase is "explicit-flush")
                await writer.FlushAsync(CancellationToken.None).ConfigureAwait(false);
            else
                await writer.CompleteAsync(CancellationToken.None).ConfigureAwait(false);
        }).ConfigureAwait(true);

        Assert.Same(injected, failure);
        var calls = callbacks;
        await AssertIrreversibleAsync(writer, wire).ConfigureAwait(true);
        Assert.Equal(calls, callbacks);
    }

    [Theory]
    [InlineData("io")]
    [InlineData("cancellation")]
    [InlineData("oom")]
    [InlineData("nested-fatal")]
    public async Task FailedBodyPreservesOriginalExceptionIncludingCancellationAndFatalGraphs(string kind)
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync().ConfigureAwait(true);
        Exception injected = kind switch
        {
            "io" => new IOException("controlled partial write"),
            "cancellation" => new OperationCanceledException(cancellation.Token),
#pragma warning disable CA2201 // Synthetic identity controls, not actual exhaustion/corruption.
            "oom" => new OutOfMemoryException("synthetic output OOM"),
            _ => new AggregateException(new IOException("ordinary sibling"), new AccessViolationException("synthetic output fatal")),
#pragma warning restore CA2201
        };
        using var wire = new ObservedDestination(2, false, injected);
        using var writer = new FramedWriteStream(wire, 4);

        var escaped = await Record.ExceptionAsync(() => writer.WriteAsync(new byte[4]).AsTask()).ConfigureAwait(true);

        Assert.Same(injected, escaped);
        if (escaped is OperationCanceledException canceled)
            Assert.Equal(cancellation.Token, canceled.CancellationToken);
        // The destination advanced by a controlled prefix before throwing; the writer cannot roll it back.
        Assert.Equal(5, wire.Length);
        Assert.Equal(0, writer.Length);
        await AssertIrreversibleAsync(writer, wire).ConfigureAwait(true);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(65536)]
    [InlineData(65541)]
    [InlineData(131073)]
    public async Task ValidCompletionRetainsExactBytesSingleCallbackAndBorrowedDestination(int length)
    {
        var content = Enumerable.Range(0, length).Select(index => (byte)(index % 251)).ToArray();
        using var wire = new ObservedDestination();
        var callbacks = 0;
        using (var writer = new FramedWriteStream(wire, length, _ =>
        {
            callbacks++;
            return Task.CompletedTask;
        }))
        {
            await writer.FlushAsync(CancellationToken.None).ConfigureAwait(true);
            await writer.WriteAsync(ReadOnlyMemory<byte>.Empty).ConfigureAwait(true);
            var midpoint = length / 2;
            await writer.WriteAsync(content.AsMemory(0, midpoint)).ConfigureAwait(true);
            await writer.WriteAsync(content.AsMemory(midpoint)).ConfigureAwait(true);
            await writer.CompleteAsync(CancellationToken.None).ConfigureAwait(true);
            var calls = wire.Writes;
            await Assert.ThrowsAsync<InvalidOperationException>(() => writer.CompleteAsync(CancellationToken.None)).ConfigureAwait(true);
            await Assert.ThrowsAsync<InvalidOperationException>(() => writer.WriteAsync(new byte[1]).AsTask()).ConfigureAwait(true);
            await writer.FlushAsync(CancellationToken.None).ConfigureAwait(true);
            Assert.False(writer.CanWrite);
            Assert.Equal(calls, wire.Writes);
            Assert.Equal(length, writer.Length);
            Assert.Equal(1, callbacks);
        }
        Assert.True(wire.CanWrite);
        wire.Position = 0;
        using var reader = new FramedReadStream(wire, length);
        using var received = new MemoryStream();
        await reader.CopyToAsync(received).ConfigureAwait(true);
        await reader.EnsureCompletedAsync(CancellationToken.None).ConfigureAwait(true);
        Assert.Equal(content, received.ToArray());
        Assert.Equal(wire.Length, wire.Position);
    }

    private static async Task AssertIrreversibleAsync(FramedWriteStream writer, ObservedDestination wire)
    {
        var position = wire.Position;
        var writes = wire.Writes;
        var flushes = wire.Flushes;
        await Assert.ThrowsAsync<InvalidDataException>(() => writer.CompleteAsync(CancellationToken.None)).ConfigureAwait(false);
        await Assert.ThrowsAsync<InvalidDataException>(() => writer.WriteAsync(ReadOnlyMemory<byte>.Empty).AsTask()).ConfigureAwait(false);
        await Assert.ThrowsAsync<InvalidDataException>(() => writer.WriteAsync(new byte[1]).AsTask()).ConfigureAwait(false);
        await Assert.ThrowsAsync<InvalidDataException>(() => writer.FlushAsync(CancellationToken.None)).ConfigureAwait(false);
        Assert.False(writer.CanWrite);
        Assert.Equal(position, wire.Position);
        Assert.Equal(writes, wire.Writes);
        Assert.Equal(flushes, wire.Flushes);
        Assert.True(wire.CanWrite);
    }

    private sealed class ObservedDestination(int failedWrite = 0, bool failedFlush = false, Exception? injected = null) : MemoryStream
    {
        internal int Writes { get; private set; }
        internal int Flushes { get; private set; }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Writes++;
            if (Writes == failedWrite && injected is not null)
            {
                Write(buffer.Span[..Math.Min(buffer.Length, 1)]);
                return ValueTask.FromException(injected);
            }
            return base.WriteAsync(buffer, cancellationToken);
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            Flushes++;
            return failedFlush && Flushes == 1 && injected is not null
                ? Task.FromException(injected) : base.FlushAsync(cancellationToken);
        }
    }
}
