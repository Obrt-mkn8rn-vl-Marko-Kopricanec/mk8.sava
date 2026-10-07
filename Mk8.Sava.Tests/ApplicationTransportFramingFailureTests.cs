using System.Buffers.Binary;
using System.Security.Cryptography;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Tests;

public sealed class ApplicationTransportFramingFailureTests
{
    [Theory]
    [InlineData("negative")]
    [InlineData("oversized")]
    [InlineData("total")]
    [InlineData("aggregate")]
    [InlineData("proof")]
    [InlineData("trailing")]
    public async Task RejectedFramingCannotBecomeSuccessfulOnASecondCompletionAttempt(string failure)
    {
        using var wire = CreateRejectedWire(failure);
        using var observed = new ObservedReadStream(wire.ToArray());
        using var reader = new FramedReadStream(observed, failure is "total" or "aggregate" ? 5 : long.MaxValue);

        await Assert.ThrowsAsync<InvalidDataException>(() => reader.CopyToAsync(Stream.Null)).ConfigureAwait(true);
        var position = observed.Position;
        var reads = observed.Reads;

        await Assert.ThrowsAsync<InvalidDataException>(() => reader.EnsureCompletedAsync(CancellationToken.None)).ConfigureAwait(true);
        await Assert.ThrowsAsync<InvalidDataException>(() => reader.ReadAsync(Memory<byte>.Empty).AsTask()).ConfigureAwait(true);

        Assert.Equal(position, observed.Position);
        Assert.Equal(reads, observed.Reads);
        Assert.True(observed.CanRead);
    }

    [Theory]
    [InlineData("io")]
    [InlineData("cancellation")]
    [InlineData("oom")]
    [InlineData("nested-fatal")]
    public async Task FailedReadPreservesTheOriginalExceptionAndCannotBeResumed(string failure)
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync().ConfigureAwait(true);
        Exception injected = failure switch
        {
            "io" => new IOException("injected read failure"),
            "cancellation" => new OperationCanceledException(cancellation.Token),
#pragma warning disable CA2201 // Deliberately inject reserved exception objects to verify identity-preserving rethrow; no real exhaustion/corruption.
            "oom" => new OutOfMemoryException("injected allocation failure"),
            _ => new AggregateException(new IOException("ordinary sibling"), new AccessViolationException("injected fatal failure")),
#pragma warning restore CA2201
        };
        using var wire = new ObservedReadStream(EmptyTerminal(), injected);
        using var reader = new FramedReadStream(wire, 0);

        var escaped = await Assert.ThrowsAnyAsync<Exception>(() => reader.EnsureCompletedAsync(CancellationToken.None)).ConfigureAwait(true);
        Assert.Same(injected, escaped);
        if (escaped is OperationCanceledException canceled)
            Assert.Equal(cancellation.Token, canceled.CancellationToken);
        var reads = wire.Reads;
        await Assert.ThrowsAsync<InvalidDataException>(() => reader.EnsureCompletedAsync(CancellationToken.None)).ConfigureAwait(true);

        Assert.Equal(reads, wire.Reads);
        Assert.Equal(0, wire.Position);
        Assert.True(wire.CanRead);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(7)]
    [InlineData(14)]
    public async Task TruncationCannotBeRepairedByAppendingBytesAndRetryingTheSameReader(int length)
    {
        using var wire = new MemoryStream();
        var header = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(header, 4);
        wire.Write(header);
        wire.Write(new byte[4]);
        wire.Write(EmptyTerminal(new byte[4]));
        var complete = wire.ToArray();
        wire.SetLength(length);
        wire.Position = 0;
        using var reader = new FramedReadStream(wire, 4);

        await Assert.ThrowsAsync<EndOfStreamException>(() => reader.CopyToAsync(Stream.Null)).ConfigureAwait(true);
        var consumed = wire.Position;
        wire.Position = wire.Length;
        wire.Write(complete.AsSpan(length));
        wire.Position = consumed;
        await Assert.ThrowsAsync<InvalidDataException>(() => reader.EnsureCompletedAsync(CancellationToken.None)).ConfigureAwait(true);

        Assert.Equal(consumed, wire.Position);
        Assert.True(wire.CanRead);
    }

    [Fact]
    public async Task VerifiedCompletionRemainsIdempotentWithoutReadingTheBorrowedSourceAgain()
    {
        using var wire = new ObservedReadStream(EmptyTerminal());
        using (var reader = new FramedReadStream(wire, 0))
        {
            Assert.Equal(0, await reader.ReadAsync(Memory<byte>.Empty).ConfigureAwait(true));
            Assert.Equal(0, wire.Reads);
            await reader.EnsureCompletedAsync(CancellationToken.None).ConfigureAwait(true);
            var reads = wire.Reads;
            await reader.EnsureCompletedAsync(CancellationToken.None).ConfigureAwait(true);
            Assert.Equal(0, await reader.ReadAsync(new byte[1]).ConfigureAwait(true));
            Assert.Equal(reads, wire.Reads);
        }
        Assert.True(wire.CanRead);
        Assert.Equal(wire.Length, wire.Position);
    }

    private static MemoryStream CreateRejectedWire(string failure)
    {
        var wire = new MemoryStream();
        var header = new byte[4];
        if (failure is "proof" or "trailing")
        {
            var terminal = EmptyTerminal();
            if (failure is "proof")
                terminal[^1] ^= 1;
            wire.Write(terminal);
            if (failure is "trailing")
                wire.WriteByte(255);
            wire.Write(EmptyTerminal());
        }
        else
        {
            var accepted = failure is "aggregate" ? new byte[4] : [];
            if (accepted.Length != 0)
            {
                BinaryPrimitives.WriteInt32BigEndian(header, accepted.Length);
                wire.Write(header);
                wire.Write(accepted);
            }
            BinaryPrimitives.WriteInt32BigEndian(header, failure switch
            {
                "negative" => -1,
                "oversized" => 65537,
                "total" => 6,
                _ => 4,
            });
            wire.Write(header);
            wire.Write(EmptyTerminal(accepted));
        }
        wire.Position = 0;
        return wire;
    }

    private static byte[] EmptyTerminal(byte[]? bytes = null)
    {
        var terminal = new byte[4 + SHA256.HashSizeInBytes];
        SHA256.HashData(bytes ?? []).CopyTo(terminal, 4);
        return terminal;
    }

    private sealed class ObservedReadStream(byte[] bytes, Exception? firstFailure = null) : MemoryStream(bytes, writable: false)
    {
        internal int Reads { get; private set; }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Reads++;
            if (Reads == 1 && firstFailure is not null)
                return ValueTask.FromException<int>(firstFailure);
            return base.ReadAsync(buffer, cancellationToken);
        }
    }
}
