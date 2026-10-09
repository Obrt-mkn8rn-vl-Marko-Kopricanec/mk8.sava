using Mk8.Sava.Protocol;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class TransactionalChecksumReadStreamTests(ITestOutputHelper output)
{
    private static readonly byte[] Content = "0123456789abcdefghijklmnopqrstu"u8.ToArray();

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void SynchronousInactiveReadsRefuseBorrowedSourceAccess(bool disposed, bool atEof)
    {
        using var source = new ObservedSource(Content);
        using var stream = new TransactionalChecksumReadStream(source);
        var prefix = new byte[atEof ? Content.Length : 3];
        Assert.Equal(prefix.Length, stream.Read(prefix.AsSpan()));
        var checksums = stream.Complete();
        AssertChecksums(checksums, prefix);
        Assert.Same(checksums, stream.Complete());
        if (disposed)
            stream.Dispose();
        var calls = source.ReadCalls;
        var position = source.Position;
        var buffer = new byte[] { 0xCC, 0xCC, 0xCC };
        var failure = Record.Exception(() => stream.Read(buffer.AsSpan()));
        output.WriteLine($"Sync inactive checksum stream: Disposed={disposed}, Eof={atEof}, " +
            $"Failure={failure?.GetType().Name ?? "none"}, ReadCalls={calls}->{source.ReadCalls}, " +
            $"SourcePosition={position}->{source.Position}, Buffer={Convert.ToHexString(buffer)}.");
        AssertInactiveFailure(failure, disposed);
        Assert.Equal(calls, source.ReadCalls);
        Assert.Equal(position, source.Position);
        Assert.Equal(new byte[] { 0xCC, 0xCC, 0xCC }, buffer);
        AssertInactiveFailure(Record.Exception(() => stream.Read(buffer, 0, buffer.Length)), disposed);
        AssertInactiveFailure(Record.Exception(() => stream.Read(Span<byte>.Empty)), disposed);
        AssertInactiveFailure(Record.Exception(() => stream.Read(buffer, 0, 0)), disposed);
        AssertInactiveFailure(Record.Exception(() => stream.ReadByte()), disposed);
        using var destination = new MemoryStream();
        AssertInactiveFailure(Record.Exception(() => stream.CopyTo(destination)), disposed);
        Assert.Equal(0, destination.Length);
        Assert.Equal(calls, source.ReadCalls);
        Assert.True(source.CanRead);
        Assert.Equal(!disposed, stream.CanRead);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task AsynchronousInactiveReadsRefuseBorrowedSourceAccess(bool disposed, bool atEof)
    {
        using var source = new ObservedSource(Content);
        using var stream = new TransactionalChecksumReadStream(source);
        var prefix = new byte[atEof ? Content.Length : 3];
        Assert.Equal(prefix.Length, await stream.ReadAsync(prefix.AsMemory()).ConfigureAwait(true));
        var checksums = stream.Complete();
        AssertChecksums(checksums, prefix);
        Assert.Same(checksums, stream.Complete());
        if (disposed)
            await stream.DisposeAsync().ConfigureAwait(true);
        var calls = source.ReadCalls;
        var position = source.Position;
        var buffer = new byte[] { 0xCD, 0xCD, 0xCD };
        using var cancellation = new CancellationTokenSource();
        var failure = await Record.ExceptionAsync(() => stream.ReadAsync(buffer.AsMemory(), cancellation.Token).AsTask()).ConfigureAwait(true);
        output.WriteLine($"Async inactive checksum stream: Disposed={disposed}, Eof={atEof}, " +
            $"Failure={failure?.GetType().Name ?? "none"}, ReadCalls={calls}->{source.ReadCalls}, " +
            $"SourcePosition={position}->{source.Position}, Buffer={Convert.ToHexString(buffer)}.");
        AssertInactiveFailure(failure, disposed);
        Assert.Equal(calls, source.ReadCalls);
        Assert.Equal(position, source.Position);
        Assert.Equal(new byte[] { 0xCD, 0xCD, 0xCD }, buffer);
        AssertInactiveFailure(await Record.ExceptionAsync(() => stream.ReadAsync(Memory<byte>.Empty, cancellation.Token).AsTask()).ConfigureAwait(true), disposed);
        AssertInactiveFailure(await Record.ExceptionAsync(() => stream.ReadAsync(buffer, 0, buffer.Length, cancellation.Token)).ConfigureAwait(true), disposed);
        AssertInactiveFailure(await Record.ExceptionAsync(() => stream.ReadAsync(buffer, 0, 0, cancellation.Token)).ConfigureAwait(true), disposed);
        using var destination = new MemoryStream();
        AssertInactiveFailure(await Record.ExceptionAsync(() => stream.CopyToAsync(destination, cancellation.Token)).ConfigureAwait(true), disposed);
        Assert.Equal(0, destination.Length);
        Assert.Equal(calls, source.ReadCalls);
        Assert.True(source.CanRead);
        Assert.Equal(!disposed, stream.CanRead);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task RetirementBeforeCompletionRefusesSourceAccessAndLeavesItBorrowed(bool asynchronous, bool atEof)
    {
        using var source = new ObservedSource(Content);
        using var stream = new TransactionalChecksumReadStream(source);
        if (atEof)
            Assert.Equal(Content.Length, await stream.ReadAsync(new byte[Content.Length].AsMemory()).ConfigureAwait(true));
        await RetireAsync(stream, asynchronous).ConfigureAwait(true);
        var calls = source.ReadCalls;
        var position = source.Position;
        var buffer = new byte[] { 0xCE, 0xCE, 0xCE };
        var failure = await Record.ExceptionAsync(() => ReadAsync(stream, buffer, asynchronous, CancellationToken.None)).ConfigureAwait(true);
        output.WriteLine($"Retired unfinished checksum stream: Async={asynchronous}, Eof={atEof}, " +
            $"Failure={failure?.GetType().Name ?? "none"}, ReadCalls={calls}->{source.ReadCalls}, " +
            $"SourcePosition={position}->{source.Position}, Buffer={Convert.ToHexString(buffer)}.");
        Assert.IsType<ObjectDisposedException>(failure);
        Assert.Equal(calls, source.ReadCalls);
        Assert.Equal(position, source.Position);
        Assert.Equal(new byte[] { 0xCE, 0xCE, 0xCE }, buffer);
        Assert.False(stream.CanRead);
        Assert.Throws<ObjectDisposedException>(() => stream.Complete());
        Assert.True(source.CanRead);
        Assert.Equal(atEof ? -1 : Content[0], source.ReadByte());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EmptyCompletionIsIdempotentButRetirementRefusesCachedCompletion(bool asynchronous)
    {
        using var source = new ObservedSource(Content);
        using var stream = new TransactionalChecksumReadStream(source);
        var checksums = stream.Complete();
        AssertChecksums(checksums, []);
        Assert.Same(checksums, stream.Complete());
        await RetireAsync(stream, asynchronous).ConfigureAwait(true);
        var failure = Record.Exception(() => stream.Complete());
        output.WriteLine($"Retired cached completion: Async={asynchronous}, Failure={failure?.GetType().Name ?? "none"}, ReadCalls={source.ReadCalls}.");
        Assert.IsType<ObjectDisposedException>(failure);
        Assert.False(stream.CanRead);
        Assert.False(stream.CanSeek);
        Assert.False(stream.CanWrite);
        await RetireAsync(stream, !asynchronous).ConfigureAwait(true);
        Assert.True(source.CanRead);
        Assert.Equal(0, source.ReadCalls);
        Assert.Equal(0, source.Position);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ActiveFailuresPreserveOriginalIdentityAndTokenBeforeSuccessfulRetry(bool asynchronous)
    {
        using var source = new ObservedSource(Content);
        using var stream = new TransactionalChecksumReadStream(source);
        var original = new IOException("controlled checksum source failure");
        source.Failure = original;
        var buffer = new byte[Content.Length];
        var failure = await Record.ExceptionAsync(() => ReadAsync(stream, buffer, asynchronous, CancellationToken.None)).ConfigureAwait(true);
        Assert.Same(original, failure);
        Assert.Equal(0, source.Position);
        source.Failure = null;
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync().ConfigureAwait(true);
        var canceled = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => stream.ReadAsync(buffer.AsMemory(), cancellation.Token).AsTask()).ConfigureAwait(true);
        Assert.Equal(cancellation.Token, canceled.CancellationToken);
        Assert.Equal(cancellation.Token, source.LastToken);
        Assert.Equal(0, source.Position);
        Assert.Equal(Content.Length, await ReadAsync(stream, buffer, asynchronous, CancellationToken.None).ConfigureAwait(true));
        Assert.Equal(Content, buffer);
        Assert.Equal(0, await ReadAsync(stream, buffer, asynchronous, CancellationToken.None).ConfigureAwait(true));
        AssertChecksums(stream.Complete(), Content);
        Assert.True(source.CanRead);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ActiveFragmentedReadsAndEmptyOperationsProduceExactChecksums(bool asynchronous)
    {
        using var source = new ObservedSource(Content) { MaximumRead = 2 };
        using var stream = new TransactionalChecksumReadStream(source);
        using var cancellation = new CancellationTokenSource();
        Assert.Equal(0, await ReadAsync(stream, [], asynchronous, cancellation.Token).ConfigureAwait(true));
        var bytes = new List<byte>();
        var buffer = new byte[7];
        for (var attempt = 0; attempt < Content.Length; attempt++)
        {
            var read = await ReadAsync(stream, buffer, asynchronous, cancellation.Token).ConfigureAwait(true);
            bytes.AddRange(buffer.AsSpan(0, read).ToArray());
            if (read == 0)
                break;
        }
        Assert.Equal(Content, bytes);
        Assert.Equal(Content.Length, source.Position);
        Assert.Equal(0, await ReadAsync(stream, buffer, asynchronous, cancellation.Token).ConfigureAwait(true));
        Assert.Equal(0, await ReadAsync(stream, [], asynchronous, cancellation.Token).ConfigureAwait(true));
        var checksums = stream.Complete();
        AssertChecksums(checksums, Content);
        Assert.Same(checksums, stream.Complete());
        Assert.True(source.CanRead);
    }

    private static void AssertInactiveFailure(Exception? failure, bool disposed)
    {
        if (disposed)
            Assert.IsType<ObjectDisposedException>(failure);
        else
            Assert.IsType<InvalidOperationException>(failure);
    }

    private static void AssertChecksums(TransactionalChecksums checksums, byte[] bytes)
    {
        Assert.Equal(AzureProtocolChecksum.Md5(bytes), checksums.Md5);
        var crc = new StorageCrc64();
        crc.Append(bytes);
        Assert.Equal(crc.GetHash(), checksums.Crc64);
    }

    private static Task<int> ReadAsync(TransactionalChecksumReadStream stream, byte[] buffer, bool asynchronous, CancellationToken token) =>
        asynchronous ? stream.ReadAsync(buffer.AsMemory(), token).AsTask() : Task.FromResult(stream.Read(buffer.AsSpan()));

    private static async Task RetireAsync(TransactionalChecksumReadStream stream, bool asynchronous)
    {
        if (asynchronous)
            await stream.DisposeAsync().ConfigureAwait(false);
        else
        {
#pragma warning disable CA1849, VSTHRD103 // Exercise the supported synchronous Dispose path; the async path is an independent theory row.
            stream.Dispose();
#pragma warning restore CA1849, VSTHRD103
        }
    }

    private sealed class ObservedSource(byte[] content) : Stream
    {
        private readonly MemoryStream _source = new(content, writable: false);
        public int ReadCalls { get; private set; }
        public CancellationToken LastToken { get; private set; }
        public Exception? Failure { get; set; }
        public int MaximumRead { get; init; } = int.MaxValue;
        public override bool CanRead => _source.CanRead;
        public override bool CanSeek => _source.CanSeek;
        public override bool CanWrite => false;
        public override long Length => _source.Length;
        public override long Position { get => _source.Position; set => _source.Position = value; }

        public override int Read(Span<byte> buffer)
        {
            ReadCalls++;
            if (Failure is not null)
                throw Failure;
            return _source.Read(buffer[..Math.Min(buffer.Length, MaximumRead)]);
        }

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            LastToken = cancellationToken;
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(Read(buffer.Span));
        }

        public override void Flush() => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => _source.Seek(offset, origin);
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _source.Dispose();
            base.Dispose(disposing);
        }
    }
}
