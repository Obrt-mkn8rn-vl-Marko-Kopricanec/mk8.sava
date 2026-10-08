using System.Diagnostics;
using Mk8.Sava.Storage;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class ContentDefinedChunkerEmptyTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(0)]
    [InlineData(1048576)]
    [InlineData(long.MaxValue)]
    public async Task EmptyInputHasABoundedMeasuredAllocationCost(long maximumLength)
    {
        using var source = new MemoryStream();
        var chunker = new ContentDefinedChunker(65536, 262144, 1048576);
        for (var warmup = 0; warmup < 3; warmup++)
            Assert.Equal(0, await ConsumeAsync(chunker, source, maximumLength).ConfigureAwait(true));
        const int operations = 8;
        var thread = Environment.CurrentManagedThreadId;
        var minimum = long.MaxValue;
        for (var batch = 0; batch < 3; batch++)
        {
            var watch = Stopwatch.StartNew();
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var iteration = 0; iteration < operations; iteration++)
                Assert.Equal(0, await ConsumeAsync(chunker, source, maximumLength).ConfigureAwait(true));
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            watch.Stop();
            Assert.Equal(thread, Environment.CurrentManagedThreadId);
            minimum = Math.Min(minimum, allocated);
            output.WriteLine("chunker_empty_allocation,bound={0},batch={1},operations={2},managed_bytes={3},elapsed_ms={4:F3}",
                maximumLength, batch, operations, allocated, watch.Elapsed.TotalMilliseconds);
        }
        Assert.True(source.CanRead);
        Assert.InRange(minimum, 0, operations * 128L * 1024);
    }

    [Fact]
    public async Task FirstReadFailureHasTheSameBoundAndKeepsItsIdentityWithoutClosingSource()
    {
        var original = new IOException("controlled first chunker read failure");
        using var source = new FirstReadFailure(original);
        var chunker = new ContentDefinedChunker(65536, 262144, 1048576);
        for (var warmup = 0; warmup < 3; warmup++)
            Assert.Same(original, await Record.ExceptionAsync(() => ConsumeAsync(chunker, source, long.MaxValue)).ConfigureAwait(true));
        var thread = Environment.CurrentManagedThreadId;
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var iteration = 0; iteration < 8; iteration++)
            Assert.Same(original, await Record.ExceptionAsync(() => ConsumeAsync(chunker, source, long.MaxValue)).ConfigureAwait(true));
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(thread, Environment.CurrentManagedThreadId);
        Assert.True(source.CanRead);
        output.WriteLine("chunker_first_failure_allocation,operations=8,managed_bytes={0}", allocated);
        Assert.InRange(allocated, 0, 8L * 128 * 1024);
    }

    [Fact]
    public async Task InvalidCapacityStillFailsBeforeReadingBorrowedInput()
    {
        var readFailure = new IOException("invalid capacity must not read the source");
        using var source = new FirstReadFailure(readFailure);
        var chunker = new ContentDefinedChunker(4096, 8192, -1);

        var failure = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => ConsumeAsync(chunker, source, long.MaxValue))
            .ConfigureAwait(true);

        Assert.Equal("capacity", failure.ParamName);
        Assert.Equal(-1, failure.ActualValue);
        Assert.Equal(0, source.Reads);
        Assert.True(source.CanRead);
    }

    [Fact]
    public async Task PositiveInputKeepsExactBytesAndTheAllocationControl()
    {
        var bytes = new byte[1048607];
        DeterministicTestBytes.Fill(0x6c30, bytes);
        using var source = new MemoryStream(bytes, writable: false);
        var chunker = new ContentDefinedChunker(65536, 262144, 1048576);
        for (var warmup = 0; warmup < 3; warmup++)
            Assert.Equal(bytes.Length, await ConsumeAsync(chunker, source, bytes.Length, bytes).ConfigureAwait(true));
        var thread = Environment.CurrentManagedThreadId;
        var before = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(bytes.Length, await ConsumeAsync(chunker, source, bytes.Length, bytes).ConfigureAwait(true));
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(thread, Environment.CurrentManagedThreadId);
        Assert.True(source.CanRead);
        output.WriteLine("chunker_positive_allocation,input_bytes={0},managed_bytes={1}", bytes.Length, allocated);
        Assert.InRange(allocated, 0, 3L * 1024 * 1024);
    }

    private static async Task<int> ConsumeAsync(ContentDefinedChunker chunker, MemoryStream source, long maximumLength,
        ReadOnlyMemory<byte> expected = default)
    {
        source.Position = 0;
        var length = 0;
        await foreach (var bytes in chunker.ReadChunksAsync(source, maximumLength, CancellationToken.None).ConfigureAwait(false))
        {
            Assert.InRange(bytes.Length, 1, 1048576);
            if (!expected.IsEmpty)
                Assert.True(expected.Span.Slice(length, bytes.Length).SequenceEqual(bytes));
            length = checked(length + bytes.Length);
        }
        return length;
    }

    private sealed class FirstReadFailure(Exception failure) : MemoryStream
    {
        internal int Reads { get; private set; }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Reads++;
            return ValueTask.FromException<int>(failure);
        }
    }
}
