using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Mk8.Sava.Storage;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class ContentDefinedChunkerTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(1, 16384)]
    [InlineData(7, 16384)]
    [InlineData(4093, 16384)]
    [InlineData(65536, 16384)]
    [InlineData(1, 10000)]
    [InlineData(7, 10000)]
    [InlineData(4093, 10000)]
    [InlineData(65536, 10000)]
    public async Task FragmentationPreservesAcceptedBoundariesAndEveryByte(int readSize, int maximum)
    {
        var bytes = new byte[196625];
        DeterministicTestBytes.Fill(0x6c10, bytes);
        using var source = new FragmentedSource(bytes, readSize);
        var chunks = await CollectAsync(new ContentDefinedChunker(4096, 8192, maximum), source, bytes.Length);
        var descriptor = string.Join('\n', chunks.Select(chunk =>
            string.Create(CultureInfo.InvariantCulture, $"{chunk.Length}:{Convert.ToHexStringLower(SHA256.HashData(chunk))}")));
        var signature = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(descriptor)));
        // Frozen from the accepted aae6f2d producer, not recomputed by a second chunking algorithm.
        var acceptedSignature = maximum == 16384
            ? "f502cc976116211e19cc4e0f981f43d754060c79fff641d8e53d695283c237ad"
            : "22e80ec485884aab6caa3c2a3f486c324b94e1b49740cf3d564eb0b2f0279f92";
        Assert.Equal(acceptedSignature, signature);
        output.WriteLine("chunker_golden,read_size={0},maximum={1},lengths={2},descriptor_sha256={3}",
            readSize, maximum, string.Join(',', chunks.Select(chunk => chunk.Length)), signature);
        Assert.Equal(bytes, chunks.SelectMany(chunk => chunk).ToArray());
        Assert.All(chunks.SkipLast(1), chunk => Assert.InRange(chunk.Length, 4096, maximum));
        Assert.True(source.CanRead);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(4095)]
    [InlineData(4096)]
    [InlineData(4097)]
    [InlineData(16384)]
    [InlineData(16385)]
    [InlineData(65537)]
    public async Task ZeroInputRetainsMinimumCutsAndShortTail(int length)
    {
        using var source = new FragmentedSource(new byte[length], 4093);
        var chunks = await CollectAsync(new ContentDefinedChunker(4096, 8192, 16384), source, length);
        // With a zero window, incoming/outgoing gear values cancel: each minimum is a boundary.
        var expected = Enumerable.Repeat(4096, length / 4096).ToList();
        if (length % 4096 != 0)
            expected.Add(length % 4096);
        Assert.Equal(expected, chunks.Select(chunk => chunk.Length));
        Assert.All(chunks, chunk => Assert.All(chunk, value => Assert.Equal((byte)0, value)));
        Assert.True(source.CanRead);
    }

    [Fact]
    public async Task YieldedArraysRemainIndependentWhileEnumeratorResumesAndIsRetiredEarly()
    {
        var bytes = new byte[65537];
        DeterministicTestBytes.Fill(0x6c20, bytes);
        using var source = new FragmentedSource(bytes, 65536);
        var iterator = new ContentDefinedChunker(4096, 8192, 16384)
            .ReadChunksAsync(source, bytes.Length, CancellationToken.None).GetAsyncEnumerator();
        await using (iterator.ConfigureAwait(false))
        {
            Assert.True(await iterator.MoveNextAsync());
            var first = iterator.Current;
            var firstCopy = first.ToArray();
            Assert.True(await iterator.MoveNextAsync());
            Assert.NotSame(first, iterator.Current);
            Assert.Equal(firstCopy, first);
            first.AsSpan().Fill(0xff);
            Assert.Equal(bytes.AsSpan(first.Length, iterator.Current.Length).ToArray(), iterator.Current);
        }
        Assert.True(source.CanRead);
    }

    [Theory]
    [InlineData(4096)]
    [InlineData(65536)]
    public async Task OverlongReadIsRejectedBeforeItsBytesAreYielded(int readSize)
    {
        var bytes = new byte[32769];
        using var source = new FragmentedSource(bytes, readSize);
        var yielded = 0;
        var failure = await Assert.ThrowsAsync<RequestBodyTooLargeException>(async () =>
        {
            await foreach (var chunk in new ContentDefinedChunker(4096, 8192, 16384)
                               .ReadChunksAsync(source, 32768, CancellationToken.None).ConfigureAwait(false))
                yielded += chunk.Length;
        });
        Assert.Equal(32768, failure.MaximumLength);
        Assert.Equal(readSize == 65536 ? 0 : 32768, yielded);
        Assert.True(source.CanRead);
    }

    [Fact]
    public async Task ReadFailureRetainsIdentityAndBorrowedSource()
    {
        var failure = new IOException("controlled chunk input failure");
        using var source = new FragmentedSource(new byte[65537], 65536) { Failure = failure };
        var observed = await Assert.ThrowsAsync<IOException>(() =>
            CollectAsync(new ContentDefinedChunker(4096, 8192, 16384), source, long.MaxValue));
        Assert.Same(failure, observed);
        Assert.True(source.CanRead);
    }

    [Fact]
    public async Task CancellationBetweenReadsPreservesTokenAndBorrowedSource()
    {
        using var cancellation = new CancellationTokenSource();
        using var source = new FragmentedSource(new byte[65537], 4096);
        var iterator = new ContentDefinedChunker(4096, 8192, 16384)
            .ReadChunksAsync(source, long.MaxValue, cancellation.Token).GetAsyncEnumerator();
        await using (iterator.ConfigureAwait(false))
        {
            Assert.True(await iterator.MoveNextAsync());
            await cancellation.CancelAsync();
            var observed = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                await iterator.MoveNextAsync().ConfigureAwait(false);
            });
            Assert.Equal(cancellation.Token, observed.CancellationToken);
        }
        Assert.True(source.CanRead);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WarmedChunkingReportsComparableManagedAllocationAndElapsedTime(bool zeros)
    {
        const int operations = 8;
        var bytes = new byte[1024 * 1024 + 31];
        if (!zeros)
            DeterministicTestBytes.Fill(0x6c30, bytes);
        using var source = new MemoryStream(bytes, writable: false);
        var chunker = new ContentDefinedChunker(65536, 262144, 1048576);
        var expected = await DescribeAsync(chunker, source);
        var acceptedSignature = zeros
            ? "62d300bbcd8305b262d9e6b36dc4c0a1cf5e2c3d05e1f8b3c658522757444868"
            : "45b281148693d7f5c94c0054f3415bfa18d4cedce66ce19d5802979b341e215f";
        Assert.Equal(acceptedSignature, Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(expected))));
        output.WriteLine("chunker_large_anchor,zeros={0},descriptor_sha256={1}",
            zeros, Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(expected))));
        for (var warmup = 0; warmup < 4; warmup++)
            Assert.Equal(expected, await DescribeAsync(chunker, source));
        var thread = Environment.CurrentManagedThreadId;
        for (var batch = 0; batch < 7; batch++)
        {
            var watch = Stopwatch.StartNew();
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var iteration = 0; iteration < operations; iteration++)
                Assert.Equal(expected, await DescribeAsync(chunker, source));
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            watch.Stop();
            Assert.Equal(thread, Environment.CurrentManagedThreadId);
            output.WriteLine("chunker_measurement,zeros={0},batch={1},operations={2},input_bytes={3},managed_bytes={4},elapsed_ms={5:F3}",
                zeros, batch, operations, bytes.Length, allocated, watch.Elapsed.TotalMilliseconds);
        }
    }

    private static async Task<string> DescribeAsync(ContentDefinedChunker chunker, MemoryStream source)
    {
        source.Position = 0;
        var descriptor = new StringBuilder();
        await foreach (var chunk in chunker.ReadChunksAsync(source, source.Length, CancellationToken.None).ConfigureAwait(false))
            descriptor.Append(chunk.Length.ToString(CultureInfo.InvariantCulture)).Append(':')
                .Append(Convert.ToHexStringLower(SHA256.HashData(chunk))).Append(';');
        return descriptor.ToString();
    }

    private static async Task<List<byte[]>> CollectAsync(ContentDefinedChunker chunker, Stream source, long maximumLength)
    {
        var chunks = new List<byte[]>();
        await foreach (var chunk in chunker.ReadChunksAsync(source, maximumLength, CancellationToken.None).ConfigureAwait(false))
            chunks.Add(chunk);
        return chunks;
    }

    private sealed class FragmentedSource(byte[] bytes, int readSize) : MemoryStream(bytes, writable: false)
    {
        public Exception? Failure { get; init; }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (Failure is not null && Position > 0)
                return ValueTask.FromException<int>(Failure);
            return base.ReadAsync(buffer[..Math.Min(readSize, buffer.Length)], cancellationToken);
        }
    }
}
