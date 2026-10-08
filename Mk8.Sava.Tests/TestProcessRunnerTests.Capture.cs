using System.Text;

namespace Mk8.Sava.Tests;

public sealed partial class TestProcessRunnerTests
{
    [Theory]
    [InlineData(128)]
    [InlineData(4096)]
    public async Task DecodedFullByteBufferIsVisibleBeforePendingReadCancellation(int byteBufferSize)
    {
        var prefix = new string('ž', byteBufferSize / 2);
        using var input = new PendingSuffixStream(Encoding.UTF8.GetBytes(prefix));
        using var reader = new StreamReader(input, Encoding.UTF8, detectEncodingFromByteOrderMarks: true,
            bufferSize: byteBufferSize, leaveOpen: true);
        using var cancellation = new CancellationTokenSource();
        var capture = new ProcessOutputCapture();
#pragma warning disable CA2025 // Guaranteed cleanup cancels and joins this owned read before reader/source/token disposal.
        var work = capture.ReadAsync(reader, onLine: null, cancellation.Token);
#pragma warning restore CA2025
        await RunWithCleanupAsync(async () =>
        {
            await input.PendingRead.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            Assert.False(work.IsCompleted);
            Assert.False(capture.ReachedEof);
            Assert.Equal(prefix, capture.Text);
        }, async () =>
        {
            await cancellation.CancelAsync().WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => work.WaitAsync(TimeSpan.FromSeconds(5))).ConfigureAwait(false);
            Assert.Equal(cancellation.Token, failure.CancellationToken);
        }).ConfigureAwait(true);
        Assert.True(input.CanRead);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DecodedPrefixSurvivesOriginalReadFailure(bool catastrophic)
    {
        var prefix = new string('ž', 64);
#pragma warning disable CA2201 // Synthetic fatal identity control, not actual memory exhaustion.
        Exception failure = catastrophic
            ? new InvalidOperationException("controlled wrapped failure", new OutOfMemoryException())
            : new IOException("controlled read failure");
#pragma warning restore CA2201
        using var input = new PendingSuffixStream(Encoding.UTF8.GetBytes(prefix), failure);
        using var reader = new StreamReader(input, Encoding.UTF8, detectEncodingFromByteOrderMarks: true,
            bufferSize: 128, leaveOpen: true);
        var capture = new ProcessOutputCapture();
        var escaped = await Assert.ThrowsAnyAsync<Exception>(() => capture.ReadAsync(reader, onLine: null, CancellationToken.None))
            .ConfigureAwait(true);
        Assert.Same(failure, escaped);
        Assert.Equal(prefix, capture.Text);
        Assert.False(capture.ReachedEof);
        Assert.True(input.CanRead);
    }

    [Theory]
    [InlineData("utf8")]
    [InlineData("utf16")]
    [InlineData("utf32")]
    public async Task FragmentedBomUnicodeAndFinalLinePreserveReaderSemantics(string encodingName)
    {
        const string expected = "ž🧪\r\nsecond\nunterminated";
        var encoding = encodingName switch
        {
            "utf16" => Encoding.Unicode,
            "utf32" => Encoding.UTF32,
            _ => Encoding.UTF8
        };
        using var input = new FragmentedCaptureStream([.. encoding.GetPreamble(), .. encoding.GetBytes(expected)]);
        using var reader = new StreamReader(input, encoding, detectEncodingFromByteOrderMarks: true,
            bufferSize: 128, leaveOpen: true);
        var lines = new List<string>();
        var capture = new ProcessOutputCapture();
        await capture.ReadAsync(reader, lines.Add, CancellationToken.None).ConfigureAwait(true);
        Assert.Equal(expected, capture.Text);
        Assert.True(capture.ReachedEof);
        Assert.Equal(["ž🧪\r", "second", "unterminated"], lines, StringComparer.Ordinal);
        Assert.True(input.CanRead);
    }

    private sealed class PendingSuffixStream(byte[] prefix, Exception? failure = null) : MemoryStream(prefix, writable: false)
    {
        internal TaskCompletionSource PendingRead { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (Position < Length)
                return await base.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            if (failure is not null)
                throw failure;
            PendingRead.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            return 0;
        }
    }

    private sealed class FragmentedCaptureStream(byte[] content) : MemoryStream(content, writable: false)
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            base.ReadAsync(buffer[..Math.Min(1, buffer.Length)], cancellationToken);
    }
}
