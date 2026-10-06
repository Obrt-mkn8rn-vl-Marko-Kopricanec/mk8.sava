using Mk8.Sava.Storage;

namespace Mk8.Sava.Transport;

internal static class PageCopyFrames
{
    internal static async Task WriteAsync(
        IPageCopySource source, PageCopyPlan plan, FramedWriteStream destination,
        CancellationToken cancellationToken)
    {
        foreach (var range in plan.EnumerateRanges())
        {
            var callbacks = 0;
            await source.ReadRangeAsync(range, async (input, token) =>
            {
                callbacks++;
                if (callbacks != 1)
                    throw new InvalidDataException("The application page-copy source provided a range more than once.");
                using var bounded = new ExactRangeReadStream(input, range.End - range.Start + 1);
                await bounded.CopyToAsync(destination, RpcFrames.MaximumDataFrameBytes, token).ConfigureAwait(false);
                bounded.RequireComplete();
                var extra = new byte[1];
                if (await input.ReadAsync(extra, token).ConfigureAwait(false) != 0)
                    throw new InvalidDataException("The application page-copy source exceeded the requested range.");
            }, cancellationToken).ConfigureAwait(false);
            if (callbacks != 1)
                throw new InvalidDataException("The application page-copy source did not provide the requested range.");
        }
    }

    internal sealed class ServerSource(PageCopyPlan plan, FramedReadStream input) : IPageCopySource, IDisposable
    {
        private readonly IEnumerator<PageRange> ranges = plan.EnumerateRanges().GetEnumerator();
        private bool initialized;
        private PageRange? next;

        public async Task<PageRangeDiff> ReadChangesAsync(
            string? previousSnapshot, long previousLength, CancellationToken cancellationToken)
        {
            if (initialized)
                throw new InvalidDataException("The application page-copy descriptor was read more than once.");
            initialized = true;
            next = ranges.MoveNext() ? ranges.Current : null;
            if (next is null)
                await input.EnsureCompletedAsync(cancellationToken).ConfigureAwait(false);
            return plan.Descriptor;
        }

        public async Task ReadRangeAsync(
            PageRange range, Func<Stream, CancellationToken, Task> consume, CancellationToken cancellationToken)
        {
            if (!initialized || next != range)
                throw new InvalidDataException("The application page-copy ranges were requested out of order.");
            using var bounded = new ExactRangeReadStream(input, range.End - range.Start + 1);
            await consume(bounded, cancellationToken).ConfigureAwait(false);
            bounded.RequireComplete();
            next = ranges.MoveNext() ? ranges.Current : null;
            // Publication may follow the last callback immediately. Prove that
            // the producer completed successfully before letting it return.
            if (next is null)
                await input.EnsureCompletedAsync(cancellationToken).ConfigureAwait(false);
        }

        public void Dispose() => ranges.Dispose();
    }

    private sealed class ExactRangeReadStream(Stream source, long length) : Stream
    {
        private readonly long rangeLength = length;
        private long remaining = length;
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => rangeLength;
        public override long Position { get => rangeLength - remaining; set => throw new NotSupportedException(); }

        internal void RequireComplete()
        {
            if (remaining != 0)
                throw new InvalidDataException("The application page-copy range was truncated or not consumed.");
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (buffer.Length == 0 || remaining == 0)
                return 0;
            var count = await source.ReadAsync(buffer[..(int)Math.Min(buffer.Length, remaining)], cancellationToken).ConfigureAwait(false);
            if (count == 0)
                throw new EndOfStreamException("The application page-copy range was truncated.");
            remaining -= count;
            return count;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override void Flush() => throw new NotSupportedException();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
