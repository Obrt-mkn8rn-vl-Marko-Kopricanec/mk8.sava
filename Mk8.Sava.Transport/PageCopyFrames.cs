using Mk8.Sava.Storage;

namespace Mk8.Sava.Transport;

internal static class PageCopyFrames
{
    private const long MaximumPageBlobBytes = 8L * 1024 * 1024 * 1024 * 1024;
    private const long RangeFrameBytes = 4 * 1024 * 1024;

    internal static long Validate(PageRangeDiff changes, long sourceLength)
    {
        if (sourceLength < 0 || sourceLength > MaximumPageBlobBytes || sourceLength % 512 != 0)
            throw new InvalidDataException("The application page-copy source length is invalid.");
        ValidateRanges(changes.ClearRanges, MaximumPageBlobBytes);
        return ValidateRanges(changes.PageRanges, sourceLength);
    }

    private static long ValidateRanges(IReadOnlyList<PageRange> ranges, long maximumLength)
    {
        long total = 0;
        long lastEnd = -1;
        foreach (var range in ranges)
        {
            if (range.Start < 0 || range.Start <= lastEnd || range.End < range.Start ||
                range.End >= maximumLength || range.Start % 512 != 0 || (range.End + 1) % 512 != 0)
                throw new InvalidDataException("The application page-copy ranges are invalid or out of order.");
            total = checked(total + range.End - range.Start + 1);
            lastEnd = range.End;
        }
        return total;
    }

    internal static IEnumerable<PageRange> EnumerateRanges(PageRangeDiff changes)
    {
        foreach (var range in changes.PageRanges)
        {
            for (var start = range.Start; start <= range.End;)
            {
                var end = Math.Min(range.End, start + RangeFrameBytes - 1);
                yield return new PageRange(start, end);
                start = end + 1;
            }
        }
    }

    internal static async Task WriteAsync(
        IPageCopySource source, PageRangeDiff changes, FramedWriteStream destination,
        CancellationToken cancellationToken)
    {
        foreach (var range in EnumerateRanges(changes))
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

    internal sealed class ServerSource(PageRangeDiff changes, FramedReadStream input) : IPageCopySource, IDisposable
    {
        private readonly IEnumerator<PageRange> ranges = EnumerateRanges(changes).GetEnumerator();
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
            return changes;
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
