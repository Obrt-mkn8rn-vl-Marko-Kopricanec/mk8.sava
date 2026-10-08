namespace Mk8.Sava.Storage;

internal sealed class ContentDefinedChunker
{
    private const int WindowSize = 64;
    private static readonly ulong[] Gear = CreateGearTable();

    private readonly int _minimum;
    private readonly int _maximum;
    private readonly ulong _mask;

    public ContentDefinedChunker(int minimum, int target, int maximum)
    {
        _minimum = minimum;
        _maximum = maximum;
        var power = (int)Math.Round(Math.Log2(target));
        _mask = (1UL << Math.Clamp(power, 10, 30)) - 1;
    }

    public async IAsyncEnumerable<byte[]> ReadChunksAsync(
        Stream source,
        long maximumLength,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Preserve the original MemoryStream capacity validation before any source read.
        using var chunk = new MemoryStream(Math.Min(_maximum, 0));
        var buffer = new byte[64 * 1024];
        var window = new byte[WindowSize];
        var windowPosition = 0;
        ulong fingerprint = 0;
        long total = 0;

        while (true)
        {
            var read = await source.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            if (read == 0)
                break;

            total = checked(total + read);
            if (total > maximumLength)
                throw new RequestBodyTooLargeException(maximumLength);

            // Empty/rejected input needs no data reserve; admitted input keeps the original maximum capacity.
            if (chunk.Capacity == 0)
                chunk.Capacity = _maximum;

            var pendingStart = 0;
            var chunkLength = checked((int)chunk.Length);
#pragma warning disable HLQ013 // Only the bytes actually read from the reused buffer are valid input.
            for (var index = 0; index < read; index++)
#pragma warning restore HLQ013
            {
                var value = buffer[index];
                var outgoing = window[windowPosition];
                window[windowPosition] = value;
                windowPosition = (windowPosition + 1) % WindowSize;
                fingerprint = BitOperations.RotateLeft(fingerprint, 1) ^ Gear[value] ^ BitOperations.RotateLeft(Gear[outgoing], WindowSize);
                chunkLength++;

                if (chunkLength >= _minimum && ((fingerprint & _mask) == 0 || chunkLength >= _maximum))
                {
                    // Private in-memory accumulation; like WriteByte, it does not add cancellation inside an already-read buffer.
                    await chunk.WriteAsync(buffer.AsMemory(pendingStart, index - pendingStart + 1), CancellationToken.None)
                        .ConfigureAwait(false);
                    yield return chunk.ToArray();
                    chunk.SetLength(0);
                    pendingStart = index + 1;
                    chunkLength = 0;
                    Array.Clear(window);
                    windowPosition = 0;
                    fingerprint = 0;
                }
            }
            await chunk.WriteAsync(buffer.AsMemory(pendingStart, read - pendingStart), CancellationToken.None)
                .ConfigureAwait(false);
        }

        if (chunk.Length > 0)
            yield return chunk.ToArray();
    }

    private static ulong[] CreateGearTable()
    {
        var table = new ulong[256];
        var value = 0x9E3779B97F4A7C15UL;
        foreach (ref var entry in table.AsSpan())
        {
            value += 0x9E3779B97F4A7C15UL;
            var mixed = value;
            mixed = (mixed ^ (mixed >> 30)) * 0xBF58476D1CE4E5B9UL;
            mixed = (mixed ^ (mixed >> 27)) * 0x94D049BB133111EBUL;
            entry = mixed ^ (mixed >> 31);
        }

        return table;
    }
}
