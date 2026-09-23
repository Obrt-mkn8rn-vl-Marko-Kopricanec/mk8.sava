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
        var chunk = new MemoryStream(_maximum);
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

            for (var index = 0; index < read; index++)
            {
                var value = buffer[index];
                var outgoing = window[windowPosition];
                window[windowPosition] = value;
                windowPosition = (windowPosition + 1) % WindowSize;
                fingerprint = BitOperations.RotateLeft(fingerprint, 1) ^ Gear[value] ^ BitOperations.RotateLeft(Gear[outgoing], WindowSize);
                chunk.WriteByte(value);

                if (chunk.Length >= _minimum && ((fingerprint & _mask) == 0 || chunk.Length >= _maximum))
                {
                    yield return chunk.ToArray();
                    chunk.SetLength(0);
                    Array.Clear(window);
                    windowPosition = 0;
                    fingerprint = 0;
                }
            }
        }

        if (chunk.Length > 0)
            yield return chunk.ToArray();
    }

    private static ulong[] CreateGearTable()
    {
        var table = new ulong[256];
        var value = 0x9E3779B97F4A7C15UL;
        for (var index = 0; index < table.Length; index++)
        {
            value += 0x9E3779B97F4A7C15UL;
            var mixed = value;
            mixed = (mixed ^ (mixed >> 30)) * 0xBF58476D1CE4E5B9UL;
            mixed = (mixed ^ (mixed >> 27)) * 0x94D049BB133111EBUL;
            table[index] = mixed ^ (mixed >> 31);
        }

        return table;
    }
}

public sealed class RequestBodyTooLargeException(long maximumLength)
    : Exception($"The request body exceeds the configured limit of {maximumLength} bytes.")
{
    public long MaximumLength { get; } = maximumLength;
}

