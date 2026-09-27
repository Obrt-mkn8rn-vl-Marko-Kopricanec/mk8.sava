namespace Mk8.Sava.Protocol;

// Validate wire geometry before a metadata decoder can allocate containers.
// Compact-Thrift container counts and binary lengths are unsigned varints;
// integral field values use zigzag encoding. No collection is materialized.
internal ref struct ParquetCompactReader(ReadOnlySpan<byte> bytes, CancellationToken cancellationToken)
{
    private readonly ReadOnlySpan<byte> _bytes = bytes;
    private int _position;

    public readonly int Position => _position;
    private readonly int Remaining => _bytes.Length - _position;

    public bool ReadField(ref int lastField, out int field, out byte type)
    {
        var header = ReadByte();
        type = (byte)(header & 15);
        if (header == 0)
        {
            field = 0;
            return false;
        }
        if (type is 0 or > 12)
            throw Invalid();
        field = (header >> 4) == 0 ? checked((int)ReadInteger(4)) : checked(lastField + (header >> 4));
        if (field is <= 0 or > short.MaxValue)
            throw Invalid();
        lastField = field;
        return true;
    }

    public long ReadInteger(byte type)
    {
        if (type is < 4 or > 6)
            throw Invalid();
        var encoded = ReadUnsigned(type == 6 ? 64 : 32);
        var value = unchecked((long)(encoded >> 1) ^ -(long)(encoded & 1));
        if ((type == 4 && value is < short.MinValue or > short.MaxValue) ||
            (type == 5 && value is < int.MinValue or > int.MaxValue))
        {
            throw Invalid();
        }
        return value;
    }

    public void SkipStruct(int depth = 0)
    {
        if (depth > 64)
            throw Invalid();
        var last = 0;
        while (ReadField(ref last, out _, out var type))
            Skip(type, depth, inlineBoolean: true);
    }

    public void Skip(byte type, int depth = 0, bool inlineBoolean = true)
    {
        switch (type)
        {
            case 1:
            case 2:
                if (!inlineBoolean && ReadByte() is not (1 or 2))
                    throw Invalid();
                break;
            case 3:
                Advance(1);
                break;
            case 4:
            case 5:
            case 6:
                _ = ReadInteger(type);
                break;
            case 7:
                Advance(8);
                break;
            case 8:
                Advance(checked((int)ReadUnsigned(32)));
                break;
            case 9:
            case 10:
                SkipList(depth + 1);
                break;
            case 11:
                SkipMap(depth + 1);
                break;
            case 12:
                SkipStruct(depth + 1);
                break;
            default:
                throw Invalid();
        }
    }

    public (int Count, byte Type) ReadListHeader()
    {
        var header = ReadByte();
        var count = header >> 4;
        if (count == 15)
            count = checked((int)ReadUnsigned(32));
        // Even a boolean or an empty struct consumes at least one byte.
        if (count > Remaining)
            throw new EndOfStreamException();
        return (count, (byte)(header & 15));
    }

    private void SkipList(int depth)
    {
        var (count, type) = ReadListHeader();
        if (depth > 64)
            throw Invalid();
        for (var item = 0; item < count; item++)
            Skip(type, depth, inlineBoolean: false);
    }

    private void SkipMap(int depth)
    {
        var count = checked((int)ReadUnsigned(32));
        if (count == 0)
            return;
        var types = ReadByte();
        if (depth > 64)
            throw Invalid();
        if (count > Remaining / 2)
            throw new EndOfStreamException();
        for (var item = 0; item < count; item++)
        {
            Skip((byte)(types >> 4), depth, inlineBoolean: false);
            Skip((byte)(types & 15), depth, inlineBoolean: false);
        }
    }

    private ulong ReadUnsigned(int bits)
    {
        ulong result = 0;
        for (var shift = 0; shift < bits; shift += 7)
        {
            var next = ReadByte();
            var value = (byte)(next & 127);
            if (shift + 7 > bits && value >= (1 << (bits - shift)))
                throw Invalid();
            result |= (ulong)value << shift;
            if (next < 128)
                return result;
        }
        throw Invalid();
    }

    private byte ReadByte()
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (Remaining == 0)
            throw new EndOfStreamException();
        return _bytes[_position++];
    }

    private void Advance(int count)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (count < 0)
            throw Invalid();
        if (count > Remaining)
            throw new EndOfStreamException();
        _position += count;
    }

    private static BlobQueryDataException Invalid() => new("InvalidParquetFile", "The Parquet compact-Thrift metadata is invalid.", 0);
}
