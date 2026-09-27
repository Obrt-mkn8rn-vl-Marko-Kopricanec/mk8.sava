namespace Mk8.Sava.Protocol;

internal readonly record struct ParquetPageGeometry(
    int HeaderBytes,
    int Type,
    int CompressedBytes,
    int UncompressedBytes,
    int Values,
    int Nulls,
    int Rows,
    int LevelBytes,
    bool IsCompressed)
{
    public static ParquetPageGeometry Read(ReadOnlySpan<byte> bytes, CancellationToken cancellationToken)
    {
        var reader = new ParquetCompactReader(bytes, cancellationToken);
        var last = 0;
        var seen = 0;
        var pageType = -1;
        var compressed = -1;
        var uncompressed = -1;
        var section = 0;
        var values = -1;
        var nulls = 0;
        var rows = -1;
        var levels = 0;
        var isCompressed = true;
        while (reader.ReadField(ref last, out var field, out var type))
        {
            if (field is >= 1 and <= 8)
            {
                if ((seen & (1 << field)) != 0)
                    throw Invalid();
                seen |= 1 << field;
            }
            if (field is >= 1 and <= 3)
            {
                var number = ReadInt(ref reader, type);
                switch (field)
                {
                    case 1: pageType = number; break;
                    case 2: uncompressed = number; break;
                    case 3: compressed = number; break;
                }
            }
            else if (field is 5 or 7 or 8)
            {
                if (type != 12 || section != 0)
                    throw Invalid();
                section = field;
                (values, nulls, rows, levels, isCompressed) = ReadDataHeader(ref reader, field);
            }
            else
            {
                reader.Skip(type);
            }
        }
        if ((seen & 14) != 14 || compressed < 0 || uncompressed < 0 ||
            pageType is < 0 or > 3 ||
            (pageType == 0 && section != 5) || (pageType == 2 && section != 7) ||
            (pageType == 3 && section != 8) || (pageType == 1 && section != 0) ||
            (pageType != 1 && (values < 0 || nulls < 0 || nulls > values)) ||
            levels < 0 || levels > compressed || levels > uncompressed ||
            (pageType == 3 && rows != values))
        {
            throw Invalid();
        }
        return new ParquetPageGeometry(reader.Position, pageType, compressed, uncompressed,
            values, nulls, rows, levels, isCompressed);
    }

    private static (int Values, int Nulls, int Rows, int Levels, bool Compressed) ReadDataHeader(
        ref ParquetCompactReader reader, int section)
    {
        Span<int> numbers = stackalloc int[7];
        numbers.Clear();
        var last = 0;
        var seen = 0;
        var compressed = true;
        var required = section switch { 5 => 30, 7 => 6, 8 => 126, _ => throw Invalid() };
        while (reader.ReadField(ref last, out var field, out var type))
        {
            if (field is >= 1 and <= 7)
            {
                if ((seen & (1 << field)) != 0)
                    throw Invalid();
                seen |= 1 << field;
            }
            if (field <= 6 && (required & (1 << field)) != 0)
                numbers[field] = ReadInt(ref reader, type);
            else if (section == 8 && field == 7)
                compressed = type switch { 1 => true, 2 => false, _ => throw Invalid() };
            else
                reader.Skip(type, depth: 1);
        }
        if ((seen & required) != required)
            throw Invalid();
        var levels = section == 8 ? checked(numbers[5] + numbers[6]) : 0;
        if (section == 8 && (numbers[5] < 0 || numbers[6] < 0))
            throw Invalid();
        return (numbers[1], section == 8 ? numbers[2] : 0,
            section == 8 ? numbers[3] : -1, levels, compressed);
    }

    private static int ReadInt(ref ParquetCompactReader reader, byte type) => type == 5
        ? checked((int)reader.ReadInteger(type)) : throw Invalid();

    private static BlobQueryDataException Invalid() => new("InvalidParquetFile", "The Parquet page header is invalid.", 0);
}
