using System.Buffers.Binary;

namespace Mk8.Sava.Protocol;

internal sealed class StorageCrc64
{
    private const ulong Polynomial = 0x9A6C9329AC4BC9B5UL;
    private static readonly ulong[] Table = CreateTable();
    private ulong _value;

    public void Append(ReadOnlySpan<byte> bytes)
    {
        var crc = _value ^ ulong.MaxValue;
        foreach (ref readonly var value in bytes)
            crc = crc >> 8 ^ Table[(byte)(crc ^ value)];
        _value = crc ^ ulong.MaxValue;
    }

    public byte[] GetHash()
    {
        var bytes = new byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64LittleEndian(bytes, _value);
        return bytes;
    }

    private static ulong[] CreateTable()
    {
        var table = new ulong[256];
        for (var index = 0; index < table.Length; index++)
        {
            var value = (ulong)index;
            for (var bit = 0; bit < 8; bit++)
                value = (value & 1) == 0 ? value >> 1 : value >> 1 ^ Polynomial;
            table[index] = value;
        }
        return table;
    }
}
