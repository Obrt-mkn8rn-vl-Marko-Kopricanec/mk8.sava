using System.Text;

namespace Mk8.Sava.Protocol;

internal sealed class BlobQueryAvroWriter(Stream destination) : IDisposable
{
    private const int DataBlockSize = 64 * 1024;
    private const string Schema = "[" +
        "{\"type\":\"record\",\"name\":\"com.microsoft.azure.storage.queryBlobContents.resultData\",\"fields\":[{\"name\":\"data\",\"type\":\"bytes\"}]}," +
        "{\"type\":\"record\",\"name\":\"com.microsoft.azure.storage.queryBlobContents.error\",\"fields\":[{\"name\":\"fatal\",\"type\":\"boolean\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"description\",\"type\":\"string\"},{\"name\":\"position\",\"type\":\"long\"}]}," +
        "{\"type\":\"record\",\"name\":\"com.microsoft.azure.storage.queryBlobContents.progress\",\"fields\":[{\"name\":\"bytesScanned\",\"type\":\"long\"},{\"name\":\"totalBytes\",\"type\":\"long\"}]}," +
        "{\"type\":\"record\",\"name\":\"com.microsoft.azure.storage.queryBlobContents.end\",\"fields\":[{\"name\":\"totalBytes\",\"type\":\"long\"}]}]";

    private readonly byte[] _syncMarker = RandomNumberGenerator.GetBytes(16);
    private readonly MemoryStream _data = new(DataBlockSize);
    private bool _initialized;
    private bool _completed;

    public void Dispose() => _data.Dispose();

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        if (_initialized)
            throw new InvalidOperationException("The Avro writer is already initialized.");
        _initialized = true;

        using var header = new MemoryStream();
        header.Write("Obj\x01"u8);
        WriteLong(header, 2);
        WriteString(header, "avro.schema");
        WriteBytes(header, Encoding.UTF8.GetBytes(Schema));
        WriteString(header, "avro.codec");
        WriteBytes(header, "null"u8);
        WriteLong(header, 0);
        header.Write(_syncMarker);
        await destination.WriteAsync(header.GetBuffer().AsMemory(0, checked((int)header.Length)), cancellationToken).ConfigureAwait(false);
    }

    public async Task AppendDataAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        EnsureWritable();
        if (_data.Length > 0 && _data.Length + data.Length > DataBlockSize)
            await FlushDataAsync(cancellationToken).ConfigureAwait(false);
        if (data.Length >= DataBlockSize)
        {
            await WriteRecordAsync(0, payload => WriteBytes(payload, data.Span), cancellationToken).ConfigureAwait(false);
            return;
        }
        await _data.WriteAsync(data, cancellationToken).ConfigureAwait(false);
    }

    public async Task WriteErrorAsync(
        bool fatal,
        string name,
        string description,
        long position,
        CancellationToken cancellationToken)
    {
        EnsureWritable();
        await FlushDataAsync(cancellationToken).ConfigureAwait(false);
        await WriteRecordAsync(1, payload =>
        {
            payload.WriteByte(fatal ? (byte)1 : (byte)0);
            WriteString(payload, name);
            WriteString(payload, description);
            WriteLong(payload, position);
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task CompleteAsync(long totalBytes, CancellationToken cancellationToken)
    {
        EnsureWritable();
        await FlushDataAsync(cancellationToken).ConfigureAwait(false);
        await WriteRecordAsync(2, payload =>
        {
            WriteLong(payload, totalBytes);
            WriteLong(payload, totalBytes);
        }, cancellationToken).ConfigureAwait(false);
        await WriteRecordAsync(3, payload => WriteLong(payload, totalBytes), cancellationToken).ConfigureAwait(false);
        await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
        _completed = true;
    }

    private async Task FlushDataAsync(CancellationToken cancellationToken)
    {
        if (_data.Length == 0)
            return;
        var length = checked((int)_data.Length);
        await WriteRecordAsync(0, payload => WriteBytes(payload, _data.GetBuffer().AsSpan(0, length)), cancellationToken).ConfigureAwait(false);
        _data.SetLength(0);
    }

    private async Task WriteRecordAsync(
        long branch,
        Action<MemoryStream> encode,
        CancellationToken cancellationToken)
    {
        using var payload = new MemoryStream();
        WriteLong(payload, branch);
        encode(payload);

        using var block = new MemoryStream();
        WriteLong(block, 1);
        WriteLong(block, payload.Length);
        payload.Position = 0;
        await payload.CopyToAsync(block, cancellationToken).ConfigureAwait(false);
        block.Write(_syncMarker);
        await destination.WriteAsync(block.GetBuffer().AsMemory(0, checked((int)block.Length)), cancellationToken).ConfigureAwait(false);
    }

    private void EnsureWritable()
    {
        if (!_initialized || _completed)
            throw new InvalidOperationException("The Avro writer is not writable.");
    }

    private static void WriteBytes(Stream stream, ReadOnlySpan<byte> value)
    {
        WriteLong(stream, value.Length);
        stream.Write(value);
    }

    private static void WriteString(Stream stream, string value) => WriteBytes(stream, Encoding.UTF8.GetBytes(value));

    private static void WriteLong(Stream stream, long value)
    {
        var encoded = unchecked((ulong)((value << 1) ^ (value >> 63)));
        while ((encoded & ~0x7FUL) != 0)
        {
            stream.WriteByte((byte)((encoded & 0x7F) | 0x80));
            encoded >>= 7;
        }
        stream.WriteByte((byte)encoded);
    }
}
