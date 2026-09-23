namespace Mk8.Sava.Protocol;

internal sealed class BlobQueryDataException : Exception
{
    public BlobQueryDataException()
        : base("Blob query data is invalid.")
    {
        Name = "InvalidData";
    }

    public BlobQueryDataException(string message)
        : base(message)
    {
        Name = "InvalidData";
    }

    public BlobQueryDataException(string message, Exception innerException)
        : base(message, innerException)
    {
        Name = "InvalidData";
    }

    public BlobQueryDataException(string name, string message, long position)
        : base(message)
    {
        Name = name;
        Position = position;
    }

    public string Name { get; }
    public long Position { get; }
}
