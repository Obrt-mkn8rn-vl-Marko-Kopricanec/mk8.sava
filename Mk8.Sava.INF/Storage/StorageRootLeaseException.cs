namespace Mk8.Sava.Storage;

public sealed class StorageRootLeaseException : IOException
{
    public StorageRootLeaseException() : base()
    {
    }

    public StorageRootLeaseException(string message) : base(message)
    {
    }

    public StorageRootLeaseException(string message, Exception innerException) : base(message, innerException)
    {
    }

    public StorageRootLeaseException(string message, int hresult) : base(message, hresult)
    {
    }
}
