namespace Mk8.Sava.Storage;

public sealed class StorageBlobTypeMismatchException : Exception
{
    public StorageBlobTypeMismatchException() : this("The blob type is invalid for this operation.") { }

    public StorageBlobTypeMismatchException(string message) : base(message) { }

    public StorageBlobTypeMismatchException(string message, Exception innerException) : base(message, innerException) { }
}
