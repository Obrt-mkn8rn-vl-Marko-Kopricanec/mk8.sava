namespace Mk8.Sava.Storage;

public sealed class StoragePendingCopyException : Exception
{
    public StoragePendingCopyException() : this("There is currently a pending copy operation.") { }

    public StoragePendingCopyException(string message) : base(message) { }

    public StoragePendingCopyException(string message, Exception innerException) : base(message, innerException) { }
}
