namespace Mk8.Sava.Storage;

public sealed class StorageDirectoryNotEmptyException : Exception
{
    public StorageDirectoryNotEmptyException() : this("The directory is not empty.") { }

    public StorageDirectoryNotEmptyException(string message) : base(message) { }

    public StorageDirectoryNotEmptyException(string message, Exception innerException) : base(message, innerException) { }
}
