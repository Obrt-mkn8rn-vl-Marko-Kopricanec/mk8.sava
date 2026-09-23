namespace Mk8.Sava.Storage;

public sealed class StoragePathConflictException : Exception
{
    public StoragePathConflictException() : this("A hierarchical path component has an incompatible resource type.") { }

    public StoragePathConflictException(string message) : base(message) { }

    public StoragePathConflictException(string message, Exception innerException) : base(message, innerException) { }
}
