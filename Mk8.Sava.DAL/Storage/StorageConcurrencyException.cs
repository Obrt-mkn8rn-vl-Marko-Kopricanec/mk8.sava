namespace Mk8.Sava.Storage;

public sealed class StorageConcurrencyException : Exception
{
    public StorageConcurrencyException() : this("The logical storage resource changed concurrently.") { }

    public StorageConcurrencyException(string message) : base(message) { }

    public StorageConcurrencyException(string message, Exception innerException) : base(message, innerException) { }
}
