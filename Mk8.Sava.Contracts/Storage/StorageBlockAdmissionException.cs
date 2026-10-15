namespace Mk8.Sava.Storage;

public sealed class StorageBlockAdmissionException : Exception
{
    public StorageBlockAdmissionException() : this("All block IDs for a blob must have the same decoded length.") { }

    public StorageBlockAdmissionException(string message) : base(message) { }

    public StorageBlockAdmissionException(string message, Exception innerException) : base(message, innerException) { }

    public StorageBlockAdmissionException(bool capacityExceeded) : this(capacityExceeded
        ? "The uncommitted block count exceeds the maximum permitted value."
        : "All block IDs for a blob must have the same decoded length.")
    {
        CapacityExceeded = capacityExceeded;
    }

    public bool CapacityExceeded { get; }
}
