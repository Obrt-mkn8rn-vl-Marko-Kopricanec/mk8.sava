namespace Mk8.Sava.Storage;

public sealed class StorageRootLeaseException(string root, IOException innerException)
    : IOException($"Unable to acquire the exclusive data-root lease for '{root}'.", innerException);
