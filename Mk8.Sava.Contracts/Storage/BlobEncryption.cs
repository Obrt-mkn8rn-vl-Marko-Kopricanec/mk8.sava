namespace Mk8.Sava.Storage;

#pragma warning disable CA1819 // The key must remain a byte array so the caller can zero it after use.
public readonly record struct BlobEncryption(
    string? Scope,
    string? CustomerProvidedKeySha256,
    byte[]? CustomerProvidedKey = null);
#pragma warning restore CA1819
