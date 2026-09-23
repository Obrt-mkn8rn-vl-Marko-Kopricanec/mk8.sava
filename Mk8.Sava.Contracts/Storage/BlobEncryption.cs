namespace Mk8.Sava.Storage;

public readonly record struct BlobEncryption(
    string? Scope,
    string? CustomerProvidedKeySha256,
    byte[]? CustomerProvidedKey = null);
