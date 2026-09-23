namespace Mk8.Sava.Storage;

#pragma warning disable CA1819 // The key must remain a byte array so the caller can zero it after use.
public sealed record BlobWriteOptions(
    BlobHttpProperties Http,
    IReadOnlyDictionary<string, string> Metadata,
    IReadOnlyDictionary<string, string>? Tags = null,
    string? AccessTier = null,
    DateTimeOffset? ImmutabilityUntil = null,
    bool ImmutabilityLocked = false,
    bool HasLegalHold = false,
    string? EncryptionScope = null,
    string? CustomerProvidedKeySha256 = null,
    byte[]? CustomerProvidedKey = null,
    bool? AccessTierInferred = null,
    bool GenerateContentMd5 = false,
    bool AccessTierSpecified = false,
    string? EncryptionContext = null,
    DateTimeOffset? ExpiresAt = null,
    string? RehydratePriority = null,
    string? CreatorObjectId = null);
#pragma warning restore CA1819
