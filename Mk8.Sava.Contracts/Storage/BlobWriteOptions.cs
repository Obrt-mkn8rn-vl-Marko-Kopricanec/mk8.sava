namespace Mk8.Sava.Storage;

public sealed record BlobWriteOptions(
    BlobHttpProperties Http,
    Dictionary<string, string> Metadata,
    Dictionary<string, string>? Tags = null,
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
