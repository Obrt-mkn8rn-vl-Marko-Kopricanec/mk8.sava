namespace Mk8.Sava.Storage;

public sealed record UserDelegationKeyIssue(
    UserDelegationKeyIdentity Identity,
    string Nonce,
    string KeyFingerprint,
    IReadOnlyCollection<string> Roles,
    string IssuedPermissions,
    DateTimeOffset ExpiresAt);
