namespace Mk8.Sava.Protocol;

internal sealed record UserDelegationKey(
    string SignedObjectId,
    string SignedTenantId,
    string SignedStart,
    string SignedExpiry,
    string SignedService,
    string SignedVersion,
    string? SignedDelegatedUserTenantId,
    string Value);
