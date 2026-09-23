namespace Mk8.Sava.Protocol;

internal sealed record UserDelegationKeyRequest(
    DateTimeOffset StartsAt,
    DateTimeOffset ExpiresAt,
    string? DelegatedUserTenantId);
