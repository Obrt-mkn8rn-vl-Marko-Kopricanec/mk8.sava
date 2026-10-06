namespace Mk8.Sava.Storage;

public sealed record UserDelegationKeyIdentity(
    string Account,
    string ObjectId,
    string TenantId,
    string StartText,
    string ExpiryText,
    string Service,
    string Version,
    string DelegatedUserTenantId);
