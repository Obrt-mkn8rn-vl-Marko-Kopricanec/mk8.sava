namespace Mk8.Sava.Protocol;

internal sealed record StorageAuthorization(
    StorageAuthorizationKind Kind,
    string Permissions,
    DateTimeOffset? StartsAt = null,
    DateTimeOffset? ExpiresAt = null,
    string? Identifier = null,
    bool IsAccountSas = false,
    string? SignedResource = null,
    string? TenantId = null,
    bool CanGenerateUserDelegationKey = false,
    string? ApplicationId = null,
    string? Audience = null,
    string? Issuer = null,
    string? UserPrincipalName = null,
    string AccountWidePermissions = "",
    string? DelegatedObjectId = null,
    bool AclReadChecked = false,
    string? AclAuthorizedGenerationId = null,
    bool AclListChecked = false,
    string? AclListObjectId = null,
    IReadOnlySet<string>? AclListGroups = null,
    bool AclMutationChecked = false,
    string? AclMutationObjectId = null,
    IReadOnlySet<string>? AclMutationGroups = null,
    bool AclAppendChecked = false,
    string? AclAppendObjectId = null,
    IReadOnlySet<string>? AclAppendGroups = null)
{
    public static StorageAuthorization Anonymous { get; } = new(StorageAuthorizationKind.Anonymous, string.Empty);
    public static StorageAuthorization Owner { get; } = new(StorageAuthorizationKind.SharedKey, "racwdxltmeop");

    public bool Allows(char permission) =>
        Kind == StorageAuthorizationKind.SharedKey || Permissions.Contains(permission, StringComparison.Ordinal);

    public string? CreatorObjectId => Kind switch
    {
        StorageAuthorizationKind.Bearer => Identifier,
        StorageAuthorizationKind.Sas => DelegatedObjectId,
        _ => null
    };
}
