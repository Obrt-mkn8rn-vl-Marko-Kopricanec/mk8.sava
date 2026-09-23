namespace Mk8.Sava.Configuration;

public sealed class BearerPrincipalAccess
{
    public string Permissions { get; init; } = string.Empty;
    public IList<string> Accounts { get; init; } = [];
    public IList<string> Containers { get; init; } = [];
    public bool CanGenerateUserDelegationKey { get; init; }
    public bool CanManageOwnership { get; init; }
    public string? UserPrincipalName { get; init; }
}
