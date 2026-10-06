namespace Mk8.Sava.Application;

public sealed record UserDelegationRoleGrants(IReadOnlyList<string> Roles, string IssuedPermissions);
