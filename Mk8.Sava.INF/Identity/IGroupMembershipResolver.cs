using System.Security.Claims;

namespace Mk8.Sava.Identity;

internal interface IGroupMembershipResolver
{
    Task<HashSet<string>> ResolveAsync(
        ClaimsPrincipal principal,
        string objectId,
        CancellationToken cancellationToken);
}
