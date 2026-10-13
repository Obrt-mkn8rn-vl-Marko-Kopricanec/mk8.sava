using Azure.Core;
using Azure.Identity;

namespace Mk8.Sava.Hosting;

internal sealed class DisabledGraphCredential : TokenCredential
{
    public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
        throw new CredentialUnavailableException("Graph group resolution is disabled.");

    public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
        ValueTask.FromException<AccessToken>(new CredentialUnavailableException("Graph group resolution is disabled."));
}
