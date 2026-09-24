using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class StorageResourcePathTests
{
    [Theory]
    [InlineData("/container/a!file", new[] { "/container/a!file", "/container/a%21file" })]
    [InlineData("/container/a%21file", new[] { "/container/a%21file" })]
    [InlineData("/container/a%2521file", new[] { "/container/a%2521file" })]
    public void SignatureAliasesDoNotConflateEncodedExclamationWithLiteralPercentName(
        string rawTarget,
        string[] expected)
    {
        var context = new DefaultHttpContext();
        context.Features.Get<IHttpRequestFeature>()!.RawTarget = rawTarget;
        Assert.Equal(expected, StorageResourcePath.GetSignaturePathCandidates(context.Request));
    }
}
