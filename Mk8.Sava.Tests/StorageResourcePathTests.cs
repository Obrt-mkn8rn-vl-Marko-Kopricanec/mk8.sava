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

    [Theory]
    [InlineData("/container/a+file", "/container/a%2Bfile")]
    [InlineData("/container/a%2Bfile", "/container/a%252Bfile")]
    [InlineData("/container/a&file", "/container/a%26file")]
    [InlineData("/container/a:;@file", "/container/a%3A%3B%40file")]
    [InlineData("/container/escaped +%/file", "/container/escaped%20%2B%25/file")]
    [InlineData("/container/a//ü", "/container/a//%C3%BC")]
    public void MissingRawTargetReconstructsSdkEscapingFromDecodedSegments(string decodedPath, string signedPath)
    {
        ArgumentNullException.ThrowIfNull(decodedPath);
        var context = new DefaultHttpContext();
        context.Request.Path = new PathString(decodedPath);
        context.Features.Get<IHttpRequestFeature>()!.RawTarget = string.Empty;
        var candidates = StorageResourcePath.GetSignaturePathCandidates(context.Request);
        Assert.Contains(signedPath, candidates, StringComparer.Ordinal);
        Assert.Equal(decodedPath[1..].Split('/'), StorageResourcePath.DecodeRequestSegments(context.Request));
        foreach (var candidate in candidates)
            Assert.Equal(decodedPath, Uri.UnescapeDataString(candidate));
    }

    [Theory]
    [InlineData("/container/a%2Bfile", "/container/a+file")]
    [InlineData("/container/a%252Bfile", "/container/a%2Bfile")]
    [InlineData("/container/a%25252Bfile", "/container/a%252Bfile")]
    public void ActualRawTargetIsNotReconstructedFromDecodedPath(string rawTarget, string decodedPath)
    {
        var context = new DefaultHttpContext();
        context.Request.Path = new PathString(decodedPath);
        context.Features.Get<IHttpRequestFeature>()!.RawTarget = rawTarget + "?sig=opaque";
        Assert.Equal(rawTarget, Assert.Single(StorageResourcePath.GetSignaturePathCandidates(context.Request)));
        Assert.Equal(decodedPath[1..].Split('/'), StorageResourcePath.DecodeRequestSegments(context.Request));
    }
}
