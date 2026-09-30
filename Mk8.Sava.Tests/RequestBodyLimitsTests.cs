using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class RequestBodyLimitsTests
{
    [Theory]
    [InlineData(1024L, false, 1024L)]
    [InlineData(1024L, true, 1_180_675L)]
    [InlineData(5_242_880_000L, false, 5_242_880_000L)]
    [InlineData(5_242_880_000L, true, 5_244_059_651L)]
    [InlineData(long.MaxValue, true, long.MaxValue)]
    [InlineData(long.MaxValue - 1, true, long.MaxValue)]
    public void TransportLimitUsesLogicalBudgetAndOnlyBoundedFraming(long maximum, bool structured, long expected)
    {
        var context = new DefaultHttpContext();
        var feature = new BodySizeFeature();
        context.Features.Set<IHttpMaxRequestBodySizeFeature>(feature);

        RequestBodyLimits.Apply(context.Request, maximum, structured);

        Assert.Equal(expected, feature.MaxRequestBodySize);
    }

    [Fact]
    public void AlreadyReadRequestIsNotReconfigured()
    {
        var context = new DefaultHttpContext();
        var feature = new BodySizeFeature { IsReadOnly = true };
        context.Features.Set<IHttpMaxRequestBodySizeFeature>(feature);

        RequestBodyLimits.Apply(context.Request, 5_242_880_000L, structured: true);

        Assert.Equal(30_000_000L, feature.MaxRequestBodySize);
    }

    [Fact]
    public void HostsWithoutTheFeatureKeepApplicationEnforcement()
    {
        var context = new DefaultHttpContext();

        RequestBodyLimits.Apply(context.Request, 1024, structured: true);

        Assert.Null(context.Features.Get<IHttpMaxRequestBodySizeFeature>());
    }

    private sealed class BodySizeFeature : IHttpMaxRequestBodySizeFeature
    {
        public bool IsReadOnly { get; init; }
        public long? MaxRequestBodySize { get; set; } = 30_000_000;
    }
}
