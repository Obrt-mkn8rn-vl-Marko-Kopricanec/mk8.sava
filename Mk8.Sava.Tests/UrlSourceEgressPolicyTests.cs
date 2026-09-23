using System.Net;
using Mk8.Sava.Configuration;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class UrlSourceEgressPolicyTests
{
    [Theory]
    [InlineData("1.1.1.1", true)]
    [InlineData("8.8.8.8", true)]
    [InlineData("2606:4700:4700::1111", true)]
    [InlineData("0.0.0.0", false)]
    [InlineData("10.0.0.1", false)]
    [InlineData("100.100.100.200", false)]
    [InlineData("127.0.0.1", false)]
    [InlineData("169.254.169.254", false)]
    [InlineData("172.16.0.1", false)]
    [InlineData("192.0.2.1", false)]
    [InlineData("192.168.1.1", false)]
    [InlineData("198.18.0.1", false)]
    [InlineData("198.51.100.1", false)]
    [InlineData("203.0.113.1", false)]
    [InlineData("224.0.0.1", false)]
    [InlineData("::1", false)]
    [InlineData("::ffff:127.0.0.1", false)]
    [InlineData("fc00::1", false)]
    [InlineData("fe80::1", false)]
    [InlineData("2001:db8::1", false)]
    [InlineData("2002::1", false)]
    public void OnlyGlobalUnicastAddressesAreAllowedByDefault(string text, bool expected)
    {
        Assert.Equal(expected, UrlSourceEgressPolicy.IsPublicAddress(IPAddress.Parse(text)));
    }

    [Fact]
    public void PrivateSourceExceptionMatchesOnlyItsConfiguredHost()
    {
        var policy = new UrlSourceEgressPolicy(new SavaOptions
        {
            UrlTransferAllowedPrivateHosts = ["trusted.internal"]
        });
        var privateAddress = IPAddress.Parse("10.0.0.1");

        Assert.True(policy.Allows("TRUSTED.INTERNAL", privateAddress));
        Assert.False(policy.Allows("eviltrusted.internal", privateAddress));
        Assert.False(policy.Allows("trusted.internal.evil.example", privateAddress));
        Assert.True(policy.Allows("public.example", IPAddress.Parse("1.1.1.1")));
    }
}
