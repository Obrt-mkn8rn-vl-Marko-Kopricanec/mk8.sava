using System.Net;
using System.Net.Sockets;
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

    [Fact]
    public async Task MixedDnsAnswerConnectsOnlyToAllowedAddresses()
    {
        var loopback = IPAddress.Loopback;
        var publicAddress = IPAddress.Parse("1.1.1.1");
        var attempted = new List<IPAddress>();
        var policy = new UrlSourceEgressPolicy(
            new SavaOptions(),
            (host, _) =>
            {
                Assert.Equal("mixed.example", host);
                return Task.FromResult(new[] { loopback, publicAddress });
            },
            (address, port, _) =>
            {
                Assert.Equal(443, port);
                attempted.Add(address);
                return ValueTask.FromResult<Stream>(new MemoryStream());
            });

        using var stream = await policy.ConnectEndpointAsync(new DnsEndPoint("mixed.example", 443), CancellationToken.None);

        Assert.Equal([publicAddress], attempted);
    }

    [Fact]
    public async Task PrivateOnlyDnsAnswerFailsBeforeAnyConnection()
    {
        var attempted = new List<IPAddress>();
        var policy = new UrlSourceEgressPolicy(
            new SavaOptions(),
            (_, _) => Task.FromResult(new[] { IPAddress.Loopback, IPAddress.Parse("169.254.169.254") }),
            (address, _, _) =>
            {
                attempted.Add(address);
                return ValueTask.FromResult<Stream>(new MemoryStream());
            });

        var rejected = await Assert.ThrowsAsync<IOException>(async () =>
            await policy.ConnectEndpointAsync(new DnsEndPoint("private.example", 80), CancellationToken.None)
                .ConfigureAwait(false));

        Assert.Contains("blocked", rejected.Message, StringComparison.Ordinal);
        Assert.Empty(attempted);
    }

    [Fact]
    public async Task AllowedAddressFailureNeverFallsBackToBlockedAnswer()
    {
        var publicAddress = IPAddress.Parse("8.8.8.8");
        var attempted = new List<IPAddress>();
        var policy = new UrlSourceEgressPolicy(
            new SavaOptions(),
            (_, _) => Task.FromResult(new[] { publicAddress, IPAddress.Loopback }),
            (address, _, _) =>
            {
                attempted.Add(address);
                throw new SocketException((int)SocketError.ConnectionRefused);
            });

        var rejected = await Assert.ThrowsAsync<IOException>(async () =>
            await policy.ConnectEndpointAsync(new DnsEndPoint("mixed.example", 80), CancellationToken.None)
                .ConfigureAwait(false));

        Assert.Equal([publicAddress], attempted);
        Assert.IsType<SocketException>(rejected.InnerException);
    }

    [Fact]
    public async Task PrivateHostExceptionAppliesOnlyToTheExactHostAtConnectionTime()
    {
        var privateAddress = IPAddress.Parse("10.0.0.1");
        var attempted = new List<IPAddress>();
        var policy = new UrlSourceEgressPolicy(
            new SavaOptions { UrlTransferAllowedPrivateHosts = ["trusted.internal"] },
            (_, _) => Task.FromResult(new[] { privateAddress }),
            (address, _, _) =>
            {
                attempted.Add(address);
                return ValueTask.FromResult<Stream>(new MemoryStream());
            });

        using var stream = await policy.ConnectEndpointAsync(
            new DnsEndPoint("TRUSTED.INTERNAL", 443), CancellationToken.None);
        await Assert.ThrowsAsync<IOException>(async () =>
            await policy.ConnectEndpointAsync(new DnsEndPoint("trusted.internal.evil.example", 443), CancellationToken.None)
                .ConfigureAwait(false));

        Assert.Equal([privateAddress], attempted);
    }

    [Fact]
    public async Task EveryConnectionRechecksItsCurrentDnsAnswer()
    {
        var publicAddress = IPAddress.Parse("1.1.1.1");
        var resolutions = 0;
        var attempted = new List<IPAddress>();
        var policy = new UrlSourceEgressPolicy(
            new SavaOptions(),
            (_, _) => Task.FromResult(++resolutions == 1 ? new[] { publicAddress } : new[] { IPAddress.Loopback }),
            (address, _, _) =>
            {
                attempted.Add(address);
                return ValueTask.FromResult<Stream>(new MemoryStream());
            });

        using var first = await policy.ConnectEndpointAsync(new DnsEndPoint("rebind.example", 443), CancellationToken.None);
        await Assert.ThrowsAsync<IOException>(async () =>
            await policy.ConnectEndpointAsync(new DnsEndPoint("rebind.example", 443), CancellationToken.None)
                .ConfigureAwait(false));

        Assert.Equal(2, resolutions);
        Assert.Equal([publicAddress], attempted);
    }
}
