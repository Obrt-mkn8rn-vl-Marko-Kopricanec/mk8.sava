using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Mk8.Sava.Tests;

public sealed class PortableListenerConfigurationTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GatewayBindsTheNamedSnapshotEndpointWithoutUrls(bool https)
    {
        var host = await SplitProcessHost.PrepareAsync().ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        var start = host.CreateStart(typeof(Mk8.Sava.Gateway.Program).Assembly.Location, out var settings);
        settings["urls"] = null;
        settings["Sava:DataPath"] = host.GatewayUnusedDataPath;
        settings["Gateway:StagingPath"] = Path.Combine(host.Root, "named-listener-staging");
        var scheme = https ? Uri.UriSchemeHttps : Uri.UriSchemeHttp;
        settings["Kestrel:Endpoints:Selected:Url"] = scheme + "://127.0.0.1:0";
        settings["Kestrel:Endpoints:Selected:Protocols"] = "Http1";
        using var certificate = CreateCertificate();
        if (https)
            settings["Service:CertificateBase64"] = Convert.ToBase64String(certificate.Export(X509ContentType.Pfx));
        await PortableTestConfiguration.MaterializeAsync(start, host.Root, settings).ConfigureAwait(true);
        var child = new SplitServiceProcess(start);
        await using var childLifetime = child.ConfigureAwait(false);

        var address = await child.WaitForAddressAsync("Gateway", TimeSpan.FromSeconds(60), CancellationToken.None).ConfigureAwait(true);

        Assert.Equal(scheme, address.Scheme);
        Assert.Equal("127.0.0.1", address.Host);
        Assert.InRange(address.Port, 1, 65535);
        using var handler = new HttpClientHandler { CheckCertificateRevocationList = true };
        if (https)
        {
            var expected = certificate.GetCertHashString(HashAlgorithmName.SHA256);
#pragma warning disable MA0039 // Only this disposable loopback fixture's exact synthetic leaf may bypass an untrusted chain; hostname validation remains mandatory.
            handler.ServerCertificateCustomValidationCallback = (_, actual, _, errors) =>
                errors == System.Net.Security.SslPolicyErrors.None ||
                (errors == System.Net.Security.SslPolicyErrors.RemoteCertificateChainErrors && actual is not null &&
                 string.Equals(actual.GetCertHashString(HashAlgorithmName.SHA256), expected, StringComparison.Ordinal));
#pragma warning restore MA0039
        }
        using var client = new HttpClient(handler, disposeHandler: false) { Timeout = TimeSpan.FromSeconds(15) };
        using var response = await client.GetAsync(new Uri(address, "/health/live"), CancellationToken.None).ConfigureAwait(true);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(HttpVersion.Version11, response.Version);
        Assert.False(Directory.Exists(host.StoragePath));
        Assert.False(Directory.Exists(host.GatewayUnusedDataPath));
    }

    [Fact]
    public async Task NamedHttpsWithoutAProtectedCertificateRefusesBeforeStaging()
    {
        var host = await SplitProcessHost.PrepareAsync().ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        var start = host.CreateStart(typeof(Mk8.Sava.Gateway.Program).Assembly.Location, out var settings);
        var staging = Path.Combine(host.Root, "missing-certificate-staging");
        settings["urls"] = null;
        settings["Gateway:StagingPath"] = staging;
        settings["Kestrel:Endpoints:Selected:Url"] = "https://127.0.0.1:0";
        await PortableTestConfiguration.MaterializeAsync(start, host.Root, settings).ConfigureAwait(true);
        using var child = System.Diagnostics.Process.Start(start);
        Assert.NotNull(child);

        var (exitCode, output, error) = await TestProcessRunner.ObserveAsync(child, TimeSpan.FromSeconds(45), TimeSpan.FromSeconds(5))
            .ConfigureAwait(true);

        Assert.NotEqual(0, exitCode);
        Assert.Contains("An HTTPS listener requires Service:CertificateBase64", error, StringComparison.Ordinal);
        Assert.DoesNotContain("Now listening on:", output, StringComparison.Ordinal);
        Assert.False(Directory.Exists(staging));
        Assert.False(Directory.Exists(host.StoragePath));
    }

    [Fact]
    public async Task UnsupportedNamedSchemeRefusesRatherThanBindingAnImplicitListener()
    {
        var host = await SplitProcessHost.PrepareAsync().ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        var start = host.CreateStart(typeof(Mk8.Sava.Gateway.Program).Assembly.Location, out var settings);
        settings["urls"] = null;
        settings["Sava:DataPath"] = host.GatewayUnusedDataPath;
        settings["Gateway:StagingPath"] = Path.Combine(host.Root, "unsupported-listener-staging");
        settings["Kestrel:Endpoints:Selected:Url"] = "unsupported://127.0.0.1:0";
        await PortableTestConfiguration.MaterializeAsync(start, host.Root, settings).ConfigureAwait(true);
        using var child = System.Diagnostics.Process.Start(start);
        Assert.NotNull(child);

        var (exitCode, output, error) = await TestProcessRunner.ObserveAsync(child, TimeSpan.FromSeconds(45), TimeSpan.FromSeconds(5))
            .ConfigureAwait(true);

        Assert.NotEqual(0, exitCode);
        Assert.Contains("unsupported://127.0.0.1:0", error, StringComparison.Ordinal);
        Assert.DoesNotContain("Now listening on:", output, StringComparison.Ordinal);
        Assert.False(Directory.Exists(host.StoragePath));
        Assert.False(Directory.Exists(host.GatewayUnusedDataPath));
    }

    [Fact]
    public async Task ApplicationRpcListenerReplacesNamedSnapshotEndpoints()
    {
        var host = await SplitProcessHost.PrepareAsync().ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);

        await host.StartApplicationAsync(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Kestrel:Endpoints:Rejected:Url"] = "unsupported://127.0.0.1:0",
            ["urls"] = "http://127.0.0.1:1",
        }).ConfigureAwait(true);

        Assert.Equal(Uri.UriSchemeHttp, host.ApplicationAddress.Scheme);
        Assert.Equal("127.0.0.1", host.ApplicationAddress.Host);
        Assert.InRange(host.ApplicationAddress.Port, 2, 65535);
        Assert.False(Directory.Exists(host.GatewayUnusedDataPath));
    }

    private static X509Certificate2 CreateCertificate()
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var names = new SubjectAlternativeNameBuilder();
        names.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(names.Build());
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(certificateAuthority: false,
            hasPathLengthConstraint: false, pathLengthConstraint: 0, critical: false));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, critical: false));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new("1.3.6.1.5.5.7.3.1")], critical: false));
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddMinutes(20));
    }
}
