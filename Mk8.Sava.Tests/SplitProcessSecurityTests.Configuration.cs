using System.Net;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Application;

namespace Mk8.Sava.Tests;

public sealed partial class SplitProcessSecurityTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ; ; ")]
    public async Task GatewayWithoutAConfiguredListenerRefusesBeforeCreatingStaging(string? urls)
    {
        var host = await SplitProcessHost.StartAsync(application: false).ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        var staging = Path.Combine(host.Root, "unconfigured-listener-staging");
        var start = host.CreateStart(typeof(Mk8.Sava.Gateway.Program).Assembly.Location);
        foreach (var key in start.Environment.Keys.Where(key =>
                     string.Equals(key, "URLS", StringComparison.OrdinalIgnoreCase) ||
                     key.EndsWith("_URLS", StringComparison.OrdinalIgnoreCase) ||
                     key.EndsWith("HTTP_PORTS", StringComparison.OrdinalIgnoreCase) ||
                     key.EndsWith("HTTPS_PORTS", StringComparison.OrdinalIgnoreCase)).ToArray())
        {
            start.Environment.Remove(key);
        }
        start.Environment["ASPNETCORE_URLS"] = urls;
        start.Environment["Gateway__StagingPath"] = staging;

        var result = await RunOperatorProcessAsync(start).ConfigureAwait(true);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("A Gateway listener must be explicitly configured", result.Error, StringComparison.Ordinal);
        Assert.DoesNotContain("Now listening on:", result.Output, StringComparison.Ordinal);
        Assert.False(Directory.Exists(staging));
        Assert.False(Directory.Exists(host.StoragePath));
    }

    [Theory]
    [InlineData(";;", "8080", null)]
    [InlineData(";;", null, "8443")]
    [InlineData(";;", "8080", "8443")]
    [InlineData(" ; ; ", "8080", null)]
    [InlineData(" ; ; ", null, "8443")]
    [InlineData(" ; ; ", "8080", "8443")]
    public async Task GatewayPortsCannotAuthorizeAnEmptyOverridingUrlList(string urls, string? httpPorts, string? httpsPorts)
    {
        var host = await SplitProcessHost.StartAsync(application: false).ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        var staging = Path.Combine(host.Root, "ignored-port-listener-staging");
        var start = host.CreateStart(typeof(Mk8.Sava.Gateway.Program).Assembly.Location);
        foreach (var key in start.Environment.Keys.Where(key =>
                     string.Equals(key, "URLS", StringComparison.OrdinalIgnoreCase) ||
                     key.EndsWith("_URLS", StringComparison.OrdinalIgnoreCase) ||
                     key.EndsWith("HTTP_PORTS", StringComparison.OrdinalIgnoreCase) ||
                     key.EndsWith("HTTPS_PORTS", StringComparison.OrdinalIgnoreCase)).ToArray())
        {
            start.Environment.Remove(key);
        }
        // Fixed ports are refusal inputs only: this child must bind no listener.
        start.Environment["ASPNETCORE_URLS"] = urls;
        start.Environment["ASPNETCORE_HTTP_PORTS"] = httpPorts;
        start.Environment["ASPNETCORE_HTTPS_PORTS"] = httpsPorts;
        start.Environment["Gateway__StagingPath"] = staging;

        var result = await RunOperatorProcessAsync(start).ConfigureAwait(true);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("A Gateway listener must be explicitly configured", result.Error, StringComparison.Ordinal);
        Assert.DoesNotContain("Now listening on:", result.Output, StringComparison.Ordinal);
        Assert.False(Directory.Exists(staging));
        Assert.False(Directory.Exists(host.StoragePath));
    }

    [Theory]
    [InlineData("http://127.0.0.1:0", "ignored-port", "ignored-port", null)]
    [InlineData(null, "0", null, null)]
    [InlineData("", null, "0", null)]
    [InlineData(null, "0", "0", null)]
    [InlineData(null, null, null, "http://127.0.0.1:0")]
    [InlineData(";;", "0", "0", "http://127.0.0.1:0")]
    public async Task GatewayExplicitEffectiveListenerProfilesAreAdmitted(string? urls, string? httpPorts,
        string? httpsPorts, string? endpoint)
    {
        // Actual Program admission with owned TestServer, not native binds/TLS.
        var settings = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["urls"] = urls,
            ["http_ports"] = httpPorts,
            ["https_ports"] = httpsPorts,
        };
        if (endpoint is not null)
        {
            settings["Kestrel:Endpoints:Selected:Url"] = endpoint;
        }
        var factory = new SavaWebApplicationFactory(settings);
        await using var lifetime = factory.ConfigureAwait(false);
        using var client = factory.CreateClient();
        using var response = await client.GetAsync(new Uri("/health/live", UriKind.Relative)).ConfigureAwait(true);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var configuration = factory.Services.GetRequiredService<IConfiguration>();
        Assert.Equal(urls, configuration["urls"]);
        Assert.Equal(httpPorts, configuration["http_ports"]);
        Assert.Equal(httpsPorts, configuration["https_ports"]);
        Assert.Equal(endpoint, configuration["Kestrel:Endpoints:Selected:Url"]);
    }

    [Fact]
    public async Task ApplicationWithoutAnEndpointRefusesBeforeOpeningItsDataRoot()
    {
        var host = await SplitProcessHost.StartAsync(application: false).ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        var data = Path.Combine(host.Root, "unconfigured-endpoint-storage");
        var start = host.CreateStart(typeof(ApplicationProgram).Assembly.Location);
        start.Environment.Remove("ApplicationTransport__Endpoint");
        start.Environment["Sava__DataPath"] = data;

        var result = await RunOperatorProcessAsync(start).ConfigureAwait(true);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("An Application endpoint is required", result.Error, StringComparison.Ordinal);
        Assert.DoesNotContain("Now listening on:", result.Output, StringComparison.Ordinal);
        Assert.False(Directory.Exists(data));
        Assert.False(Directory.Exists(host.StoragePath));
    }
}
