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
