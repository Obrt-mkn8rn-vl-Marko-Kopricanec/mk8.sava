namespace Mk8.Sava.Tests;

public sealed class SplitProcessHostPreparationTests
{
    [Fact]
    public async Task PreparationOwnsKeyAndRootWithoutStartingAnInvalidGateway()
    {
        var settings = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Gateway:MaximumConcurrentRequests"] = "0",
        };
        var host = await SplitProcessHost.PrepareAsync(settings).ConfigureAwait(true);
        var root = host.Root;
        await using (host.ConfigureAwait(false))
        {
            Assert.True(Directory.Exists(root));
            var keyText = await File.ReadAllTextAsync(host.AccessKeyFile).ConfigureAwait(true);
            Assert.Equal(32, Convert.FromBase64String(keyText).Length);
            if (!OperatingSystem.IsWindows())
                Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, File.GetUnixFileMode(host.AccessKeyFile));
            Assert.Throws<InvalidOperationException>(() => host.GatewayProcessId);
            Assert.Empty(host.CapturedLogs);
            Assert.False(Directory.Exists(host.StoragePath));
            Assert.False(Directory.Exists(host.GatewayUnusedDataPath));
            Assert.False(Directory.Exists(Path.Combine(root, "gateway-staging")));

            var start = host.CreateStart(typeof(Mk8.Sava.Gateway.Program).Assembly.Location);
            Assert.Equal("0", start.Environment["Gateway__MaximumConcurrentRequests"]);
            Assert.Equal(host.AccessKeyFile, start.Environment["ApplicationTransport__AccessKeyFile"]);
            Assert.Equal(new Uri(host.ApplicationAddress, "/internal/application").AbsoluteUri,
                start.Environment["ApplicationTransport__Endpoint"]);
        }
        Assert.False(Directory.Exists(root));
    }
}
