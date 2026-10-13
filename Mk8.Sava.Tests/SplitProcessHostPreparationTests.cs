using System.Security.Cryptography;

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
            var keyText = host.AccessKey;
            Assert.Equal(32, Convert.FromBase64String(keyText).Length);
            if (!OperatingSystem.IsWindows())
                Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, File.GetUnixFileMode(Path.Combine(root, "Environment", ".installation.key")));
            Assert.Throws<InvalidOperationException>(() => host.GatewayProcessId);
            Assert.Empty(host.CapturedLogs);
            Assert.False(Directory.Exists(host.StoragePath));
            Assert.False(Directory.Exists(host.GatewayUnusedDataPath));
            Assert.False(Directory.Exists(Path.Combine(root, "gateway-staging")));

            var start = host.CreateStart(typeof(Mk8.Sava.Gateway.Program).Assembly.Location, out var childSettings);
            Assert.Equal("0", childSettings["Gateway:MaximumConcurrentRequests"]);
            Assert.True(CryptographicOperations.FixedTimeEquals(
                Convert.FromBase64String(host.AccessKey),
                Convert.FromBase64String(childSettings["ApplicationTransport:AccessKey"] ?? string.Empty)));
            Assert.Equal(new Uri(host.ApplicationAddress, "/internal/application").AbsoluteUri,
                childSettings["ApplicationTransport:Endpoint"]);
            Assert.False(start.Environment.ContainsKey("ApplicationTransport__AccessKey"));
        }
        Assert.False(Directory.Exists(root));
    }
}
