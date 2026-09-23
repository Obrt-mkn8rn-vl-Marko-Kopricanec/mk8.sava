using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageRootLeaseTests
{
    private const string WorkerPathVariable = "MK8_SAVA_ROOT_LEASE_PATH";
    private const string WorkerRoleVariable = "MK8_SAVA_ROOT_LEASE_ROLE";

    [Fact]
    public async Task OnlyOneServiceCanOpenADataRootAtATime()
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-root-lease-{Guid.NewGuid():N}");
        {
            var first = new SavaWebApplicationFactory(dataPath, deleteDataPath: false);
            await using (first.ConfigureAwait(false))
            {
                await first.InitializeAsync();
                Assert.True(File.Exists(Path.Combine(dataPath, ".mk8-sava.lock")));

                var environment = first.Services.GetRequiredService<IHostEnvironment>();
                var options = first.Services.GetRequiredService<IOptions<SavaOptions>>();
                Assert.Throws<IOException>(() => new StoragePaths(environment, options));
            }
        }

        {
            var reopened = new SavaWebApplicationFactory(dataPath, deleteDataPath: true);
            await using (reopened.ConfigureAwait(false))
                await reopened.InitializeAsync();
        }
    }

    [Fact]
    public async Task CrossProcessRootLeaseWorker()
    {
        var dataPath = Environment.GetEnvironmentVariable(WorkerPathVariable);
        if (string.IsNullOrEmpty(dataPath))
            return;

        dataPath = Path.GetFullPath(dataPath);
        var temporaryRoot = Path.TrimEndingDirectorySeparator(Path.GetFullPath(Path.GetTempPath()));
        if (!string.Equals(Path.GetDirectoryName(dataPath), temporaryRoot, StringComparison.Ordinal) ||
            !Path.GetFileName(dataPath).StartsWith("mk8-sava-root-lease-", StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The root-lease harness must use a dedicated temporary data path.");
        }

        switch (Environment.GetEnvironmentVariable(WorkerRoleVariable))
        {
            case "holder":
                {
                    var holder = new SavaWebApplicationFactory(dataPath, deleteDataPath: true);
                    await using (holder.ConfigureAwait(false))
                    {
                        await holder.InitializeAsync();
                        File.WriteAllText(Path.Combine(dataPath, "holder-ready"), string.Empty);
                        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
                        while (!File.Exists(Path.Combine(dataPath, "release-holder")))
                            await Task.Delay(50, timeout.Token);
                    }
                }
                break;
            case "contender":
                {
                    var contender = new SavaWebApplicationFactory(dataPath, deleteDataPath: false);
                    await using (contender.ConfigureAwait(false))
                        await Assert.ThrowsAsync<IOException>(contender.InitializeAsync);
                }
                break;
            default:
                throw new InvalidOperationException("The root-lease harness role is invalid.");
        }
    }
}
