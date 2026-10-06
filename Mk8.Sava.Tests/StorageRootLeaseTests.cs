using System.Diagnostics;
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
                var exception = Assert.Throws<StorageRootLeaseException>(() => new StoragePaths(environment, options));
                Assert.IsType<IOException>(exception.InnerException);
            }
        }

        {
            var reopened = new SavaWebApplicationFactory(dataPath, deleteDataPath: true);
            await using (reopened.ConfigureAwait(false))
                await reopened.InitializeAsync();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompetingProcessExitsWithoutAborting(bool operatorCommand)
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-root-lease-{Guid.NewGuid():N}");
        try
        {
            var holder = new SavaWebApplicationFactory(dataPath, deleteDataPath: false);
            await using (holder.ConfigureAwait(false))
            {
                await holder.InitializeAsync().ConfigureAwait(true);
                var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
                {
                    WorkingDirectory = AppContext.BaseDirectory,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false
                };
                start.ArgumentList.Add(typeof(Mk8.Sava.Application.ApplicationProgram).Assembly.Location);
                if (operatorCommand)
                {
                    start.ArgumentList.Add("--backup-create");
                    start.ArgumentList.Add(Path.Combine(dataPath, "rejected-backup"));
                }
                await ConfigureContenderAsync(start, dataPath).ConfigureAwait(true);
                using var contender = Process.Start(start);
                Assert.NotNull(contender);
                var output = contender.StandardOutput.ReadToEndAsync();
                var error = contender.StandardError.ReadToEndAsync();
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                try
                {
                    await contender.WaitForExitAsync(timeout.Token).ConfigureAwait(true);
                }
                finally
                {
                    if (!contender.HasExited)
                    {
                        contender.Kill(entireProcessTree: true);
                        await contender.WaitForExitAsync().ConfigureAwait(true);
                    }
                }
                Assert.Equal(1, contender.ExitCode);
                Assert.Contains("Unable to acquire the exclusive data-root lease", await error.ConfigureAwait(true),
                    StringComparison.Ordinal);
                Assert.DoesNotContain("Unhandled exception", await error.ConfigureAwait(true), StringComparison.Ordinal);
                _ = await output.ConfigureAwait(true);
                Assert.True(await holder.Services.GetRequiredService<MetadataStore>()
                    .IsReadyAsync(CancellationToken.None).ConfigureAwait(true));
            }
        }
        finally
        {
            if (Directory.Exists(dataPath))
                await SavaWebApplicationFactory.DeleteDataPathAsync(dataPath).ConfigureAwait(true);
        }
    }

    private static async Task ConfigureContenderAsync(ProcessStartInfo start, string dataPath)
    {
        start.Environment["Sava__DataPath"] = dataPath;
        start.Environment["Sava__DefaultAccount"] = SavaWebApplicationFactory.AccountName;
        start.Environment[$"Sava__Accounts__{SavaWebApplicationFactory.AccountName}"] = SavaWebApplicationFactory.AccountKey;
        start.Environment[$"Sava__Accounts__{SavaWebApplicationFactory.SecondAccountName}"] = SavaWebApplicationFactory.SecondAccountKey;
        var keyPath = Path.Combine(dataPath, "contender.key");
        start.Environment["ApplicationTransport__AccessKeyFile"] = keyPath;
        await File.WriteAllTextAsync(keyPath,
            Convert.ToBase64String(System.Security.Cryptography.RandomNumberGenerator.GetBytes(32))).ConfigureAwait(true);
        if (!OperatingSystem.IsWindows())
            File.SetUnixFileMode(keyPath, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        start.Environment["ASPNETCORE_URLS"] = "http://127.0.0.1:0";
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
                        await File.WriteAllTextAsync(Path.Combine(dataPath, "holder-ready"), string.Empty).ConfigureAwait(true);
                        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
                        while (!File.Exists(Path.Combine(dataPath, "release-holder")))
                            await Task.Delay(50, timeout.Token).ConfigureAwait(true);
                    }
                }
                break;
            case "contender":
                {
                    using var host = Host.CreateDefaultBuilder().Build();
                    var environment = host.Services.GetRequiredService<IHostEnvironment>();
                    var options = Options.Create(new SavaOptions { DataPath = dataPath });
                    var exception = Assert.Throws<StorageRootLeaseException>(
                        () => new StoragePaths(environment, options));
                    Assert.IsType<IOException>(exception.InnerException);
                }
                break;
            default:
                throw new InvalidOperationException("The root-lease harness role is invalid.");
        }
    }
}
