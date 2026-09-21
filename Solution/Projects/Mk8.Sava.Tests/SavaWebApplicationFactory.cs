using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace Mk8.Sava.Tests;

public sealed class SavaWebApplicationFactory : WebApplicationFactory<Program>, IAsyncLifetime
{
    public const string AccountName = "devstoreaccount1";
    public const string AccountKey = "Eby8vdM02xNOcqFeqCnf2WmjO1GwSW3eF4J6tq/K1SZFPTOtr/KBHBeksoGMGwBNPajQKDaZhQ==";

    public string DataPath { get; } = Path.Combine(Path.GetTempPath(), $"mk8-sava-tests-{Guid.NewGuid():N}");

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureAppConfiguration((_, configuration) =>
        {
            configuration.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Sava:DataPath"] = DataPath,
                ["Sava:DefaultAccount"] = AccountName,
                [$"Sava:Accounts:{AccountName}"] = AccountKey,
                ["Sava:MinimumChunkBytes"] = "4096",
                ["Sava:TargetChunkBytes"] = "8192",
                ["Sava:MaximumChunkBytes"] = "16384",
                ["Sava:CompressionQuality"] = "5",
                ["Sava:CompressionMinimumSavingsBytes"] = "32"
            });
        });
    }

    public Task InitializeAsync()
    {
        _ = Server;
        return Task.CompletedTask;
    }

    public new async Task DisposeAsync()
    {
        await base.DisposeAsync();
        if (Directory.Exists(DataPath))
            Directory.Delete(DataPath, recursive: true);
    }
}
