using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace Mk8.Sava.Tests;

public sealed class SavaWebApplicationFactory : WebApplicationFactory<Program>, IAsyncLifetime
{
    public const string AccountName = "devstoreaccount1";
    public const string AccountKey = "Eby8vdM02xNOcqFeqCnf2WmjO1GwSW3eF4J6tq/K1SZFPTOtr/KBHBeksoGMGwBNPajQKDaZhQ==";
    public const string SecondAccountName = "devstoreaccount2";
    public const string SecondAccountKey = "mQ9zP2jx0fSgXK7cZ4aNH3kv1VYw+eR8sL6dB5uC0iFqW7oT2rA9gE4hJ1nM8pUxZcK5bQ3sD6vF0yL7wN2tGA==";
    public const string DelegatorObjectId = "c618c4f3-ec92-4df7-bf22-3a4c599cfb78";
    public const string TenantId = "27cb1b93-a01c-4f4c-8674-cf52973c2fe2";

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
                [$"Sava:Accounts:{SecondAccountName}"] = SecondAccountKey,
                ["Sava:BearerAuthentication:Enabled"] = "true",
                ["Sava:BearerAuthentication:ValidAudiences:0"] = "https://storage.azure.com/",
                ["Sava:BearerAuthentication:ValidIssuers:0"] = "https://issuer.mk8.test",
                ["Sava:BearerAuthentication:SymmetricSigningKeys:test-key"] = AccountKey,
                ["Sava:BearerAuthentication:Principals:reader-1:Permissions"] = "rl",
                ["Sava:BearerAuthentication:Principals:reader-1:Accounts:0"] = AccountName,
                [$"Sava:BearerAuthentication:Principals:{DelegatorObjectId}:Permissions"] = "r",
                [$"Sava:BearerAuthentication:Principals:{DelegatorObjectId}:Accounts:0"] = AccountName,
                [$"Sava:BearerAuthentication:Principals:{DelegatorObjectId}:CanGenerateUserDelegationKey"] = "true",
                ["Sava:MinimumChunkBytes"] = "4096",
                ["Sava:TargetChunkBytes"] = "8192",
                ["Sava:MaximumChunkBytes"] = "16384",
                ["Sava:CompressionQuality"] = "5",
                ["Sava:CompressionMinimumSavingsBytes"] = "32",
                ["Sava:StandardRehydrationDelay"] = "00:00:05",
                ["Sava:HighPriorityRehydrationDelay"] = "00:00:00.200"
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
