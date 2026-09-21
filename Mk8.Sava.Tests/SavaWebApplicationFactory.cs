using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class SavaWebApplicationFactory : WebApplicationFactory<Program>, IAsyncLifetime
{
    private readonly Func<HttpMessageHandler>? _urlTransferHandlerFactory;
    private readonly IReadOnlyDictionary<string, string?>? _configurationOverrides;

    public const string AccountName = "devstoreaccount1";
    public const string AccountKey = "Eby8vdM02xNOcqFeqCnf2WmjO1GwSW3eF4J6tq/K1SZFPTOtr/KBHBeksoGMGwBNPajQKDaZhQ==";
    public const string SecondAccountName = "devstoreaccount2";
    public const string SecondAccountKey = "mQ9zP2jx0fSgXK7cZ4aNH3kv1VYw+eR8sL6dB5uC0iFqW7oT2rA9gE4hJ1nM8pUxZcK5bQ3sD6vF0yL7wN2tGA==";
    public const string DelegatorObjectId = "c618c4f3-ec92-4df7-bf22-3a4c599cfb78";
    public const string TenantId = "27cb1b93-a01c-4f4c-8674-cf52973c2fe2";

    public SavaWebApplicationFactory()
        : this(Path.Combine(Path.GetTempPath(), $"mk8-sava-tests-{Guid.NewGuid():N}"), null, null)
    {
    }

    internal SavaWebApplicationFactory(string dataPath)
        : this(dataPath, null, null)
    {
    }

    internal SavaWebApplicationFactory(Func<HttpMessageHandler> urlTransferHandlerFactory)
        : this(
            Path.Combine(Path.GetTempPath(), $"mk8-sava-tests-{Guid.NewGuid():N}"),
            urlTransferHandlerFactory,
            null)
    {
    }

    internal SavaWebApplicationFactory(IReadOnlyDictionary<string, string?> configurationOverrides)
        : this(
            Path.Combine(Path.GetTempPath(), $"mk8-sava-tests-{Guid.NewGuid():N}"),
            null,
            configurationOverrides)
    {
    }

    private SavaWebApplicationFactory(
        string dataPath,
        Func<HttpMessageHandler>? urlTransferHandlerFactory,
        IReadOnlyDictionary<string, string?>? configurationOverrides)
    {
        DataPath = dataPath;
        _urlTransferHandlerFactory = urlTransferHandlerFactory;
        _configurationOverrides = configurationOverrides;
    }

    public string DataPath { get; }

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
                ["Sava:BackgroundCompressionQuality"] = "11",
                ["Sava:BackgroundCompressionMinimumSavingsBytes"] = "32",
                ["Sava:BackgroundCompressionMinimumAge"] = "01:00:00",
                ["Sava:BackgroundCompressionChunksPerMaintenancePass"] = "8",
                ["Sava:StandardRehydrationDelay"] = "00:00:05",
                ["Sava:HighPriorityRehydrationDelay"] = "00:00:00.200",
                ["Sava:AsyncCopyCompletionDelay"] = "00:00:02",
                ["Sava:MaintenanceScanInterval"] = "00:00:00.050",
                ["Sava:AbandonedStagingRetention"] = "1.00:00:00",
                ["Sava:MaximumStagingFilesPerMaintenancePass"] = "1000",
                ["Sava:IntegrityScanChunksPerMaintenancePass"] = "100000"
            });
            if (_configurationOverrides is not null)
                configuration.AddInMemoryCollection(_configurationOverrides);
        });
        if (_urlTransferHandlerFactory is not null)
        {
            builder.ConfigureServices(services =>
                services.AddHttpClient<UrlTransferClient>(client => client.Timeout = Timeout.InfiniteTimeSpan)
                    .ConfigurePrimaryHttpMessageHandler(_urlTransferHandlerFactory));
        }
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
