using Azure.Core;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Mk8.Sava.Identity;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

// Public xUnit test classes use this fixture as a public primary-constructor parameter.
#pragma warning disable CA1515
public sealed class SavaWebApplicationFactory : WebApplicationFactory<Program>, IAsyncLifetime
#pragma warning restore CA1515
{
    private readonly Func<HttpMessageHandler>? _urlTransferHandlerFactory;
    private readonly Func<HttpMessageHandler>? _graphHandlerFactory;
    private readonly TokenCredential? _graphCredential;
    private readonly IReadOnlyDictionary<string, string?>? _configurationOverrides;
    private readonly TimeProvider? _timeProvider;
    private readonly IStorageFaultInjector? _faultInjector;
    private readonly IStorageAnalyticsSink? _analyticsSink;
    private readonly bool _disableMaintenance;
    private readonly bool _deleteDataPath;
    private StoragePaths? _storagePaths;
    private MetadataStore? _metadataStore;
    private ChunkStore? _chunkStore;

    public const string AccountName = "devstoreaccount1";
    public const string AccountKey = "Eby8vdM02xNOcqFeqCnf2WmjO1GwSW3eF4J6tq/K1SZFPTOtr/KBHBeksoGMGwBNPajQKDaZhQ==";
    public const string SecondAccountName = "devstoreaccount2";
    public const string SecondAccountKey = "mQ9zP2jx0fSgXK7cZ4aNH3kv1VYw+eR8sL6dB5uC0iFqW7oT2rA9gE4hJ1nM8pUxZcK5bQ3sD6vF0yL7wN2tGA==";
    public const string DelegatorObjectId = "c618c4f3-ec92-4df7-bf22-3a4c599cfb78";
    public const string TenantId = "27cb1b93-a01c-4f4c-8674-cf52973c2fe2";

    public SavaWebApplicationFactory()
        : this(
            Path.Combine(Path.GetTempPath(), $"mk8-sava-tests-{Guid.NewGuid():N}"),
            urlTransferHandlerFactory: null,
            configurationOverrides: null,
            timeProvider: null,
            deleteDataPath: true)
    {
    }

    internal SavaWebApplicationFactory(string dataPath)
        : this(
            dataPath,
            urlTransferHandlerFactory: null,
            configurationOverrides: null,
            timeProvider: null,
            deleteDataPath: true)
    {
    }

    internal SavaWebApplicationFactory(string dataPath, bool deleteDataPath)
        : this(
            dataPath,
            urlTransferHandlerFactory: null,
            configurationOverrides: null,
            timeProvider: null,
            deleteDataPath)
    {
    }

    internal SavaWebApplicationFactory(
        string dataPath,
        IReadOnlyDictionary<string, string?> configurationOverrides,
        bool deleteDataPath)
        : this(dataPath, null, configurationOverrides, null, deleteDataPath)
    {
    }

    internal SavaWebApplicationFactory(Func<HttpMessageHandler> urlTransferHandlerFactory)
        : this(
            Path.Combine(Path.GetTempPath(), $"mk8-sava-tests-{Guid.NewGuid():N}"),
            urlTransferHandlerFactory,
            null,
            null,
            true)
    {
    }

    internal SavaWebApplicationFactory(IReadOnlyDictionary<string, string?> configurationOverrides)
        : this(
            Path.Combine(Path.GetTempPath(), $"mk8-sava-tests-{Guid.NewGuid():N}"),
            null,
            configurationOverrides,
            null,
            true)
    {
    }

    internal SavaWebApplicationFactory(
        IReadOnlyDictionary<string, string?> configurationOverrides,
        Func<HttpMessageHandler> graphHandlerFactory,
        TokenCredential graphCredential)
        : this(
            Path.Combine(Path.GetTempPath(), $"mk8-sava-tests-{Guid.NewGuid():N}"),
            null,
            configurationOverrides,
            null,
            true)
    {
        _graphHandlerFactory = graphHandlerFactory;
        _graphCredential = graphCredential;
    }

    internal SavaWebApplicationFactory(
        TimeProvider timeProvider,
        IReadOnlyDictionary<string, string?> configurationOverrides)
        : this(
            Path.Combine(Path.GetTempPath(), $"mk8-sava-tests-{Guid.NewGuid():N}"),
            null,
            configurationOverrides,
            timeProvider,
            true)
    {
    }

    internal SavaWebApplicationFactory(
        string dataPath,
        IStorageFaultInjector faultInjector,
        IStorageAnalyticsSink? analyticsSink,
        IReadOnlyDictionary<string, string?>? configurationOverrides,
        bool deleteDataPath,
        bool disableMaintenance = false)
        : this(dataPath, null, configurationOverrides, null, deleteDataPath)
    {
        _faultInjector = faultInjector;
        _analyticsSink = analyticsSink;
        _disableMaintenance = disableMaintenance;
    }

    private SavaWebApplicationFactory(
        string dataPath,
        Func<HttpMessageHandler>? urlTransferHandlerFactory,
        IReadOnlyDictionary<string, string?>? configurationOverrides,
        TimeProvider? timeProvider,
        bool deleteDataPath)
    {
        DataPath = dataPath;
        _urlTransferHandlerFactory = urlTransferHandlerFactory;
        _configurationOverrides = configurationOverrides;
        _timeProvider = timeProvider;
        _deleteDataPath = deleteDataPath;
    }

    public string DataPath { get; }

    public new TestServer Server
    {
        get
        {
            var server = base.Server;
            CaptureServices(base.Services);
            return server;
        }
    }

    public override IServiceProvider Services
    {
        get
        {
            var services = base.Services;
            CaptureServices(services);
            return services;
        }
    }

    public new HttpClient CreateClient()
    {
        _ = Server;
        return base.CreateClient();
    }

    private void CaptureServices(IServiceProvider services)
    {
        _storagePaths ??= services.GetRequiredService<StoragePaths>();
        _metadataStore ??= services.GetRequiredService<MetadataStore>();
        _chunkStore ??= services.GetRequiredService<ChunkStore>();
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.ConfigureAppConfiguration((_, configuration) =>
        {
            configuration.AddInMemoryCollection(CreateBaseConfiguration());
            if (_configurationOverrides is not null)
                configuration.AddInMemoryCollection(_configurationOverrides);
        });
        if (_urlTransferHandlerFactory is not null)
        {
            builder.ConfigureServices(services =>
                services.AddHttpClient<UrlTransferClient>(client => client.Timeout = Timeout.InfiniteTimeSpan)
                    .ConfigurePrimaryHttpMessageHandler(_urlTransferHandlerFactory));
        }
        ConfigureGraphResolverServices(builder);
        if (_timeProvider is not null)
        {
            builder.ConfigureServices(services =>
            {
                services.RemoveAll<TimeProvider>();
                services.AddSingleton(_timeProvider);
            });
        }
        if (_faultInjector is not null || _analyticsSink is not null)
        {
            builder.ConfigureServices(services =>
            {
                if (_faultInjector is not null)
                {
                    services.RemoveAll<IStorageFaultInjector>();
                    services.AddSingleton(_faultInjector);
                }
                if (_analyticsSink is not null)
                {
                    services.RemoveAll<IStorageAnalyticsSink>();
                    services.AddSingleton(_analyticsSink);
                }
            });
        }
        if (_disableMaintenance)
        {
            builder.ConfigureServices(services =>
            {
                var registrations = services
                    .Where(descriptor =>
                        descriptor.ServiceType == typeof(IHostedService) &&
                        descriptor.ImplementationType == typeof(StorageMaintenanceService))
                    .ToArray();
                foreach (var registration in registrations)
                    services.Remove(registration);
            });
        }
    }

    private void ConfigureGraphResolverServices(IWebHostBuilder builder)
    {
        if (_graphHandlerFactory is null || _graphCredential is null)
            return;
        builder.ConfigureServices(services =>
        {
            services.RemoveAll<TokenCredential>();
            services.AddSingleton(_graphCredential);
            services.AddHttpClient<MicrosoftGraphGroupMembershipResolver>()
                .ConfigurePrimaryHttpMessageHandler(_graphHandlerFactory);
        });
    }

    private Dictionary<string, string?> CreateBaseConfiguration() => new(StringComparer.Ordinal)
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
        ["Sava:SmallChunkPackingThresholdBytes"] = "2048",
        ["Sava:StandardRehydrationDelay"] = "00:00:05",
        ["Sava:HighPriorityRehydrationDelay"] = "00:00:00.200",
        ["Sava:AsyncCopyCompletionDelay"] = "00:00:02",
        ["Sava:MaintenanceScanInterval"] = "00:00:00.050",
        ["Sava:AbandonedStagingRetention"] = "1.00:00:00",
        ["Sava:MaximumStagingFilesPerMaintenancePass"] = "1000",
        ["Sava:UrlTransferAllowedPrivateHosts:0"] = "127.0.0.1",
        ["Sava:BlobRecordsPerMaintenancePass"] = "100000",
        ["Sava:ContainerRecordsPerMaintenancePass"] = "100000",
        ["Sava:UncommittedBlocksPerMaintenancePass"] = "100000",
        ["Sava:GarbageCollectionChunksPerMaintenancePass"] = "100000",
        ["Sava:IntegrityScanChunksPerMaintenancePass"] = "100000"
    };

    public Task InitializeAsync()
    {
        _ = Server;
        return Task.CompletedTask;
    }

    Task IAsyncLifetime.DisposeAsync() => DisposeAsync().AsTask();

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync().ConfigureAwait(false);
        // The test root is removed immediately below; explicitly release its
        // scanner, SQLite pool, and lease even if a host defers singleton disposal.
        _chunkStore?.Dispose();
        _metadataStore?.Dispose();
        _storagePaths?.Dispose();
        if (_deleteDataPath)
            await DeleteDataPathAsync(DataPath).ConfigureAwait(false);
    }

    internal static async Task DeleteDataPathAsync(string dataPath)
    {
        for (var attempt = 0; attempt < 8; attempt++)
        {
            if (!Directory.Exists(dataPath))
                return;
            try
            {
                Directory.Delete(dataPath, recursive: true);
                return;
            }
            catch (IOException) when (OperatingSystem.IsWindows() && attempt < 7)
            {
                // Windows can release the last SQLite or test-server handle
                // just after host shutdown. Persistent locks still fail.
                await Task.Delay(100).ConfigureAwait(false);
            }
        }
    }
}
