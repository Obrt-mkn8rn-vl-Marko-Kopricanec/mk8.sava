using Microsoft.Extensions.DependencyInjection.Extensions;
using Mk8.Sava.Storage;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Application;

#pragma warning disable CA1515 // External regression fixtures compose Application-only services without loading its listener or sharing storage with Gateway.
public static class ApplicationServiceCollectionExtensions
#pragma warning restore CA1515
{
    public static IServiceCollection AddSavaApplication(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddOptions<ApplicationHostingOptions>()
            .BindConfiguration(ApplicationHostingOptions.SectionName)
            .ValidateDataAnnotations()
            .Validate(options => options.ReadSessionIdleTimeout >= TimeSpan.FromSeconds(1) &&
                options.ReadSessionIdleTimeout <= TimeSpan.FromHours(1),
                "Read-session idle timeout must be at least one second and at most one hour.")
            .Validate(options => options.RpcQueueTimeout >= TimeSpan.FromMilliseconds(100) &&
                options.RpcQueueTimeout <= TimeSpan.FromSeconds(30),
                "Application RPC queue timeout must be between 100 milliseconds and 30 seconds.");
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<StoragePaths>();
        services.TryAddSingleton<IStoragePaths>(provider => provider.GetRequiredService<StoragePaths>());
        services.TryAddSingleton<StorageTelemetry>();
        services.TryAddSingleton<IStorageTelemetry>(provider => provider.GetRequiredService<StorageTelemetry>());
        services.TryAddSingleton<IStorageFaultInjector, NullStorageFaultInjector>();
        services.TryAddSingleton<ChunkStore>();
        services.TryAddSingleton<MetadataStore>();
        services.TryAddSingleton<LeaseService>();
        services.TryAddSingleton<StorageAnalyticsService>();
        services.TryAddSingleton<IStorageAnalyticsSink>(provider => provider.GetRequiredService<StorageAnalyticsService>());
        services.TryAddSingleton<BlobService>();
        services.TryAddSingleton<StorageBackupService>();
        services.TryAddSingleton<StorageDataKeyContinuity>();
        services.TryAddSingleton<ApplicationInitializationService>();
        services.AddHostedService(provider => provider.GetRequiredService<ApplicationInitializationService>());
        services.AddHostedService<StorageMaintenanceService>();
        services.TryAddSingleton<ApplicationReadSessions>();
        services.TryAddSingleton<IApplicationReadSessions>(provider => provider.GetRequiredService<ApplicationReadSessions>());
        services.AddHostedService(provider => provider.GetRequiredService<ApplicationReadSessions>());
        services.TryAddSingleton<ApplicationRpcAdmission>();
        services.TryAddSingleton<IApplicationRpcAdmission>(provider => provider.GetRequiredService<ApplicationRpcAdmission>());
        services.TryAddSingleton<ApplicationReadinessService>();
        services.TryAddSingleton<IApplicationReadiness>(provider => provider.GetRequiredService<ApplicationReadinessService>());
        services.TryAddSingleton<IApplicationRpcDispatcher, ApplicationRpcDispatcher>();
        return services;
    }
}
