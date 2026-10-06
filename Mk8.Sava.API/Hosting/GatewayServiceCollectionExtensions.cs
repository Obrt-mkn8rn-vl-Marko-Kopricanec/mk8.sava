using System.Net;
using Azure.Core;
using Azure.Identity;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using Mk8.Sava.Configuration;
using Mk8.Sava.Identity;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Hosting;

public static class GatewayServiceCollectionExtensions
{
    public static IServiceCollection AddSavaGateway(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        services.AddOptions<SavaOptions>().Bind(configuration.GetSection(SavaOptions.SectionName))
            .ValidateDataAnnotations().ValidateOnStart();
        services.AddOptions<GatewayOptions>().Bind(configuration.GetSection(GatewayOptions.SectionName))
            .ValidateDataAnnotations().ValidateOnStart();
        services.AddOptions<ApplicationTransportOptions>()
            .Bind(configuration.GetSection(ApplicationTransportOptions.SectionName))
            .ValidateDataAnnotations().ValidateOnStart();
        services.AddSingleton(provider => new ApplicationRpcClient(
            provider.GetRequiredService<IOptions<ApplicationTransportOptions>>().Value,
            provider.GetRequiredService<IOptions<SavaOptions>>().Value));
        services.AddSingleton<GatewayBlobCapabilities>();
        services.AddSingleton<IBlobApplication>(provider => provider.GetRequiredService<ApplicationRpcClient>()
            .CreateProxy<IBlobApplication>(provider.GetRequiredService<GatewayBlobCapabilities>()));
        services.AddSingleton<IMetadataApplication>(provider => provider.GetRequiredService<ApplicationRpcClient>()
            .CreateProxy<IMetadataApplication>());
        services.AddSingleton<IApplicationReadSessions>(provider => provider.GetRequiredService<ApplicationRpcClient>()
            .CreateProxy<IApplicationReadSessions>());
        services.AddSingleton<IApplicationReadiness>(provider => provider.GetRequiredService<ApplicationRpcClient>()
            .CreateProxy<IApplicationReadiness>());
        services.AddSingleton<IStorageAnalyticsSink>(provider => provider.GetRequiredService<ApplicationRpcClient>()
            .CreateProxy<IStorageAnalyticsSink>());
        services.AddSingleton<GatewayStagingPaths>();
        services.AddHostedService<GatewayInitializationService>();
        services.AddSingleton<GatewayAdmission>();
        services.AddSingleton<StorageTelemetry>();
        services.AddSingleton<IStorageTelemetry>(provider => provider.GetRequiredService<StorageTelemetry>());
        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<LeaseService>();
        services.AddSingleton<StorageAuthenticator>();
        AddIdentityServices(services);
        services.AddHttpClient<UrlTransferClient>(client => client.Timeout = Timeout.InfiniteTimeSpan)
            .RemoveAllLoggers().ConfigurePrimaryHttpMessageHandler(provider =>
            {
                var egress = new UrlSourceEgressPolicy(provider.GetRequiredService<IOptions<SavaOptions>>().Value);
                return new SocketsHttpHandler
                {
                    AllowAutoRedirect = false,
                    AutomaticDecompression = DecompressionMethods.None,
                    UseProxy = false,
                    ConnectTimeout = TimeSpan.FromSeconds(10),
                    ConnectCallback = egress.ConnectAsync
                };
            });
        return services;
    }

    private static void AddIdentityServices(IServiceCollection services)
    {
        services.AddAuthentication().AddJwtBearer(StorageAuthenticator.BearerScheme, _ => { });
        services.AddOptions<JwtBearerOptions>(StorageAuthenticator.BearerScheme)
            .Configure<IOptions<SavaOptions>>((jwt, options) => ConfigureBearer(jwt, options.Value.BearerAuthentication));
        services.AddSingleton<TokenCredential>(provider =>
        {
            var cloud = provider.GetRequiredService<IOptions<SavaOptions>>().Value.BearerAuthentication.GraphGroupResolution.Cloud;
            var authority = cloud switch
            {
                MicrosoftGraphCloud.Global => AzureAuthorityHosts.AzurePublicCloud,
                MicrosoftGraphCloud.UsGovernment or MicrosoftGraphCloud.UsGovernmentDod => AzureAuthorityHosts.AzureGovernment,
                MicrosoftGraphCloud.China => AzureAuthorityHosts.AzureChina,
                _ => throw new InvalidOperationException("Unsupported Microsoft Graph cloud.")
            };
            return new DefaultAzureCredential(new DefaultAzureCredentialOptions { AuthorityHost = authority });
        });
        services.AddHttpClient<MicrosoftGraphGroupMembershipResolver>(client => client.Timeout = TimeSpan.FromSeconds(5))
            .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler { AllowAutoRedirect = false });
        services.AddSingleton<IGroupMembershipResolver>(provider =>
            provider.GetRequiredService<MicrosoftGraphGroupMembershipResolver>());
    }

    public static WebApplication MapSavaGateway(this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);
        app.UseMiddleware<StorageTelemetryMiddleware>();
        app.UseMiddleware<AzureExceptionMiddleware>();
        app.UseMiddleware<GatewayAdmissionMiddleware>();
        app.UseMiddleware<RequestContextMiddleware>();
        app.MapGet("/health/live", () => Results.Ok(new { status = "live", component = "gateway" }));
        app.MapGet("/health/ready", CheckReadinessAsync);
        app.MapGet("/metrics", (StorageTelemetry telemetry, GatewayStagingPaths staging, GatewayAdmission admission) =>
            Results.Text(telemetry.RenderRequestPrometheus() + staging.RenderMetrics() + admission.RenderMetrics(),
                "text/plain; version=0.0.4; charset=utf-8"));
        app.Map("/health", () => Results.NotFound());
        app.Map("/health/{**unmatchedPath}", () => Results.NotFound());
        app.Map("/metrics/{**unmatchedPath}", () => Results.NotFound());
        app.Map("/{**storagePath}", BlobProtocolEndpoint.HandleAsync);
        return app;
    }

    private static async Task<IResult> CheckReadinessAsync(IApplicationReadiness application,
        CancellationToken cancellationToken)
    {
        try
        {
            var readiness = await application.GetAsync(cancellationToken).ConfigureAwait(false);
            return Results.Json(new
            {
                status = readiness.Ready ? "ready" : "unavailable",
                component = "gateway",
                application = readiness.Ready ? "ready" : "unavailable",
                metadata = readiness.MetadataReady ? "ready" : "unavailable",
                integrity = new
                {
                    status = readiness.Integrity.Healthy ? "healthy" : "corrupt",
                    readiness.Integrity.Complete,
                    readiness.Integrity.CheckedChunks,
                    readiness.Integrity.MissingChunks,
                    readiness.Integrity.CorruptChunks,
                    readiness.Integrity.CustomerKeyChunks,
                    checkedAt = readiness.Integrity.CheckedAt == DateTimeOffset.MinValue
                        ? null : readiness.Integrity.CheckedAt.ToString("O")
                }
            }, statusCode: readiness.Ready ? 200 : 503);
        }
#pragma warning disable CA1031 // Operator readiness reports backend unavailability while Gateway liveness stays independent.
        catch (Exception exception) when (!CatastrophicExceptionPolicy.Contains(exception))
        {
            return Results.Json(new { status = "unavailable", component = "gateway", application = "unavailable" }, statusCode: 503);
        }
#pragma warning restore CA1031
    }

    private static void ConfigureBearer(JwtBearerOptions jwt, BearerAuthenticationOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.Authority))
            jwt.Authority = options.Authority;
        if (!string.IsNullOrWhiteSpace(options.MetadataAddress))
            jwt.MetadataAddress = options.MetadataAddress;
        jwt.RequireHttpsMetadata = options.RequireHttpsMetadata;
        jwt.TokenValidationParameters = new TokenValidationParameters
        {
            ValidateIssuer = true,
            ValidIssuers = options.ValidIssuers,
            ValidateAudience = true,
            ValidAudiences = options.ValidAudiences,
            ValidateLifetime = true,
            RequireExpirationTime = true,
            ValidateIssuerSigningKey = true,
            IssuerSigningKeys = options.SymmetricSigningKeys.Select(pair =>
                new SymmetricSecurityKey(Convert.FromBase64String(pair.Value)) { KeyId = pair.Key }),
            ClockSkew = TimeSpan.FromMinutes(5),
            NameClaimType = "oid",
            RoleClaimType = "roles"
        };
        jwt.MapInboundClaims = false;
    }
}
