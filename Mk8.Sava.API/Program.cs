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

var builder = WebApplication.CreateBuilder(args);
var operatorCommand = ParseOperatorCommand(args);
builder.Logging.AddFilter("Microsoft.AspNetCore.Hosting.Diagnostics", LogLevel.Warning);

builder.Services.AddOptions<SavaOptions>()
    .Bind(builder.Configuration.GetSection(SavaOptions.SectionName))
    .ValidateDataAnnotations()
    .ValidateOnStart();
builder.Services
    .AddAuthentication()
    .AddJwtBearer(StorageAuthenticator.BearerScheme, _ => { });
builder.Services.AddOptions<JwtBearerOptions>(StorageAuthenticator.BearerScheme)
    .Configure<IOptions<SavaOptions>>((jwt, configuredOptions) =>
    {
        var bearerConfiguration = configuredOptions.Value.BearerAuthentication;
        if (!string.IsNullOrWhiteSpace(bearerConfiguration.Authority))
            jwt.Authority = bearerConfiguration.Authority;
        if (!string.IsNullOrWhiteSpace(bearerConfiguration.MetadataAddress))
            jwt.MetadataAddress = bearerConfiguration.MetadataAddress;
        jwt.RequireHttpsMetadata = bearerConfiguration.RequireHttpsMetadata;
        jwt.TokenValidationParameters = new TokenValidationParameters
        {
            ValidateIssuer = true,
            ValidIssuers = bearerConfiguration.ValidIssuers,
            ValidateAudience = true,
            ValidAudiences = bearerConfiguration.ValidAudiences,
            ValidateLifetime = true,
            RequireExpirationTime = true,
            ValidateIssuerSigningKey = true,
            IssuerSigningKeys = bearerConfiguration.SymmetricSigningKeys.Select(pair =>
                new SymmetricSecurityKey(Convert.FromBase64String(pair.Value)) { KeyId = pair.Key }),
            ClockSkew = TimeSpan.FromMinutes(5),
            NameClaimType = "oid",
            RoleClaimType = "roles"
        };
        jwt.MapInboundClaims = false;
    });
builder.Services.AddSingleton<StoragePaths>();
builder.Services.AddSingleton<IStoragePaths>(services => services.GetRequiredService<StoragePaths>());
builder.Services.AddSingleton<StorageTelemetry>();
builder.Services.AddSingleton<IStorageTelemetry>(services => services.GetRequiredService<StorageTelemetry>());
builder.Services.AddSingleton<IStorageFaultInjector, NullStorageFaultInjector>();
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<ChunkStore>();
builder.Services.AddSingleton<MetadataStore>();
builder.Services.AddSingleton<LeaseService>();
builder.Services.AddSingleton<StorageAnalyticsService>();
builder.Services.AddSingleton<IStorageAnalyticsSink>(services =>
    services.GetRequiredService<StorageAnalyticsService>());
builder.Services.AddSingleton<BlobService>();
builder.Services.AddSingleton<StorageBackupService>();
builder.Services.AddSingleton<StorageDataKeyContinuity>();
builder.Services.AddHostedService<StorageMaintenanceService>();
builder.Services.AddSingleton<StorageAuthenticator>();
builder.Services.AddSingleton<TokenCredential, DefaultAzureCredential>();
builder.Services.AddHttpClient<MicrosoftGraphGroupMembershipResolver>(client =>
    client.Timeout = TimeSpan.FromSeconds(5))
    .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
    {
        AllowAutoRedirect = false
    });
builder.Services.AddSingleton<IGroupMembershipResolver>(services =>
    services.GetRequiredService<MicrosoftGraphGroupMembershipResolver>());
builder.Services.AddHttpClient<UrlTransferClient>(client => client.Timeout = Timeout.InfiniteTimeSpan)
    .RemoveAllLoggers()
    .ConfigurePrimaryHttpMessageHandler(services =>
    {
        var egress = new UrlSourceEgressPolicy(services.GetRequiredService<IOptions<SavaOptions>>().Value);
        return new SocketsHttpHandler
        {
            // Source-only authorization and customer-provided encryption headers must not
            // be forwarded to an authority chosen by a redirecting copy source.
            AllowAutoRedirect = false,
            AutomaticDecompression = DecompressionMethods.None,
            UseProxy = false,
            ConnectTimeout = TimeSpan.FromSeconds(10),
            ConnectCallback = egress.ConnectAsync
        };
    });

var app = builder.Build();

if (operatorCommand is { Name: "restore" })
{
    var options = app.Services.GetRequiredService<IOptions<SavaOptions>>().Value;
    var environment = app.Services.GetRequiredService<IHostEnvironment>();
    var target = StoragePaths.ResolveRoot(environment.ContentRootPath, options.DataPath);
    var restored = await StorageBackupService.RestoreAsync(
        operatorCommand.Value.Path,
        target,
        options,
        CancellationToken.None).ConfigureAwait(false);
    Console.WriteLine(
        $"Restored {restored.BlobRecordCount} blob records and {restored.ChunkCount} chunks into '{target}'.");
    return;
}

if (operatorCommand is { Name: "validate" })
{
    var options = app.Services.GetRequiredService<IOptions<SavaOptions>>().Value;
    var validated = await StorageBackupService.ValidateBackupAsync(
        operatorCommand.Value.Path,
        options,
        CancellationToken.None).ConfigureAwait(false);
    Console.WriteLine(
        $"Validated backup '{validated.BackupPath}' with {validated.BlobRecordCount} blob records and {validated.ChunkCount} chunks.");
    return;
}

try
{
    await app.Services.GetRequiredService<MetadataStore>().InitializeAsync().ConfigureAwait(false);
    await app.Services.GetRequiredService<StorageDataKeyContinuity>().EnsureAsync(CancellationToken.None).ConfigureAwait(false);
    await app.Services.GetRequiredService<BlobService>().ApplyConfiguredAccountCapabilitiesAsync().ConfigureAwait(false);
    var prunedChunkDirectories = app.Services.GetRequiredService<StoragePaths>()
        .PruneLegacyEmptyChunkDirectories();
    if (prunedChunkDirectories > 0)
        StorageLogMessages.LegacyChunkDirectoriesPruned(app.Logger, prunedChunkDirectories);
}
catch
{
    await app.DisposeAsync().ConfigureAwait(false);
    throw;
}

if (operatorCommand is { Name: "create" })
{
    var created = await app.Services.GetRequiredService<StorageBackupService>().CreateAsync(
        operatorCommand.Value.Path,
        CancellationToken.None).ConfigureAwait(false);
    Console.WriteLine(
        $"Created backup '{created.BackupPath}' with {created.BlobRecordCount} blob records and {created.ChunkCount} chunks.");
    return;
}

if (operatorCommand is { Name: "acl-apply" })
{
    var applied = await app.Services.GetRequiredService<BlobService>().ApplyHierarchicalAclManifestAsync(
        operatorCommand.Value.Path,
        CancellationToken.None).ConfigureAwait(false);
    Console.WriteLine($"Applied HNS access ACLs to {applied} existing targets.");
    return;
}

app.UseMiddleware<StorageTelemetryMiddleware>();
app.UseMiddleware<AzureExceptionMiddleware>();
app.UseMiddleware<RequestContextMiddleware>();
app.MapGet("/health/live", () => Results.Ok(new { status = "live" }));
app.MapGet("/health/ready", async (
    MetadataStore store,
    IStorageTelemetry telemetry,
    CancellationToken cancellationToken) =>
{
    var metadataReady = await store.IsReadyAsync(cancellationToken).ConfigureAwait(false);
    var integrity = telemetry.Integrity;
    var ready = metadataReady && integrity.Healthy;
    return Results.Json(
        new
        {
            status = ready ? "ready" : "unavailable",
            metadata = metadataReady ? "ready" : "unavailable",
            integrity = new
            {
                status = integrity.Healthy ? "healthy" : "corrupt",
                integrity.Complete,
                integrity.CheckedChunks,
                integrity.MissingChunks,
                integrity.CorruptChunks,
                integrity.CustomerKeyChunks,
                checkedAt = integrity.CheckedAt == DateTimeOffset.MinValue ? null : integrity.CheckedAt.ToString("O")
            }
        },
        statusCode: ready ? StatusCodes.Status200OK : StatusCodes.Status503ServiceUnavailable);
});
app.MapGet("/metrics", (IStorageTelemetry telemetry) =>
    Results.Text(telemetry.RenderPrometheus(), "text/plain; version=0.0.4; charset=utf-8"));
app.Map("/{**storagePath}", BlobProtocolEndpoint.HandleAsync);

await app.RunAsync().ConfigureAwait(false);

static (string Name, string Path)? ParseOperatorCommand(string[] arguments)
{
    (string Name, string Path)? command = null;
    var names = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["--backup-create"] = "create",
        ["--backup-validate"] = "validate",
        ["--restore-from"] = "restore",
        ["--hns-acl-apply"] = "acl-apply"
    };
    for (var index = 0; index < arguments.Length; index++)
    {
        if (!names.TryGetValue(arguments[index], out var name))
            continue;
        if (command is not null)
            throw new ArgumentException("Specify only one operator command.", nameof(arguments));
        if (++index >= arguments.Length || string.IsNullOrWhiteSpace(arguments[index]))
            throw new ArgumentException($"The {arguments[index - 1]} command requires a path.", nameof(arguments));
        command = (name, arguments[index]);
    }
    return command;
}

#pragma warning disable CA1515 // Public test host entry point is required by WebApplicationFactory<Program> fixtures.
public partial class Program;
#pragma warning restore CA1515
