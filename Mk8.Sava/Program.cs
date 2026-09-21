using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using Mk8.Sava.Configuration;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

var builder = WebApplication.CreateBuilder(args);

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
builder.Services.AddSingleton<ChunkStore>();
builder.Services.AddSingleton<MetadataStore>();
builder.Services.AddSingleton<BlobService>();
builder.Services.AddSingleton<StorageAuthenticator>();
builder.Services.AddSingleton<AzureResponseWriter>();

var app = builder.Build();

await app.Services.GetRequiredService<MetadataStore>().InitializeAsync();

app.UseMiddleware<AzureExceptionMiddleware>();
app.UseMiddleware<RequestContextMiddleware>();
app.MapGet("/health/live", () => Results.Ok(new { status = "live" }));
app.MapGet("/health/ready", async (MetadataStore store, CancellationToken cancellationToken) =>
    await store.IsReadyAsync(cancellationToken)
        ? Results.Ok(new { status = "ready" })
        : Results.StatusCode(StatusCodes.Status503ServiceUnavailable));
app.Map("/{**storagePath}", BlobProtocolEndpoint.HandleAsync);

await app.RunAsync();

public partial class Program;
