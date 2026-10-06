using System.Net;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Application;

#pragma warning disable CA1515 // The independently executable host is also a typed entry-point boundary for external integration fixtures.
public static class ApplicationProgram
#pragma warning restore CA1515
{
    public static async Task Main(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);
        var command = ParseOperatorCommand(args);
        var builder = CreateBuilder(args);
        using var certificate = command is null ? ConfigureRpcHost(builder) : null;
        var app = builder.Build();
        await using var appDisposal = app.ConfigureAwait(false);
        if (await RunOperatorAsync(app, command).ConfigureAwait(false))
            return;
        MapEndpoints(app);
        try
        {
            await app.RunAsync().ConfigureAwait(false);
        }
        catch (StorageRootLeaseException exception)
        {
            ReportRootLeaseFailure(exception);
        }
    }

    private static WebApplicationBuilder CreateBuilder(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.Logging.AddFilter("Microsoft.AspNetCore.Hosting.Diagnostics", LogLevel.Warning);
        builder.Services.AddOptions<SavaOptions>()
            .Bind(builder.Configuration.GetSection(SavaOptions.SectionName))
            .ValidateDataAnnotations().ValidateOnStart();
        builder.Services.AddSavaApplication();
        return builder;
    }

    private static X509Certificate2? ConfigureRpcHost(WebApplicationBuilder builder)
    {
        builder.Services.AddOptions<ApplicationTransportOptions>()
            .Bind(builder.Configuration.GetSection(ApplicationTransportOptions.SectionName))
            .ValidateDataAnnotations().ValidateOnStart();
        var transport = builder.Configuration.GetSection(ApplicationTransportOptions.SectionName)
            .Get<ApplicationTransportOptions>() ?? new ApplicationTransportOptions();
        var endpoint = transport.Endpoint ?? throw new InvalidOperationException("An Application endpoint is required.");
        if (!IPAddress.TryParse(endpoint.IdnHost, out var address))
            throw new InvalidOperationException("The Application listener endpoint must use a literal IP address.");
        if (endpoint.AbsolutePath is not "/internal/application" || endpoint.Query.Length != 0 || endpoint.Fragment.Length != 0)
            throw new InvalidOperationException("The Application endpoint must use the exact /internal/application path.");
        if (!string.Equals(endpoint.Scheme, Uri.UriSchemeHttps, StringComparison.Ordinal) &&
            (!string.Equals(endpoint.Scheme, Uri.UriSchemeHttp, StringComparison.Ordinal) || !IPAddress.IsLoopback(address)))
        {
            throw new InvalidOperationException("A non-loopback Application listener requires HTTPS.");
        }
        var certificate = LoadCertificate(builder, endpoint);
        // Explicit endpoints replace inherited URLs/HTTP_PORTS/Kestrel endpoints.
        builder.WebHost.UseSetting(WebHostDefaults.ServerUrlsKey, string.Empty);
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.Configure(new ConfigurationBuilder().Build(), reloadOnChange: false);
            // This private listener accepts authenticated framed RPC, not public
            // Blob bodies. The endpoint imposes a finite envelope bound after
            // authentication, then checks control and logical stream limits.
            kestrel.Limits.MaxRequestBodySize = null;
            kestrel.Listen(address, endpoint.Port, listen =>
            {
                if (certificate is not null)
                    listen.UseHttps(certificate);
            });
        });
        builder.Services.AddSingleton(provider => new ApplicationRpcEndpoint(
            provider.GetRequiredService<IOptions<ApplicationTransportOptions>>().Value,
            provider.GetRequiredService<IOptions<SavaOptions>>().Value,
            provider.GetRequiredService<ILogger<ApplicationRpcEndpoint>>(),
            provider.GetRequiredService<IApplicationRpcAdmission>()));
        return certificate;
    }

    private static X509Certificate2? LoadCertificate(WebApplicationBuilder builder, Uri endpoint)
    {
        if (!string.Equals(endpoint.Scheme, Uri.UriSchemeHttps, StringComparison.Ordinal))
            return null;
        var hosting = builder.Configuration.GetSection(ApplicationHostingOptions.SectionName)
            .Get<ApplicationHostingOptions>() ?? new ApplicationHostingOptions();
        if (string.IsNullOrWhiteSpace(hosting.CertificateFile))
            throw new InvalidOperationException("An HTTPS Application listener requires ApplicationHosting:CertificateFile.");
        var certificate = X509CertificateLoader.LoadPkcs12FromFile(
            Path.GetFullPath(hosting.CertificateFile, builder.Environment.ContentRootPath),
            hosting.CertificatePassword, X509KeyStorageFlags.EphemeralKeySet);
        if (!certificate.HasPrivateKey)
        {
            certificate.Dispose();
            throw new InvalidOperationException("The Application TLS certificate must contain its private key.");
        }
        return certificate;
    }

    private static async Task<bool> RunOperatorAsync(WebApplication app, (string Name, string Path)? command)
    {
        if (command is null)
            return false;
        var options = app.Services.GetRequiredService<IOptions<SavaOptions>>().Value;
        if (command is { Name: "restore" })
        {
            var target = StoragePaths.ResolveRoot(app.Environment.ContentRootPath, options.DataPath);
            var restored = await StorageBackupService.RestoreAsync(
                command.Value.Path, target, options, CancellationToken.None).ConfigureAwait(false);
            Console.WriteLine($"Restored {restored.BlobRecordCount} blob records and {restored.ChunkCount} chunks into '{target}'.");
            return true;
        }
        if (command is { Name: "validate" })
        {
            var validated = await StorageBackupService.ValidateBackupAsync(
                command.Value.Path, options, CancellationToken.None).ConfigureAwait(false);
            Console.WriteLine($"Validated backup '{validated.BackupPath}' with {validated.BlobRecordCount} blob records and {validated.ChunkCount} chunks.");
            return true;
        }
        try
        {
            await app.Services.GetRequiredService<ApplicationInitializationService>()
                .InitializeAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (StorageRootLeaseException exception)
        {
            ReportRootLeaseFailure(exception);
            return true;
        }
        if (command is { Name: "create" })
        {
            var backup = await app.Services.GetRequiredService<StorageBackupService>()
                .CreateAsync(command.Value.Path, CancellationToken.None).ConfigureAwait(false);
            Console.WriteLine($"Created backup '{backup.BackupPath}' with {backup.BlobRecordCount} blob records and {backup.ChunkCount} chunks.");
            return true;
        }
        if (command is { Name: "acl-apply" })
        {
            var applied = await app.Services.GetRequiredService<BlobService>()
                .ApplyHierarchicalAclManifestAsync(command.Value.Path, CancellationToken.None).ConfigureAwait(false);
            Console.WriteLine($"Applied HNS access ACLs to {applied} existing targets.");
            return true;
        }
        throw new InvalidOperationException("The operator command is not supported.");
    }

    private static void MapEndpoints(WebApplication app)
    {
        // Fail missing/malformed transport credentials before any listener starts.
        var rpc = app.Services.GetRequiredService<ApplicationRpcEndpoint>();
        app.Use(async (context, next) =>
        {
            if ((context.Request.Path.StartsWithSegments("/health", StringComparison.OrdinalIgnoreCase) ||
                 context.Request.Path.StartsWithSegments("/metrics", StringComparison.OrdinalIgnoreCase)) &&
                (context.Connection.RemoteIpAddress is not { } remote || !IPAddress.IsLoopback(remote)) &&
                !rpc.IsAuthenticated(context))
            {
                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                return;
            }
            await next(context).ConfigureAwait(false);
        });
        app.MapGet("/health/live", () => Results.Ok(new { status = "live" }));
        app.MapGet("/health/ready", async (IApplicationReadiness readiness, CancellationToken cancellationToken) =>
        {
            var state = await readiness.GetAsync(cancellationToken).ConfigureAwait(false);
            return Results.Json(state, statusCode: state.Ready ? StatusCodes.Status200OK : StatusCodes.Status503ServiceUnavailable);
        });
        app.MapGet("/metrics", async (IApplicationReadiness readiness, CancellationToken cancellationToken) =>
            Results.Text(await readiness.RenderStorageMetricsAsync(cancellationToken).ConfigureAwait(false),
                "text/plain; version=0.0.4; charset=utf-8"));
        app.MapPost("/internal/application", (HttpContext context, IApplicationRpcDispatcher dispatcher) =>
            rpc.HandleAsync(context, dispatcher));
        app.MapPost("/internal/application/control", (HttpContext context, IApplicationRpcDispatcher dispatcher) =>
            rpc.HandleControlAsync(context, dispatcher));
        app.MapFallback(() => Results.NotFound());
    }

    private static void ReportRootLeaseFailure(StorageRootLeaseException exception)
    {
        Console.Error.WriteLine(exception.Message);
        Environment.ExitCode = 1;
    }

    private static (string Name, string Path)? ParseOperatorCommand(string[] arguments)
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
            if (command.HasValue)
                throw new ArgumentException("Specify only one operator command.", nameof(arguments));
            if (++index >= arguments.Length || string.IsNullOrWhiteSpace(arguments[index]))
                throw new ArgumentException($"The {arguments[index - 1]} command requires a path.", nameof(arguments));
            command = (name, arguments[index]);
        }
        return command;
    }
}
