using System.Globalization;
using System.Net;
using Mk8.Sava.Hosting;

namespace Mk8.Sava.Gateway;

#pragma warning disable CA1515 // WebApplicationFactory<Program> requires a public, non-static host entry type.
public sealed partial class Program
#pragma warning restore CA1515
{
    private Program()
    {
    }

    public static async Task Main(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);
        if (args.Any(argument => argument is "--backup-create" or "--backup-validate" or "--restore-from" or "--hns-acl-apply"))
        {
            await Console.Error.WriteLineAsync("Storage operator commands belong to Mk8.Sava.Application, not Gateway.").ConfigureAwait(false);
            Environment.ExitCode = 1;
            return;
        }
        var builder = WebApplication.CreateBuilder(args);
        builder.Logging.AddFilter("Microsoft.AspNetCore.Hosting.Diagnostics", LogLevel.Warning);
        builder.Services.AddSavaGateway(builder.Configuration);
        var app = builder.Build();
        await using var disposal = app.ConfigureAwait(false);
        // Include deferred host configuration, but refuse before service startup.
        RequireListenerConfiguration(builder.Configuration);
        app.MapSavaGateway();
        await app.RunAsync().ConfigureAwait(false);
    }

    private static void RequireListenerConfiguration(ConfigurationManager configuration)
    {
        // Refuse the framework's implicit listener; Kestrel validates the supplied
        // URLs/endpoints and certificates when it starts. Ports explicitly select
        // the framework's wildcard semantics, not a site address chosen here.
        if (HasConfiguredUrls(configuration[WebHostDefaults.ServerUrlsKey]) ||
            HasConfiguredPorts(configuration[WebHostDefaults.HttpPortsKey]) ||
            HasConfiguredPorts(configuration[WebHostDefaults.HttpsPortsKey]) ||
            configuration.GetSection("Kestrel:Endpoints").GetChildren()
                .Any(endpoint => HasConfiguredUrls(endpoint["Url"])))
        {
            return;
        }
        throw new InvalidOperationException("A Gateway listener must be explicitly configured using URLs, ports, or Kestrel endpoints.");
    }

    private static bool HasConfiguredUrls(string? value) =>
        !string.IsNullOrWhiteSpace(value) &&
        value.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).Length > 0;

    private static bool HasConfiguredPorts(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return false;
        var ports = value.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        return ports.Length > 0 && ports.All(port =>
            int.TryParse(port, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) &&
            parsed is >= IPEndPoint.MinPort and <= IPEndPoint.MaxPort);
    }
}
