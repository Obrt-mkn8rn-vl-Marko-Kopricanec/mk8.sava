using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Supprocom.Secrets;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Hosting;

public static class SavaServiceConfiguration
{
    public static async Task<WebApplicationBuilder> CreateBuilderAsync(string[] arguments)
    {
        ArgumentNullException.ThrowIfNull(arguments);
        for (var index = 0; index < arguments.Length; index++)
        {
            if (arguments[index] is "--backup-create" or "--backup-validate" or "--restore-from" or "--hns-acl-apply")
            {
                if (++index >= arguments.Length || string.IsNullOrWhiteSpace(arguments[index]))
                    throw new ArgumentException("The operator command requires a path.", nameof(arguments));
            }
            else
            {
                throw new ArgumentException("Service settings must be supplied through Supprocom.Secrets, not command-line configuration.", nameof(arguments));
            }
        }
        var builder = WebApplication.CreateEmptyBuilder(new WebApplicationOptions
        {
            Args = [],
            EnvironmentName = Environments.Production,
            ContentRootPath = AppContext.BaseDirectory,
        });
        await LoadAsync(builder.Configuration, Path.Combine(AppContext.BaseDirectory, "Environment")).ConfigureAwait(false);
        builder.WebHost.UseKestrel(server =>
            server.Configure(builder.Configuration.GetSection("Kestrel"), reloadOnChange: false));
        builder.Host.UseConsoleLifetime();
        builder.Services.AddRouting();
        builder.Logging.AddConsole();
        return builder;
    }

    public static async Task LoadAsync(ConfigurationManager configuration, string directory)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        var options = new SupprocomSecretsOptions
        {
            FileOverridesProcessEnvironment = true,
            EnvironmentName = Environments.Production,
        };
        options.File.Directory = Path.GetFullPath(directory);
        options.File.Import = SecretFileImport.None;
        options.File.Recovery = SecretFileRecovery.None;
        options.File.Protection = SecretFileProtection.InstallationBoundAesGcm;
        // The built-in store refuses pointers before any adapter discovery.
        // Only its once-loaded snapshot is projected into framework configuration.
        configuration.Sources.Clear();
        try
        {
            var store = new SupprocomSecretFileStore(options);
            var state = await store.GetStateAsync(options.StartupCancellationToken).ConfigureAwait(false);
            if (state == SecretFileProtectionState.Plaintext)
                ValidatePlaintextPermissions(Path.Combine(options.File.Directory, ".env"));
            var snapshot = await store.LoadAsync(options.StartupCancellationToken).ConfigureAwait(false);
            configuration.AddInMemoryCollection(snapshot.Select(pair =>
                new KeyValuePair<string, string?>(pair.Key, pair.Value)));
        }
        catch (SupprocomSecretsException exception) when (!CatastrophicExceptionPolicy.Contains(exception))
        {
            // Expose only the stable code, never values or an exception payload.
            throw new InvalidOperationException("Supprocom.Secrets startup rejected: " + exception.Code);
        }
    }

    public static void RequireRole(IConfiguration configuration, string role)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(role);
        if (!string.Equals(configuration["Service:Role"], role, StringComparison.Ordinal))
            throw new InvalidOperationException("Required configuration key Service:Role is missing or invalid.");
        foreach (var key in new[] { "Sava:DefaultAccount", "Sava:DataPath", "ApplicationTransport:AccessKey" })
        {
            if (string.IsNullOrWhiteSpace(configuration[key]))
                throw new InvalidOperationException("Required configuration key " + key + " is missing or invalid.");
        }
        if (string.Equals(role, "Gateway", StringComparison.Ordinal) && string.IsNullOrWhiteSpace(configuration["Gateway:StagingPath"]))
            throw new InvalidOperationException("Required configuration key Gateway:StagingPath is missing or invalid.");
    }

    private static void ValidatePlaintextPermissions(string path)
    {
        if (OperatingSystem.IsWindows())
            return;
        const UnixFileMode allowed = UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupRead;
        if ((File.GetUnixFileMode(path) & ~allowed) != UnixFileMode.None)
            throw new InvalidOperationException("The Supprocom.Secrets collection permits access outside its owner and optional trusted read-only group.");
    }
}
