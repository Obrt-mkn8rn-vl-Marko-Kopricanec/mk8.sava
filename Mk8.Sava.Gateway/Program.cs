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
        app.MapSavaGateway();
        await app.RunAsync().ConfigureAwait(false);
    }
}
