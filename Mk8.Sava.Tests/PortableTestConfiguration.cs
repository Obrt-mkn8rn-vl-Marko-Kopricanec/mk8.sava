using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace Mk8.Sava.Tests;

// Existing fixture overrides remain caller inputs, but the actual child receives
// an independent installation-local collection loaded with Supprocom.Secrets.
internal static class PortableTestConfiguration
{
    public static async Task MaterializeAsync(ProcessStartInfo start, string root)
    {
        ArgumentNullException.ThrowIfNull(start);
        ArgumentException.ThrowIfNullOrWhiteSpace(root);
        var settings = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        foreach (var pair in start.Environment.ToArray())
        {
            var key = pair.Key;
            if (key.StartsWith("ASPNETCORE_", StringComparison.OrdinalIgnoreCase))
            {
                key = key[11..];
            }
            else if (!key.StartsWith("Sava__", StringComparison.OrdinalIgnoreCase) &&
                     !key.StartsWith("Service__", StringComparison.OrdinalIgnoreCase) &&
                     !key.StartsWith("Gateway__", StringComparison.OrdinalIgnoreCase) &&
                     !key.StartsWith("ApplicationTransport__", StringComparison.OrdinalIgnoreCase) &&
                     !key.StartsWith("ApplicationHosting__", StringComparison.OrdinalIgnoreCase) &&
                     !key.StartsWith("Kestrel__", StringComparison.OrdinalIgnoreCase) &&
                     !key.StartsWith("Logging__", StringComparison.OrdinalIgnoreCase) &&
                     !key.Equals("URLS", StringComparison.OrdinalIgnoreCase) &&
                     !key.Equals("HTTP_PORTS", StringComparison.OrdinalIgnoreCase) &&
                     !key.Equals("HTTPS_PORTS", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }
            start.Environment.Remove(pair.Key);
            if (key.Equals("ENVIRONMENT", StringComparison.OrdinalIgnoreCase))
                continue;
            settings[key.Replace("__", ":", StringComparison.Ordinal)] = pair.Value;
        }
        await MaterializeCertificateAsync(settings).ConfigureAwait(false);
        start.Environment.Remove("DOTNET_ENVIRONMENT");
        var publishRoot = Path.Combine(root, "installation-" + Guid.NewGuid().ToString("N"));
        settings["Service:Role"] = Path.GetFileName(start.ArgumentList[0]).Contains("Application", StringComparison.Ordinal)
            ? "Application" : "Gateway";
        Directory.CreateDirectory(publishRoot);
        foreach (var path in Directory.EnumerateFiles(AppContext.BaseDirectory))
            File.Copy(path, Path.Combine(publishRoot, Path.GetFileName(path)));
        foreach (var path in Directory.EnumerateDirectories(AppContext.BaseDirectory).Where(path =>
                     !string.Equals(Path.GetFileName(path), "Environment", StringComparison.Ordinal)))
        {
            CopyDirectory(path, Path.Combine(publishRoot, Path.GetFileName(path)));
        }
        start.ArgumentList[0] = Path.Combine(publishRoot, Path.GetFileName(start.ArgumentList[0]));
        start.WorkingDirectory = publishRoot;
        var directory = Path.Combine(publishRoot, "Environment");
        Directory.CreateDirectory(directory);
        var creation = new FileStreamOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write };
        if (!OperatingSystem.IsWindows())
            creation.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        var file = new FileStream(Path.Combine(directory, ".env"), creation);
        await using (file.ConfigureAwait(false))
        {
            var document = string.Join('\n', settings.Where(pair => pair.Value is not null).Select(pair =>
                pair.Key.Replace(":", "__", StringComparison.Ordinal) + "=" + JsonSerializer.Serialize(pair.Value))) + "\n";
            await file.WriteAsync(Encoding.UTF8.GetBytes(document)).ConfigureAwait(false);
        }
    }

    private static async Task MaterializeCertificateAsync(Dictionary<string, string?> settings)
    {
        // Existing synthetic PFX fixtures are constructor inputs, not a child
        // secret source. Actual services receive only encrypted-store values.
        if (settings.Remove("Kestrel:Certificates:Default:Path", out var certificateFile) && certificateFile is not null)
            settings["Service:CertificateBase64"] = Convert.ToBase64String(await File.ReadAllBytesAsync(certificateFile).ConfigureAwait(false));
        if (settings.Remove("Kestrel:Certificates:Default:Password", out var certificatePassword))
            settings["Service:CertificatePassword"] = certificatePassword;
    }

    private static void CopyDirectory(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (var file in Directory.EnumerateFiles(source))
            File.Copy(file, Path.Combine(destination, Path.GetFileName(file)));
        foreach (var directory in Directory.EnumerateDirectories(source))
            CopyDirectory(directory, Path.Combine(destination, Path.GetFileName(directory)));
    }
}
