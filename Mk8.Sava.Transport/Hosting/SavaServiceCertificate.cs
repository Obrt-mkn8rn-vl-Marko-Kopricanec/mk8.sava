using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Configuration;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Hosting;

public static class SavaServiceCertificate
{
    public static X509Certificate2? Load(IConfiguration configuration, bool required)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var encoded = configuration["Service:CertificateBase64"];
        if (string.IsNullOrWhiteSpace(encoded))
        {
            if (required)
                throw new InvalidOperationException("An HTTPS listener requires Service:CertificateBase64.");
            return null;
        }
        byte[] payload;
        try
        {
            payload = Convert.FromBase64String(encoded);
        }
        catch (FormatException exception) when (!CatastrophicExceptionPolicy.Contains(exception))
        {
            throw new InvalidOperationException("Service:CertificateBase64 is invalid.");
        }
        try
        {
            var certificate = X509CertificateLoader.LoadPkcs12(payload,
                configuration["Service:CertificatePassword"], X509KeyStorageFlags.EphemeralKeySet);
            if (certificate.HasPrivateKey)
                return certificate;
            certificate.Dispose();
            throw new InvalidOperationException("Service:CertificateBase64 must contain a private key.");
        }
        catch (CryptographicException exception) when (!CatastrophicExceptionPolicy.Contains(exception))
        {
            throw new InvalidOperationException("Service certificate configuration is invalid.");
        }
        finally
        {
            CryptographicOperations.ZeroMemory(payload);
        }
    }

    public static bool RequiresGatewayHttps(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        if (configuration.GetSection("Kestrel:Endpoints").GetChildren().Any(endpoint => IsHttps(endpoint["Url"])))
            return true;
        var urls = configuration["urls"];
        if (!string.IsNullOrEmpty(urls))
            return urls.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).Any(IsHttps);
        return !string.IsNullOrWhiteSpace(configuration["https_ports"]);
    }

    public static void RefuseLegacySources(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        if (configuration.AsEnumerable().Any(pair =>
            (pair.Key.StartsWith("Kestrel:", StringComparison.OrdinalIgnoreCase) &&
             (pair.Key.Contains(":Certificate:", StringComparison.OrdinalIgnoreCase) ||
              pair.Key.StartsWith("Kestrel:Certificates:", StringComparison.OrdinalIgnoreCase))) ||
            pair.Key.StartsWith("ApplicationHosting:Certificate", StringComparison.OrdinalIgnoreCase)))
        {
            throw new InvalidOperationException("Certificate files and stores are not approved; use Service:CertificateBase64 through Supprocom.Secrets.");
        }
    }

    private static bool IsHttps(string? value) => Uri.TryCreate(value, UriKind.Absolute, out var endpoint) &&
        string.Equals(endpoint.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase);
}
