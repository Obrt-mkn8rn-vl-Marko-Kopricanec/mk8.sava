using System.ComponentModel.DataAnnotations;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Mk8.Sava.Configuration;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Transport;

public static class ApplicationTransportSecurity
{
    public const string ProtocolVersion = "mk8-sava-rpc-2";
    public const string ProtocolHeader = "X-Mk8-Sava-Transport";
    public const string PolicyHeader = "X-Mk8-Sava-Policy";
    public const string AccessKeyHeader = "X-Mk8-Sava-Access-Key";
    public const string ContentType = "application/vnd.mk8.sava.rpc";

    public static string CreatePolicyFingerprint(SavaOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        // Account/signing secrets participate only through SHA-256 fingerprints.
        // The resulting digest binds independently running hosts to the same
        // authentication and account policy, without serializing keys on the wire.
        var serialized = JsonSerializer.SerializeToElement(new
        {
            options.DefaultAccount,
            Accounts = options.Accounts.ToDictionary(pair => pair.Key, pair => HashSecret(pair.Value), StringComparer.Ordinal),
            options.AccountCapabilities,
            options.ObjectReplicationPolicies,
            options.AllowAnonymousPublicAccess,
            options.MaximumRequestBodyBytes,
            options.BearerAuthentication,
        });
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
            WriteCanonical(writer, serialized);
        return Convert.ToHexStringLower(SHA256.HashData(buffer.GetBuffer().AsSpan(0, checked((int)buffer.Length))));
    }

    internal static byte[] ReadAccessKey(ApplicationTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Validator.ValidateObject(options, new ValidationContext(options), validateAllProperties: true);
        var text = ReadProtectedKeyFile(options.AccessKeyFile);
        byte[] key;
        try
        {
            key = Convert.FromBase64String(text);
        }
        catch (FormatException exception) when (!CatastrophicExceptionPolicy.Contains(exception))
        {
            throw new InvalidOperationException("The application transport access-key file must contain a base64 key.");
        }
        if (key.Length < 32 || key.Length > 512)
        {
            CryptographicOperations.ZeroMemory(key);
            throw new InvalidOperationException("The application transport access key must contain between 256 and 4096 bits.");
        }
        return key;
    }

    private static string ReadProtectedKeyFile(string path)
    {
        try
        {
            if (!OperatingSystem.IsWindows())
            {
                var permissions = File.GetUnixFileMode(path);
                const UnixFileMode allowed = UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupRead;
                if ((permissions & ~allowed) != UnixFileMode.None)
                    throw new InvalidOperationException("The application transport access-key file permits access outside its owner and optional trusted read-only group.");
            }
            if (new FileInfo(path).Length > 4096)
                throw new InvalidOperationException("The application transport access-key file exceeds its permitted length.");
            return File.ReadAllText(path).Trim();
        }
        catch (IOException exception) when (!CatastrophicExceptionPolicy.Contains(exception))
        {
            throw new InvalidOperationException("The application transport access-key file could not be read.");
        }
        catch (UnauthorizedAccessException exception) when (!CatastrophicExceptionPolicy.Contains(exception))
        {
            throw new InvalidOperationException("The application transport access-key file could not be read.");
        }
    }

    internal static string HashSecret(string value) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(value)));

    private static void WriteCanonical(Utf8JsonWriter writer, JsonElement value)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.Object:
                writer.WriteStartObject();
                foreach (var property in value.EnumerateObject().OrderBy(property => property.Name, StringComparer.Ordinal))
                {
                    writer.WritePropertyName(property.Name);
                    WriteCanonical(writer, property.Value);
                }
                writer.WriteEndObject();
                break;
            case JsonValueKind.Array:
                writer.WriteStartArray();
                foreach (var item in value.EnumerateArray())
                    WriteCanonical(writer, item);
                writer.WriteEndArray();
                break;
            default:
                value.WriteTo(writer);
                break;
        }
    }
}
