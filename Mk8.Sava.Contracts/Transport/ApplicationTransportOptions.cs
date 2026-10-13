using System.ComponentModel.DataAnnotations;
using System.Net;
using System.Security.Cryptography;

namespace Mk8.Sava.Transport;

public sealed class ApplicationTransportOptions : IValidatableObject
{
    public const string SectionName = "ApplicationTransport";

    public Uri? Endpoint { get; init; }
    public string AccessKey { get; init; } = string.Empty;
    public int MaximumControlFrameBytes { get; init; } = 64 * 1024 * 1024;
    public TimeSpan ConnectTimeout { get; init; } = TimeSpan.FromSeconds(5);
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromMinutes(10);

    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (Endpoint is null || !IsAllowedEndpoint(Endpoint))
            yield return new ValidationResult("Application transport requires an absolute HTTPS endpoint, or HTTP on a literal loopback address, without credentials, query or fragment.", [nameof(Endpoint)]);
        if (string.IsNullOrWhiteSpace(AccessKey))
            yield return new ValidationResult("Application transport requires a Supprocom.Secrets access key.", [nameof(AccessKey)]);
        else if (!IsAllowedKey(AccessKey))
            yield return new ValidationResult("Application transport access key must be base64 with between 256 and 4096 decoded bits.", [nameof(AccessKey)]);
        if (MaximumControlFrameBytes is < 4096 or > 64 * 1024 * 1024)
            yield return new ValidationResult("MaximumControlFrameBytes must be between 4 KiB and 64 MiB.", [nameof(MaximumControlFrameBytes)]);
        if (ConnectTimeout <= TimeSpan.Zero || ConnectTimeout > TimeSpan.FromMinutes(1))
            yield return new ValidationResult("ConnectTimeout must be positive and no longer than one minute.", [nameof(ConnectTimeout)]);
        if (RequestTimeout <= TimeSpan.Zero || RequestTimeout > TimeSpan.FromHours(1))
            yield return new ValidationResult("RequestTimeout must be positive and no longer than one hour.", [nameof(RequestTimeout)]);
    }

    private static bool IsAllowedEndpoint(Uri endpoint) =>
        endpoint.IsAbsoluteUri && endpoint.UserInfo.Length == 0 && endpoint.Query.Length == 0 &&
        endpoint.Fragment.Length == 0 &&
        (string.Equals(endpoint.Scheme, Uri.UriSchemeHttps, StringComparison.Ordinal) ||
         (string.Equals(endpoint.Scheme, Uri.UriSchemeHttp, StringComparison.Ordinal) &&
          IPAddress.TryParse(endpoint.IdnHost, out var address) && IPAddress.IsLoopback(address)));

    private static bool IsAllowedKey(string value)
    {
        if (value.Length > 4096)
            return false;
        Span<byte> decoded = stackalloc byte[512];
        try
        {
            return Convert.TryFromBase64String(value, decoded, out var length) && length is >= 32 and <= 512;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(decoded);
        }
    }
}
