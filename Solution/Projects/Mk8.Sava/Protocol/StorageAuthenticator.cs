using System.Globalization;
using System.Net;
using System.Text;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Protocol;

public enum StorageAuthorizationKind
{
    Anonymous,
    SharedKey,
    Sas,
    Bearer
}

public sealed record StorageAuthorization(
    StorageAuthorizationKind Kind,
    string Permissions,
    DateTimeOffset? StartsAt = null,
    DateTimeOffset? ExpiresAt = null,
    string? Identifier = null)
{
    public static StorageAuthorization Anonymous { get; } = new(StorageAuthorizationKind.Anonymous, string.Empty);
    public static StorageAuthorization Owner { get; } = new(StorageAuthorizationKind.SharedKey, "racwdxltmeop");

    public bool Allows(char permission) => Kind == StorageAuthorizationKind.SharedKey || Permissions.Contains(permission, StringComparison.Ordinal);
}

public sealed class StorageAuthenticator(IOptions<SavaOptions> options)
{
    private readonly SavaOptions _options = options.Value;

    public Task<StorageAuthorization> AuthenticateAsync(
        HttpContext context,
        StorageRequestContext request,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var authorization = context.Request.Headers.Authorization.ToString();
        if (authorization.StartsWith("SharedKey ", StringComparison.Ordinal))
            return Task.FromResult(AuthenticateSharedKey(context.Request, request, authorization, lite: false));
        if (authorization.StartsWith("SharedKeyLite ", StringComparison.Ordinal))
            return Task.FromResult(AuthenticateSharedKey(context.Request, request, authorization, lite: true));
        if (authorization.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
            throw AzureStorageException.AuthenticationFailed("Bearer authentication is not configured for this deployment.");
        if (context.Request.Query.ContainsKey("sig"))
            return Task.FromResult(AuthenticateSas(context, request));
        return Task.FromResult(StorageAuthorization.Anonymous);
    }

    private StorageAuthorization AuthenticateSharedKey(
        HttpRequest httpRequest,
        StorageRequestContext request,
        string authorization,
        bool lite)
    {
        var separator = authorization.IndexOf(' ');
        var value = authorization[(separator + 1)..];
        var colon = value.IndexOf(':');
        if (colon <= 0 || colon == value.Length - 1)
            throw AzureStorageException.AuthenticationFailed();

        var account = value[..colon];
        var suppliedSignature = value[(colon + 1)..];
        if (!string.Equals(account, request.Account, StringComparison.Ordinal) || !_options.Accounts.TryGetValue(account, out var encodedKey))
            throw AzureStorageException.AuthenticationFailed();

        ValidateRequestTime(httpRequest);
        var stringToSign = lite
            ? BuildSharedKeyLiteString(httpRequest, request)
            : BuildSharedKeyString(httpRequest, request);
        var expected = Sign(encodedKey, stringToSign);
        if (!FixedTimeEquals(expected, suppliedSignature))
            throw AzureStorageException.AuthenticationFailed("Server failed to authenticate the request. Make sure the value of the Authorization header is formed correctly including the signature.");
        return StorageAuthorization.Owner;
    }

    private StorageAuthorization AuthenticateSas(HttpContext context, StorageRequestContext request)
    {
        var query = context.Request.Query;
        var version = query["sv"].ToString();
        var suppliedSignature = query["sig"].ToString();
        if (string.IsNullOrEmpty(version) || string.IsNullOrEmpty(suppliedSignature))
            throw AzureStorageException.AuthenticationFailed();
        if (!_options.Accounts.TryGetValue(request.Account, out var encodedKey))
            throw AzureStorageException.AuthenticationFailed();

        var permissions = query["sp"].ToString();
        var startsAt = ParseSasTime(query["st"].ToString());
        var expiresAt = ParseSasTime(query["se"].ToString());
        var now = DateTimeOffset.UtcNow;
        if (startsAt is { } start && now < start.AddMinutes(-5))
            throw AzureStorageException.AuthenticationFailed("Signature not valid in the specified time frame.");
        if (expiresAt is null || now > expiresAt.Value.AddMinutes(5))
            throw AzureStorageException.AuthenticationFailed("Signature not valid in the specified time frame.");

        var protocol = query["spr"].ToString();
        if (protocol == "https" && !context.Request.IsHttps)
            throw AzureStorageException.AuthenticationFailed("The request protocol is not permitted by the signed protocol field.");

        var signedIp = query["sip"].ToString();
        if (!string.IsNullOrEmpty(signedIp) && !MatchesIpRange(context.Connection.RemoteIpAddress, signedIp))
            throw AzureStorageException.AuthenticationFailed("The request IP address is not permitted by the signed IP field.");

        string stringToSign;
        if (query.ContainsKey("ss"))
        {
            stringToSign = string.Join('\n',
                request.Account,
                permissions,
                query["ss"].ToString(),
                query["srt"].ToString(),
                query["st"].ToString(),
                query["se"].ToString(),
                signedIp,
                protocol,
                version,
                query["ses"].ToString(),
                string.Empty);
        }
        else
        {
            var resourceType = query["sr"].ToString();
            var canonicalizedResource = $"/blob/{request.CanonicalResourcePath.TrimStart('/')}";
            stringToSign = string.Join('\n',
                permissions,
                query["st"].ToString(),
                query["se"].ToString(),
                canonicalizedResource,
                query["si"].ToString(),
                signedIp,
                protocol,
                version,
                resourceType,
                query["sdd"].ToString(),
                query["ses"].ToString(),
                query["rscc"].ToString(),
                query["rscd"].ToString(),
                query["rsce"].ToString(),
                query["rscl"].ToString(),
                query["rsct"].ToString());
        }

        var expected = Sign(encodedKey, stringToSign);
        if (!FixedTimeEquals(expected, suppliedSignature))
            throw AzureStorageException.AuthenticationFailed();

        return new StorageAuthorization(StorageAuthorizationKind.Sas, permissions, startsAt, expiresAt, query["si"].ToString());
    }

    private static string BuildSharedKeyString(HttpRequest request, StorageRequestContext context)
    {
        var contentLength = request.ContentLength is > 0 ? request.ContentLength.Value.ToString(CultureInfo.InvariantCulture) : string.Empty;
        return string.Join('\n',
            request.Method,
            Header(request, "Content-Encoding"),
            Header(request, "Content-Language"),
            contentLength,
            Header(request, "Content-MD5"),
            Header(request, "Content-Type"),
            Header(request, "Date"),
            Header(request, "If-Modified-Since"),
            Header(request, "If-Match"),
            Header(request, "If-None-Match"),
            Header(request, "If-Unmodified-Since"),
            Header(request, "Range"),
            BuildCanonicalizedHeaders(request) + BuildCanonicalizedResource(request, context));
    }

    private static string BuildSharedKeyLiteString(HttpRequest request, StorageRequestContext context) =>
        Header(request, "Date") + "\n" + BuildCanonicalizedHeaders(request) + BuildCanonicalizedResource(request, context);

    private static string BuildCanonicalizedHeaders(HttpRequest request)
    {
        var builder = new StringBuilder();
        foreach (var header in request.Headers
                     .Where(header => header.Key.StartsWith("x-ms-", StringComparison.OrdinalIgnoreCase))
                     .OrderBy(header => header.Key, StringComparer.OrdinalIgnoreCase))
        {
            var name = header.Key.ToLowerInvariant();
            var value = string.Join(',', header.Value.Select(CollapseWhitespace));
            builder.Append(name).Append(':').Append(value).Append('\n');
        }
        return builder.ToString();
    }

    private static string BuildCanonicalizedResource(HttpRequest request, StorageRequestContext context)
    {
        var builder = new StringBuilder()
            .Append('/')
            .Append(context.Account)
            .Append(request.Path.Value ?? "/");
        foreach (var parameter in request.Query
                     .OrderBy(parameter => parameter.Key, StringComparer.OrdinalIgnoreCase))
        {
            builder.Append('\n')
                .Append(parameter.Key.ToLowerInvariant())
                .Append(':')
                .Append(string.Join(',', parameter.Value.OrderBy(value => value, StringComparer.Ordinal)));
        }
        return builder.ToString();
    }

    private static string Header(HttpRequest request, string name) => request.Headers[name].ToString();

    private static string CollapseWhitespace(string? value) =>
        string.Join(' ', (value ?? string.Empty).Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

    private static string Sign(string encodedKey, string value)
    {
        using var hmac = new HMACSHA256(Convert.FromBase64String(encodedKey));
        return Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes(value)));
    }

    private static bool FixedTimeEquals(string expected, string supplied)
    {
        try
        {
            return CryptographicOperations.FixedTimeEquals(Convert.FromBase64String(expected), Convert.FromBase64String(supplied));
        }
        catch (FormatException)
        {
            return false;
        }
    }

    private static void ValidateRequestTime(HttpRequest request)
    {
        var value = request.Headers["x-ms-date"].FirstOrDefault() ?? request.Headers.Date.FirstOrDefault();
        if (!DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var timestamp) ||
            Math.Abs((DateTimeOffset.UtcNow - timestamp).TotalMinutes) > 15)
        {
            throw AzureStorageException.AuthenticationFailed("The date header in the request is invalid or outside the permitted time window.");
        }
    }

    private static DateTimeOffset? ParseSasTime(string value)
    {
        if (string.IsNullOrEmpty(value))
            return null;
        return DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed)
            ? parsed
            : throw AzureStorageException.AuthenticationFailed("The signed time is invalid.");
    }

    private static bool MatchesIpRange(IPAddress? address, string range)
    {
        if (address is null)
            return false;
        var values = range.Split('-', 2);
        if (!IPAddress.TryParse(values[0], out var start))
            return false;
        if (values.Length == 1)
            return address.Equals(start);
        if (!IPAddress.TryParse(values[1], out var end))
            return false;
        var candidateBytes = address.MapToIPv6().GetAddressBytes();
        return Compare(candidateBytes, start.MapToIPv6().GetAddressBytes()) >= 0 &&
               Compare(candidateBytes, end.MapToIPv6().GetAddressBytes()) <= 0;
    }

    private static int Compare(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right) => left.SequenceCompareTo(right);
}
