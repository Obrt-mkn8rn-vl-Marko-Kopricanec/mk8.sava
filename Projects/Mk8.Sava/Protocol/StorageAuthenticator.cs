using System.Globalization;
using System.Net;
using System.Text;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;

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
    string? Identifier = null,
    bool IsAccountSas = false,
    string? SignedResource = null)
{
    public static StorageAuthorization Anonymous { get; } = new(StorageAuthorizationKind.Anonymous, string.Empty);
    public static StorageAuthorization Owner { get; } = new(StorageAuthorizationKind.SharedKey, "racwdxltmeop");

    public bool Allows(char permission) => Kind == StorageAuthorizationKind.SharedKey || Permissions.Contains(permission, StringComparison.Ordinal);
}

public sealed class StorageAuthenticator(IOptions<SavaOptions> options, MetadataStore metadata)
{
    private readonly SavaOptions _options = options.Value;

    public async Task<StorageAuthorization> AuthenticateAsync(
        HttpContext context,
        StorageRequestContext request,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var authorization = context.Request.Headers.Authorization.ToString();
        if (authorization.StartsWith("SharedKey ", StringComparison.Ordinal))
            return AuthenticateSharedKey(context.Request, request, authorization, lite: false);
        if (authorization.StartsWith("SharedKeyLite ", StringComparison.Ordinal))
            return AuthenticateSharedKey(context.Request, request, authorization, lite: true);
        if (authorization.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
            throw AzureStorageException.AuthenticationFailed("Bearer authentication is not configured for this deployment.");
        if (context.Request.Query.ContainsKey("sig"))
            return await AuthenticateSasAsync(context, request, cancellationToken);
        return StorageAuthorization.Anonymous;
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

    private async Task<StorageAuthorization> AuthenticateSasAsync(
        HttpContext context,
        StorageRequestContext request,
        CancellationToken cancellationToken)
    {
        var query = context.Request.Query;
        var version = query["sv"].ToString();
        var suppliedSignature = query["sig"].ToString();
        if (!DateOnly.TryParseExact(version, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var signedVersion) ||
            signedVersion < new DateOnly(2015, 4, 5) ||
            string.IsNullOrEmpty(suppliedSignature))
            throw AzureStorageException.AuthenticationFailed();
        if (!_options.Accounts.TryGetValue(request.Account, out var encodedKey))
            throw AzureStorageException.AuthenticationFailed();

        var protocol = query["spr"].ToString();
        if (protocol is not ("" or "https" or "https,http") || protocol == "https" && !context.Request.IsHttps)
            throw AzureStorageException.AuthenticationFailed("The request protocol is not permitted by the signed protocol field.");

        var signedIp = query["sip"].ToString();
        if (!string.IsNullOrEmpty(signedIp) && !MatchesIpRange(context.Connection.RemoteIpAddress, signedIp))
            throw AzureStorageException.AuthenticationFailed("The request IP address is not permitted by the signed IP field.");

        var permissions = query["sp"].ToString();
        var startsAt = ParseSasTime(query["st"].ToString());
        var expiresAt = ParseSasTime(query["se"].ToString());
        string stringToSign;
        var isAccountSas = query.ContainsKey("ss");
        var signedResource = string.Empty;
        if (isAccountSas)
        {
            var services = query["ss"].ToString();
            var resourceTypes = query["srt"].ToString();
            if (!services.Contains('b') || !AccountSasCoversRequest(resourceTypes, request))
                throw AzureStorageException.AuthorizationFailure();
            var fields = new List<string>
            {
                request.Account,
                permissions,
                services,
                resourceTypes,
                query["st"].ToString(),
                query["se"].ToString(),
                signedIp,
                protocol,
                version
            };
            if (signedVersion >= new DateOnly(2020, 12, 6))
                fields.Add(query["ses"].ToString());
            stringToSign = string.Join('\n', fields) + "\n";
        }
        else
        {
            var resourceType = query["sr"].ToString();
            signedResource = resourceType;
            if (!ServiceSasCoversRequest(resourceType, request))
                throw AzureStorageException.AuthorizationFailure();
            var canonicalizedResource = BuildSasCanonicalResource(request, resourceType);
            var fields = new List<string>
            {
                permissions,
                query["st"].ToString(),
                query["se"].ToString(),
                canonicalizedResource,
                query["si"].ToString(),
                signedIp,
                protocol,
                version
            };
            if (signedVersion >= new DateOnly(2018, 11, 9))
            {
                fields.Add(resourceType);
                fields.Add(query["snapshot"].ToString());
            }
            if (signedVersion >= new DateOnly(2020, 12, 6))
                fields.Add(query["ses"].ToString());
            fields.Add(query["rscc"].ToString());
            fields.Add(query["rscd"].ToString());
            fields.Add(query["rsce"].ToString());
            fields.Add(query["rscl"].ToString());
            fields.Add(query["rsct"].ToString());
            stringToSign = string.Join('\n', fields);

            var identifier = query["si"].ToString();
            if (!string.IsNullOrEmpty(identifier))
            {
                if (request.Container is null)
                    throw AzureStorageException.AuthorizationFailure();
                var container = await metadata.GetContainerAsync(request.Account, request.Container, includeDeleted: false, cancellationToken);
                if (container is null || !container.AccessPolicies.TryGetValue(identifier, out var policy))
                    throw AzureStorageException.AuthorizationFailure();
                permissions = IntersectPermissions(permissions, policy.Permission);
                startsAt = Latest(startsAt, policy.StartsAt);
                expiresAt = Earliest(expiresAt, policy.ExpiresAt);
            }
        }

        var expected = Sign(encodedKey, stringToSign);
        if (!FixedTimeEquals(expected, suppliedSignature))
            throw AzureStorageException.AuthenticationFailed();

        var now = DateTimeOffset.UtcNow;
        if (startsAt is { } start && now < start || expiresAt is null || now > expiresAt.Value)
            throw AzureStorageException.AuthenticationFailed("Signature not valid in the specified time frame.");
        if (string.IsNullOrEmpty(permissions))
            throw AzureStorageException.AuthorizationFailure();

        return new StorageAuthorization(
            StorageAuthorizationKind.Sas,
            permissions,
            startsAt,
            expiresAt,
            query["si"].ToString(),
            isAccountSas,
            signedResource);
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

    private static bool AccountSasCoversRequest(string resourceTypes, StorageRequestContext request) =>
        request.ResourceKind switch
        {
            StorageResourceKind.Service => resourceTypes.Contains('s'),
            StorageResourceKind.Container => resourceTypes.Contains('c'),
            StorageResourceKind.Blob => resourceTypes.Contains('o'),
            _ => false
        };

    private static bool ServiceSasCoversRequest(string resourceType, StorageRequestContext request) => resourceType switch
    {
        "c" => request.Container is not null,
        "b" => request.ResourceKind == StorageResourceKind.Blob && request.Blob is not null,
        "bs" => request.ResourceKind == StorageResourceKind.Blob && request.Blob is not null && !string.IsNullOrEmpty(request.Snapshot),
        "bv" => request.ResourceKind == StorageResourceKind.Blob && request.Blob is not null && !string.IsNullOrEmpty(request.VersionId),
        _ => false
    };

    private static string BuildSasCanonicalResource(StorageRequestContext request, string resourceType)
    {
        var path = $"/blob/{request.Account}";
        if (request.Container is not null)
            path += "/" + request.Container;
        if (resourceType is not "c" && request.Blob is not null)
            path += "/" + request.Blob;
        return path;
    }

    private static string IntersectPermissions(string token, string policy)
    {
        if (string.IsNullOrEmpty(token))
            return policy;
        if (string.IsNullOrEmpty(policy))
            return token;
        return new string(token.Where(policy.Contains).ToArray());
    }

    private static DateTimeOffset? Latest(DateTimeOffset? left, DateTimeOffset? right) =>
        left.HasValue && right.HasValue ? (left > right ? left : right) : left ?? right;

    private static DateTimeOffset? Earliest(DateTimeOffset? left, DateTimeOffset? right) =>
        left.HasValue && right.HasValue ? (left < right ? left : right) : left ?? right;

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
