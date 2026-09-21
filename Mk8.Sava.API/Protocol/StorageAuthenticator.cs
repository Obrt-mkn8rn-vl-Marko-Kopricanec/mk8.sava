using System.Globalization;
using System.Net;
using System.Text;
using Microsoft.AspNetCore.Authentication;
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
    string? SignedResource = null,
    string? TenantId = null,
    bool CanGenerateUserDelegationKey = false)
{
    public static StorageAuthorization Anonymous { get; } = new(StorageAuthorizationKind.Anonymous, string.Empty);
    public static StorageAuthorization Owner { get; } = new(StorageAuthorizationKind.SharedKey, "racwdxltmeop");

    public bool Allows(char permission) => Kind == StorageAuthorizationKind.SharedKey || Permissions.Contains(permission, StringComparison.Ordinal);
}

public sealed record UserDelegationKey(
    string SignedObjectId,
    string SignedTenantId,
    string SignedStart,
    string SignedExpiry,
    string SignedService,
    string SignedVersion,
    string? SignedDelegatedUserTenantId,
    string Value);

public sealed class StorageAuthenticator(IOptions<SavaOptions> options, MetadataStore metadata)
{
    public const string BearerScheme = "StorageBearer";

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
        {
            var bearer = await AuthenticateBearerAsync(context, request);
            return context.Request.Query.ContainsKey("sig")
                ? await AuthenticateSasAsync(context, request, cancellationToken, bearer)
                : bearer;
        }
        if (context.Request.Query.ContainsKey("sig"))
            return await AuthenticateSasAsync(context, request, cancellationToken, null);
        return StorageAuthorization.Anonymous;
    }

    internal UserDelegationKey IssueUserDelegationKey(
        StorageRequestContext request,
        UserDelegationKeyRequest keyRequest)
    {
        if (request.Authorization.Kind != StorageAuthorizationKind.Bearer ||
            !request.Authorization.CanGenerateUserDelegationKey)
        {
            throw AzureStorageException.AuthorizationFailure();
        }

        if (!Guid.TryParse(request.Authorization.Identifier, out _) ||
            !Guid.TryParse(request.Authorization.TenantId, out _))
        {
            throw AzureStorageException.AuthorizationFailure();
        }

        if (!DateOnly.TryParseExact(request.ServiceVersion, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var version) ||
            version < new DateOnly(2018, 11, 9))
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "FeatureVersionMismatch",
                "The requested operation requires service version 2018-11-09 or later.");
        }

        var now = DateTimeOffset.UtcNow;
        if (keyRequest.StartsAt >= keyRequest.ExpiresAt ||
            keyRequest.StartsAt < now.AddMinutes(-15) ||
            keyRequest.StartsAt > now.AddDays(7) ||
            keyRequest.ExpiresAt > now.AddDays(7) ||
            keyRequest.ExpiresAt - keyRequest.StartsAt > TimeSpan.FromDays(7))
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "InvalidInput",
                "The user delegation key start and expiry must define a valid period of no more than seven days.");
        }

        var signedStart = FormatSasTime(keyRequest.StartsAt);
        var signedExpiry = FormatSasTime(keyRequest.ExpiresAt);
        var key = DeriveUserDelegationKey(
            request.Account,
            request.Authorization.Identifier!,
            request.Authorization.TenantId!,
            signedStart,
            signedExpiry,
            "b",
            request.ServiceVersion,
            keyRequest.DelegatedUserTenantId);
        return new UserDelegationKey(
            request.Authorization.Identifier!,
            request.Authorization.TenantId!,
            signedStart,
            signedExpiry,
            "b",
            request.ServiceVersion,
            keyRequest.DelegatedUserTenantId,
            key);
    }

    private async Task<StorageAuthorization> AuthenticateBearerAsync(
        HttpContext context,
        StorageRequestContext request)
    {
        var configuration = _options.BearerAuthentication;
        if (!configuration.Enabled)
            throw AzureStorageException.BearerAuthenticationRequired();

        var result = await context.AuthenticateAsync(BearerScheme);
        if (!result.Succeeded || result.Principal?.Identity?.IsAuthenticated != true)
            throw AzureStorageException.BearerAuthenticationRequired();

        var principal = result.Principal;
        var subject = principal.FindFirst("oid")?.Value
                      ?? principal.FindFirst("sub")?.Value
                      ?? principal.FindFirst("appid")?.Value;
        if (string.IsNullOrEmpty(subject))
            throw AzureStorageException.AuthorizationFailure();

        var granted = new HashSet<char>();
        var mappedAccessApplies = false;
        if (configuration.Principals.TryGetValue(subject, out var access) &&
            Covers(access.Accounts, request.Account) &&
            (request.Container is null || Covers(access.Containers, request.Container)))
        {
            granted.UnionWith(access.Permissions);
            mappedAccessApplies = true;
        }

        foreach (var role in principal.FindAll("roles").Select(claim => claim.Value))
        {
            if (configuration.RolePermissions.TryGetValue(role, out var rolePermissions))
                granted.UnionWith(rolePermissions);
        }

        if (granted.Count == 0)
            throw AzureStorageException.AuthorizationFailure();
        var permissions = new string("racwdxytlfmeiopk".Where(granted.Contains).ToArray());
        return new StorageAuthorization(
            StorageAuthorizationKind.Bearer,
            permissions,
            Identifier: subject,
            TenantId: principal.FindFirst("tid")?.Value,
            CanGenerateUserDelegationKey: mappedAccessApplies && access!.CanGenerateUserDelegationKey);
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
        CancellationToken cancellationToken,
        StorageAuthorization? bearer)
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
        var isUserDelegationSas = query.ContainsKey("skoid");
        if (isAccountSas && isUserDelegationSas)
            throw AzureStorageException.AuthenticationFailed();
        var signedResource = string.Empty;
        var signingKey = encodedKey;
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
        else if (isUserDelegationSas)
        {
            if (signedVersion < new DateOnly(2018, 11, 9) || !string.IsNullOrEmpty(query["si"]))
                throw AzureStorageException.AuthenticationFailed();

            var resourceType = query["sr"].ToString();
            signedResource = resourceType;
            if (!ServiceSasCoversRequest(resourceType, request))
                throw AzureStorageException.AuthorizationFailure();

            var objectId = query["skoid"].ToString();
            var tenantId = query["sktid"].ToString();
            var keyStartText = query["skt"].ToString();
            var keyExpiryText = query["ske"].ToString();
            var keyService = query["sks"].ToString();
            var keyVersion = query["skv"].ToString();
            if (!Guid.TryParse(objectId, out _) ||
                !Guid.TryParse(tenantId, out _) ||
                keyService != "b" ||
                !DateOnly.TryParseExact(keyVersion, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsedKeyVersion) ||
                parsedKeyVersion < new DateOnly(2018, 11, 9))
            {
                throw AzureStorageException.AuthenticationFailed();
            }

            var keyStartsAt = ParseSasTime(keyStartText);
            var keyExpiresAt = ParseSasTime(keyExpiryText);
            if (keyStartsAt is null || keyExpiresAt is null ||
                keyStartsAt >= keyExpiresAt ||
                keyExpiresAt.Value - keyStartsAt.Value > TimeSpan.FromDays(7))
            {
                throw AzureStorageException.AuthenticationFailed();
            }

            if (!_options.BearerAuthentication.Principals.TryGetValue(objectId, out var delegatedPrincipal) ||
                !Covers(delegatedPrincipal.Accounts, request.Account) ||
                request.Container is not null && !Covers(delegatedPrincipal.Containers, request.Container))
            {
                throw AzureStorageException.AuthorizationFailure();
            }
            permissions = IntersectPermissions(permissions, delegatedPrincipal.Permissions);
            startsAt = Latest(startsAt, keyStartsAt);
            expiresAt = Earliest(expiresAt, keyExpiresAt);

            var authorizedObjectId = query["saoid"].ToString();
            var unauthorizedObjectId = query["suoid"].ToString();
            if (!string.IsNullOrEmpty(authorizedObjectId) || !string.IsNullOrEmpty(unauthorizedObjectId))
                throw AzureStorageException.AuthorizationFailure();

            var delegatedUserTenantId = query["skdutid"].ToString();
            var delegatedUserObjectId = query["sduoid"].ToString();
            if (!string.IsNullOrEmpty(delegatedUserObjectId))
            {
                if (signedVersion < new DateOnly(2025, 7, 5) ||
                    bearer is null ||
                    !string.Equals(bearer.Identifier, delegatedUserObjectId, StringComparison.Ordinal) ||
                    !string.IsNullOrEmpty(delegatedUserTenantId) && !string.Equals(bearer.TenantId, delegatedUserTenantId, StringComparison.Ordinal))
                {
                    throw AzureStorageException.AuthorizationFailure();
                }
            }
            else if (!string.IsNullOrEmpty(delegatedUserTenantId))
            {
                throw AzureStorageException.AuthenticationFailed();
            }

            signingKey = DeriveUserDelegationKey(
                request.Account,
                objectId,
                tenantId,
                FormatSasTime(keyStartsAt.Value),
                FormatSasTime(keyExpiresAt.Value),
                keyService,
                keyVersion,
                NullIfEmpty(delegatedUserTenantId));

            var fields = new List<string>
            {
                query["sp"].ToString(),
                query["st"].ToString(),
                query["se"].ToString(),
                BuildSasCanonicalResource(request, resourceType),
                objectId,
                tenantId,
                keyStartText,
                keyExpiryText,
                keyService,
                keyVersion,
                authorizedObjectId,
                unauthorizedObjectId,
                query["scid"].ToString()
            };
            if (signedVersion >= new DateOnly(2025, 7, 5))
            {
                fields.Add(delegatedUserTenantId);
                fields.Add(delegatedUserObjectId);
            }
            fields.Add(signedIp);
            fields.Add(protocol);
            fields.Add(version);
            fields.Add(resourceType);
            if (signedVersion >= new DateOnly(2020, 2, 10))
                fields.Add(query["snapshot"].ToString());
            if (signedVersion >= new DateOnly(2020, 12, 6))
                fields.Add(query["ses"].ToString());
            if (signedVersion >= new DateOnly(2026, 4, 6))
            {
                fields.Add(BuildSignedRequestHeaders(context.Request, query["srh"].ToString()));
                fields.Add(BuildSignedRequestQuery(context.Request, query["srq"].ToString()));
            }
            fields.Add(query["rscc"].ToString());
            fields.Add(query["rscd"].ToString());
            fields.Add(query["rsce"].ToString());
            fields.Add(query["rscl"].ToString());
            fields.Add(query["rsct"].ToString());
            stringToSign = string.Join('\n', fields);
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

        var expected = Sign(signingKey, stringToSign);
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
            signedResource,
            TenantId: isUserDelegationSas ? query["sktid"].ToString() : null);
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

    private string DeriveUserDelegationKey(
        string account,
        string objectId,
        string tenantId,
        string signedStart,
        string signedExpiry,
        string signedService,
        string signedVersion,
        string? delegatedUserTenantId)
    {
        if (!_options.Accounts.TryGetValue(account, out var encodedAccountKey))
            throw AzureStorageException.AuthenticationFailed();
        var derivationContext = string.Join('\n',
            "mk8.sava:user-delegation-key:v1",
            account,
            objectId,
            tenantId,
            signedStart,
            signedExpiry,
            signedService,
            signedVersion,
            delegatedUserTenantId ?? string.Empty);
        return Sign(encodedAccountKey, derivationContext);
    }

    private static string FormatSasTime(DateTimeOffset value) =>
        value.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

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

    private static string BuildSignedRequestHeaders(HttpRequest request, string names)
    {
        if (string.IsNullOrEmpty(names))
            return string.Empty;

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var builder = new StringBuilder();
        foreach (var item in names.Split(','))
        {
            var name = item.Trim().ToLowerInvariant();
            if (string.IsNullOrEmpty(name) || name.Contains('\n', StringComparison.Ordinal) || !seen.Add(name) ||
                !request.Headers.TryGetValue(name, out var values))
            {
                throw AzureStorageException.AuthenticationFailed("A signed request header is missing or invalid.");
            }
            var value = values.ToString();
            if (value.Contains('\n', StringComparison.Ordinal) || value.Contains('\r', StringComparison.Ordinal))
                throw AzureStorageException.AuthenticationFailed("A signed request header is invalid.");
            builder.Append(name).Append(':').Append(value).Append('\n');
        }
        return builder.ToString();
    }

    private static string BuildSignedRequestQuery(HttpRequest request, string names)
    {
        if (string.IsNullOrEmpty(names))
            return string.Empty;

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var builder = new StringBuilder();
        foreach (var item in names.Split(','))
        {
            var name = item.Trim();
            if (string.IsNullOrEmpty(name) || name.Contains('\n', StringComparison.Ordinal) || !seen.Add(name) ||
                !request.Query.TryGetValue(name, out var values))
            {
                throw AzureStorageException.AuthenticationFailed("A signed request query parameter is missing or invalid.");
            }
            var value = values.ToString();
            if (value.Contains('\n', StringComparison.Ordinal) || value.Contains('\r', StringComparison.Ordinal))
                throw AzureStorageException.AuthenticationFailed("A signed request query parameter is invalid.");
            builder.Append('\n').Append(name).Append('=').Append(value);
        }
        return builder.ToString();
    }

    private static DateTimeOffset? Latest(DateTimeOffset? left, DateTimeOffset? right) =>
        left.HasValue && right.HasValue ? (left > right ? left : right) : left ?? right;

    private static DateTimeOffset? Earliest(DateTimeOffset? left, DateTimeOffset? right) =>
        left.HasValue && right.HasValue ? (left < right ? left : right) : left ?? right;

    private static string? NullIfEmpty(string value) => string.IsNullOrEmpty(value) ? null : value;

    private static bool Covers(IReadOnlyCollection<string> configuredValues, string value) =>
        configuredValues.Count == 0 || configuredValues.Contains("*", StringComparer.Ordinal) || configuredValues.Contains(value, StringComparer.Ordinal);

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
