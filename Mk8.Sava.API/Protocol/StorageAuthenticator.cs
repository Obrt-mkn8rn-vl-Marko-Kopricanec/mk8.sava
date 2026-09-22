using System.Globalization;
using System.Net;
using System.Net.Sockets;
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
    bool CanGenerateUserDelegationKey = false,
    string? ApplicationId = null,
    string? Audience = null,
    string? Issuer = null,
    string? UserPrincipalName = null,
    string AccountWidePermissions = "")
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
            var hasSas = context.Request.Query.ContainsKey("sig");
            var bearer = await AuthenticateBearerAsync(
                context,
                request,
                requireDataAuthorization: !hasSas);
            return hasSas
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
            throw AzureStorageException.FeatureVersionMismatch(
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

    internal void EnsureContainerPermission(
        StorageRequestContext request,
        string container,
        char permission)
    {
        var authorization = request.Authorization;
        if (authorization.Kind == StorageAuthorizationKind.SharedKey)
            return;
        if (authorization.Kind == StorageAuthorizationKind.Sas)
        {
            if (authorization.IsAccountSas && authorization.Allows(permission))
                return;
            throw AzureStorageException.AuthorizationPermissionMismatch();
        }
        if (authorization.Kind == StorageAuthorizationKind.Bearer)
        {
            if (authorization.AccountWidePermissions.Contains(permission, StringComparison.Ordinal))
                return;
            if (authorization.Identifier is not null &&
                _options.BearerAuthentication.Principals.TryGetValue(authorization.Identifier, out var access) &&
                Covers(access.Accounts, request.Account) &&
                Covers(access.Containers, container) &&
                access.Permissions.Contains(permission, StringComparison.Ordinal))
            {
                return;
            }
            throw AzureStorageException.AuthorizationPermissionMismatch();
        }

        throw AzureStorageException.AuthenticationFailed();
    }

    private async Task<StorageAuthorization> AuthenticateBearerAsync(
        HttpContext context,
        StorageRequestContext request,
        bool requireDataAuthorization)
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

        var accountWide = new HashSet<char>();
        foreach (var role in principal.FindAll("roles").Select(claim => claim.Value))
        {
            if (configuration.RolePermissions.TryGetValue(role, out var rolePermissions))
            {
                granted.UnionWith(rolePermissions);
                accountWide.UnionWith(rolePermissions);
            }
        }

        if (requireDataAuthorization && granted.Count == 0)
            throw AzureStorageException.AuthorizationFailure();
        var permissions = new string("racwdxytlfmeiopk".Where(granted.Contains).ToArray());
        return new StorageAuthorization(
            StorageAuthorizationKind.Bearer,
            permissions,
            Identifier: subject,
            TenantId: principal.FindFirst("tid")?.Value,
            CanGenerateUserDelegationKey: mappedAccessApplies && access!.CanGenerateUserDelegationKey,
            ApplicationId: principal.FindFirst("appid")?.Value ?? principal.FindFirst("azp")?.Value,
            Audience: principal.FindFirst("aud")?.Value,
            Issuer: principal.FindFirst("iss")?.Value,
            UserPrincipalName: principal.FindFirst("upn")?.Value ?? principal.FindFirst("preferred_username")?.Value,
            AccountWidePermissions: new string("racwdxytlfmeiopk".Where(accountWide.Contains).ToArray()));
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
        var authenticated = false;
        foreach (var escapedPath in StorageResourcePath.GetSignaturePathCandidates(httpRequest))
        {
            var stringToSign = lite
                ? BuildSharedKeyLiteString(httpRequest, request, escapedPath)
                : BuildSharedKeyString(httpRequest, request, escapedPath);
            authenticated |= FixedTimeEquals(Sign(encodedKey, stringToSign), suppliedSignature);
        }
        if (!authenticated)
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
        var hasSignedVersion = !string.IsNullOrEmpty(version);
        var signedVersion = new DateOnly(2009, 9, 19);
        if (hasSignedVersion &&
            (!DateOnly.TryParseExact(
                 version,
                 "yyyy-MM-dd",
                 CultureInfo.InvariantCulture,
                 DateTimeStyles.None,
                 out signedVersion) ||
             signedVersion < new DateOnly(2012, 2, 12)) ||
            string.IsNullOrEmpty(suppliedSignature))
        {
            throw AzureStorageException.AuthenticationFailed();
        }
        if (!_options.Accounts.TryGetValue(request.Account, out var encodedKey))
            throw AzureStorageException.AuthenticationFailed();

        var isAccountSas = query.ContainsKey("ss");
        var isUserDelegationSas = query.ContainsKey("skoid");
        if (isAccountSas && isUserDelegationSas ||
            isAccountSas && (!hasSignedVersion || signedVersion < new DateOnly(2015, 4, 5)) ||
            isUserDelegationSas && (!hasSignedVersion || signedVersion < new DateOnly(2018, 11, 9)))
        {
            throw AzureStorageException.AuthenticationFailed();
        }
        var hasSignedRequestFields = query.ContainsKey("srh") || query.ContainsKey("srq");
        if (hasSignedRequestFields &&
            (!isUserDelegationSas || signedVersion < new DateOnly(2026, 4, 6)))
        {
            throw AzureStorageException.AuthenticationFailed();
        }

        var protocol = query["spr"].ToString();
        var signedIp = query["sip"].ToString();
        if ((!hasSignedVersion || signedVersion < new DateOnly(2015, 4, 5)) &&
            (!string.IsNullOrEmpty(protocol) || !string.IsNullOrEmpty(signedIp)))
        {
            throw AzureStorageException.AuthenticationFailed();
        }
        if (protocol is not ("" or "https" or "https,http") ||
            protocol == "https" && !context.Request.IsHttps)
        {
            throw AzureStorageException.AuthenticationFailed("The request protocol is not permitted by the signed protocol field.");
        }

        if (!string.IsNullOrEmpty(signedIp) && !MatchesIpRange(context.Connection.RemoteIpAddress, signedIp))
            throw AzureStorageException.AuthenticationFailed("The request IP address is not permitted by the signed IP field.");

        var permissions = query["sp"].ToString();
        var startsAt = ParseSasTime(query["st"].ToString());
        var expiresAt = ParseSasTime(query["se"].ToString());
        var signedEncryptionScope = query["ses"].ToString();
        if (!string.IsNullOrEmpty(signedEncryptionScope) && signedVersion < new DateOnly(2020, 12, 6))
            throw AzureStorageException.AuthorizationFailure();
        string stringToSign;
        var signedResource = string.Empty;
        var signingKey = encodedKey;
        if (isAccountSas)
        {
            var services = query["ss"].ToString();
            var resourceTypes = query["srt"].ToString();
            ValidateAccountSasFields(
                services,
                resourceTypes,
                permissions,
                signedVersion,
                expiresAt);
            if (!services.Contains('b'))
                throw AzureStorageException.AuthorizationServiceMismatch();
            if (!AccountSasCoversRequest(resourceTypes, request, context.Request))
                throw AzureStorageException.AuthorizationResourceTypeMismatch();
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
            if (string.IsNullOrEmpty(permissions) || expiresAt is null)
                throw AzureStorageException.AuthenticationFailed();
            ValidateServiceSasPermissions(permissions, signedVersion);
            signedResource = resourceType;
            if (resourceType == "d" && !IsHierarchicalNamespaceEnabled(request.Account))
                throw AzureStorageException.AuthorizationFailure();
            if (!ServiceSasCoversRequest(resourceType, request, signedVersion))
                throw AzureStorageException.AuthorizationFailure();
            var canonicalizedResource = BuildSasCanonicalResource(
                request,
                resourceType,
                query,
                signedVersion);

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
            var correlationId = query["scid"].ToString();
            var hasAuthorizedObjectId = !string.IsNullOrEmpty(authorizedObjectId);
            var hasUnauthorizedObjectId = !string.IsNullOrEmpty(unauthorizedObjectId);
            var hasCorrelationId = !string.IsNullOrEmpty(correlationId);
            if (hasAuthorizedObjectId && hasUnauthorizedObjectId ||
                (hasAuthorizedObjectId || hasUnauthorizedObjectId || hasCorrelationId) &&
                signedVersion < new DateOnly(2020, 2, 10) ||
                hasAuthorizedObjectId && !Guid.TryParse(authorizedObjectId, out _) ||
                hasUnauthorizedObjectId && !Guid.TryParse(unauthorizedObjectId, out _) ||
                hasCorrelationId && !Guid.TryParse(correlationId, out _))
            {
                throw AzureStorageException.AuthenticationFailed();
            }
            if ((hasAuthorizedObjectId || hasUnauthorizedObjectId) &&
                !IsHierarchicalNamespaceEnabled(request.Account))
            {
                throw AzureStorageException.AuthorizationFailure();
            }
            if (hasUnauthorizedObjectId)
            {
                // suoid requires a POSIX ACL decision for an HNS path. The Blob
                // endpoint does not treat a signed identity as an ACL grant.
                throw AzureStorageException.AuthorizationFailure();
            }

            var delegatedUserTenantId = query["skdutid"].ToString();
            var delegatedUserObjectId = query["sduoid"].ToString();
            if ((!string.IsNullOrEmpty(delegatedUserTenantId) &&
                 !Guid.TryParse(delegatedUserTenantId, out _)) ||
                (!string.IsNullOrEmpty(delegatedUserObjectId) &&
                 !Guid.TryParse(delegatedUserObjectId, out _)))
            {
                throw AzureStorageException.AuthenticationFailed();
            }
            if (!string.IsNullOrEmpty(delegatedUserObjectId))
            {
                var expectedBearerTenantId = string.IsNullOrEmpty(delegatedUserTenantId)
                    ? tenantId
                    : delegatedUserTenantId;
                if (signedVersion < new DateOnly(2025, 7, 5) ||
                    bearer is null ||
                    !string.Equals(bearer.Identifier, delegatedUserObjectId, StringComparison.Ordinal) ||
                    !string.Equals(bearer.TenantId, expectedBearerTenantId, StringComparison.Ordinal))
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
                canonicalizedResource,
                objectId,
                tenantId,
                keyStartText,
                keyExpiryText,
                keyService,
                keyVersion,
                authorizedObjectId,
                unauthorizedObjectId,
                correlationId
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
                fields.Add(GetSignedSnapshotOrVersion(query, resourceType));
            if (signedVersion >= new DateOnly(2020, 12, 6))
                fields.Add(query["ses"].ToString());
            if (signedVersion >= new DateOnly(2026, 4, 6))
            {
                fields.Add(BuildSignedRequestHeaders(context.Request, query["srh"].ToString()));
                fields.Add(BuildSignedRequestQuery(context.Request));
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
            ValidateServiceSasPermissions(permissions, signedVersion);
            signedResource = resourceType;
            if (resourceType == "d" && !IsHierarchicalNamespaceEnabled(request.Account))
                throw AzureStorageException.AuthorizationFailure();
            if (!ServiceSasCoversRequest(resourceType, request, signedVersion))
                throw AzureStorageException.AuthorizationFailure();
            var canonicalizedResource = BuildSasCanonicalResource(
                request,
                resourceType,
                query,
                signedVersion);
            var fields = new List<string>
            {
                permissions,
                query["st"].ToString(),
                query["se"].ToString(),
                canonicalizedResource,
                query["si"].ToString()
            };
            if (hasSignedVersion)
            {
                if (signedVersion >= new DateOnly(2015, 4, 5))
                {
                    fields.Add(signedIp);
                    fields.Add(protocol);
                }
                fields.Add(version);
                if (signedVersion >= new DateOnly(2018, 11, 9))
                {
                    fields.Add(resourceType);
                    fields.Add(GetSignedSnapshotOrVersion(query, resourceType));
                }
                if (signedVersion >= new DateOnly(2020, 12, 6))
                    fields.Add(query["ses"].ToString());
                if (signedVersion >= new DateOnly(2013, 8, 15))
                {
                    fields.Add(query["rscc"].ToString());
                    fields.Add(query["rscd"].ToString());
                    fields.Add(query["rsce"].ToString());
                    fields.Add(query["rscl"].ToString());
                    fields.Add(query["rsct"].ToString());
                }
                else if (HasSasResponseOverrides(query))
                {
                    throw AzureStorageException.AuthenticationFailed();
                }
            }
            else if (HasSasResponseOverrides(query))
            {
                throw AzureStorageException.AuthenticationFailed();
            }
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
        if (!hasSignedVersion &&
            string.IsNullOrEmpty(query["si"].ToString()) &&
            expiresAt.HasValue &&
            expiresAt.Value - (startsAt ?? now) > TimeSpan.FromHours(1))
        {
            throw AzureStorageException.AuthenticationFailed("Signature not valid in the specified time frame.");
        }
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

    private static string BuildSharedKeyString(
        HttpRequest request,
        StorageRequestContext context,
        string escapedPath)
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
            BuildCanonicalizedHeaders(request) + BuildCanonicalizedResource(request, context, escapedPath));
    }

    private static string BuildSharedKeyLiteString(
        HttpRequest request,
        StorageRequestContext context,
        string escapedPath) =>
        Header(request, "Date") + "\n" + BuildCanonicalizedHeaders(request) +
        BuildCanonicalizedResource(request, context, escapedPath);

    private static string BuildCanonicalizedHeaders(HttpRequest request)
    {
        var builder = new StringBuilder();
        foreach (var header in request.Headers
                     .Where(header => header.Key.StartsWith("x-ms-", StringComparison.OrdinalIgnoreCase))
                     .Select(header => new { Key = header.Key.ToLowerInvariant(), header.Value })
                     .OrderBy(header => header.Key, AzureCanonicalHeaderNameComparer.Instance))
        {
            var value = string.Join(',', header.Value.Select(CollapseWhitespace));
            builder.Append(header.Key).Append(':').Append(value).Append('\n');
        }
        return builder.ToString();
    }

    private static string BuildCanonicalizedResource(
        HttpRequest request,
        StorageRequestContext context,
        string escapedPath)
    {
        var builder = new StringBuilder()
            .Append('/')
            .Append(context.Account)
            .Append(escapedPath);
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

    private static bool AccountSasCoversRequest(
        string resourceTypes,
        StorageRequestContext request,
        HttpRequest httpRequest) =>
        request.ResourceKind switch
        {
            StorageResourceKind.Service => resourceTypes.Contains('s'),
            StorageResourceKind.Container => resourceTypes.Contains('c'),
            StorageResourceKind.Blob when
                HttpMethods.IsPut(httpRequest.Method) &&
                string.Equals(httpRequest.Query["comp"], "undelete", StringComparison.OrdinalIgnoreCase) =>
                resourceTypes.Contains('c'),
            StorageResourceKind.Blob => resourceTypes.Contains('o'),
            _ => false
        };

    private static bool ServiceSasCoversRequest(
        string resourceType,
        StorageRequestContext request,
        DateOnly signedVersion) => resourceType switch
        {
            "c" => request.Container is not null,
            "b" => request.ResourceKind == StorageResourceKind.Blob &&
                   request.Blob is not null &&
                   (signedVersion < new DateOnly(2018, 11, 9) ||
                    request.Snapshot is null && request.VersionId is null),
            "bs" => signedVersion >= new DateOnly(2018, 11, 9) &&
                    request.ResourceKind == StorageResourceKind.Blob &&
                    request.Blob is not null &&
                    request.Snapshot is not null &&
                    request.VersionId is null,
            "bv" => signedVersion >= new DateOnly(2018, 11, 9) &&
                    request.ResourceKind == StorageResourceKind.Blob &&
                    request.Blob is not null &&
                    request.VersionId is not null &&
                    request.Snapshot is null,
            "d" => signedVersion >= new DateOnly(2020, 2, 10) &&
                   request.ResourceKind == StorageResourceKind.Blob &&
                   request.Container is not null &&
                   request.Blob is not null,
            _ => false
        };

    private static string GetSignedSnapshotOrVersion(IQueryCollection query, string resourceType) =>
        resourceType == "bv"
            ? query["versionid"].ToString()
            : resourceType == "bs"
                ? query["snapshot"].ToString()
                : string.Empty;

    private static string BuildSasCanonicalResource(
        StorageRequestContext request,
        string resourceType,
        IQueryCollection query,
        DateOnly signedVersion)
    {
        var path = signedVersion >= new DateOnly(2015, 2, 21)
            ? $"/blob/{request.Account}"
            : $"/{request.Account}";
        if (request.Container is not null)
            path += "/" + request.Container;
        if (resourceType == "d")
        {
            var directoryDepthText = query["sdd"].ToString();
            if (!int.TryParse(
                    directoryDepthText,
                    NumberStyles.None,
                    CultureInfo.InvariantCulture,
                    out var directoryDepth) ||
                request.Blob is null)
            {
                throw AzureStorageException.AuthenticationFailed();
            }

            var segments = request.Blob.Split('/', StringSplitOptions.None);
            if (directoryDepth > segments.Length)
                throw AzureStorageException.AuthenticationFailed();
            return path + "/" + string.Join('/', segments.Take(directoryDepth));
        }
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

    private static void ValidateAccountSasFields(
        string services,
        string resourceTypes,
        string permissions,
        DateOnly signedVersion,
        DateTimeOffset? expiresAt)
    {
        if (string.IsNullOrEmpty(services) ||
            string.IsNullOrEmpty(resourceTypes) ||
            string.IsNullOrEmpty(permissions) ||
            expiresAt is null ||
            !IsOrderedSubset(services, "bqtf") ||
            !IsOrderedSubset(resourceTypes, "sco") ||
            !IsOrderedSubset(permissions, "rwdxylacuptfi"))
        {
            throw AzureStorageException.AuthenticationFailed();
        }

        foreach (var permission in permissions)
        {
            var minimumVersion = permission switch
            {
                'x' or 't' or 'f' => new DateOnly(2019, 12, 12),
                'y' => new DateOnly(2020, 2, 10),
                'i' => new DateOnly(2020, 6, 12),
                _ => DateOnly.MinValue
            };
            if (signedVersion < minimumVersion)
                throw AzureStorageException.AuthenticationFailed();
        }
    }

    private static void ValidateServiceSasPermissions(
        string permissions,
        DateOnly signedVersion)
    {
        if (string.IsNullOrEmpty(permissions))
            return;
        if (!IsOrderedSubset(permissions, "racwdxyltfmeiop"))
            throw AzureStorageException.AuthenticationFailed();

        foreach (var permission in permissions)
        {
            var minimumVersion = permission switch
            {
                'x' or 't' or 'f' => new DateOnly(2019, 12, 12),
                'y' or 'm' or 'e' or 'o' or 'p' => new DateOnly(2020, 2, 10),
                'i' => new DateOnly(2020, 6, 12),
                _ => DateOnly.MinValue
            };
            if (signedVersion < minimumVersion)
                throw AzureStorageException.AuthenticationFailed();
        }
    }

    private static bool IsOrderedSubset(string value, string order)
    {
        var previous = -1;
        foreach (var character in value)
        {
            var current = order.IndexOf(character);
            if (current <= previous)
                return false;
            previous = current;
        }
        return true;
    }

    private static string BuildSignedRequestHeaders(HttpRequest request, string names)
    {
        if (string.IsNullOrEmpty(names))
            return string.Empty;

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var builder = new StringBuilder();
        foreach (var item in names.Split(','))
        {
            var name = item.ToLowerInvariant();
            if (string.IsNullOrEmpty(name) ||
                name.Any(character => char.IsControl(character) || char.IsWhiteSpace(character)) ||
                !seen.Add(name) ||
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

    private static string BuildSignedRequestQuery(HttpRequest request)
    {
        var names = ReadEncodedCommaSeparatedQueryValue(request, "srq");
        if (names.Count == 0)
            return string.Empty;

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var builder = new StringBuilder();
        foreach (var name in names)
        {
            if (string.IsNullOrEmpty(name) ||
                name.Any(char.IsControl) ||
                !seen.Add(name))
            {
                throw AzureStorageException.AuthenticationFailed("A signed request query parameter is missing or invalid.");
            }
            var parameter = request.Query.FirstOrDefault(candidate =>
                string.Equals(candidate.Key, name, StringComparison.Ordinal));
            if (parameter.Key is null)
                throw AzureStorageException.AuthenticationFailed("A signed request query parameter is missing or invalid.");
            var values = parameter.Value;
            var value = values.ToString();
            if (value.Contains('\n', StringComparison.Ordinal) || value.Contains('\r', StringComparison.Ordinal))
                throw AzureStorageException.AuthenticationFailed("A signed request query parameter is invalid.");
            builder.Append('\n').Append(name).Append('=').Append(value);
        }
        return builder.ToString();
    }

    private static IReadOnlyList<string> ReadEncodedCommaSeparatedQueryValue(
        HttpRequest request,
        string parameterName)
    {
        var rawQuery = request.QueryString.Value;
        if (string.IsNullOrEmpty(rawQuery))
            return [];

        string? encodedValue = null;
        foreach (var pair in rawQuery.AsSpan(1).ToString().Split('&'))
        {
            var separator = pair.IndexOf('=');
            var encodedName = separator < 0 ? pair : pair[..separator];
            if (!string.Equals(WebUtility.UrlDecode(encodedName), parameterName, StringComparison.Ordinal))
                continue;
            if (encodedValue is not null)
                throw AzureStorageException.AuthenticationFailed("A signed request query parameter list is invalid.");
            encodedValue = separator < 0 ? string.Empty : pair[(separator + 1)..];
        }

        if (encodedValue is null)
            return [];
        return encodedValue
            .Split(',', StringSplitOptions.None)
            .Select(value => WebUtility.UrlDecode(value) ?? string.Empty)
            .ToArray();
    }

    private static bool HasSasResponseOverrides(IQueryCollection query) =>
        query.ContainsKey("rscc") ||
        query.ContainsKey("rscd") ||
        query.ContainsKey("rsce") ||
        query.ContainsKey("rscl") ||
        query.ContainsKey("rsct");

    private static DateTimeOffset? Latest(DateTimeOffset? left, DateTimeOffset? right) =>
        left.HasValue && right.HasValue ? (left > right ? left : right) : left ?? right;

    private static DateTimeOffset? Earliest(DateTimeOffset? left, DateTimeOffset? right) =>
        left.HasValue && right.HasValue ? (left < right ? left : right) : left ?? right;

    private static string? NullIfEmpty(string value) => string.IsNullOrEmpty(value) ? null : value;

    private static bool Covers(IReadOnlyCollection<string> configuredValues, string value) =>
        configuredValues.Count == 0 || configuredValues.Contains("*", StringComparer.Ordinal) || configuredValues.Contains(value, StringComparer.Ordinal);

    private bool IsHierarchicalNamespaceEnabled(string account) =>
        _options.AccountCapabilities.TryGetValue(account, out var capabilities) &&
        capabilities.HierarchicalNamespaceEnabled;

    private static bool MatchesIpRange(IPAddress? address, string range)
    {
        if (address is null || address.AddressFamily != AddressFamily.InterNetwork)
            return false;
        var values = range.Split('-', 2);
        if (!IPAddress.TryParse(values[0], out var start) ||
            start.AddressFamily != AddressFamily.InterNetwork)
            return false;
        if (values.Length == 1)
            return address.Equals(start);
        if (!IPAddress.TryParse(values[1], out var end) ||
            end.AddressFamily != AddressFamily.InterNetwork)
            return false;
        var candidateBytes = address.GetAddressBytes();
        return Compare(candidateBytes, start.GetAddressBytes()) >= 0 &&
               Compare(candidateBytes, end.GetAddressBytes()) <= 0;
    }

    private static int Compare(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right) => left.SequenceCompareTo(right);
}
