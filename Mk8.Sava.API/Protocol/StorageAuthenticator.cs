using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Identity;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

internal sealed class StorageAuthenticator(
    IOptions<SavaOptions> options,
    MetadataStore metadata,
    IGroupMembershipResolver groupMembershipResolver,
    ILogger<StorageAuthenticator> logger)
{
    public const string BearerScheme = "StorageBearer";
    private static readonly HashSet<string> NoGroups = new(StringComparer.OrdinalIgnoreCase);

    private readonly SavaOptions _options = options.Value;

    public async Task<StorageAuthorization> AuthenticateAsync(
        HttpContext context,
        StorageRequestContext request,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var authorization = context.Request.Headers.Authorization.ToString();
        if (authorization.StartsWith("SharedKey ", StringComparison.Ordinal))
        {
            EnsureSharedKeyAccessAllowed(request.Account);
            return AuthenticateSharedKey(context.Request, request, authorization, lite: false);
        }
        if (authorization.StartsWith("SharedKeyLite ", StringComparison.Ordinal))
        {
            EnsureSharedKeyAccessAllowed(request.Account);
            return AuthenticateSharedKey(context.Request, request, authorization, lite: true);
        }
        if (authorization.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
        {
            var hasSas = context.Request.Query.ContainsKey("sig");
            var bearer = await AuthenticateBearerAsync(
                context,
                request,
                requireDataAuthorization: !hasSas).ConfigureAwait(false);
            return hasSas
                ? await AuthenticateSasAsync(context, request, bearer, cancellationToken)
.ConfigureAwait(false) : bearer;
        }
        if (context.Request.Query.ContainsKey("sig"))
            return await AuthenticateSasAsync(context, request, null, cancellationToken).ConfigureAwait(false);
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
        if (keyRequest.DelegatedUserTenantId is not null &&
            !SameTenant(keyRequest.DelegatedUserTenantId, request.Authorization.TenantId!) &&
            !AllowsCrossTenantDelegationSas(request.Account))
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
        var (principal, subject, objectId, granted, accountWide, canGenerateUserDelegationKey) =
            await AuthenticateBearerClaimsAsync(context, request).ConfigureAwait(false);

        var acl = await GrantBearerAclsAsync(
            context,
            request,
            principal,
            objectId,
            granted,
            requireDataAuthorization).ConfigureAwait(false);

        if (requireDataAuthorization && granted.Count == 0)
            throw AzureStorageException.AuthorizationFailure();
        var permissions = new string("racwdxytlfmeiopk".Where(granted.Contains).ToArray());
        return new StorageAuthorization(
            StorageAuthorizationKind.Bearer,
            permissions,
            Identifier: subject,
            TenantId: principal.FindFirst("tid")?.Value,
            CanGenerateUserDelegationKey: canGenerateUserDelegationKey,
            ApplicationId: principal.FindFirst("appid")?.Value ?? principal.FindFirst("azp")?.Value,
            Audience: principal.FindFirst("aud")?.Value,
            Issuer: principal.FindFirst("iss")?.Value,
            UserPrincipalName: principal.FindFirst("upn")?.Value ?? principal.FindFirst("preferred_username")?.Value,
            AccountWidePermissions: new string("racwdxytlfmeiopk".Where(accountWide.Contains).ToArray()),
            AclReadChecked: acl.ReadChecked,
            AclAuthorizedGenerationId: acl.AuthorizedGenerationId,
            AclListChecked: acl.ListChecked,
            AclListObjectId: acl.ListChecked ? objectId : null,
            AclListGroups: acl.ListGroups,
            AclMutationChecked: acl.MutationChecked,
            AclMutationObjectId: acl.MutationChecked ? objectId : null,
            AclMutationGroups: acl.MutationGroups,
            AclAppendChecked: acl.AppendChecked,
            AclAppendObjectId: acl.AppendChecked ? objectId : null,
            AclAppendGroups: acl.AppendGroups);
    }

    private async Task<BearerAclGrants> GrantBearerAclsAsync(
        HttpContext context,
        StorageRequestContext request,
        ClaimsPrincipal principal,
        string? objectId,
        HashSet<char> granted,
        bool requireDataAuthorization)
    {
        var (readChecked, authorizedGenerationId) = await TryGrantAclReadAsync(
            context, request, principal, objectId, granted, requireDataAuthorization).ConfigureAwait(false);
        var (listChecked, listGroups) = await TryGrantAclListAsync(
            context, request, principal, objectId, granted, requireDataAuthorization).ConfigureAwait(false);
        var (mutationChecked, mutationGroups) = await TryGrantAclMutationAsync(
            context, request, principal, objectId, granted, requireDataAuthorization).ConfigureAwait(false);
        var (appendChecked, appendGroups, appendGenerationId) = await TryGrantAclAppendAsync(
            context, request, principal, objectId, granted, requireDataAuthorization).ConfigureAwait(false);
        if (appendChecked)
            authorizedGenerationId = appendGenerationId;
        return new BearerAclGrants(
            readChecked,
            authorizedGenerationId,
            listChecked,
            listGroups,
            mutationChecked,
            mutationGroups,
            appendChecked,
            appendGroups);
    }

    private readonly record struct BearerAclGrants(
        bool ReadChecked,
        string? AuthorizedGenerationId,
        bool ListChecked,
        HashSet<string>? ListGroups,
        bool MutationChecked,
        HashSet<string>? MutationGroups,
        bool AppendChecked,
        HashSet<string>? AppendGroups);

    private async Task<(ClaimsPrincipal Principal, string Subject, string? ObjectId,
        HashSet<char> Granted, HashSet<char> AccountWide, bool CanGenerateUserDelegationKey)>
        AuthenticateBearerClaimsAsync(HttpContext context, StorageRequestContext request)
    {
        var configuration = _options.BearerAuthentication;
        if (!configuration.Enabled)
            throw AzureStorageException.BearerAuthenticationRequired();

        var result = await context.AuthenticateAsync(BearerScheme).ConfigureAwait(false);
        if (!result.Succeeded || result.Principal?.Identity?.IsAuthenticated != true)
            throw AzureStorageException.BearerAuthenticationRequired();

        var principal = result.Principal;
        var objectId = principal.FindFirst("oid")?.Value;
        var subject = objectId
                      ?? principal.FindFirst("sub")?.Value
                      ?? principal.FindFirst("appid")?.Value;
        if (string.IsNullOrEmpty(subject))
            throw AzureStorageException.AuthorizationFailure();

        var granted = new HashSet<char>();
        var canGenerateUserDelegationKey = false;
        if (configuration.Principals.TryGetValue(subject, out var access) &&
            Covers(access.Accounts, request.Account) &&
            (request.Container is null || Covers(access.Containers, request.Container)))
        {
            granted.UnionWith(access.Permissions);
            canGenerateUserDelegationKey = access.CanGenerateUserDelegationKey;
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
        return (principal, subject, objectId, granted, accountWide, canGenerateUserDelegationKey);
    }

    private async Task<(bool Checked, string? GenerationId)> TryGrantAclReadAsync(
        HttpContext context,
        StorageRequestContext request,
        ClaimsPrincipal principal,
        string? objectId,
        HashSet<char> granted,
        bool requireDataAuthorization)
    {
        if (!requireDataAuthorization ||
            granted.Contains('r') ||
            objectId is null ||
            !Guid.TryParse(objectId, out _) ||
            !IsHierarchicalNamespaceEnabled(request.Account) ||
            request.ResourceKind != StorageResourceKind.Blob ||
            !HierarchicalAclAuthorization.IsBlobReadOperation(context.Request))
            return (false, null);
        try
        {
            var generationId = await HierarchicalAclAuthorization.EnsureReadAsync(
                metadata,
                context.Request,
                request,
                objectId,
                await groupMembershipResolver.ResolveAsync(principal, objectId, context.RequestAborted)
                    .ConfigureAwait(false),
                "r",
                context.RequestAborted).ConfigureAwait(false);
            granted.Add('r');
            return (true, generationId);
        }
        catch (AzureStorageException error) when (string.Equals(error.ErrorCode, "AuthorizationFailure", StringComparison.Ordinal))
        {
            // An ACL cannot grant this read; retain only the configured RBAC grants.
            return (false, null);
        }
    }

    private async Task<(bool Checked, HashSet<string>? Groups)> TryGrantAclListAsync(
        HttpContext context,
        StorageRequestContext request,
        ClaimsPrincipal principal,
        string? objectId,
        HashSet<char> granted,
        bool requireDataAuthorization)
    {
        if (!requireDataAuthorization ||
            granted.Contains('l') ||
            objectId is null ||
            !Guid.TryParse(objectId, out _) ||
            !IsHierarchicalNamespaceEnabled(request.Account) ||
            !HierarchicalAclAuthorization.IsDirectoryListOperation(context.Request, request))
            return (false, null);
        try
        {
            var groups = await groupMembershipResolver.ResolveAsync(principal, objectId, context.RequestAborted)
                .ConfigureAwait(false);
            await HierarchicalAclAuthorization.EnsureDirectoryListAsync(
                metadata,
                context.Request,
                request,
                objectId,
                groups,
                "l",
                context.RequestAborted).ConfigureAwait(false);
            granted.Add('l');
            return (true, groups);
        }
        catch (AzureStorageException error) when (string.Equals(error.ErrorCode, "AuthorizationFailure", StringComparison.Ordinal))
        {
            // An ACL cannot grant this list; retain only the configured RBAC grants.
            return (false, null);
        }
    }

    private async Task<(bool Checked, HashSet<string>? Groups)> TryGrantAclMutationAsync(
        HttpContext context,
        StorageRequestContext request,
        ClaimsPrincipal principal,
        string? objectId,
        HashSet<char> granted,
        bool requireDataAuthorization)
    {
        var parentMutationPermission = HierarchicalAclAuthorization.GetParentMutationPermission(
            context.Request, request);
        if (!requireDataAuthorization ||
            parentMutationPermission is not { } mutationPermission ||
            granted.Contains(mutationPermission) ||
            objectId is null ||
            !Guid.TryParse(objectId, out _) ||
            !IsHierarchicalNamespaceEnabled(request.Account))
            return (false, null);
        try
        {
            var groups = await groupMembershipResolver.ResolveAsync(principal, objectId, context.RequestAborted)
                .ConfigureAwait(false);
            await HierarchicalAclAuthorization.EnsureParentMutationAsync(
                metadata,
                context.Request,
                request,
                objectId,
                groups,
                mutationPermission.ToString(),
                context.RequestAborted).ConfigureAwait(false);
            granted.Add(mutationPermission);
            return (true, groups);
        }
        catch (AzureStorageException error) when (string.Equals(error.ErrorCode, "AuthorizationFailure", StringComparison.Ordinal))
        {
            // An ACL cannot grant this mutation; retain only the configured RBAC grants.
            return (false, null);
        }
    }

    private async Task<(bool Checked, HashSet<string>? Groups, string? GenerationId)> TryGrantAclAppendAsync(
        HttpContext context,
        StorageRequestContext request,
        ClaimsPrincipal principal,
        string? objectId,
        HashSet<char> granted,
        bool requireDataAuthorization)
    {
        if (!requireDataAuthorization ||
            granted.Contains('a') ||
            granted.Contains('w') ||
            objectId is null ||
            !Guid.TryParse(objectId, out _) ||
            !IsHierarchicalNamespaceEnabled(request.Account) ||
            !HierarchicalAclAuthorization.IsAppendOperation(context.Request, request))
            return (false, null, null);
        try
        {
            var groups = await groupMembershipResolver.ResolveAsync(principal, objectId, context.RequestAborted)
                .ConfigureAwait(false);
            var generationId = await HierarchicalAclAuthorization.EnsureAppendAsync(
                metadata,
                context.Request,
                request,
                objectId,
                groups,
                "a",
                context.RequestAborted).ConfigureAwait(false);
            granted.Add('a');
            return (true, groups, generationId);
        }
        catch (AzureStorageException error) when (string.Equals(error.ErrorCode, "AuthorizationFailure", StringComparison.Ordinal))
        {
            // An ACL cannot grant this append; retain only the configured RBAC grants.
            return (false, null, null);
        }
    }

    private StorageAuthorization AuthenticateSharedKey(
        HttpRequest httpRequest,
        StorageRequestContext request,
        string authorization,
        bool lite)
    {
        var separator = authorization.IndexOf(' ', StringComparison.Ordinal);
        var value = authorization[(separator + 1)..];
        var colon = value.IndexOf(':', StringComparison.Ordinal);
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
        StorageAuthorization? bearer,
        CancellationToken cancellationToken)
    {
        var query = context.Request.Query;
        var (version, suppliedSignature, hasSignedVersion, signedVersion, encodedKey, isAccountSas, isUserDelegationSas) =
            ValidateSasRequestIdentity(request, query);
        var (protocol, signedIp) = ValidateSasTransport(context, query, hasSignedVersion, signedVersion);

        var permissions = query["sp"].ToString();
        var signedStartsAt = ParseSasTime(query["st"].ToString());
        var signedExpiresAt = ParseSasTime(query["se"].ToString());
        var startsAt = signedStartsAt;
        var expiresAt = signedExpiresAt;
        var signedEncryptionScope = query["ses"].ToString();
        if (!string.IsNullOrEmpty(signedEncryptionScope) && signedVersion < new DateOnly(2020, 12, 6))
            throw AzureStorageException.AuthorizationFailure();
        string stringToSign;
        var signedResource = string.Empty;
        var signingKey = encodedKey;
        var isCrossTenantUserBoundSas = false;
        string? delegatedCreatorObjectId = null;
        string? aclObjectId = null;
        if (isAccountSas)
        {
            stringToSign = BuildAccountSasStringToSign(
                context, request, query, permissions, signedVersion, expiresAt, signedIp, protocol, version);
        }
        else if (isUserDelegationSas)
        {
            var plan = BuildUserDelegationSasPlan(
                context, request, query, signedVersion, version, permissions,
                startsAt, expiresAt, signedIp, protocol, bearer);
            stringToSign = plan.StringToSign;
            signingKey = plan.SigningKey;
            permissions = plan.Permissions;
            startsAt = plan.StartsAt;
            expiresAt = plan.ExpiresAt;
            signedResource = plan.SignedResource;
            delegatedCreatorObjectId = plan.CreatorObjectId;
            aclObjectId = plan.AclObjectId;
            isCrossTenantUserBoundSas = plan.IsCrossTenantUserBoundSas;
        }
        else
        {
            signedResource = query["sr"].ToString();
            stringToSign = BuildServiceSasStringToSign(
                request, query, permissions, signedResource, signedVersion,
                hasSignedVersion, signedIp, protocol, version);
            (permissions, startsAt, expiresAt) = await ApplyStoredAccessPolicyAsync(
                request, query, signedVersion, permissions, startsAt, expiresAt, cancellationToken).ConfigureAwait(false);
        }

        VerifySasSignature(signingKey, stringToSign, suppliedSignature);
        ValidateSasTimeAndPolicies(
            request, query, permissions, startsAt, expiresAt,
            signedStartsAt, signedExpiresAt, hasSignedVersion,
            isAccountSas, isUserDelegationSas, isCrossTenantUserBoundSas);
        var acl = await EvaluateSasAclAsync(
            context, request, aclObjectId, permissions, cancellationToken).ConfigureAwait(false);
        return CreateSasAuthorization(
            query, permissions, startsAt, expiresAt, isAccountSas, isUserDelegationSas,
            signedResource, delegatedCreatorObjectId, aclObjectId, acl);
    }

    private (string Version, string SuppliedSignature, bool HasSignedVersion, DateOnly SignedVersion,
        string EncodedKey, bool IsAccountSas, bool IsUserDelegationSas) ValidateSasRequestIdentity(
            StorageRequestContext request,
            IQueryCollection query)
    {
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
        if (!isUserDelegationSas)
            EnsureSharedKeyAccessAllowed(request.Account);
        var hasSignedRequestFields = query.ContainsKey("srh") || query.ContainsKey("srq");
        if (hasSignedRequestFields &&
            (!isUserDelegationSas || signedVersion < new DateOnly(2026, 4, 6)))
        {
            throw AzureStorageException.AuthenticationFailed();
        }
        return (version, suppliedSignature, hasSignedVersion, signedVersion, encodedKey, isAccountSas, isUserDelegationSas);
    }

    private static void VerifySasSignature(string signingKey, string stringToSign, string suppliedSignature)
    {
        var expected = Sign(signingKey, stringToSign);
        if (!FixedTimeEquals(expected, suppliedSignature))
            throw AzureStorageException.AuthenticationFailed();
    }

    private void ValidateSasTimeAndPolicies(
        StorageRequestContext request,
        IQueryCollection query,
        string permissions,
        DateTimeOffset? startsAt,
        DateTimeOffset? expiresAt,
        DateTimeOffset? signedStartsAt,
        DateTimeOffset? signedExpiresAt,
        bool hasSignedVersion,
        bool isAccountSas,
        bool isUserDelegationSas,
        bool isCrossTenantUserBoundSas)
    {
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
        if (isCrossTenantUserBoundSas && !AllowsCrossTenantDelegationSas(request.Account))
            throw AzureStorageException.AuthorizationFailure();
        ApplySasExpirationPolicy(
            request,
            isStoredAccessPolicySas: !isAccountSas &&
                                     !isUserDelegationSas &&
                                     !string.IsNullOrEmpty(query["si"].ToString()),
            signedStartsAt,
            signedExpiresAt);
        ApplyUserBoundSasPolicy(
            request,
            isUserDelegationSas && !string.IsNullOrEmpty(query["sduoid"].ToString()));
    }

    private static StorageAuthorization CreateSasAuthorization(
        IQueryCollection query,
        string permissions,
        DateTimeOffset? startsAt,
        DateTimeOffset? expiresAt,
        bool isAccountSas,
        bool isUserDelegationSas,
        string signedResource,
        string? delegatedCreatorObjectId,
        string? aclObjectId,
        SasAclGrant acl)
    {
        return new StorageAuthorization(
            StorageAuthorizationKind.Sas,
            permissions,
            startsAt,
            expiresAt,
            query["si"].ToString(),
            isAccountSas,
            signedResource,
            TenantId: isUserDelegationSas ? query["sktid"].ToString() : null,
            DelegatedObjectId: delegatedCreatorObjectId,
            AclReadChecked: aclObjectId is not null && !acl.ListChecked && !acl.MutationChecked && !acl.AppendChecked,
            AclAuthorizedGenerationId: acl.AuthorizedGenerationId,
            AclListChecked: acl.ListChecked,
            AclListObjectId: acl.ListChecked ? aclObjectId : null,
            AclListGroups: acl.ListChecked ? NoGroups : null,
            AclMutationChecked: acl.MutationChecked,
            AclMutationObjectId: acl.MutationChecked ? aclObjectId : null,
            AclMutationGroups: acl.MutationChecked ? NoGroups : null,
            AclAppendChecked: acl.AppendChecked,
            AclAppendObjectId: acl.AppendChecked ? aclObjectId : null,
            AclAppendGroups: acl.AppendChecked ? NoGroups : null);
    }

    private async Task<SasAclGrant> EvaluateSasAclAsync(
        HttpContext context,
        StorageRequestContext request,
        string? aclObjectId,
        string permissions,
        CancellationToken cancellationToken)
    {
        if (aclObjectId is null)
            return new SasAclGrant(null, false, false, false);
        if (HierarchicalAclAuthorization.IsAppendOperation(context.Request, request))
        {
            var generationId = await HierarchicalAclAuthorization.EnsureAppendAsync(
                metadata, context.Request, request, aclObjectId, NoGroups,
                permissions, cancellationToken).ConfigureAwait(false);
            return new SasAclGrant(generationId, false, false, true);
        }
        if (HierarchicalAclAuthorization.GetParentMutationPermission(context.Request, request) is not null)
        {
            await HierarchicalAclAuthorization.EnsureParentMutationAsync(
                metadata, context.Request, request, aclObjectId, NoGroups,
                permissions, cancellationToken).ConfigureAwait(false);
            return new SasAclGrant(null, false, true, false);
        }
        if (HierarchicalAclAuthorization.IsDirectoryListOperation(context.Request, request))
        {
            await HierarchicalAclAuthorization.EnsureDirectoryListAsync(
                metadata, context.Request, request, aclObjectId, NoGroups,
                permissions, cancellationToken).ConfigureAwait(false);
            return new SasAclGrant(null, true, false, false);
        }
        var readGenerationId = await HierarchicalAclAuthorization.EnsureReadAsync(
            metadata,
            context.Request,
            request,
            aclObjectId,
            NoGroups,
            permissions,
            cancellationToken).ConfigureAwait(false);
        return new SasAclGrant(readGenerationId, false, false, false);
    }

    private readonly record struct SasAclGrant(
        string? AuthorizedGenerationId,
        bool ListChecked,
        bool MutationChecked,
        bool AppendChecked);

    private static string BuildAccountSasStringToSign(
        HttpContext context,
        StorageRequestContext request,
        IQueryCollection query,
        string permissions,
        DateOnly signedVersion,
        DateTimeOffset? expiresAt,
        string signedIp,
        string protocol,
        string version)
    {
        if (!string.IsNullOrEmpty(query["si"].ToString()))
            throw AzureStorageException.AuthenticationFailed();
        var services = query["ss"].ToString();
        var resourceTypes = query["srt"].ToString();
        ValidateAccountSasFields(services, resourceTypes, permissions, signedVersion, expiresAt);
        if (!services.Contains('b', StringComparison.Ordinal))
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
        return string.Join('\n', fields) + "\n";
    }

    private static (string Protocol, string SignedIp) ValidateSasTransport(
        HttpContext context,
        IQueryCollection query,
        bool hasSignedVersion,
        DateOnly signedVersion)
    {
        var protocol = query["spr"].ToString();
        var signedIp = query["sip"].ToString();
        if ((!hasSignedVersion || signedVersion < new DateOnly(2015, 4, 5)) &&
            (!string.IsNullOrEmpty(protocol) || !string.IsNullOrEmpty(signedIp)))
        {
            throw AzureStorageException.AuthenticationFailed();
        }
        if (protocol is not ("" or "https" or "https,http"))
            throw AzureStorageException.AuthenticationFailed("The signed protocol field is invalid.");
        if (string.Equals(protocol, "https", StringComparison.Ordinal) && !context.Request.IsHttps)
            throw AzureStorageException.AuthorizationProtocolMismatch();
        if (!string.IsNullOrEmpty(signedIp))
        {
            if (!TryParseIpRange(signedIp, out var rangeStart, out var rangeEnd))
                throw AzureStorageException.AuthenticationFailed("The signed IP field is invalid.");
            if (!MatchesIpRange(context.Connection.RemoteIpAddress, rangeStart, rangeEnd))
                throw AzureStorageException.AuthorizationSourceIpMismatch(context.Connection.RemoteIpAddress);
        }
        return (protocol, signedIp);
    }

    private string BuildServiceSasStringToSign(
        StorageRequestContext request,
        IQueryCollection query,
        string permissions,
        string resourceType,
        DateOnly signedVersion,
        bool hasSignedVersion,
        string signedIp,
        string protocol,
        string version)
    {
        ValidateServiceSasPermissions(permissions, signedVersion);
        if (string.Equals(resourceType, "d", StringComparison.Ordinal) && !IsHierarchicalNamespaceEnabled(request.Account))
            throw AzureStorageException.AuthorizationFailure();
        if (!ServiceSasCoversRequest(resourceType, request, signedVersion))
            throw AzureStorageException.AuthorizationFailure();
        var canonicalizedResource = BuildSasCanonicalResource(request, resourceType, query, signedVersion);
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
        return string.Join('\n', fields);
    }

    private async Task<(string Permissions, DateTimeOffset? StartsAt, DateTimeOffset? ExpiresAt)>
        ApplyStoredAccessPolicyAsync(
            StorageRequestContext request,
            IQueryCollection query,
            DateOnly signedVersion,
            string permissions,
            DateTimeOffset? startsAt,
            DateTimeOffset? expiresAt,
            CancellationToken cancellationToken)
    {
        var identifier = query["si"].ToString();
        if (string.IsNullOrEmpty(identifier))
            return (permissions, startsAt, expiresAt);
        if (request.Container is null)
            throw AzureStorageException.AuthorizationFailure();
        var container = await metadata.GetContainerAsync(
            request.Account, request.Container, includeDeleted: false, cancellationToken).ConfigureAwait(false);
        if (container is null || !container.AccessPolicies.TryGetValue(identifier, out var policy))
            throw AzureStorageException.AuthorizationFailure();
        ValidateServiceSasPermissions(policy.Permission, signedVersion);
        if (!string.IsNullOrEmpty(permissions) && !string.IsNullOrEmpty(policy.Permission))
            throw AzureStorageException.InvalidQuery("sp");
        if (startsAt.HasValue && policy.StartsAt.HasValue)
            throw AzureStorageException.InvalidQuery("st");
        if (expiresAt.HasValue && policy.ExpiresAt.HasValue)
            throw AzureStorageException.InvalidQuery("se");
        permissions = string.IsNullOrEmpty(permissions) ? policy.Permission : permissions;
        startsAt ??= policy.StartsAt;
        expiresAt ??= policy.ExpiresAt;
        return (permissions, startsAt, expiresAt);
    }

    private UserDelegationKeyValues ReadUserDelegationKeyValues(
        StorageRequestContext request,
        IQueryCollection query)
    {
        var objectId = query["skoid"].ToString();
        var tenantId = query["sktid"].ToString();
        var keyStartText = query["skt"].ToString();
        var keyExpiryText = query["ske"].ToString();
        var keyService = query["sks"].ToString();
        var keyVersion = query["skv"].ToString();
        if (!Guid.TryParse(objectId, out _) ||
            !Guid.TryParse(tenantId, out _) ||
            !string.Equals(keyService, "b", StringComparison.Ordinal) ||
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
        return new UserDelegationKeyValues(
            objectId,
            tenantId,
            keyStartText,
            keyExpiryText,
            keyService,
            keyVersion,
            keyStartsAt.Value,
            keyExpiresAt.Value,
            delegatedPrincipal.Permissions,
            delegatedPrincipal.CanManageOwnership);
    }

    private sealed record UserDelegationKeyValues(
        string ObjectId,
        string TenantId,
        string StartText,
        string ExpiryText,
        string Service,
        string Version,
        DateTimeOffset StartsAt,
        DateTimeOffset ExpiresAt,
        string GrantedPermissions,
        bool CanManageOwnership);

    private UserDelegationIdentityValues ValidateUserDelegationIdentity(
        StorageRequestContext request,
        IQueryCollection query,
        DateOnly signedVersion,
        UserDelegationKeyValues key,
        StorageAuthorization? bearer)
    {
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
        if ((hasAuthorizedObjectId || hasUnauthorizedObjectId) && !key.CanManageOwnership)
            throw AzureStorageException.AuthorizationFailure();
        var creatorObjectId = hasAuthorizedObjectId
            ? authorizedObjectId
            : hasUnauthorizedObjectId ? unauthorizedObjectId : key.ObjectId;
        var aclObjectId = hasUnauthorizedObjectId ? unauthorizedObjectId : null;

        var (delegatedUserTenantId, delegatedUserObjectId, isCrossTenantUserBoundSas) =
            ValidateUserBoundDelegation(query, signedVersion, key.TenantId, bearer);
        return new UserDelegationIdentityValues(
            authorizedObjectId,
            unauthorizedObjectId,
            correlationId,
            delegatedUserTenantId,
            delegatedUserObjectId,
            creatorObjectId,
            aclObjectId,
            isCrossTenantUserBoundSas);
    }

    private sealed record UserDelegationIdentityValues(
        string AuthorizedObjectId,
        string UnauthorizedObjectId,
        string CorrelationId,
        string DelegatedUserTenantId,
        string DelegatedUserObjectId,
        string CreatorObjectId,
        string? AclObjectId,
        bool IsCrossTenantUserBoundSas);

    private static (string TenantId, string ObjectId, bool IsCrossTenant) ValidateUserBoundDelegation(
        IQueryCollection query,
        DateOnly signedVersion,
        string keyTenantId,
        StorageAuthorization? bearer)
    {
        var delegatedUserTenantId = query["skdutid"].ToString();
        var delegatedUserObjectId = query["sduoid"].ToString();
        if ((!string.IsNullOrEmpty(delegatedUserTenantId) &&
             !Guid.TryParse(delegatedUserTenantId, out _)) ||
            (!string.IsNullOrEmpty(delegatedUserObjectId) &&
             !Guid.TryParse(delegatedUserObjectId, out _)))
        {
            throw AzureStorageException.AuthenticationFailed();
        }
        var isCrossTenantUserBoundSas = false;
        if (!string.IsNullOrEmpty(delegatedUserObjectId))
        {
            var expectedBearerTenantId = string.IsNullOrEmpty(delegatedUserTenantId)
                ? keyTenantId
                : delegatedUserTenantId;
            isCrossTenantUserBoundSas = !string.IsNullOrEmpty(delegatedUserTenantId) &&
                                        !SameTenant(delegatedUserTenantId, keyTenantId);
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
        return (delegatedUserTenantId, delegatedUserObjectId, isCrossTenantUserBoundSas);
    }

    private static string BuildUserDelegationSasStringToSign(
        HttpContext context,
        IQueryCollection query,
        DateOnly signedVersion,
        string version,
        string resourceType,
        string canonicalizedResource,
        string signedIp,
        string protocol,
        UserDelegationKeyValues key,
        UserDelegationIdentityValues identity)
    {
        var fields = new List<string>
        {
            query["sp"].ToString(),
            query["st"].ToString(),
            query["se"].ToString(),
            canonicalizedResource,
            key.ObjectId,
            key.TenantId,
            key.StartText,
            key.ExpiryText,
            key.Service,
            key.Version,
            identity.AuthorizedObjectId,
            identity.UnauthorizedObjectId,
            identity.CorrelationId
        };
        if (signedVersion >= new DateOnly(2025, 7, 5))
        {
            fields.Add(identity.DelegatedUserTenantId);
            fields.Add(identity.DelegatedUserObjectId);
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
        return string.Join('\n', fields);
    }

    private UserDelegationSasPlan BuildUserDelegationSasPlan(
        HttpContext context,
        StorageRequestContext request,
        IQueryCollection query,
        DateOnly signedVersion,
        string version,
        string permissions,
        DateTimeOffset? startsAt,
        DateTimeOffset? expiresAt,
        string signedIp,
        string protocol,
        StorageAuthorization? bearer)
    {
        if (signedVersion < new DateOnly(2018, 11, 9) || !string.IsNullOrEmpty(query["si"]))
            throw AzureStorageException.AuthenticationFailed();
        var resourceType = query["sr"].ToString();
        if (string.IsNullOrEmpty(permissions) || expiresAt is null)
            throw AzureStorageException.AuthenticationFailed();
        ValidateServiceSasPermissions(permissions, signedVersion);
        if (string.Equals(resourceType, "d", StringComparison.Ordinal) && !IsHierarchicalNamespaceEnabled(request.Account))
            throw AzureStorageException.AuthorizationFailure();
        if (!ServiceSasCoversRequest(resourceType, request, signedVersion))
            throw AzureStorageException.AuthorizationFailure();
        var canonicalizedResource = BuildSasCanonicalResource(request, resourceType, query, signedVersion);

        var key = ReadUserDelegationKeyValues(request, query);
        permissions = IntersectPermissions(permissions, key.GrantedPermissions);
        startsAt = Latest(startsAt, key.StartsAt);
        expiresAt = Earliest(expiresAt, key.ExpiresAt);
        var identity = ValidateUserDelegationIdentity(request, query, signedVersion, key, bearer);
        var signingKey = DeriveUserDelegationKey(
            request.Account,
            key.ObjectId,
            key.TenantId,
            FormatSasTime(key.StartsAt),
            FormatSasTime(key.ExpiresAt),
            key.Service,
            key.Version,
            NullIfEmpty(identity.DelegatedUserTenantId));
        var stringToSign = BuildUserDelegationSasStringToSign(
            context, query, signedVersion, version, resourceType, canonicalizedResource,
            signedIp, protocol, key, identity);
        return new UserDelegationSasPlan(
            stringToSign,
            signingKey,
            permissions,
            startsAt,
            expiresAt,
            resourceType,
            identity.CreatorObjectId,
            identity.AclObjectId,
            identity.IsCrossTenantUserBoundSas);
    }

    private sealed record UserDelegationSasPlan(
        string StringToSign,
        string SigningKey,
        string Permissions,
        DateTimeOffset? StartsAt,
        DateTimeOffset? ExpiresAt,
        string SignedResource,
        string CreatorObjectId,
        string? AclObjectId,
        bool IsCrossTenantUserBoundSas);

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
                     .Select(header => new { Key = header.Key.ToRequiredLowerInvariant(), header.Value })
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
                .Append(parameter.Key.ToRequiredLowerInvariant())
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
        HttpRequest httpRequest)
    {
        var component = httpRequest.Query["comp"].ToString();
        if (HttpMethods.IsPost(httpRequest.Method) &&
            string.Equals(component, "batch", StringComparison.OrdinalIgnoreCase))
        {
            return resourceTypes.Contains('o', StringComparison.Ordinal);
        }
        if (request.ResourceKind == StorageResourceKind.Service &&
            HttpMethods.IsGet(httpRequest.Method) &&
            string.Equals(component, "blobs", StringComparison.OrdinalIgnoreCase))
        {
            return resourceTypes.Contains('o', StringComparison.Ordinal);
        }

        return request.ResourceKind switch
        {
            StorageResourceKind.Service => resourceTypes.Contains('s', StringComparison.Ordinal),
            StorageResourceKind.Container => resourceTypes.Contains('c', StringComparison.Ordinal),
            StorageResourceKind.Blob when
                HttpMethods.IsPut(httpRequest.Method) &&
                string.Equals(httpRequest.Query["comp"], "undelete", StringComparison.OrdinalIgnoreCase) =>
                resourceTypes.Contains('c', StringComparison.Ordinal),
            StorageResourceKind.Blob => resourceTypes.Contains('o', StringComparison.Ordinal),
            _ => false
        };
    }

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

    private static string GetSignedSnapshotOrVersion(IQueryCollection query, string resourceType) => string.Equals(resourceType, "bv"
, StringComparison.Ordinal) ? query["versionid"].ToString()
            : string.Equals(resourceType, "bs"
, StringComparison.Ordinal) ? query["snapshot"].ToString()
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
        if (string.Equals(resourceType, "d", StringComparison.Ordinal))
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
            var current = order.IndexOf(character, StringComparison.Ordinal);
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
            var name = item.ToRequiredLowerInvariant();
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
        if (names.Length == 0)
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

    private static string[] ReadEncodedCommaSeparatedQueryValue(
        HttpRequest request,
        string parameterName)
    {
        var rawQuery = request.QueryString.Value;
        if (string.IsNullOrEmpty(rawQuery))
            return [];

        string? encodedValue = null;
        foreach (var pair in rawQuery.AsSpan(1).ToString().Split('&'))
        {
            var separator = pair.IndexOf('=', StringComparison.Ordinal);
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

    private static bool Covers(ICollection<string> configuredValues, string value) =>
        configuredValues.Count == 0 || configuredValues.Contains("*", StringComparer.Ordinal) || configuredValues.Contains(value, StringComparer.Ordinal);

    private bool IsHierarchicalNamespaceEnabled(string account) =>
        _options.AccountCapabilities.TryGetValue(account, out var capabilities) &&
        capabilities.HierarchicalNamespaceEnabled;

    private void EnsureSharedKeyAccessAllowed(string account)
    {
        if (_options.AccountCapabilities.TryGetValue(account, out var capabilities) &&
            (!capabilities.AllowSharedKeyAccess ||
             capabilities.AllowSharedKeyAccessForServices.Blob?.Enabled == false))
        {
            throw AzureStorageException.KeyBasedAuthenticationNotPermitted();
        }
    }

    private bool AllowsCrossTenantDelegationSas(string account) =>
        _options.AccountCapabilities.TryGetValue(account, out var capabilities) &&
        capabilities.AllowCrossTenantDelegationSas;

    private void ApplyUserBoundSasPolicy(StorageRequestContext request, bool isUserBound)
    {
        if (isUserBound ||
            !_options.AccountCapabilities.TryGetValue(request.Account, out var capabilities) ||
            !capabilities.RequireUserBoundUserDelegationSas)
        {
            return;
        }

        if (capabilities.RequireUserBoundUserDelegationSasAction == SasPolicyViolationAction.Block)
            throw AzureStorageException.AuthorizationFailure();
        if (capabilities.RequireUserBoundUserDelegationSasAction == SasPolicyViolationAction.Log)
        {
            StorageLogMessages.UserBoundSasMissing(logger, request.RequestId, request.Account);
        }
    }

    private void ApplySasExpirationPolicy(
        StorageRequestContext request,
        bool isStoredAccessPolicySas,
        DateTimeOffset? signedStartsAt,
        DateTimeOffset? signedExpiresAt)
    {
        if (isStoredAccessPolicySas ||
            !_options.AccountCapabilities.TryGetValue(request.Account, out var capabilities) ||
            capabilities.SasExpirationPeriod is not { } maximumInterval)
        {
            return;
        }

        var violation = signedStartsAt is not { } signedStart
            ? "missing a signed start time"
            : signedExpiresAt is { } signedExpiry && signedExpiry - signedStart > maximumInterval
                ? "exceeds the configured validity interval"
                : null;
        if (violation is null)
            return;

        if (capabilities.SasExpirationAction == SasExpirationPolicyAction.Block)
            throw AzureStorageException.AuthorizationFailure();
        StorageLogMessages.SasExpirationPolicyViolated(logger, request.RequestId, request.Account, violation);
    }

    private static bool SameTenant(string left, string right) =>
        Guid.TryParse(left, out var leftTenant) &&
        Guid.TryParse(right, out var rightTenant) &&
        leftTenant == rightTenant;

    private static bool TryParseIpRange(
        string range,
        out IPAddress start,
        out IPAddress end)
    {
        start = IPAddress.None;
        end = IPAddress.None;
        var values = range.Split('-', 2);
        if (!IPAddress.TryParse(values[0], out var parsedStart) ||
            parsedStart.AddressFamily != AddressFamily.InterNetwork)
            return false;

        start = parsedStart;
        if (values.Length == 1)
        {
            end = start;
            return true;
        }

        if (!IPAddress.TryParse(values[1], out var parsedEnd) ||
            parsedEnd.AddressFamily != AddressFamily.InterNetwork ||
            Compare(start.GetAddressBytes(), parsedEnd.GetAddressBytes()) > 0)
            return false;

        end = parsedEnd;
        return true;
    }

    private static bool MatchesIpRange(IPAddress? address, IPAddress start, IPAddress end)
    {
        if (address is null || address.AddressFamily != AddressFamily.InterNetwork)
            return false;
        var candidateBytes = address.GetAddressBytes();
        return Compare(candidateBytes, start.GetAddressBytes()) >= 0 &&
               Compare(candidateBytes, end.GetAddressBytes()) <= 0;
    }

    private static int Compare(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right) => left.SequenceCompareTo(right);
}
