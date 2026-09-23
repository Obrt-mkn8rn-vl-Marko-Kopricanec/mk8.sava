using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

public enum StorageResourceKind
{
    Service,
    Container,
    Blob,
    StaticWebsite
}

public sealed record StorageRequestContext
{
    private const string ItemKey = "Mk8.Sava.RequestContext";

    public required string RequestId { get; init; }
    public required string Account { get; init; }
    public string? Container { get; init; }
    public string? Blob { get; init; }
    public string? Snapshot { get; init; }
    public string? VersionId { get; init; }
    public required StorageResourceKind ResourceKind { get; init; }
    public required string CanonicalResourcePath { get; init; }
    public required string ServiceVersion { get; init; }
    public required StorageAuthorization Authorization { get; set; }

    public static StorageRequestContext Get(HttpContext context) =>
        TryGet(context) ?? throw new InvalidOperationException("The storage request context has not been initialized.");

    public static StorageRequestContext? TryGet(HttpContext context) =>
        context.Items.TryGetValue(ItemKey, out var value) ? value as StorageRequestContext : null;

    public static void Set(HttpContext context, StorageRequestContext value) => context.Items[ItemKey] = value;
}

public sealed class RequestContextMiddleware(
    RequestDelegate next,
    IOptions<SavaOptions> options,
    StorageAuthenticator authenticator,
    MetadataStore metadata)
{
    private enum ServiceVersionSource
    {
        Header,
        SasApiVersion,
        SasSignedVersion,
        AccountDefault,
        AnonymousFallback
    }

    private sealed record ParsedRequest(StorageRequestContext Context, ServiceVersionSource VersionSource);

    private readonly SavaOptions _options = options.Value;

    public async Task InvokeAsync(HttpContext context)
    {
        if (context.Request.Path.StartsWithSegments("/health") ||
            context.Request.Path.StartsWithSegments("/metrics"))
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        var parsed = await ParseAsync(context).ConfigureAwait(false);
        StorageRequestContext.Set(context, parsed.Context);
        if (!context.Request.IsHttps &&
            _options.AccountCapabilities.TryGetValue(parsed.Context.Account, out var capabilities) &&
            capabilities.EnableHttpsTrafficOnly)
        {
            throw AzureStorageException.AccountRequiresHttps();
        }
        BlobProtocolEndpoint.ValidateBlobVersionRequest(parsed.Context);
        var isCorsPreflight = parsed.Context.ResourceKind != StorageResourceKind.StaticWebsite &&
                              HttpMethods.IsOptions(context.Request.Method);
        parsed.Context.Authorization = parsed.Context.ResourceKind == StorageResourceKind.StaticWebsite || isCorsPreflight
            ? StorageAuthorization.Anonymous
            : await authenticator.AuthenticateAsync(context, parsed.Context, context.RequestAborted).ConfigureAwait(false);
        ValidateAuthorizationVersion(parsed);
        AzureExceptionMiddleware.AddCommonHeaders(context);
        await next(context).ConfigureAwait(false);
    }

    private async Task<ParsedRequest> ParseAsync(HttpContext context)
    {
        var segments = StorageResourcePath.DecodeRequestSegments(context.Request);
        var account = ResolveHostAccount(context.Request.Host.Host);
        var staticWebsite = account is not null && IsStaticWebsiteHost(context.Request.Host.Host);
        var pathOffset = 0;

        if (account is null)
        {
            if (segments.Length == 0)
                account = _options.DefaultAccount;
            else
            {
                account = segments[0];
                pathOffset = 1;
            }
        }

        if (!_options.Accounts.ContainsKey(account))
            throw AzureStorageException.AuthenticationFailed("The specified account does not exist.");

        var remaining = segments.Skip(pathOffset).ToArray();
        if (pathOffset == 1 && remaining is [""])
            remaining = [];
        var container = staticWebsite
            ? "$web"
            : remaining.Length > 0 ? remaining[0] : null;
        var blob = staticWebsite
            ? string.Join('/', remaining)
            : remaining.Length > 1
                ? string.Join('/', remaining.Skip(1))
                : null;
        var restype = context.Request.Query["restype"].ToString();
        var resourceKind = staticWebsite
            ? StorageResourceKind.StaticWebsite
            : container is null
            ? StorageResourceKind.Service
            : blob is not null
                ? StorageResourceKind.Blob
                : string.Equals(restype, "container", StringComparison.OrdinalIgnoreCase)
                    ? StorageResourceKind.Container
                    : StorageResourceKind.Blob;

        var canonicalPath = "/" + account;
        if (container is not null)
            canonicalPath += "/" + container;
        if (blob is not null)
            canonicalPath += "/" + blob;

        var (serviceVersion, versionSource) = await ResolveServiceVersionAsync(
            context.Request,
            account,
            context.RequestAborted).ConfigureAwait(false);
        var request = new StorageRequestContext
        {
            RequestId = Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(16)),
            Account = account,
            Container = container,
            Blob = blob,
            Snapshot = NullIfEmpty(context.Request.Query["snapshot"].ToString()),
            VersionId = NullIfEmpty(context.Request.Query["versionid"].ToString()),
            ResourceKind = resourceKind,
            CanonicalResourcePath = canonicalPath,
            ServiceVersion = serviceVersion,
            Authorization = StorageAuthorization.Anonymous
        };
        return new ParsedRequest(request, versionSource);
    }

    private string? ResolveHostAccount(string host)
    {
        var firstLabel = host.Split('.', 2)[0];
        return _options.Accounts.ContainsKey(firstLabel) ? firstLabel : null;
    }

    private static bool IsStaticWebsiteHost(string host) =>
        host.Split('.', StringSplitOptions.RemoveEmptyEntries)
            .Skip(1)
            .Any(label => string.Equals(label, "web", StringComparison.OrdinalIgnoreCase));

    private async Task<(string Version, ServiceVersionSource Source)> ResolveServiceVersionAsync(
        HttpRequest request,
        string account,
        CancellationToken cancellationToken)
    {
        var version = request.Headers["x-ms-version"].ToString();
        if (!string.IsNullOrEmpty(version))
            return (StorageServiceVersions.RequireHeader(version), ServiceVersionSource.Header);

        if (request.Query.ContainsKey("sig"))
        {
            version = request.Query["api-version"].ToString();
            if (!string.IsNullOrEmpty(version))
                return (StorageServiceVersions.RequireApiVersion(version), ServiceVersionSource.SasApiVersion);

            version = request.Query["sv"].ToString();
            if (!string.IsNullOrEmpty(version))
            {
                if (!StorageServiceVersions.TryParse(version, out _))
                    throw AzureStorageException.AuthenticationFailed();
                return (version, ServiceVersionSource.SasSignedVersion);
            }
        }

        var properties = await metadata.GetServicePropertiesAsync(account, cancellationToken).ConfigureAwait(false);
        if (!string.IsNullOrEmpty(properties.DefaultServiceVersion))
        {
            return (
                StorageServiceVersions.RequireHeader(properties.DefaultServiceVersion),
                ServiceVersionSource.AccountDefault);
        }

        return (
            StorageServiceVersions.AnonymousGeneralPurposeFallback,
            ServiceVersionSource.AnonymousFallback);
    }

    private static void ValidateAuthorizationVersion(ParsedRequest request)
    {
        if (request.Context.Authorization.Kind == StorageAuthorizationKind.Bearer)
        {
            if (request.VersionSource != ServiceVersionSource.Header)
                throw AzureStorageException.MissingHeader("x-ms-version");
            if (!StorageServiceVersions.TryParse(request.Context.ServiceVersion, out var version) ||
                version < new DateOnly(2017, 11, 9))
            {
                throw AzureStorageException.InvalidHeader("x-ms-version", request.Context.ServiceVersion);
            }
        }

        if (request.Context.Authorization.Kind == StorageAuthorizationKind.SharedKey &&
            request.VersionSource == ServiceVersionSource.AnonymousFallback)
        {
            throw AzureStorageException.MissingHeader("x-ms-version");
        }
    }

    private static string? NullIfEmpty(string value) => string.IsNullOrEmpty(value) ? null : value;
}
