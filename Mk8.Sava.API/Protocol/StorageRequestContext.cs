using System.Globalization;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;
using Mk8.Sava.Configuration;

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
    StorageAuthenticator authenticator)
{
    private readonly SavaOptions _options = options.Value;

    public async Task InvokeAsync(HttpContext context)
    {
        if (context.Request.Path.StartsWithSegments("/health") ||
            context.Request.Path.StartsWithSegments("/metrics"))
        {
            await next(context);
            return;
        }

        var parsed = Parse(context);
        StorageRequestContext.Set(context, parsed);
        BlobProtocolEndpoint.ValidateBlobVersionRequest(parsed);
        parsed.Authorization = parsed.ResourceKind == StorageResourceKind.StaticWebsite
            ? StorageAuthorization.Anonymous
            : await authenticator.AuthenticateAsync(context, parsed, context.RequestAborted);
        AzureExceptionMiddleware.AddCommonHeaders(context);
        await next(context);
    }

    private StorageRequestContext Parse(HttpContext context)
    {
        var rawPath = context.Request.Path.Value ?? "/";
        var segments = rawPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var account = ResolveHostAccount(context.Request.Host.Host);
        var staticWebsite = account is not null && IsStaticWebsiteHost(context.Request.Host.Host);
        var pathOffset = 0;

        if (account is null)
        {
            if (segments.Length == 0)
                account = _options.DefaultAccount;
            else
            {
                account = Uri.UnescapeDataString(segments[0]);
                pathOffset = 1;
            }
        }

        if (!_options.Accounts.ContainsKey(account))
            throw AzureStorageException.AuthenticationFailed("The specified account does not exist.");

        var remaining = segments.Skip(pathOffset).ToArray();
        var container = staticWebsite
            ? "$web"
            : remaining.Length > 0 ? Uri.UnescapeDataString(remaining[0]) : null;
        var blob = staticWebsite
            ? string.Join('/', remaining.Select(Uri.UnescapeDataString))
            : remaining.Length > 1
                ? string.Join('/', remaining.Skip(1).Select(Uri.UnescapeDataString))
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

        var serviceVersion = ResolveServiceVersion(context.Request);
        return new StorageRequestContext
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

    private static string ResolveServiceVersion(HttpRequest request)
    {
        var version = request.Headers["x-ms-version"].ToString();
        if (string.IsNullOrEmpty(version))
            version = request.Query["api-version"].FirstOrDefault() ?? request.Query["sv"].FirstOrDefault() ?? "2023-11-03";

        if (!DateOnly.TryParseExact(version, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
            throw AzureStorageException.InvalidHeader("x-ms-version", version);
        return version;
    }

    private static string? NullIfEmpty(string value) => string.IsNullOrEmpty(value) ? null : value;
}
