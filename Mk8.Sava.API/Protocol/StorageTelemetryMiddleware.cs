using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Text;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

internal sealed class StorageTelemetryMiddleware(
    RequestDelegate next,
    IStorageTelemetry telemetry,
    IStorageAnalyticsSink analytics,
    TimeProvider timeProvider,
    ILogger<StorageTelemetryMiddleware> logger)
{
    public async Task InvokeAsync(HttpContext context)
    {
        if (context.Request.Path.StartsWithSegments("/health", StringComparison.OrdinalIgnoreCase) ||
            context.Request.Path.StartsWithSegments("/metrics", StringComparison.OrdinalIgnoreCase))
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        var startedAt = timeProvider.GetUtcNow();
        var started = Stopwatch.GetTimestamp();
        try
        {
            await next(context).ConfigureAwait(false);
        }
        finally
        {
            var elapsed = Stopwatch.GetTimestamp() - started;
            telemetry.RecordRequest(context.Response.StatusCode, elapsed);
            var request = StorageRequestContext.TryGet(context);
            logger.LogInformation(
                "Storage request {RequestId} {Method} {ResourceKind} completed with {StatusCode} in {ElapsedMilliseconds:F3} ms.",
                request?.RequestId ?? context.TraceIdentifier,
                context.Request.Method,
                request?.ResourceKind.ToString() ?? "Unknown",
                context.Response.StatusCode,
                elapsed * 1000d / Stopwatch.Frequency);

            if (request is not null)
            {
                try
                {
                    var completedAt = timeProvider.GetUtcNow();
                    await analytics.RecordAsync(
                        CaptureAnalyticsRequest(context, request, startedAt, completedAt, elapsed),
                        CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    logger.LogWarning(
                        exception,
                        "Storage Analytics could not persist request {RequestId}.",
                        request.RequestId);
                }
            }
        }
    }

    private static StorageAnalyticsRequest CaptureAnalyticsRequest(
        HttpContext http,
        StorageRequestContext request,
        DateTimeOffset startedAt,
        DateTimeOffset completedAt,
        long elapsedTicks)
    {
        var authorization = request.Authorization;
        var authenticationType = authorization.Kind switch
        {
            StorageAuthorizationKind.SharedKey => "authenticated",
            StorageAuthorizationKind.Sas => "sas",
            StorageAuthorizationKind.Bearer => "bearer",
            _ => "anonymous"
        };
        var elapsedMilliseconds = Math.Max(
            0,
            (long)Math.Round(
                elapsedTicks * 1000d / Stopwatch.Frequency,
                MidpointRounding.AwayFromZero));
        var requestContentLength = http.Request.ContentLength ?? 0;
        var responseContentLength = http.Response.ContentLength ?? 0;
        return new StorageAnalyticsRequest
        {
            Account = request.Account,
            StartedAt = startedAt,
            CompletedAt = completedAt,
            Operation = ResolveOperation(http.Request, request),
            Category = ResolveCategory(http.Request.Method),
            RequestStatus = ResolveRequestStatus(http.Response, authorization.Kind),
            StatusCode = http.Response.StatusCode,
            EndToEndLatencyMilliseconds = elapsedMilliseconds,
            ServerLatencyMilliseconds = elapsedMilliseconds,
            AuthenticationType = authenticationType,
            RequesterAccountName = authorization.Kind is StorageAuthorizationKind.SharedKey or StorageAuthorizationKind.Bearer
                ? request.Account
                : null,
            RequestUrl = BuildLoggedUrl(http.Request),
            RequestedObjectKey = request.CanonicalResourcePath,
            RequestId = request.RequestId,
            RequesterIpAddress = FormatRemoteAddress(http.Connection),
            RequestVersion = request.ServiceVersion,
            RequestHeaderSize = EstimateRequestHeaderSize(http.Request),
            RequestPacketSize = requestContentLength,
            ResponseHeaderSize = EstimateResponseHeaderSize(http.Response),
            ResponsePacketSize = responseContentLength,
            RequestContentLength = requestContentLength,
            RequestMd5 = Header(http.Request.Headers, "Content-MD5"),
            ServerMd5 = Header(http.Response.Headers, "Content-MD5") ??
                        Header(http.Response.Headers, "x-ms-content-md5"),
            ETag = Header(http.Response.Headers, "ETag"),
            LastModified = Header(http.Response.Headers, "Last-Modified"),
            Conditions = FormatConditions(http.Request.Headers),
            UserAgent = Header(http.Request.Headers, "User-Agent"),
            Referrer = Header(http.Request.Headers, "Referer"),
            ClientRequestId = Header(http.Request.Headers, "x-ms-client-request-id"),
            UserObjectId = authorization.Kind == StorageAuthorizationKind.Bearer ? authorization.Identifier : null,
            TenantId = authorization.Kind == StorageAuthorizationKind.Bearer ? authorization.TenantId : null,
            ApplicationId = authorization.Kind == StorageAuthorizationKind.Bearer ? authorization.ApplicationId : null,
            Audience = authorization.Kind == StorageAuthorizationKind.Bearer ? authorization.Audience : null,
            Issuer = authorization.Kind == StorageAuthorizationKind.Bearer ? authorization.Issuer : null,
            UserPrincipalName = authorization.Kind == StorageAuthorizationKind.Bearer
                ? authorization.UserPrincipalName
                : null
        };
    }

    private static StorageAnalyticsOperationCategory ResolveCategory(string method) => method switch
    {
        "GET" or "HEAD" or "OPTIONS" => StorageAnalyticsOperationCategory.Read,
        "DELETE" => StorageAnalyticsOperationCategory.Delete,
        _ => StorageAnalyticsOperationCategory.Write
    };

    private static string ResolveOperation(HttpRequest http, StorageRequestContext request)
    {
        if (HttpMethods.IsOptions(http.Method))
            return "PreflightBlobRequest";
        var comp = http.Query["comp"].ToString().ToRequiredLowerInvariant();
        if (string.Equals(http.Query["restype"], "account", StringComparison.OrdinalIgnoreCase))
            return "GetAccountInformation";

        return request.ResourceKind switch
        {
            StorageResourceKind.Service => ResolveServiceOperation(http.Method, comp),
            StorageResourceKind.Container => ResolveContainerOperation(http, comp),
            StorageResourceKind.Blob => ResolveBlobOperation(http, comp),
            StorageResourceKind.StaticWebsite => "GetBlob",
            _ => "Unknown"
        };
    }

    private static string ResolveServiceOperation(string method, string comp) => (method, comp) switch
    {
        ("GET", "list") => "ListContainers",
        ("GET", "properties") => "GetBlobServiceProperties",
        ("PUT", "properties") => "SetBlobServiceProperties",
        ("GET", "stats") => "GetBlobServiceStats",
        ("GET", "blobs") => "FindBlobsByTags",
        ("POST", "batch") => "BlobBatch",
        ("POST", "userdelegationkey") => "GetUserDelegationKey",
        _ => "Unknown"
    };

    private static string ResolveContainerOperation(HttpRequest http, string comp) => (http.Method, comp) switch
    {
        ("PUT", "") => "CreateContainer",
        ("DELETE", "") => "DeleteContainer",
        ("GET" or "HEAD", "") => "GetContainerProperties",
        ("GET", "list") => "ListBlobs",
        ("GET" or "HEAD", "metadata") => "GetContainerMetadata",
        ("PUT", "metadata") => "SetContainerMetadata",
        ("GET" or "HEAD", "acl") => "GetContainerACL",
        ("PUT", "acl") => "SetContainerACL",
        ("PUT", "lease") => ResolveLeaseOperation(http, "Container"),
        ("PUT", "undelete") => "RestoreContainer",
        ("PUT", "rename") => "RenameContainer",
        ("GET", "blobs") => "FindBlobsByTags",
        ("POST", "batch") => "BlobBatch",
        _ => "Unknown"
    };

    private static string ResolveBlobOperation(HttpRequest http, string comp)
    {
        if (string.IsNullOrEmpty(comp))
        {
            if (HttpMethods.IsPut(http.Method))
                return http.Headers.ContainsKey("x-ms-copy-source") ? "CopyBlob" : "PutBlob";
            if (HttpMethods.IsGet(http.Method))
                return "GetBlob";
            if (HttpMethods.IsHead(http.Method))
                return "GetBlobProperties";
            if (HttpMethods.IsDelete(http.Method))
                return "DeleteBlob";
        }

        return (http.Method, comp) switch
        {
            ("PUT", "block") => http.Headers.ContainsKey("x-ms-copy-source") ? "PutBlockFromURL" : "PutBlock",
            ("PUT", "blocklist") => "PutBlockList",
            ("GET", "blocklist") => "GetBlockList",
            ("PUT", "appendblock") => http.Headers.ContainsKey("x-ms-copy-source") ? "AppendBlockFromURL" : "AppendBlock",
            ("PUT", "page") => http.Headers.ContainsKey("x-ms-copy-source") ? "PutPageFromURL" : "PutPage",
            ("GET", "pagelist") => "GetPageRanges",
            ("PUT", "incrementalcopy") => "IncrementalCopyBlob",
            ("PUT", "undelete") => "UndeleteBlob",
            ("POST", "query") => "QueryBlobContents",
            ("PUT", "immutabilitypolicies") => "SetBlobImmutabilityPolicy",
            ("DELETE", "immutabilitypolicies") => "DeleteBlobImmutabilityPolicy",
            ("PUT", "legalhold") => "SetBlobLegalHold",
            ("PUT", "copy") => "AbortCopyBlob",
            ("GET" or "HEAD", "metadata") => "GetBlobMetadata",
            ("PUT", "metadata") => "SetBlobMetadata",
            ("GET", "tags") => "GetBlobTags",
            ("PUT", "tags") => "SetBlobTags",
            ("PUT", "properties") => "SetBlobProperties",
            ("PUT", "snapshot") => "SnapshotBlob",
            ("PUT", "seal") => "SealBlob",
            ("PUT", "tier") => "SetBlobTier",
            ("PUT", "expiry") => "SetBlobExpiry",
            ("PUT", "lease") => ResolveLeaseOperation(http, "Blob"),
            _ => "Unknown"
        };
    }

    private static string ResolveLeaseOperation(HttpRequest request, string resource) =>
        request.Headers["x-ms-lease-action"].ToString().ToRequiredLowerInvariant() switch
        {
            "acquire" => $"Acquire{resource}Lease",
            "renew" => $"Renew{resource}Lease",
            "change" => $"Change{resource}Lease",
            "release" => $"Release{resource}Lease",
            "break" => $"Break{resource}Lease",
            _ => $"Lease{resource}"
        };

    private static string ResolveRequestStatus(HttpResponse response, StorageAuthorizationKind authorization)
    {
        if (response.StatusCode is >= 200 and < 300)
        {
            return authorization switch
            {
                StorageAuthorizationKind.Sas => "SASSuccess",
                StorageAuthorizationKind.Bearer => "OAuthSuccess",
                StorageAuthorizationKind.Anonymous => "AnonymousSuccess",
                _ => "Success"
            };
        }
        if (response.StatusCode == StatusCodes.Status304NotModified)
            return "ConditionNotMet";
        return Header(response.Headers, "x-ms-error-code") ??
               (response.StatusCode >= 500 ? "ServerOtherError" : "ClientOtherError");
    }

    private static string BuildLoggedUrl(HttpRequest request)
    {
        var builder = new StringBuilder()
            .Append(request.Scheme)
            .Append("://")
            .Append(request.Host.Value)
            .Append(request.PathBase.ToUriComponent())
            .Append(request.Path.ToUriComponent());
        var rawQuery = request.QueryString.Value;
        if (string.IsNullOrEmpty(rawQuery))
            return builder.ToString();

        var parts = rawQuery.TrimStart('?').Split('&');
        for (var index = 0; index < parts.Length; index++)
        {
            var separator = parts[index].IndexOf('=', StringComparison.Ordinal);
            var encodedName = separator < 0 ? parts[index] : parts[index][..separator];
            if (string.Equals(WebUtility.UrlDecode(encodedName), "sig", StringComparison.OrdinalIgnoreCase))
                parts[index] = encodedName + "=XXXXX";
        }
        return builder.Append('?').AppendJoin('&', parts).ToString();
    }

    private static long EstimateRequestHeaderSize(HttpRequest request)
    {
        var size = Encoding.UTF8.GetByteCount(
            $"{request.Method} {request.PathBase}{request.Path}{request.QueryString} HTTP/1.1\r\n");
        return size + EstimateHeaders(request.Headers);
    }

    private static long EstimateResponseHeaderSize(HttpResponse response) =>
        Encoding.UTF8.GetByteCount($"HTTP/1.1 {response.StatusCode}\r\n") + EstimateHeaders(response.Headers);

    private static long EstimateHeaders(IHeaderDictionary headers) => headers.Sum(header =>
        (long)Encoding.UTF8.GetByteCount(header.Key) + 2 +
        Encoding.UTF8.GetByteCount(header.Value.ToString()) + 2) + 2;

    private static string? FormatConditions(IHeaderDictionary headers)
    {
        string[] names =
        [
            "If-Match",
            "If-None-Match",
            "If-Modified-Since",
            "If-Unmodified-Since",
            "x-ms-if-tags",
            "x-ms-lease-id",
            "x-ms-blob-condition-appendpos",
            "x-ms-blob-condition-maxsize"
        ];
        var conditions = names
            .Where(headers.ContainsKey)
            .Select(name => $"{name}={headers[name]}")
            .ToArray();
        return conditions.Length == 0 ? null : string.Join(';', conditions);
    }

    private static string? Header(IHeaderDictionary headers, string name) =>
        headers.TryGetValue(name, out var value) && !string.IsNullOrEmpty(value)
            ? value.ToString()
            : null;

    private static string? FormatRemoteAddress(ConnectionInfo connection)
    {
        if (connection.RemoteIpAddress is null)
            return null;
        return connection.RemotePort > 0
            ? $"{connection.RemoteIpAddress}:{connection.RemotePort.ToString(CultureInfo.InvariantCulture)}"
            : connection.RemoteIpAddress.ToString();
    }
}
