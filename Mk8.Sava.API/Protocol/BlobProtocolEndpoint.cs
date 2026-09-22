using System.Globalization;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using System.Xml;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

public static class BlobProtocolEndpoint
{
    private static readonly IReadOnlyDictionary<string, string> EmptyBlobTags =
        new Dictionary<string, string>(StringComparer.Ordinal);

    public static async Task HandleAsync(HttpContext http)
    {
        var request = StorageRequestContext.Get(http);
        var service = http.RequestServices.GetRequiredService<BlobService>();
        var writer = http.RequestServices.GetRequiredService<AzureResponseWriter>();
        var cancellationToken = http.RequestAborted;

        if (request.ResourceKind != StorageResourceKind.StaticWebsite &&
            HttpMethods.IsOptions(http.Request.Method))
        {
            await HandleCorsPreflightAsync(http, service, request, cancellationToken);
            return;
        }

        if (request.ResourceKind != StorageResourceKind.StaticWebsite)
            await ApplyCorsResponseHeadersAsync(http, service, request, cancellationToken);

        if (string.Equals(
                http.Request.Query["restype"].ToString(),
                "account",
                StringComparison.OrdinalIgnoreCase))
        {
            await HandleAccountInformationAsync(http, request);
            return;
        }

        switch (request.ResourceKind)
        {
            case StorageResourceKind.Service:
                await HandleServiceAsync(http, request, service, writer, cancellationToken);
                break;
            case StorageResourceKind.Container:
                await HandleContainerAsync(http, request, service, writer, cancellationToken);
                break;
            case StorageResourceKind.Blob:
                await HandleBlobAsync(http, request, service, writer, cancellationToken);
                break;
            case StorageResourceKind.StaticWebsite:
                await HandleStaticWebsiteAsync(http, request, service, cancellationToken);
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    private static Task HandleAccountInformationAsync(
        HttpContext http,
        StorageRequestContext request)
    {
        if (!string.Equals(
                http.Request.Query["comp"].ToString(),
                "properties",
                StringComparison.OrdinalIgnoreCase) ||
            !HttpMethods.IsGet(http.Request.Method) && !HttpMethods.IsHead(http.Request.Method))
        {
            throw UnsupportedOperation();
        }

        RequireFeatureVersion(request, new DateOnly(2018, 3, 28), "Get Account Information");
        if (request.Authorization.Kind == StorageAuthorizationKind.Anonymous)
            throw AzureStorageException.AuthenticationFailed();

        http.Response.Headers["x-ms-sku-name"] = "Standard_LRS";
        http.Response.Headers["x-ms-account-kind"] = "StorageV2";
        if (IsServiceVersionAtLeast(request, new DateOnly(2019, 7, 7)))
            http.Response.Headers["x-ms-is-hns-enabled"] = "false";
        http.Response.ContentLength = 0;
        return Task.CompletedTask;
    }

    private static async Task HandleServiceAsync(
        HttpContext http,
        StorageRequestContext request,
        BlobService service,
        AzureResponseWriter writer,
        CancellationToken cancellationToken)
    {
        var comp = http.Request.Query["comp"].ToString().ToLowerInvariant();
        if (comp == "userdelegationkey" && HttpMethods.IsPost(http.Request.Method))
        {
            RequireFeatureVersion(request, new DateOnly(2018, 11, 9), "Get User Delegation Key");
            if (!http.Request.IsHttps)
            {
                throw new AzureStorageException(
                    StatusCodes.Status400BadRequest,
                    "InvalidRequest",
                    "Get User Delegation Key requires HTTPS.");
            }
            if (request.Authorization.Kind != StorageAuthorizationKind.Bearer)
                throw AzureStorageException.AuthorizationFailure();
            var keyRequest = await ProtocolParsing.ReadUserDelegationKeyRequestAsync(
                http.Request.Body,
                request.ServiceVersion,
                cancellationToken);
            var authenticator = http.RequestServices.GetRequiredService<StorageAuthenticator>();
            var key = authenticator.IssueUserDelegationKey(request, keyRequest);
            await writer.WriteUserDelegationKeyAsync(http, key, cancellationToken);
            return;
        }

        if (request.Authorization.Kind == StorageAuthorizationKind.Sas && !request.Authorization.IsAccountSas)
            throw AzureStorageException.AuthorizationFailure();
        if (HttpMethods.IsGet(http.Request.Method) && comp == "list")
        {
            Require(request, 'l');
            var prefix = http.Request.Query["prefix"].ToString();
            var marker = http.Request.Query["marker"].ToString();
            var maxResults = ParseMaxResults(http.Request.Query["maxresults"].ToString(), 5000);
            var includes = SplitCsv(http.Request.Query["include"].ToString());
            ValidateContainerListFeatures(request, includes);
            var containers = await service.ListContainersPageAsync(
                request.Account,
                includes.Contains("deleted"),
                prefix,
                marker,
                maxResults,
                cancellationToken);
            await writer.WriteContainersAsync(
                http,
                containers,
                prefix,
                marker,
                maxResults,
                includes.Contains("metadata"),
                includes.Contains("deleted"),
                cancellationToken);
            return;
        }

        if (comp == "properties" && HttpMethods.IsGet(http.Request.Method))
        {
            Require(request, 'r');
            var properties = await service.GetServicePropertiesAsync(request.Account, cancellationToken);
            await writer.WriteServicePropertiesAsync(http, properties, cancellationToken);
            return;
        }

        if (comp == "properties" && HttpMethods.IsPut(http.Request.Method))
        {
            Require(request, 'w');
            var current = await service.GetServicePropertiesAsync(request.Account, cancellationToken);
            var updated = await ProtocolParsing.ReadServicePropertiesAsync(
                http.Request.Body,
                current,
                request.ServiceVersion,
                cancellationToken);
            await service.PutServicePropertiesAsync(request.Account, updated, cancellationToken);
            http.Response.StatusCode = StatusCodes.Status202Accepted;
            return;
        }

        if (comp == "stats" && HttpMethods.IsGet(http.Request.Method))
        {
            Require(request, 'r');
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "InvalidQueryParameterValue",
                "Service statistics are unavailable because this account has no geo-replicated secondary endpoint.");
        }

        if (comp == "blobs" && HttpMethods.IsGet(http.Request.Method))
        {
            Require(request, 'f');
            await WriteFindByTagsAsync(
                http,
                request,
                service,
                writer,
                scopedContainer: null,
                cancellationToken);
            return;
        }

        if (comp == "batch" && HttpMethods.IsPost(http.Request.Method))
        {
            Require(request, 'w');
            await HandleBatchAsync(http, request, service, scopedContainer: null, cancellationToken);
            return;
        }

        throw UnsupportedOperation();
    }

    private static async Task HandleStaticWebsiteAsync(
        HttpContext http,
        StorageRequestContext request,
        BlobService service,
        CancellationToken cancellationToken)
    {
        if (!HttpMethods.IsGet(http.Request.Method) && !HttpMethods.IsHead(http.Request.Method))
        {
            http.Response.Headers.Allow = "GET, HEAD";
            await WriteStaticWebsiteErrorAsync(
                http,
                StatusCodes.Status405MethodNotAllowed,
                "The resource doesn't support the specified HTTP verb.",
                cancellationToken);
            return;
        }

        var properties = await service.GetServicePropertiesAsync(request.Account, cancellationToken);
        if (!properties.StaticWebsite.Enabled)
        {
            await WriteStaticWebsiteErrorAsync(
                http,
                StatusCodes.Status404NotFound,
                "The requested content does not exist.",
                cancellationToken);
            return;
        }

        var requestedPath = request.Blob ?? string.Empty;
        var isDirectoryRequest = requestedPath.Length == 0 ||
                                 (http.Request.Path.Value?.EndsWith("/", StringComparison.Ordinal) ?? false);
        var indexDocument = properties.StaticWebsite.IndexDocument;
        var primaryPath = isDirectoryRequest && !string.IsNullOrEmpty(indexDocument)
            ? CombineWebsitePath(requestedPath, indexDocument)
            : requestedPath;
        var blob = await TryGetStaticWebsiteBlobAsync(service, request.Account, primaryPath, cancellationToken);
        var statusCode = StatusCodes.Status200OK;

        if (blob is null && properties.StaticWebsite.DefaultIndexDocumentPath is { Length: > 0 } defaultDocument)
            blob = await TryGetStaticWebsiteBlobAsync(service, request.Account, defaultDocument.TrimStart('/'), cancellationToken);

        if (blob is null && properties.StaticWebsite.ErrorDocument404Path is { Length: > 0 } errorDocument)
        {
            blob = await TryGetStaticWebsiteBlobAsync(service, request.Account, errorDocument.TrimStart('/'), cancellationToken);
            statusCode = StatusCodes.Status404NotFound;
        }

        if (blob is null)
        {
            await WriteStaticWebsiteErrorAsync(
                http,
                StatusCodes.Status404NotFound,
                "The requested content does not exist.",
                cancellationToken);
            return;
        }

        if (statusCode == StatusCodes.Status200OK)
            EvaluateReadConditions(http.Request, blob);
        await WriteStaticWebsiteBlobAsync(http, service, blob, statusCode, cancellationToken);
    }

    private static string CombineWebsitePath(string directory, string document) =>
        string.IsNullOrEmpty(directory)
            ? document.TrimStart('/')
            : $"{directory.TrimEnd('/')}/{document.TrimStart('/')}";

    private static async Task<BlobRecord?> TryGetStaticWebsiteBlobAsync(
        BlobService service,
        string account,
        string name,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(name))
            return null;
        try
        {
            return await service.GetBlobAsync(
                account,
                "$web",
                name,
                versionId: null,
                snapshot: null,
                includeDeleted: false,
                cancellationToken);
        }
        catch (AzureStorageException exception) when (exception.ErrorCode is "BlobNotFound" or "ContainerNotFound")
        {
            return null;
        }
    }

    private static async Task WriteStaticWebsiteBlobAsync(
        HttpContext http,
        BlobService service,
        BlobRecord blob,
        int statusCode,
        CancellationToken cancellationToken)
    {
        long start = 0;
        long end = blob.Content.Length - 1;
        if (statusCode == StatusCodes.Status200OK &&
            (ProtocolParsing.First(http.Request.Headers, "Range") ??
             ProtocolParsing.First(http.Request.Headers, "x-ms-range")) is { Length: > 0 } range)
        {
            (start, end) = ProtocolParsing.ParseRange(range, blob.Content.Length);
            statusCode = StatusCodes.Status206PartialContent;
            http.Response.Headers.ContentRange = $"bytes {start}-{end}/{blob.Content.Length}";
        }

        var length = blob.Content.Length == 0 ? 0 : end - start + 1;
        http.Response.StatusCode = statusCode;
        http.Response.Headers.AcceptRanges = "bytes";
        http.Response.Headers.ETag = blob.ETag;
        http.Response.Headers.LastModified = blob.LastModified.ToString("R", CultureInfo.InvariantCulture);
        http.Response.ContentType = blob.Http.ContentType;
        http.Response.ContentLength = length;
        SetStaticWebsiteHeader(http.Response.Headers, "Content-Encoding", blob.Http.ContentEncoding);
        SetStaticWebsiteHeader(http.Response.Headers, "Content-Language", blob.Http.ContentLanguage);
        SetStaticWebsiteHeader(http.Response.Headers, "Cache-Control", blob.Http.CacheControl);
        SetStaticWebsiteHeader(http.Response.Headers, "Content-Disposition", blob.Http.ContentDisposition);
        if (statusCode != StatusCodes.Status206PartialContent)
            SetStaticWebsiteHeader(http.Response.Headers, "Content-MD5", blob.Http.ContentMd5);
        if (HttpMethods.IsHead(http.Request.Method) || length == 0)
            return;

        blob = await service.RecordSmartTierAccessAsync(blob, cancellationToken);
        await service.WriteContentAsync(
            blob,
            new BlobEncryption(blob.EncryptionScope, CustomerProvidedKeySha256: null),
            start,
            length,
            http.Response.Body,
            cancellationToken);
    }

    private static void SetStaticWebsiteHeader(IHeaderDictionary headers, string name, string? value)
    {
        if (value is not null)
            headers[name] = value;
    }

    private static async Task WriteStaticWebsiteErrorAsync(
        HttpContext http,
        int statusCode,
        string message,
        CancellationToken cancellationToken)
    {
        var body = Encoding.UTF8.GetBytes(
            $"<!DOCTYPE html><html><head><title>{statusCode}</title></head>" +
            $"<body><h1>{statusCode}</h1><p>{WebUtility.HtmlEncode(message)}</p></body></html>");
        http.Response.StatusCode = statusCode;
        http.Response.ContentType = "text/html; charset=utf-8";
        http.Response.ContentLength = body.Length;
        if (!HttpMethods.IsHead(http.Request.Method))
            await http.Response.Body.WriteAsync(body, cancellationToken);
    }

    private static async Task HandleContainerAsync(
        HttpContext http,
        StorageRequestContext request,
        BlobService service,
        AzureResponseWriter writer,
        CancellationToken cancellationToken)
    {
        var containerName = request.Container ?? throw AzureStorageException.ContainerNotFound();
        var comp = http.Request.Query["comp"].ToString().ToLowerInvariant();
        if (comp == "batch" && HttpMethods.IsPost(http.Request.Method))
        {
            Require(request, 'w');
            _ = await service.GetContainerAsync(request.Account, containerName, includeDeleted: false, cancellationToken);
            await HandleBatchAsync(http, request, service, containerName, cancellationToken);
            return;
        }
        if (request.Authorization.Kind == StorageAuthorizationKind.Sas &&
            !request.Authorization.IsAccountSas &&
            !(HttpMethods.IsGet(http.Request.Method) && comp is "list" or "blobs"))
        {
            throw AzureStorageException.AuthorizationFailure();
        }

        if (HttpMethods.IsGet(http.Request.Method) && comp == "blobs")
        {
            Require(request, 'f');
            _ = await service.GetContainerAsync(
                request.Account,
                containerName,
                includeDeleted: false,
                cancellationToken);
            await WriteFindByTagsAsync(
                http,
                request,
                service,
                writer,
                containerName,
                cancellationToken);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && string.IsNullOrEmpty(comp))
        {
            RequireAny(request, 'c', 'w');
            var publicAccess = ProtocolParsing.First(http.Request.Headers, "x-ms-blob-public-access");
            if (publicAccess is not null)
                RequireFeatureVersion(request, new DateOnly(2009, 9, 19), "Container public access");
            var created = await service.CreateContainerAsync(
                request.Account,
                containerName,
                ProtocolParsing.ReadMetadata(http.Request.Headers),
                publicAccess,
                cancellationToken);
            AzureResponseWriter.AddContainerHeaders(http.Response, created);
            http.Response.StatusCode = StatusCodes.Status201Created;
            return;
        }

        if (comp == "undelete" && HttpMethods.IsPut(http.Request.Method))
        {
            RequireFeatureVersion(request, new DateOnly(2019, 12, 12), "Restore Container");
            RequireAny(request, 'c', 'w');
            var deletedName = ProtocolParsing.First(http.Request.Headers, "x-ms-deleted-container-name") ?? containerName;
            var version = ProtocolParsing.First(http.Request.Headers, "x-ms-deleted-container-version")
                          ?? throw AzureStorageException.InvalidHeader("x-ms-deleted-container-version");
            var restored = await service.RestoreContainerAsync(request.Account, deletedName, version, cancellationToken);
            AzureResponseWriter.AddContainerHeaders(http.Response, restored);
            http.Response.StatusCode = StatusCodes.Status201Created;
            return;
        }

        var container = await service.GetContainerAsync(request.Account, containerName, includeDeleted: false, cancellationToken);

        if ((HttpMethods.IsGet(http.Request.Method) || HttpMethods.IsHead(http.Request.Method)) &&
            string.IsNullOrEmpty(comp))
        {
            await AuthorizeContainerReadAsync(request, service, container, allowContainerPublic: true);
            ValidateOptionalLease(http.Request, container.Lease, "container");
            AzureResponseWriter.AddContainerPropertiesHeaders(http.Response, container);
            return;
        }

        if (HttpMethods.IsGet(http.Request.Method) && comp == "list")
        {
            await AuthorizeContainerListAsync(request, service, container);
            var includes = SplitCsv(http.Request.Query["include"].ToString());
            var prefix = http.Request.Query["prefix"].ToString();
            var startFrom = http.Request.Query["startfrom"].ToString();
            var endBefore = http.Request.Query["endbefore"].ToString();
            var delimiter = http.Request.Query["delimiter"].ToString();
            var marker = http.Request.Query["marker"].ToString();
            var maxResults = ParseMaxResults(http.Request.Query["maxresults"].ToString(), 5000);
            ValidateBlobListFeatures(request, includes, delimiter);
            if (http.Request.Query.ContainsKey("startfrom") &&
                !IsServiceVersionAtLeast(request, new DateOnly(2023, 5, 3)))
            {
                throw AzureStorageException.FeatureVersionMismatch(
                    "The startFrom parameter requires service version 2023-05-03 or later.");
            }
            var arrow = IsArrowListRequest(http.Request, request);
            if (http.Request.Query.ContainsKey("endbefore"))
            {
                if (!IsServiceVersionAtLeast(request, new DateOnly(2026, 6, 6)))
                {
                    throw AzureStorageException.FeatureVersionMismatch(
                        "The endBefore parameter requires service version 2026-06-06 or later.");
                }
                if (string.IsNullOrEmpty(endBefore))
                    throw AzureStorageException.InvalidQuery("endbefore");
                if (!arrow)
                    throw AzureStorageException.InvalidQuery("endbefore");
                if (!string.IsNullOrEmpty(startFrom) &&
                    string.CompareOrdinal(endBefore, startFrom) < 0)
                {
                    throw AzureStorageException.InvalidQuery("endbefore");
                }
            }
            var decodedMarker = AzureResponseWriter.DecodeBlobMarker(
                http,
                prefix,
                startFrom,
                endBefore,
                delimiter,
                includes,
                marker);
            var blobs = await service.ListBlobsPageAsync(
                request.Account,
                containerName,
                includes.Contains("versions"),
                includes.Contains("snapshots"),
                includes.Contains("deleted") || includes.Contains("deletedwithversions"),
                includes.Contains("uncommittedblobs"),
                prefix,
                startFrom,
                endBefore,
                delimiter,
                decodedMarker,
                maxResults,
                cancellationToken);
            ValidateListedBlobTypes(request, blobs);
            await writer.WriteBlobsAsync(
                http,
                blobs,
                prefix,
                startFrom,
                endBefore,
                delimiter,
                marker,
                maxResults,
                includes,
                arrow,
                cancellationToken);
            return;
        }

        if ((HttpMethods.IsGet(http.Request.Method) || HttpMethods.IsHead(http.Request.Method)) &&
            comp == "metadata")
        {
            await AuthorizeContainerReadAsync(request, service, container, allowContainerPublic: true);
            ValidateOptionalLease(http.Request, container.Lease, "container");
            AzureResponseWriter.AddContainerMetadataHeaders(http.Response, container);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "metadata")
        {
            Require(request, 'w');
            BlobConditionEvaluator.EvaluateContainerWrite(
                http.Request,
                container.LastModified,
                supportsIfUnmodifiedSince: false);
            ValidateOptionalLease(http.Request, container.Lease, "container");
            var updated = await service.SetContainerMetadataAsync(container, ProtocolParsing.ReadMetadata(http.Request.Headers), cancellationToken);
            AzureResponseWriter.AddContainerHeaders(http.Response, updated);
            return;
        }

        if ((HttpMethods.IsGet(http.Request.Method) || HttpMethods.IsHead(http.Request.Method)) &&
            comp == "acl")
        {
            Require(request, 'r');
            ValidateOptionalLease(http.Request, container.Lease, "container");
            AzureResponseWriter.AddContainerAccessPolicyHeaders(http.Response, container);
            if (HttpMethods.IsGet(http.Request.Method))
                await writer.WriteAclAsync(http, container, cancellationToken);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "acl")
        {
            Require(request, 'w');
            BlobConditionEvaluator.EvaluateContainerWrite(
                http.Request,
                container.LastModified,
                supportsIfUnmodifiedSince: true);
            ValidateOptionalLease(http.Request, container.Lease, "container");
            var publicAccess = ProtocolParsing.First(http.Request.Headers, "x-ms-blob-public-access");
            if (publicAccess is not null)
                RequireFeatureVersion(request, new DateOnly(2009, 9, 19), "Container public access");
            var policies = http.Request.ContentLength is null or 0
                ? new Dictionary<string, StoredAccessPolicy>(StringComparer.Ordinal)
                : await ProtocolParsing.ReadAclAsync(http.Request.Body, cancellationToken);
            var updated = await service.SetContainerAclAsync(
                container,
                publicAccess,
                policies,
                cancellationToken);
            AzureResponseWriter.AddContainerHeaders(http.Response, updated);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "lease")
        {
            RequireFeatureVersion(request, new DateOnly(2012, 2, 12), "Lease Container");
            Require(request, 'w');
            BlobConditionEvaluator.EvaluateContainerWrite(
                http.Request,
                container.LastModified,
                supportsIfUnmodifiedSince: true);
            RequireZeroContentLength(http.Request);
            await HandleContainerLeaseAsync(http, request, service, container, cancellationToken);
            return;
        }

        if (HttpMethods.IsDelete(http.Request.Method) && string.IsNullOrEmpty(comp))
        {
            Require(request, 'd');
            BlobConditionEvaluator.EvaluateContainerWrite(
                http.Request,
                container.LastModified,
                supportsIfUnmodifiedSince: true);
            EnsureLease(http.Request, container.Lease, "container");
            await service.DeleteContainerAsync(container, cancellationToken);
            http.Response.StatusCode = StatusCodes.Status202Accepted;
            return;
        }

        throw UnsupportedOperation();
    }

    private static async Task HandleBatchAsync(
        HttpContext http,
        StorageRequestContext request,
        BlobService service,
        string? scopedContainer,
        CancellationToken cancellationToken)
    {
        var minimumVersion = scopedContainer is null ? new DateOnly(2018, 11, 9) : new DateOnly(2020, 4, 8);
        if (!DateOnly.TryParseExact(
                request.ServiceVersion,
                "yyyy-MM-dd",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var version) || version < minimumVersion)
        {
            throw AzureStorageException.FeatureVersionMismatch(
                $"Blob Batch requires service version {minimumVersion:yyyy-MM-dd} or later.");
        }

        var subrequests = await BlobBatchProtocol.ReadAsync(http.Request, cancellationToken);
        var resolved = subrequests
            .Select(subrequest => ResolveBatchSubrequest(request.Account, scopedContainer, subrequest))
            .ToArray();
        var responses = new List<BlobBatchSubresponse>(resolved.Length);
        foreach (var subrequest in resolved)
            responses.Add(await ExecuteBatchSubrequestAsync(http, request, service, subrequest, cancellationToken));
        await BlobBatchProtocol.WriteAsync(http.Response, responses, cancellationToken);
    }

    private static ResolvedBatchSubrequest ResolveBatchSubrequest(
        string account,
        string? scopedContainer,
        BlobBatchSubrequest subrequest)
    {
        try
        {
            var segments = StorageResourcePath.DecodeSegments(subrequest.RawPath);
            var offset = segments.Length >= 3 &&
                         string.Equals(segments[0], account, StringComparison.Ordinal)
                ? 1
                : 0;
            var (container, blob) = StorageResourcePath.ResolveBlob(segments, offset);
            if (string.IsNullOrEmpty(container) || string.IsNullOrEmpty(blob))
                throw InvalidBatchSubrequest("A batch subrequest does not identify a blob.");
            if (scopedContainer is not null && !string.Equals(container, scopedContainer, StringComparison.Ordinal))
            {
                throw new AzureStorageException(
                    StatusCodes.Status400BadRequest,
                    "InvalidInput",
                    "All subrequests in a container-scoped batch must target that container.");
            }

            var query = QueryHelpers.ParseQuery(subrequest.QueryString.Value ?? string.Empty);
            return new ResolvedBatchSubrequest(
                subrequest,
                container,
                blob,
                query.TryGetValue("snapshot", out var snapshot) ? NullIfEmpty(snapshot.ToString()) : null,
                query.TryGetValue("versionid", out var version) ? NullIfEmpty(version.ToString()) : null,
                query.TryGetValue("deletetype", out var deleteType) ? deleteType.ToString() : null);
        }
        catch (UriFormatException)
        {
            throw InvalidBatchSubrequest("A batch subrequest contains an invalid escaped URI.");
        }
    }

    private static async Task<BlobBatchSubresponse> ExecuteBatchSubrequestAsync(
        HttpContext outer,
        StorageRequestContext outerRequest,
        BlobService service,
        ResolvedBatchSubrequest resolved,
        CancellationToken cancellationToken)
    {
        var inner = new DefaultHttpContext
        {
            RequestServices = outer.RequestServices
        };
        inner.Request.Scheme = outer.Request.Scheme;
        inner.Request.Host = outer.Request.Host;
        inner.Request.Method = resolved.Request.Method;
        inner.Request.Path = PathString.FromUriComponent(resolved.Request.RawPath);
        inner.Request.QueryString = resolved.Request.QueryString;
        inner.Request.Body = Stream.Null;
        inner.Connection.RemoteIpAddress = outer.Connection.RemoteIpAddress;
        foreach (var header in resolved.Request.Headers)
            inner.Request.Headers[header.Key] = header.Value;
        inner.Features.Get<IHttpRequestFeature>()!.RawTarget =
            resolved.Request.RawPath + (resolved.Request.QueryString.Value ?? string.Empty);

        var subrequestContext = new StorageRequestContext
        {
            RequestId = Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(16)),
            Account = outerRequest.Account,
            Container = resolved.Container,
            Blob = resolved.Blob,
            Snapshot = resolved.Snapshot,
            VersionId = resolved.VersionId,
            ResourceKind = StorageResourceKind.Blob,
            CanonicalResourcePath = $"/{outerRequest.Account}/{resolved.Container}/{resolved.Blob}",
            ServiceVersion = outerRequest.ServiceVersion,
            Authorization = StorageAuthorization.Anonymous
        };
        StorageRequestContext.Set(inner, subrequestContext);

        try
        {
            ValidateBlobVersionRequest(subrequestContext);
            var authenticator = outer.RequestServices.GetRequiredService<StorageAuthenticator>();
            subrequestContext.Authorization = await authenticator.AuthenticateAsync(inner, subrequestContext, cancellationToken);
            var permanentDelete = resolved.DeleteType is not null;
            if (permanentDelete)
            {
                if (resolved.Request.Kind != BlobBatchOperationKind.Delete)
                    throw AzureStorageException.InvalidQuery("deletetype");
                ValidatePermanentDeleteRequest(subrequestContext, resolved.DeleteType!);
            }
            var blob = await service.GetBlobAsync(
                subrequestContext.Account,
                resolved.Container,
                resolved.Blob,
                resolved.VersionId,
                resolved.Snapshot,
                includeDeleted: permanentDelete,
                cancellationToken);

            var headers = CreateBatchCommonHeaders(subrequestContext, inner.Request);
            switch (resolved.Request.Kind)
            {
                case BlobBatchOperationKind.Delete:
                    var hasExplicitSnapshotOrVersion = resolved.Snapshot is not null || resolved.VersionId is not null;
                    var deleteSnapshots = ReadDeleteSnapshotsOption(inner.Request, hasExplicitSnapshotOrVersion);
                    Require(
                        subrequestContext,
                        permanentDelete ? 'y' : resolved.VersionId is not null ? 'x' : 'd');
                    EvaluateWriteConditions(inner.Request, blob);
                    EnsureLease(inner.Request, blob.Lease, "blob");
                    if (permanentDelete)
                    {
                        await service.PermanentlyDeleteBlobAsync(
                            blob,
                            hasExplicitSnapshotOrVersion,
                            cancellationToken);
                        headers["x-ms-delete-type-permanent"] = "true";
                    }
                    else
                    {
                        var properties = await service.GetServicePropertiesAsync(subrequestContext.Account, cancellationToken);
                        await service.DeleteBlobAsync(
                            blob,
                            hasExplicitSnapshotOrVersion,
                            deleteSnapshots,
                            cancellationToken);
                        if (IsServiceVersionAtLeast(subrequestContext, new DateOnly(2017, 7, 29)))
                            headers["x-ms-delete-type-permanent"] = properties.BlobSoftDeleteEnabled ? "false" : "true";
                    }
                    return new BlobBatchSubresponse(
                        StatusCodes.Status202Accepted,
                        headers,
                        [],
                        resolved.Request.ContentId);

                case BlobBatchOperationKind.SetTier:
                    Require(subrequestContext, 'w');
                    EvaluateTagCondition(inner.Request, blob, "x-ms-if-tags", source: false);
                    var tier = ProtocolParsing.First(inner.Request.Headers, "x-ms-access-tier")
                               ?? throw AzureStorageException.InvalidHeader("x-ms-access-tier");
                    ValidateAccessTierVersion(inner.Request, tier);
                    var tierUpdate = await service.SetTierAsync(
                        blob,
                        tier,
                        ProtocolParsing.First(inner.Request.Headers, "x-ms-rehydrate-priority"),
                        cancellationToken);
                    return new BlobBatchSubresponse(
                        tierUpdate.Pending ? StatusCodes.Status202Accepted : StatusCodes.Status200OK,
                        headers,
                        [],
                        resolved.Request.ContentId);

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            var storageException = MapBatchException(exception);
            if (storageException.ErrorCode == "InternalError")
            {
                outer.RequestServices
                    .GetRequiredService<ILoggerFactory>()
                    .CreateLogger(typeof(BlobProtocolEndpoint))
                    .LogError(exception, "Blob batch subrequest {RequestId} failed unexpectedly.", subrequestContext.RequestId);
            }
            var headers = CreateBatchCommonHeaders(subrequestContext, inner.Request);
            headers["x-ms-error-code"] = storageException.ErrorCode;
            foreach (var header in storageException.ResponseHeaders)
                headers[header.Key] = header.Value;
            headers["Content-Type"] = "application/xml";
            if (storageException.StatusCode == StatusCodes.Status401Unauthorized)
                headers["WWW-Authenticate"] = "Bearer resource_id=\"https://storage.azure.com/\"";
            var body = BuildBatchErrorBody(storageException, subrequestContext.RequestId);
            return new BlobBatchSubresponse(
                storageException.StatusCode,
                headers,
                body,
                resolved.Request.ContentId);
        }
    }

    private static Dictionary<string, string> CreateBatchCommonHeaders(
        StorageRequestContext request,
        HttpRequest httpRequest)
    {
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["x-ms-request-id"] = request.RequestId,
            ["x-ms-version"] = request.ServiceVersion,
            ["Date"] = DateTimeOffset.UtcNow.ToString("R", CultureInfo.InvariantCulture)
        };
        var clientRequestId = ProtocolParsing.First(httpRequest.Headers, "x-ms-client-request-id");
        if (clientRequestId is { Length: <= 1024 })
            headers["x-ms-client-request-id"] = clientRequestId;
        return headers;
    }

    private static AzureStorageException MapBatchException(Exception exception) => exception switch
    {
        AzureStorageException storage => storage,
        StorageConcurrencyException => AzureStorageException.ConditionNotMet(),
        StorageImmutabilityException immutable => new AzureStorageException(
            StatusCodes.Status409Conflict,
            immutable.LegalHold ? "BlobImmutableDueToLegalHold" : "BlobImmutableDueToPolicy",
            immutable.Message),
        StoragePendingCopyException pending => new AzureStorageException(
            StatusCodes.Status409Conflict,
            "PendingCopyOperation",
            pending.Message),
        _ => new AzureStorageException(
            StatusCodes.Status500InternalServerError,
            "InternalError",
            "The server encountered an internal error. Please retry the request.")
    };

    private static byte[] BuildBatchErrorBody(AzureStorageException exception, string requestId)
    {
        var builder = new StringBuilder();
        using (var writer = XmlWriter.Create(builder, new XmlWriterSettings
        {
            OmitXmlDeclaration = true,
            Encoding = Encoding.UTF8,
            Indent = false
        }))
        {
            writer.WriteStartElement("Error");
            writer.WriteElementString("Code", exception.ErrorCode);
            writer.WriteStartElement("Message");
            writer.WriteString(exception.Message);
            writer.WriteString("\nRequestId:");
            writer.WriteString(requestId);
            writer.WriteString("\nTime:");
            writer.WriteString(DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture));
            writer.WriteEndElement();
            if (exception.HeaderName is not null)
                writer.WriteElementString("HeaderName", exception.HeaderName);
            if (exception.HeaderValue is not null)
                writer.WriteElementString("HeaderValue", exception.HeaderValue);
            foreach (var detail in exception.Details)
                writer.WriteElementString(detail.Key, detail.Value);
            writer.WriteEndElement();
        }
        return Encoding.UTF8.GetBytes(builder.ToString());
    }

    private static AzureStorageException InvalidBatchSubrequest(string message) => new(
        StatusCodes.Status400BadRequest,
        "InvalidInput",
        message);

    private sealed record ResolvedBatchSubrequest(
        BlobBatchSubrequest Request,
        string Container,
        string Blob,
        string? Snapshot,
        string? VersionId,
        string? DeleteType);

    private sealed record ResolvedInternalCopySource(
        Uri Uri,
        string Account,
        string Container,
        string Blob,
        string? Snapshot,
        string? VersionId);

    private static async Task HandleBlobAsync(
        HttpContext http,
        StorageRequestContext request,
        BlobService service,
        AzureResponseWriter writer,
        CancellationToken cancellationToken)
    {
        var containerName = request.Container ?? "$root";
        var blobName = request.Blob ?? request.Container ?? throw AzureStorageException.BlobNotFound();
        if (request.Blob is null)
            containerName = "$root";
        var comp = http.Request.Query["comp"].ToString().ToLowerInvariant();
        var versionId = NullIfEmpty(http.Request.Query["versionid"].ToString());
        var snapshot = NullIfEmpty(http.Request.Query["snapshot"].ToString());
        var permanentDelete = HttpMethods.IsDelete(http.Request.Method) &&
                              string.IsNullOrEmpty(comp) &&
                              http.Request.Query.ContainsKey("deletetype");
        if (permanentDelete)
            ValidatePermanentDeleteRequest(request, http.Request.Query["deletetype"].ToString());
        if (HttpMethods.IsPut(http.Request.Method) && comp is "snapshot" or "lease")
            RequireFeatureVersion(request, new DateOnly(2009, 9, 19), comp == "snapshot" ? "Snapshot Blob" : "Lease Blob");
        if (HttpMethods.IsPut(http.Request.Method) && string.IsNullOrEmpty(comp))
        {
            await HandlePutBlobAsync(http, request, service, containerName, blobName, cancellationToken);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "block")
        {
            var current = await TryGetCurrentBlobAsync(service, request.Account, containerName, blobName, cancellationToken);
            ValidateBlobTypeVersion(request, current?.Kind);
            RequireBlockWrite(request, current is null);
            EnsureLease(http.Request, current?.Lease ?? LeaseRecord.Available, "blob");
            var blockId = http.Request.Query["blockid"].ToString();
            var copySource = ProtocolParsing.First(http.Request.Headers, "x-ms-copy-source");
            var encryption = ReadRequestEncryption(http.Request, write: true);
            TransactionalChecksums checksums;
            if (copySource is null)
            {
                checksums = await WithIntegrityValidationAsync(http.Request, async body =>
                    await service.StageBlockAsync(request.Account, containerName, blobName, blockId, body, encryption, cancellationToken),
                    maximumBodyBytes: GetMaximumPutBlockBytes(request));
            }
            else
            {
                RequireFeatureVersion(request, new DateOnly(2018, 3, 28), "Put Block From URL");
                RequireZeroContentLength(http.Request);
                var transfers = http.RequestServices.GetRequiredService<UrlTransferClient>();
                var transfer = await transfers.ReadAsync(
                    http.Request,
                    copySource,
                    ProtocolParsing.First(http.Request.Headers, "x-ms-source-range"),
                    allowSourceCustomerProvidedKey: true,
                    GetMaximumPutBlockFromUrlBytes(request),
                    sourceLengthConflict: false,
                    async source =>
                    {
                        await service.StageBlockAsync(request.Account, containerName, blobName, blockId, source.Content, encryption, cancellationToken);
                        return true;
                    },
                    cancellationToken);
                checksums = transfer.Checksums;
            }
            http.Response.StatusCode = StatusCodes.Status201Created;
            AddRequestServerEncryptedHeader(http.Response);
            AddEncryptionResponseHeaders(http.Response, encryption);
            AddTransactionalChecksumHeaders(http, checksums, copySource is not null);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "blocklist")
        {
            var current = await TryGetCurrentBlobAsync(service, request.Account, containerName, blobName, cancellationToken);
            ValidateBlobTypeVersion(request, current?.Kind);
            RequireBlockWrite(request, current is null);
            EvaluateWriteConditions(http.Request, current);
            if (current is not null)
                EnsureLease(http.Request, current.Lease, "blob");
            IReadOnlyList<BlockListEntry> blockIds = [];
            var checksums = await WithIntegrityValidationAsync(
                http.Request,
                async body => blockIds = await ProtocolParsing.ReadBlockListAsync(body, cancellationToken),
                allowStructured: false,
                maximumBodyBytes: ProtocolParsing.MaximumBlockListBodyBytes);
            var options = ReadWriteOptions(http.Request, current, useStandardProperties: false);
            var committed = await service.CommitBlockListAsync(
                request.Account,
                containerName,
                blobName,
                blockIds,
                options,
                current?.GenerationId,
                current?.Revision,
                cancellationToken);
            AzureResponseWriter.AddBlobWriteHeaders(http.Response, committed);
            AddRequestServerEncryptedHeader(http.Response);
            AddEncryptionResponseHeaders(http.Response, EncryptionOf(committed));
            AddTransactionalChecksumHeaders(http, checksums);
            http.Response.StatusCode = StatusCodes.Status201Created;
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "appendblock")
        {
            RequireAppendBlobVersion(request);
            RequireAny(request, 'a', 'w');
            var current = await service.GetBlobAsync(request.Account, containerName, blobName, null, null, false, cancellationToken);
            EvaluateWriteConditions(http.Request, current);
            EnsureLease(http.Request, current.Lease, "blob");
            var expectedPosition = TryParseLongHeader(http.Request.Headers, "x-ms-blob-condition-appendpos");
            var expectedMaximumSize = TryParseLongHeader(http.Request.Headers, "x-ms-blob-condition-maxsize");
            var encryption = ReadRequestEncryption(http.Request, write: true);
            BlobRecord updated = null!;
            var copySource = ProtocolParsing.First(http.Request.Headers, "x-ms-copy-source");
            TransactionalChecksums checksums;
            if (copySource is null)
            {
                checksums = await WithIntegrityValidationAsync(http.Request, async body =>
                    updated = await service.AppendBlockAsync(current, body, expectedPosition, expectedMaximumSize, encryption, cancellationToken),
                    maximumBodyBytes: GetMaximumAppendBlockBytes(request));
            }
            else
            {
                RequireFeatureVersion(request, new DateOnly(2018, 11, 9), "Append Block From URL");
                RequireZeroContentLength(http.Request);
                var transfers = http.RequestServices.GetRequiredService<UrlTransferClient>();
                var transfer = await transfers.ReadAsync(
                    http.Request,
                    copySource,
                    ProtocolParsing.First(http.Request.Headers, "x-ms-source-range"),
                    allowSourceCustomerProvidedKey: true,
                    GetMaximumAppendBlockBytes(request),
                    sourceLengthConflict: false,
                    async source => updated = await service.AppendBlockAsync(
                        current,
                        source.Content,
                        expectedPosition,
                        expectedMaximumSize,
                        encryption,
                        cancellationToken),
                    cancellationToken);
                checksums = transfer.Checksums;
            }
            AzureResponseWriter.AddBlobEntityHeaders(http.Response, updated);
            http.Response.Headers["x-ms-blob-append-offset"] = current.Content.Length.ToString(CultureInfo.InvariantCulture);
            http.Response.Headers["x-ms-blob-committed-block-count"] = updated.AppendBlockCount.ToString(CultureInfo.InvariantCulture);
            AddRequestServerEncryptedHeader(http.Response);
            AddEncryptionResponseHeaders(http.Response, EncryptionOf(updated));
            AddTransactionalChecksumHeaders(http, checksums, copySource is not null);
            http.Response.StatusCode = StatusCodes.Status201Created;
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "page")
        {
            RequirePageBlobVersion(request);
            Require(request, 'w');
            var current = await service.GetBlobAsync(request.Account, containerName, blobName, null, null, false, cancellationToken);
            EvaluateWriteConditions(http.Request, current);
            EvaluatePageSequenceConditions(http.Request, current);
            EnsureLease(http.Request, current.Lease, "blob");
            var suppliedEncryption = EnsureCustomerProvidedKey(http.Request, current, write: true);
            var encryption = new BlobEncryption(
                current.EncryptionScope,
                suppliedEncryption.CustomerProvidedKeySha256,
                suppliedEncryption.CustomerProvidedKey);
            var rangeValue = ProtocolParsing.First(http.Request.Headers, "x-ms-range")
                             ?? ProtocolParsing.First(http.Request.Headers, "Range")
                             ?? throw AzureStorageException.InvalidHeader("x-ms-range");
            var (start, end) = ParsePageWriteRange(rangeValue, current.Content.Length);
            var rangeLength = checked(end - start + 1);
            var operation = ProtocolParsing.First(http.Request.Headers, "x-ms-page-write")?.ToLowerInvariant();
            BlobRecord updated;
            var checksums = TransactionalChecksums.Empty;
            if (operation == "clear")
            {
                RequireZeroContentLength(http.Request);
                updated = await service.PutPageAsync(current, start, end, null, clear: true, encryption, cancellationToken);
            }
            else if (operation == "update")
            {
                const long maximumPageWriteBytes = 4L * 1024 * 1024;
                if (rangeLength > maximumPageWriteBytes)
                    throw new RequestBodyTooLargeException(maximumPageWriteBytes);
                var copySource = ProtocolParsing.First(http.Request.Headers, "x-ms-copy-source");
                updated = null!;
                if (copySource is null)
                {
                    var (contentLength, contentLengthHeader) = GetLogicalRequestContentLength(http.Request);
                    if (contentLength is { } suppliedLength && suppliedLength != rangeLength)
                    {
                        throw AzureStorageException.InvalidHeader(
                            contentLengthHeader,
                            suppliedLength.ToString(CultureInfo.InvariantCulture));
                    }
                    checksums = await WithIntegrityValidationAsync(http.Request, async body =>
                        updated = await service.PutPageAsync(current, start, end, body, clear: false, encryption, cancellationToken),
                        maximumBodyBytes: maximumPageWriteBytes);
                }
                else
                {
                    RequireFeatureVersion(request, new DateOnly(2018, 11, 9), "Put Page From URL");
                    RequireZeroContentLength(http.Request);
                    var transfers = http.RequestServices.GetRequiredService<UrlTransferClient>();
                    var transfer = await transfers.ReadAsync(
                        http.Request,
                        copySource,
                        ProtocolParsing.First(http.Request.Headers, "x-ms-source-range"),
                        allowSourceCustomerProvidedKey: true,
                        maximumPageWriteBytes,
                        sourceLengthConflict: false,
                        async source => updated = await service.PutPageAsync(
                            current,
                            start,
                            end,
                            source.Content,
                            clear: false,
                            encryption,
                            cancellationToken),
                        cancellationToken);
                    checksums = transfer.Checksums;
                }
            }
            else
            {
                throw AzureStorageException.InvalidHeader("x-ms-page-write", operation);
            }
            AzureResponseWriter.AddPageBlobWriteHeaders(http.Response, updated);
            AddRequestServerEncryptedHeader(http.Response);
            AddEncryptionResponseHeaders(http.Response, EncryptionOf(updated));
            AddTransactionalChecksumHeaders(
                http,
                checksums,
                ProtocolParsing.First(http.Request.Headers, "x-ms-copy-source") is not null);
            http.Response.StatusCode = StatusCodes.Status201Created;
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "incrementalcopy")
        {
            if (!DateOnly.TryParseExact(
                    request.ServiceVersion,
                    "yyyy-MM-dd",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out var serviceVersion) || serviceVersion < new DateOnly(2016, 5, 31))
            {
                throw AzureStorageException.FeatureVersionMismatch(
                    "Incremental Copy Blob requires service version 2016-05-31 or later.");
            }

            ValidateAsynchronousCopyEncryption(http.Request);
            ValidateIncrementalCopyHeaders(http.Request);

            var current = await TryGetCurrentBlobAsync(service, request.Account, containerName, blobName, cancellationToken);
            RequireAny(request, current is null ? 'c' : 'w', 'w');
            EvaluateWriteConditions(http.Request, current);
            if (current is not null)
            {
                EvaluatePageSequenceConditions(http.Request, current);
                EnsureLease(http.Request, current.Lease, "blob");
                EnsureCustomerProvidedKey(http.Request, current, write: true);
            }
            EnsureNoPendingCopyDestination(current);
            var copySource = ProtocolParsing.First(http.Request.Headers, "x-ms-copy-source")
                             ?? throw AzureStorageException.InvalidHeader("x-ms-copy-source");
            _ = SanitizeCopySource(copySource);
            var resolvedSource = ResolveInternalCopySource(http.Request, request, copySource)
                                 ?? throw new AzureStorageException(
                                     StatusCodes.Status409Conflict,
                                     "CannotVerifyCopySource",
                                     "The incremental copy source is not hosted by this Blob service endpoint.");
            var source = await ResolveCopySourceAsync(
                http,
                request,
                service,
                resolvedSource,
                cancellationToken);
            EvaluateCopySourceConditions(http.Request, source);
            var copied = await service.BeginIncrementalCopyAsync(
                request.Account,
                containerName,
                blobName,
                source,
                ReadCopyWriteOptions(http.Request, source, current),
                SanitizeCopySource(copySource),
                current,
                cancellationToken);
            AzureResponseWriter.AddBlobCopyHeaders(http.Response, copied, includeVersion: false);
            http.Response.StatusCode = StatusCodes.Status202Accepted;
            return;
        }

        if (HttpMethods.IsGet(http.Request.Method) && comp == "blocklist")
        {
            Require(request, 'r');
            var current = await TryGetCurrentBlobAsync(service, request.Account, containerName, blobName, cancellationToken);
            ValidateBlobTypeVersion(request, current?.Kind);
            if (current is null)
                EvaluateTagCondition(http.Request, null, "x-ms-if-tags", source: false);
            else
                EvaluateTagCondition(http.Request, current, "x-ms-if-tags", source: false);
            ValidateOptionalLease(
                http.Request,
                current?.Lease ?? LeaseRecord.Available,
                "blob");
            var staged = await service.ListStagedBlocksAsync(request.Account, containerName, blobName, cancellationToken);
            if (current is null && staged.Count == 0)
                throw AzureStorageException.BlobNotFound();
            var listType = http.Request.Query["blocklisttype"].ToString().ToLowerInvariant();
            if (listType is not ("all" or "committed" or "uncommitted"))
                throw AzureStorageException.InvalidQuery("blocklisttype");
            await writer.WriteBlockListAsync(http, current, staged, listType, cancellationToken);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "undelete")
        {
            RequireFeatureVersion(request, new DateOnly(2017, 7, 29), "Undelete Blob");
            Require(request, 'w');
            await service.UndeleteBlobAsync(request.Account, containerName, blobName, cancellationToken);
            return;
        }

        if (HttpMethods.IsDelete(http.Request.Method) &&
            string.IsNullOrEmpty(comp) &&
            versionId is null &&
            snapshot is null &&
            !permanentDelete &&
            IsServiceVersionAtLeast(request, new DateOnly(2013, 8, 15)) &&
            await TryGetCurrentBlobAsync(service, request.Account, containerName, blobName, cancellationToken) is null)
        {
            Require(request, 'd');
            EvaluateWriteConditions(http.Request, null);
            await service.DeleteUncommittedBlobAsync(request.Account, containerName, blobName, cancellationToken);
            http.Response.StatusCode = StatusCodes.Status202Accepted;
            return;
        }

        var blob = await service.GetBlobAsync(
            request.Account,
            containerName,
            blobName,
            versionId,
            snapshot,
            includeDeleted: permanentDelete,
            cancellationToken);
        ValidateBlobTypeVersion(request, blob.Kind);

        if (blob.IsIncrementalCopy && blob.Snapshot is null &&
            !(HttpMethods.IsHead(http.Request.Method) && string.IsNullOrEmpty(comp)) &&
            !(HttpMethods.IsDelete(http.Request.Method) && string.IsNullOrEmpty(comp)) &&
            !(HttpMethods.IsPut(http.Request.Method) && comp == "copy"))
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "OperationNotAllowedOnIncrementalCopyBlob",
                "The operation is not permitted on an incremental copy destination blob.");
        }

        if (HttpMethods.IsPost(http.Request.Method) && comp == "query")
        {
            await HandleQueryAsync(http, request, service, blob, cancellationToken);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "immutabilitypolicies")
        {
            RequireFeatureVersion(request, new DateOnly(2020, 6, 12), "Set Blob Immutability Policy");
            Require(request, 'i');
            BlobConditionEvaluator.EvaluateIfUnmodifiedSince(http.Request, blob.LastModified);
            var untilValue = ProtocolParsing.First(http.Request.Headers, "x-ms-immutability-policy-until-date")
                             ?? throw AzureStorageException.InvalidHeader("x-ms-immutability-policy-until-date");
            if (!DateTimeOffset.TryParseExact(
                    untilValue,
                    "R",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                    out var until))
            {
                throw AzureStorageException.InvalidHeader("x-ms-immutability-policy-until-date", untilValue);
            }
            var mode = ProtocolParsing.First(http.Request.Headers, "x-ms-immutability-policy-mode") ?? "unlocked";
            var locked = mode.ToLowerInvariant() switch
            {
                "locked" => true,
                "unlocked" => false,
                _ => throw AzureStorageException.InvalidHeader("x-ms-immutability-policy-mode", mode)
            };
            var updated = await service.SetBlobImmutabilityPolicyAsync(blob, until, locked, cancellationToken);
            AddImmutabilityHeaders(http.Response, updated);
            return;
        }

        if (HttpMethods.IsDelete(http.Request.Method) && comp == "immutabilitypolicies")
        {
            RequireFeatureVersion(request, new DateOnly(2020, 6, 12), "Delete Blob Immutability Policy");
            Require(request, 'i');
            BlobConditionEvaluator.EvaluateIfUnmodifiedSince(http.Request, blob.LastModified);
            await service.DeleteBlobImmutabilityPolicyAsync(blob, cancellationToken);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "legalhold")
        {
            RequireFeatureVersion(request, new DateOnly(2020, 4, 8), "Set Blob Legal Hold");
            Require(request, 'i');
            var value = ProtocolParsing.First(http.Request.Headers, "x-ms-legal-hold");
            if (!bool.TryParse(value, out var hasLegalHold))
                throw AzureStorageException.InvalidHeader("x-ms-legal-hold", value);
            var updated = await service.SetBlobLegalHoldAsync(blob, hasLegalHold, cancellationToken);
            http.Response.Headers["x-ms-legal-hold"] = updated.HasLegalHold ? "true" : "false";
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "copy")
        {
            Require(request, 'w');
            EnsureMutableVersion(blob);
            EnsureLease(http.Request, blob.Lease, "blob");
            var action = ProtocolParsing.First(http.Request.Headers, "x-ms-copy-action");
            if (!string.Equals(action, "abort", StringComparison.OrdinalIgnoreCase))
                throw AzureStorageException.InvalidHeader("x-ms-copy-action", action);
            var copyId = http.Request.Query["copyid"].ToString();
            if (string.IsNullOrEmpty(copyId))
                throw AzureStorageException.InvalidQuery("copyid");
            await service.AbortCopyAsync(blob, copyId, cancellationToken);
            http.Response.StatusCode = StatusCodes.Status204NoContent;
            return;
        }

        if ((HttpMethods.IsGet(http.Request.Method) || HttpMethods.IsHead(http.Request.Method)) && string.IsNullOrEmpty(comp))
        {
            await AuthorizeBlobReadAsync(request, service, blob, cancellationToken);
            var encryption = EnsureCustomerProvidedKey(http.Request, blob, write: false);
            EvaluateReadConditions(http.Request, blob);
            ValidateOptionalLease(http.Request, blob.Lease, "blob");
            await WriteBlobAsync(http, service, blob, encryption, cancellationToken);
            return;
        }

        if ((HttpMethods.IsGet(http.Request.Method) || HttpMethods.IsHead(http.Request.Method)) &&
            comp == "metadata")
        {
            await AuthorizeBlobReadAsync(request, service, blob, cancellationToken);
            EnsureCustomerProvidedKey(http.Request, blob, write: false);
            EvaluateReadConditions(http.Request, blob);
            ValidateOptionalLease(http.Request, blob.Lease, "blob");
            AzureResponseWriter.AddBlobMetadataHeaders(http.Response, blob);
            return;
        }

        if (HttpMethods.IsGet(http.Request.Method) && comp == "tags")
        {
            RequireFeatureVersion(request, new DateOnly(2019, 12, 12), "Get Blob Tags");
            Require(request, 't');
            EvaluateTagCondition(http.Request, blob, "x-ms-if-tags", source: false);
            EvaluateBlobTagConditions(http.Request, request, blob, write: false);
            ValidateOptionalLease(http.Request, blob.Lease, "blob");
            await writer.WriteTagsAsync(http, blob.Tags, cancellationToken);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "metadata")
        {
            Require(request, 'w');
            EnsureMutableVersion(blob);
            EnsureCustomerProvidedKey(http.Request, blob, write: true);
            EvaluateWriteConditions(http.Request, blob);
            EnsureLease(http.Request, blob.Lease, "blob");
            var updated = await service.SetBlobMetadataAsync(blob, ProtocolParsing.ReadMetadata(http.Request.Headers), cancellationToken);
            AzureResponseWriter.AddBlobEntityHeaders(http.Response, updated);
            AddRequestServerEncryptedHeader(http.Response);
            AddEncryptionResponseHeaders(http.Response, EncryptionOf(updated));
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "tags")
        {
            RequireFeatureVersion(request, new DateOnly(2019, 12, 12), "Set Blob Tags");
            Require(request, 't');
            EnsureMutableVersion(blob);
            EvaluateTagCondition(http.Request, blob, "x-ms-if-tags", source: false);
            EvaluateBlobTagConditions(http.Request, request, blob, write: true);
            EnsureLease(http.Request, blob.Lease, "blob");
            Dictionary<string, string> tags = null!;
            _ = await WithIntegrityValidationAsync(
                http.Request,
                async body => tags = await ProtocolParsing.ReadTagsBodyAsync(body, cancellationToken),
                allowStructured: false);
            await service.SetBlobTagsAsync(blob, tags, cancellationToken);
            http.Response.StatusCode = StatusCodes.Status204NoContent;
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "properties")
        {
            Require(request, 'w');
            EnsureMutableVersion(blob);
            var suppliedEncryption = EnsureCustomerProvidedKey(http.Request, blob, write: true);
            var contentEncryption = new BlobEncryption(
                blob.EncryptionScope,
                suppliedEncryption.CustomerProvidedKeySha256,
                suppliedEncryption.CustomerProvidedKey);
            EvaluateWriteConditions(http.Request, blob);
            EnsureLease(http.Request, blob.Lease, "blob");
            var resizeTo = TryParseLongHeader(http.Request.Headers, "x-ms-blob-content-length");
            var sequence = TryParseLongHeader(http.Request.Headers, "x-ms-blob-sequence-number");
            var updated = await service.SetBlobPropertiesAsync(
                blob,
                ProtocolParsing.HasBlobHttpPropertyHeaders(http.Request.Headers)
                    ? ProtocolParsing.ReadHttpProperties(http.Request.Headers, useStandardProperties: false)
                    : blob.Http,
                resizeTo,
                sequence,
                ProtocolParsing.First(http.Request.Headers, "x-ms-sequence-number-action"),
                contentEncryption,
                cancellationToken);
            if (updated.Kind == BlobKind.PageBlob)
                AzureResponseWriter.AddPageBlobWriteHeaders(http.Response, updated);
            else
                AzureResponseWriter.AddBlobEntityHeaders(http.Response, updated);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "snapshot")
        {
            Require(request, 'w');
            RequireZeroContentLength(http.Request);
            EnsureMutableVersion(blob);
            EnsureCustomerProvidedKey(http.Request, blob, write: true);
            EvaluateWriteConditions(http.Request, blob);
            EnsureLease(http.Request, blob.Lease, "blob");
            var hasSnapshotMetadata = http.Request.Headers.Keys.Any(name =>
                name.StartsWith("x-ms-meta-", StringComparison.OrdinalIgnoreCase));
            var created = await service.CreateSnapshotAsync(
                blob,
                hasSnapshotMetadata ? ProtocolParsing.ReadMetadata(http.Request.Headers) : null,
                cancellationToken);
            http.Response.Headers["x-ms-snapshot"] = created.Snapshot;
            if (created.VersionId is not null)
                http.Response.Headers["x-ms-version-id"] = created.VersionId;
            AzureResponseWriter.AddEntityTag(http.Response, created.ETag);
            http.Response.Headers.LastModified = created.LastModified.ToString("R", CultureInfo.InvariantCulture);
            AddEncryptionResponseHeaders(http.Response, EncryptionOf(created));
            http.Response.StatusCode = StatusCodes.Status201Created;
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "seal")
        {
            RequireFeatureVersion(request, new DateOnly(2019, 12, 12), "Append Blob Seal");
            Require(request, 'w');
            EnsureMutableVersion(blob);
            EnsureLease(http.Request, blob.Lease, "blob");
            var updated = await service.SealAppendBlobAsync(blob, cancellationToken);
            AzureResponseWriter.AddAppendBlobSealHeaders(http.Response, updated);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "tier")
        {
            RequireFeatureVersion(request, new DateOnly(2018, 11, 9), "Set Blob Tier");
            if (snapshot is not null)
                RequireFeatureVersion(request, new DateOnly(2019, 12, 12), "Set Blob Tier on a snapshot");
            Require(request, 'w');
            EvaluateTagCondition(http.Request, blob, "x-ms-if-tags", source: false);
            var tier = ProtocolParsing.First(http.Request.Headers, "x-ms-access-tier")
                       ?? throw AzureStorageException.InvalidHeader("x-ms-access-tier");
            ValidateAccessTierVersion(http.Request, tier);
            ValidateRehydratePriorityVersion(http.Request);
            var updated = await service.SetTierAsync(
                blob,
                tier,
                ProtocolParsing.First(http.Request.Headers, "x-ms-rehydrate-priority"),
                cancellationToken);
            http.Response.StatusCode = updated.Pending ? StatusCodes.Status202Accepted : StatusCodes.Status200OK;
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "expiry")
        {
            Require(request, 'w');
            EnsureMutableVersion(blob);
            EnsureLease(http.Request, blob.Lease, "blob");
            var expiry = ParseExpiry(http.Request.Headers, blob.CreatedAt, DateTimeOffset.UtcNow);
            var updated = await service.SetExpiryAsync(blob, expiry, cancellationToken);
            AzureResponseWriter.AddEntityTag(http.Response, updated.ETag);
            http.Response.Headers.LastModified = updated.LastModified.ToString("R", CultureInfo.InvariantCulture);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "lease")
        {
            Require(request, 'w');
            EnsureMutableVersion(blob);
            EvaluateWriteConditions(http.Request, blob);
            RequireZeroContentLength(http.Request);
            await HandleBlobLeaseAsync(http, request, service, blob, cancellationToken);
            return;
        }

        if (HttpMethods.IsGet(http.Request.Method) && comp == "pagelist")
        {
            Require(request, 'r');
            if (blob.Kind != BlobKind.PageBlob)
                throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
            EvaluateReadConditions(http.Request, blob);
            ValidateOptionalLease(http.Request, blob.Lease, "blob");
            var suppliedEncryption = EnsureCustomerProvidedKey(http.Request, blob, write: false);
            var encryption = new BlobEncryption(
                blob.EncryptionScope,
                suppliedEncryption.CustomerProvidedKeySha256,
                suppliedEncryption.CustomerProvidedKey);
            var rangeValue = ProtocolParsing.First(http.Request.Headers, "x-ms-range")
                             ?? ProtocolParsing.First(http.Request.Headers, "Range");
            var (rangeStart, rangeEnd) = ResolvePageListRange(request, blob, rangeValue);
            IReadOnlyList<PageRange> ranges;
            IReadOnlyList<PageRange> clearRanges;
            var previousSnapshot = NullIfEmpty(http.Request.Query["prevsnapshot"].ToString());
            var previousSnapshotUrl = ProtocolParsing.First(http.Request.Headers, "x-ms-previous-snapshot-url");
            if (previousSnapshot is not null && previousSnapshotUrl is not null)
                throw AzureStorageException.InvalidQuery("prevsnapshot");
            if (previousSnapshotUrl is not null)
                previousSnapshot = ParsePreviousSnapshotUrl(previousSnapshotUrl, request.Account, containerName, blobName);

            if (previousSnapshot is null || rangeEnd < rangeStart)
            {
                ranges = SelectPageRanges(request, blob, rangeValue);
                clearRanges = [];
            }
            else
            {
                if (snapshot is not null && string.CompareOrdinal(previousSnapshot, snapshot) >= 0)
                    throw AzureStorageException.InvalidQuery("prevsnapshot");
                var previous = await service.GetBlobAsync(
                    request.Account,
                    containerName,
                    blobName,
                    versionId: null,
                    snapshot: previousSnapshot,
                    includeDeleted: false,
                    cancellationToken);
                var diff = await service.GetPageRangeDiffAsync(
                    blob,
                    previous,
                    encryption,
                    rangeStart,
                    rangeEnd,
                    cancellationToken);
                ranges = diff.PageRanges;
                clearRanges = diff.ClearRanges;
            }

            var ordered = ranges.Select(range => (Range: range, IsClear: false))
                .Concat(clearRanges.Select(range => (Range: range, IsClear: true)))
                .OrderBy(item => item.Range.Start)
                .ToArray();
            var hasPageRangePaging = http.Request.Query.ContainsKey("maxresults") ||
                                     http.Request.Query.ContainsKey("marker");
            if (hasPageRangePaging)
            {
                RequireFeatureVersion(
                    request,
                    new DateOnly(2020, 10, 2),
                    "Get Page Ranges pagination");
            }
            var maxResults = ParsePageRangeMaxResults(
                http.Request.Query["maxresults"].ToString(),
                http.Request.Query.ContainsKey("maxresults"));
            var marker = ParsePageRangeMarker(http.Request.Query["marker"].ToString(), ordered.Length);
            var page = ordered.Skip(marker).Take(maxResults).ToArray();
            var nextOffset = marker + page.Length;
            var nextMarker = nextOffset < ordered.Length
                ? nextOffset.ToString(CultureInfo.InvariantCulture)
                : http.Request.Query.ContainsKey("maxresults") || http.Request.Query.ContainsKey("marker") ? string.Empty : null;
            AzureResponseWriter.AddBlobEntityHeaders(http.Response, blob);
            http.Response.Headers["x-ms-blob-content-length"] = blob.Content.Length.ToString(CultureInfo.InvariantCulture);
            await writer.WritePageRangesAsync(
                http,
                page.Where(item => !item.IsClear).Select(item => item.Range).ToArray(),
                page.Where(item => item.IsClear).Select(item => item.Range).ToArray(),
                nextMarker,
                cancellationToken);
            return;
        }

        if (HttpMethods.IsDelete(http.Request.Method) && string.IsNullOrEmpty(comp))
        {
            var hasExplicitSnapshotOrVersion = snapshot is not null || versionId is not null;
            var deleteSnapshots = ReadDeleteSnapshotsOption(http.Request, hasExplicitSnapshotOrVersion);
            Require(request, permanentDelete ? 'y' : versionId is not null ? 'x' : 'd');
            EvaluateWriteConditions(http.Request, blob);
            BlobConditionEvaluator.EvaluateAccessTierDeleteConditions(
                http.Request,
                blob.AccessTierChangedAt);
            EnsureLease(http.Request, blob.Lease, "blob");
            if (permanentDelete)
            {
                await service.PermanentlyDeleteBlobAsync(
                    blob,
                    hasExplicitSnapshotOrVersion,
                    cancellationToken);
                http.Response.Headers["x-ms-delete-type-permanent"] = "true";
            }
            else
            {
                var properties = await service.GetServicePropertiesAsync(request.Account, cancellationToken);
                await service.DeleteBlobAsync(
                    blob,
                    hasExplicitSnapshotOrVersion,
                    deleteSnapshots,
                    cancellationToken);
                if (IsServiceVersionAtLeast(request, new DateOnly(2017, 7, 29)))
                    http.Response.Headers["x-ms-delete-type-permanent"] = properties.BlobSoftDeleteEnabled ? "false" : "true";
            }
            http.Response.StatusCode = StatusCodes.Status202Accepted;
            return;
        }

        throw UnsupportedOperation();
    }

    private static async Task HandlePutBlobAsync(
        HttpContext http,
        StorageRequestContext request,
        BlobService service,
        string containerName,
        string blobName,
        CancellationToken cancellationToken)
    {
        var current = await TryGetCurrentBlobAsync(service, request.Account, containerName, blobName, cancellationToken);
        ValidateBlobTypeVersion(request, current?.Kind);
        RequireAny(request, current is null ? 'c' : 'w', 'w');
        EvaluateWriteConditions(http.Request, current);
        if (current is not null ||
            http.Request.Headers.ContainsKey("x-ms-lease-id") &&
            IsServiceVersionAtLeast(request, new DateOnly(2013, 8, 15)))
        {
            EnsureLease(http.Request, current?.Lease ?? LeaseRecord.Available, "blob");
        }
        EnsureNoPendingCopyDestination(current);

        var copySource = ProtocolParsing.First(http.Request.Headers, "x-ms-copy-source");
        if (copySource is not null)
        {
            var supportsAsynchronousCopy = IsServiceVersionAtLeast(request, new DateOnly(2012, 2, 12));
            if (supportsAsynchronousCopy)
            {
                RejectUnsupportedHeader(http.Request, "x-ms-source-lease-id");
            }
            else
            {
                if (request.Authorization.Kind != StorageAuthorizationKind.SharedKey)
                    throw AzureStorageException.AuthorizationFailure();
                RequireZeroContentLength(http.Request);
                EnsureDestinationCanBeOverwritten(current);
                var legacySourceReference = ResolveLegacyCopySource(http.Request, request, copySource);
                var legacySource = await service.GetBlobAsync(
                    legacySourceReference.Account,
                    legacySourceReference.Container,
                    legacySourceReference.Blob,
                    versionId: null,
                    legacySourceReference.Snapshot,
                    includeDeleted: false,
                    cancellationToken);
                ValidateBlobTypeVersion(request, legacySource.Kind);
                ValidateOptionalLease(
                    http.Request,
                    legacySource.Lease,
                    "blob",
                    "x-ms-source-lease-id");
                EvaluateCopySourceConditions(http.Request, legacySource);
                ValidateCopySourceTier(http.Request, legacySource, allowArchivedSource: false);
                ValidateCopyDestinationType(current, legacySource.Kind);
                var legacyCopy = await service.CopyBlobFromBlobSynchronouslyAsync(
                    request.Account,
                    containerName,
                    blobName,
                    legacySource,
                    ReadCopyWriteOptions(http.Request, legacySource, current, synchronous: true),
                    current?.Lease ?? LeaseRecord.Available,
                    current?.GenerationId,
                    current?.Revision,
                    cancellationToken);
                AzureResponseWriter.AddBlobWriteHeaders(http.Response, legacyCopy);
                http.Response.StatusCode = StatusCodes.Status201Created;
                return;
            }
            var requestedType = ProtocolParsing.First(http.Request.Headers, "x-ms-blob-type");
            var requiresSyncValue = ProtocolParsing.First(http.Request.Headers, "x-ms-requires-sync");
            var requiresSync = false;
            if (requiresSyncValue is not null && !bool.TryParse(requiresSyncValue, out requiresSync))
                throw AzureStorageException.InvalidHeader("x-ms-requires-sync", requiresSyncValue);
            EnsureDestinationCanBeOverwritten(current);

            if (requestedType is not null)
            {
                if (requestedType != "BlockBlob")
                    throw AzureStorageException.InvalidHeader("x-ms-blob-type", requestedType);
                if (requiresSync)
                    throw AzureStorageException.InvalidHeader("x-ms-requires-sync", requiresSyncValue);
                RequireFeatureVersion(request, new DateOnly(2020, 4, 8), "Put Blob From URL");
                if (http.Request.Headers.ContainsKey("x-ms-seal-blob"))
                {
                    throw AzureStorageException.InvalidHeader(
                        "x-ms-seal-blob",
                        ProtocolParsing.First(http.Request.Headers, "x-ms-seal-blob"));
                }
                RequireZeroContentLength(http.Request);
                if (ProtocolParsing.First(http.Request.Headers, "x-ms-source-range") is { } sourceRange)
                    throw AzureStorageException.InvalidHeader("x-ms-source-range", sourceRange);
                var copySourceTags = ReadCopySourceTags(http.Request);
                var transfers = http.RequestServices.GetRequiredService<UrlTransferClient>();
                var transfer = await transfers.ReadAsync(
                    http.Request,
                    copySource,
                    sourceRange: null,
                    allowSourceCustomerProvidedKey: true,
                    5_000L * 1024 * 1024,
                    sourceLengthConflict: true,
                    async source => await service.PutBlockBlobAsync(
                        request.Account,
                        containerName,
                        blobName,
                        source.Content,
                        ReadUrlWriteOptions(http.Request, source, copySourceTags, current),
                        current?.Lease ?? LeaseRecord.Available,
                        current?.GenerationId,
                        current?.Revision,
                        cancellationToken),
                    cancellationToken,
                    copySourceTags);
                var uploaded = transfer.Value;
                AzureResponseWriter.AddBlobWriteHeaders(http.Response, uploaded);
                AddRequestServerEncryptedHeader(http.Response);
                AddEncryptionResponseHeaders(http.Response, EncryptionOf(uploaded));
                AddFullBlobChecksumHeaders(http.Response, transfer.Checksums);
                http.Response.StatusCode = StatusCodes.Status201Created;
                return;
            }

            var publicSource = SanitizeCopySource(copySource);
            if (requiresSync)
            {
                RequireFeatureVersion(request, new DateOnly(2018, 3, 28), "Copy Blob From URL");
                ValidateSynchronousCopyEncryption(http.Request);
                ValidateCopyDestinationType(current, BlobKind.BlockBlob);
                RequireZeroContentLength(http.Request);
                if (ProtocolParsing.First(http.Request.Headers, "x-ms-source-range") is { } sourceRange)
                    throw AzureStorageException.InvalidHeader("x-ms-source-range", sourceRange);
                if (http.Request.Headers.ContainsKey("x-ms-copy-source-blob-properties"))
                {
                    throw AzureStorageException.InvalidHeader(
                        "x-ms-copy-source-blob-properties",
                        ProtocolParsing.First(http.Request.Headers, "x-ms-copy-source-blob-properties"));
                }
                var copySourceTags = ReadCopySourceTags(http.Request);
                BlobRecord synchronousCopy;
                var synchronousInternalSource = ResolveInternalCopySource(http.Request, request, copySource);
                if (synchronousInternalSource is not null)
                {
                    var source = await ResolveCopySourceAsync(
                        http,
                        request,
                        service,
                        synchronousInternalSource,
                        cancellationToken,
                        requireTagsPermission: copySourceTags || HasSourceTagCondition(http.Request));
                    EvaluateCopySourceConditions(http.Request, source);
                    ValidateCopySourceTier(http.Request, source, allowArchivedSource: false);
                    synchronousCopy = await service.CopyBlockBlobFromBlobAsync(
                        request.Account,
                        containerName,
                        blobName,
                        source,
                        ReadCopyWriteOptions(
                            http.Request,
                            source,
                            current,
                            copySourceTags,
                            synchronous: true),
                        publicSource,
                        current?.Lease ?? LeaseRecord.Available,
                        current?.GenerationId,
                        current?.Revision,
                        cancellationToken);
                }
                else
                {
                    var transfers = http.RequestServices.GetRequiredService<UrlTransferClient>();
                    var transfer = await transfers.ReadAsync(
                        http.Request,
                        copySource,
                        sourceRange: null,
                        allowSourceCustomerProvidedKey: false,
                        256L * 1024 * 1024,
                        sourceLengthConflict: true,
                        async source =>
                        {
                            if (source.Kind is not null && source.Kind != BlobKind.BlockBlob)
                            {
                                throw new AzureStorageException(
                                    StatusCodes.Status409Conflict,
                                    "InvalidSourceBlobType",
                                    "The source blob type is invalid for this operation.");
                            }
                            return await service.CopyBlockBlobFromStreamAsync(
                                request.Account,
                                containerName,
                                blobName,
                                source.Content,
                                source.ContentLength!.Value,
                                source.CommittedBlocks,
                                ReadUrlCopyWriteOptions(
                                    http.Request,
                                    source,
                                    copySourceTags,
                                    current,
                                    synchronous: true),
                                publicSource,
                                current?.Lease ?? LeaseRecord.Available,
                                current?.GenerationId,
                                current?.Revision,
                                cancellationToken);
                        },
                        cancellationToken,
                        copySourceTags,
                        preserveSourceShape: true);
                    synchronousCopy = transfer.Value;
                }
                AzureResponseWriter.AddBlobCopyHeaders(http.Response, synchronousCopy, includeVersion: false);
                AddRequestServerEncryptedHeader(http.Response);
                AddEncryptionResponseHeaders(http.Response, EncryptionOf(synchronousCopy));
                http.Response.StatusCode = StatusCodes.Status202Accepted;
                return;
            }

            if (http.Request.Headers.ContainsKey("x-ms-copy-source-tag-option"))
            {
                throw AzureStorageException.InvalidHeader(
                    "x-ms-copy-source-tag-option",
                    ProtocolParsing.First(http.Request.Headers, "x-ms-copy-source-tag-option"));
            }
            if (http.Request.Headers.ContainsKey("x-ms-copy-source-blob-properties"))
            {
                throw AzureStorageException.InvalidHeader(
                    "x-ms-copy-source-blob-properties",
                    ProtocolParsing.First(http.Request.Headers, "x-ms-copy-source-blob-properties"));
            }
            ValidateAsynchronousCopyEncryption(http.Request);
            EnsureAsynchronousCopyDestinationLease(http.Request, current);
            RequireZeroContentLength(http.Request);
            BlobRecord copied;
            var internalSource = ResolveInternalCopySource(http.Request, request, copySource);
            if (internalSource is not null)
            {
                var source = await ResolveCopySourceAsync(
                    http,
                    request,
                    service,
                    internalSource,
                    cancellationToken,
                    requireTagsPermission: HasSourceTagCondition(http.Request));
                EvaluateCopySourceConditions(http.Request, source);
                ValidateCopySourceTier(http.Request, source, allowArchivedSource: true);
                ValidateCopyDestinationType(current, source.Kind);
                copied = await service.BeginCopyFromBlobAsync(
                    request.Account,
                    containerName,
                    blobName,
                    source,
                    ReadCopySealDestination(http.Request, source.Kind, source.IsSealed),
                    ReadCopyWriteOptions(http.Request, source, current),
                    publicSource,
                    current?.Lease ?? LeaseRecord.Available,
                    current?.GenerationId,
                    current?.Revision,
                    cancellationToken);
            }
            else
            {
                var transfers = http.RequestServices.GetRequiredService<UrlTransferClient>();
                var transfer = await transfers.ReadAsync(
                    http.Request,
                    copySource,
                    sourceRange: null,
                    allowSourceCustomerProvidedKey: false,
                    long.MaxValue,
                    sourceLengthConflict: false,
                    async source =>
                    {
                        var sourceKind = source.Kind ?? BlobKind.BlockBlob;
                        ValidateBlobTypeVersion(request, sourceKind);
                        ValidateCopyDestinationType(current, sourceKind);
                        return await service.BeginCopyFromStreamAsync(
                            request.Account,
                            containerName,
                            blobName,
                            source.Content,
                            source.ContentLength!.Value,
                            sourceKind,
                            source.SequenceNumber,
                            ReadCopySealDestination(http.Request, sourceKind, source.IsSealed),
                            source.AppendBlockCount,
                            source.CommittedBlocks,
                            source.PageRanges,
                            ReadUrlCopyWriteOptions(
                                http.Request,
                                source,
                                copySourceTags: false,
                                current,
                                synchronous: false),
                            publicSource,
                            current?.Lease ?? LeaseRecord.Available,
                            current?.GenerationId,
                            current?.Revision,
                            cancellationToken);
                    },
                    cancellationToken,
                    preserveSourceShape: true);
                copied = transfer.Value;
            }
            AzureResponseWriter.AddBlobCopyHeaders(http.Response, copied, includeVersion: true);
            http.Response.StatusCode = StatusCodes.Status202Accepted;
            return;
        }

        EnsureDestinationCanBeOverwritten(current);
        var type = ProtocolParsing.First(http.Request.Headers, "x-ms-blob-type")
                   ?? throw AzureStorageException.InvalidHeader("x-ms-blob-type");
        var explicitlyRequestedTier = ProtocolParsing.First(http.Request.Headers, "x-ms-access-tier");
        if (explicitlyRequestedTier is not null && type != "BlockBlob")
            throw AzureStorageException.InvalidHeader("x-ms-access-tier", explicitlyRequestedTier);
        BlobRecord created;
        TransactionalChecksums? checksums = null;
        switch (type)
        {
            case "BlockBlob":
                created = null!;
                var persistedMd5 = ProtocolParsing.First(http.Request.Headers, "x-ms-blob-content-md5");
                checksums = await WithIntegrityValidationAsync(http.Request, async body =>
                    created = await service.PutBlockBlobAsync(
                        request.Account,
                        containerName,
                        blobName,
                        body,
                        ReadWriteOptions(
                            http.Request,
                            current,
                            generateContentMd5: IsServiceVersionAtLeast(request, new DateOnly(2012, 2, 12))),
                        current?.Lease ?? LeaseRecord.Available,
                        current?.GenerationId,
                        current?.Revision,
                        cancellationToken),
                    maximumBodyBytes: GetMaximumPutBlobBytes(request),
                    expectedMd5HeaderName: persistedMd5 is null
                        ? "Content-MD5"
                        : "x-ms-blob-content-md5");
                break;
            case "AppendBlob":
                RequireAppendBlobVersion(request);
                RequireZeroContentLength(http.Request);
                created = await service.CreateAppendBlobAsync(
                    request.Account,
                    containerName,
                    blobName,
                    ReadWriteOptions(http.Request, current),
                    current?.Lease ?? LeaseRecord.Available,
                    current?.GenerationId,
                    current?.Revision,
                    cancellationToken);
                break;
            case "PageBlob":
                RequirePageBlobVersion(request);
                RequireZeroContentLength(http.Request);
                var length = ProtocolParsing.ParseLongHeader(http.Request.Headers, "x-ms-blob-content-length", required: true);
                var sequence = ProtocolParsing.ParseLongHeader(http.Request.Headers, "x-ms-blob-sequence-number", defaultValue: 0);
                created = await service.CreatePageBlobAsync(
                    request.Account,
                    containerName,
                    blobName,
                    length,
                    ReadWriteOptions(http.Request, current),
                    sequence,
                    current?.Lease ?? LeaseRecord.Available,
                    current?.GenerationId,
                    current?.Revision,
                    cancellationToken);
                break;
            default:
                throw AzureStorageException.InvalidHeader("x-ms-blob-type", type);
        }

        AzureResponseWriter.AddBlobWriteHeaders(http.Response, created);
        if (checksums is not null)
            AddFullBlobChecksumHeaders(http.Response, checksums);
        AddRequestServerEncryptedHeader(http.Response);
        AddEncryptionResponseHeaders(http.Response, EncryptionOf(created));
        http.Response.StatusCode = StatusCodes.Status201Created;
    }

    private static async Task WriteBlobAsync(
        HttpContext http,
        BlobService service,
        BlobRecord blob,
        BlobEncryption encryption,
        CancellationToken cancellationToken)
    {
        long start = 0;
        long end = blob.Content.Length - 1;
        var rangeHeader = HttpMethods.IsGet(http.Request.Method)
            ? ProtocolParsing.First(http.Request.Headers, "x-ms-range")
              ?? ProtocolParsing.First(http.Request.Headers, "Range")
            : null;
        if (!string.IsNullOrEmpty(rangeHeader))
        {
            var request = StorageRequestContext.Get(http);
            (start, end) = ProtocolParsing.ParseStorageRange(
                rangeHeader,
                blob.Content.Length,
                allowOpenEnded: IsServiceVersionAtLeast(request, new DateOnly(2011, 8, 18)));
            http.Response.StatusCode = StatusCodes.Status206PartialContent;
            http.Response.Headers.ContentRange = $"bytes {start}-{end}/{blob.Content.Length}";
        }

        var length = blob.Content.Length == 0 ? 0 : end - start + 1;
        if (HttpMethods.IsHead(http.Request.Method))
        {
            AzureResponseWriter.AddBlobHeaders(http.Response, blob);
            ApplySasResponseOverrides(http);
            http.Response.ContentLength = length;
            return;
        }

        var wantMd5 = string.Equals(
            ProtocolParsing.First(http.Request.Headers, "x-ms-range-get-content-md5"),
            "true",
            StringComparison.OrdinalIgnoreCase);
        var wantCrc64 = string.Equals(
            ProtocolParsing.First(http.Request.Headers, "x-ms-range-get-content-crc64"),
            "true",
            StringComparison.OrdinalIgnoreCase);
        if (wantMd5 && wantCrc64)
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "BothCrc64AndMd5Specified",
                "Both CRC64 and MD5 were requested. Specify only one checksum.");
        }
        if (wantCrc64)
        {
            RequireFeatureVersion(
                StorageRequestContext.Get(http),
                new DateOnly(2019, 2, 2),
                "Transactional range CRC64 checksums");
        }

        var structuredBody = ProtocolParsing.First(http.Request.Headers, "x-ms-structured-body");
        if (structuredBody is not null)
        {
            if (!string.Equals(structuredBody, StructuredBodyDecoder.ContentType, StringComparison.Ordinal))
                throw AzureStorageException.InvalidHeader("x-ms-structured-body", structuredBody);
            if (wantMd5 || wantCrc64)
            {
                throw new AzureStorageException(
                    StatusCodes.Status400BadRequest,
                    "InvalidHeaderValue",
                    "A structured response cannot also request a transactional range checksum.");
            }
            var request = StorageRequestContext.Get(http);
            if (!DateOnly.TryParseExact(
                    request.ServiceVersion,
                    "yyyy-MM-dd",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out var serviceVersion) || serviceVersion < new DateOnly(2025, 1, 5))
            {
                throw AzureStorageException.FeatureVersionMismatch(
                    "Structured response bodies require service version 2025-01-05 or later.");
            }

            blob = await service.RecordSmartTierAccessAsync(blob, cancellationToken);
            AzureResponseWriter.AddBlobHeaders(http.Response, blob);
            ApplySasResponseOverrides(http);
            http.Response.Headers["x-ms-structured-body"] = structuredBody;
            http.Response.Headers["x-ms-structured-content-length"] = length.ToString(CultureInfo.InvariantCulture);
            http.Response.ContentLength = StructuredBodyEncoder.GetEncodedLength(length);
            await StructuredBodyEncoder.WriteAsync(
                length,
                async (rangeStart, rangeLength, destination, token) =>
                    await service.WriteContentAsync(
                        blob,
                        encryption,
                        start + rangeStart,
                        rangeLength,
                        destination,
                        token),
                http.Response.Body,
                cancellationToken);
            return;
        }

        if (wantMd5 || wantCrc64)
        {
            if (rangeHeader is null || length > 4 * 1024 * 1024)
            {
                throw AzureStorageException.InvalidHeader(
                    wantMd5 ? "x-ms-range-get-content-md5" : "x-ms-range-get-content-crc64",
                    "true");
            }
        }

        blob = await service.RecordSmartTierAccessAsync(blob, cancellationToken);
        AzureResponseWriter.AddBlobHeaders(http.Response, blob);
        ApplySasResponseOverrides(http);
        http.Response.ContentLength = length;
        if (length == 0)
            return;
        if (wantMd5 || wantCrc64)
        {
            using var buffer = new MemoryStream((int)length);
            await service.WriteContentAsync(blob, encryption, start, length, buffer, cancellationToken);
            var bytes = buffer.ToArray();
            if (wantMd5)
            {
                http.Response.Headers.ContentMD5 = Convert.ToBase64String(MD5.HashData(bytes));
            }
            else
            {
                var crc64 = new StorageCrc64();
                crc64.Append(bytes);
                http.Response.Headers["x-ms-content-crc64"] = Convert.ToBase64String(crc64.GetHash());
            }
            await http.Response.Body.WriteAsync(bytes, cancellationToken);
            return;
        }

        await service.WriteContentAsync(blob, encryption, start, length, http.Response.Body, cancellationToken);
    }

    private static async Task HandleQueryAsync(
        HttpContext http,
        StorageRequestContext request,
        BlobService service,
        BlobRecord blob,
        CancellationToken cancellationToken)
    {
        if (!DateOnly.TryParseExact(
                request.ServiceVersion,
                "yyyy-MM-dd",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var version) || version < new DateOnly(2019, 12, 12))
        {
            throw AzureStorageException.FeatureVersionMismatch(
                "Query Blob Contents requires service version 2019-12-12 or later.");
        }

        await AuthorizeBlobReadAsync(request, service, blob, cancellationToken);
        if (blob.Kind != BlobKind.BlockBlob)
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "InvalidBlobType",
                "Query Blob Contents is supported only for block blobs.");
        }
        if (blob.CustomerProvidedKeySha256 is not null ||
            ProtocolParsing.First(http.Request.Headers, "x-ms-encryption-key") is not null)
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "BlobOperationNotSupported",
                "Query Blob Contents is not supported for blobs encrypted with customer-provided keys.");
        }

        EvaluateReadConditions(http.Request, blob);
        ValidateOptionalLease(http.Request, blob.Lease, "blob");

        var query = await BlobQueryProtocol.ReadRequestAsync(http.Request.Body, cancellationToken);
        AzureResponseWriter.AddBlobQueryHeaders(http.Response, blob);
        http.Response.ContentType = "avro/binary";
        http.Response.StatusCode = StatusCodes.Status200OK;

        var pipe = new Pipe(new PipeOptions(
            pauseWriterThreshold: 1024 * 1024,
            resumeWriterThreshold: 512 * 1024,
            useSynchronizationContext: false));
        using var producerCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var producer = ProduceQueryInputAsync(
            service,
            blob,
            new BlobEncryption(blob.EncryptionScope, null),
            pipe.Writer,
            producerCancellation.Token);

        await using var content = pipe.Reader.AsStream(leaveOpen: true);
        try
        {
            await BlobQueryProtocol.ExecuteAsync(
                query,
                content,
                http.Response.Body,
                blob.Content.Length,
                cancellationToken);
        }
        finally
        {
            producerCancellation.Cancel();
            await pipe.Reader.CompleteAsync();
            try
            {
                await producer;
            }
            catch (OperationCanceledException) when (producerCancellation.IsCancellationRequested)
            {
            }
        }
    }

    private static async Task ProduceQueryInputAsync(
        BlobService service,
        BlobRecord blob,
        BlobEncryption encryption,
        PipeWriter writer,
        CancellationToken cancellationToken)
    {
        Exception? failure = null;
        try
        {
            await using var destination = writer.AsStream(leaveOpen: true);
            await service.WriteContentAsync(
                blob,
                encryption,
                0,
                blob.Content.Length,
                destination,
                cancellationToken);
        }
        catch (Exception exception)
        {
            failure = exception;
            throw;
        }
        finally
        {
            await writer.CompleteAsync(failure);
        }
    }

    private static ResolvedInternalCopySource? ResolveInternalCopySource(
        HttpRequest destination,
        StorageRequestContext destinationRequest,
        string sourceValue)
    {
        if (!Uri.TryCreate(sourceValue, UriKind.Absolute, out var sourceUri))
            throw AzureStorageException.InvalidHeader("x-ms-copy-source");

        var options = destination.HttpContext.RequestServices.GetRequiredService<IOptions<SavaOptions>>().Value;
        var destinationHost = destination.Host.Host;
        string? hostAccount = null;
        if (!string.Equals(sourceUri.Host, destinationHost, StringComparison.OrdinalIgnoreCase))
            return null;
        if (StorageResourcePath.HostIdentifiesAccount(destinationHost, destinationRequest.Account))
            hostAccount = destinationRequest.Account;

        var segments = StorageResourcePath.DecodeSegments(sourceUri.AbsolutePath);
        var account = hostAccount;
        var offset = 0;
        if (account is null)
        {
            if (segments.Length == 0 || !options.Accounts.ContainsKey(segments[0]))
                return null;
            account = segments[0];
            offset = 1;
        }

        if (!string.Equals(account, destinationRequest.Account, StringComparison.Ordinal))
            return null;

        var (container, name) = StorageResourcePath.ResolveBlob(segments, offset);
        if (string.IsNullOrEmpty(container) || string.IsNullOrEmpty(name))
            throw AzureStorageException.InvalidHeader("x-ms-copy-source");
        var query = QueryHelpers.ParseQuery(sourceUri.Query);
        return new ResolvedInternalCopySource(
            sourceUri,
            account,
            container,
            name,
            query.TryGetValue("snapshot", out var snapshot) ? NullIfEmpty(snapshot.ToString()) : null,
            query.TryGetValue("versionid", out var version) ? NullIfEmpty(version.ToString()) : null);
    }

    private static ResolvedInternalCopySource ResolveLegacyCopySource(
        HttpRequest destination,
        StorageRequestContext destinationRequest,
        string sourceValue)
    {
        if (sourceValue.Length > 2048 ||
            !sourceValue.StartsWith("/", StringComparison.Ordinal) ||
            sourceValue.StartsWith("//", StringComparison.Ordinal) ||
            sourceValue.Contains('#', StringComparison.Ordinal))
        {
            throw AzureStorageException.InvalidHeader("x-ms-copy-source", sourceValue);
        }

        var queryOffset = sourceValue.IndexOf('?');
        var escapedPath = queryOffset < 0 ? sourceValue : sourceValue[..queryOffset];
        var queryText = queryOffset < 0 ? string.Empty : sourceValue[queryOffset..];
        string[] segments;
        try
        {
            segments = StorageResourcePath.DecodeSegments(escapedPath);
        }
        catch (UriFormatException)
        {
            throw AzureStorageException.InvalidHeader("x-ms-copy-source", sourceValue);
        }

        if (segments.Length < 2 || string.IsNullOrEmpty(segments[0]))
            throw AzureStorageException.InvalidHeader("x-ms-copy-source", sourceValue);
        if (!string.Equals(segments[0], destinationRequest.Account, StringComparison.Ordinal))
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "CopyAcrossAccountsNotSupported",
                "The copy source account and destination account must be the same.");
        }

        var (container, blob) = StorageResourcePath.ResolveBlob(segments, 1);
        if (string.IsNullOrEmpty(container) || string.IsNullOrEmpty(blob))
            throw AzureStorageException.InvalidHeader("x-ms-copy-source", sourceValue);
        var query = QueryHelpers.ParseQuery(queryText);
        if (query.Keys.Any(key => !string.Equals(key, "snapshot", StringComparison.OrdinalIgnoreCase)))
            throw AzureStorageException.InvalidHeader("x-ms-copy-source", sourceValue);
        var snapshot = query.TryGetValue("snapshot", out var snapshotValue)
            ? NullIfEmpty(snapshotValue.ToString())
            : null;

        var sourceUri = new UriBuilder(
            destination.Scheme,
            destination.Host.Host,
            destination.Host.Port ?? -1,
            escapedPath)
        {
            Query = queryText.TrimStart('?')
        }.Uri;
        return new ResolvedInternalCopySource(
            sourceUri,
            destinationRequest.Account,
            container,
            blob,
            snapshot,
            VersionId: null);
    }

    private static async Task<BlobRecord> ResolveCopySourceAsync(
        HttpContext destination,
        StorageRequestContext destinationRequest,
        BlobService service,
        ResolvedInternalCopySource source,
        CancellationToken cancellationToken,
        bool requireTagsPermission = false)
    {
        using var sourceScope = destination.RequestServices.CreateScope();
        var sourceHttp = new DefaultHttpContext
        {
            RequestServices = sourceScope.ServiceProvider
        };
        sourceHttp.Request.Scheme = source.Uri.Scheme;
        sourceHttp.Request.Host = HostString.FromUriComponent(source.Uri);
        sourceHttp.Request.Method = HttpMethods.Get;
        sourceHttp.Request.Path = PathString.FromUriComponent(source.Uri);
        sourceHttp.Request.QueryString = QueryString.FromUriComponent(source.Uri);
        sourceHttp.Connection.RemoteIpAddress = destination.Connection.RemoteIpAddress;
        sourceHttp.Features.Get<IHttpRequestFeature>()!.RawTarget = source.Uri.PathAndQuery;

        var sourceContext = new StorageRequestContext
        {
            RequestId = Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(16)),
            Account = source.Account,
            Container = source.Container,
            Blob = source.Blob,
            Snapshot = source.Snapshot,
            VersionId = source.VersionId,
            ResourceKind = StorageResourceKind.Blob,
            CanonicalResourcePath = $"/{source.Account}/{source.Container}/{source.Blob}",
            ServiceVersion = destinationRequest.ServiceVersion,
            Authorization = StorageAuthorization.Anonymous
        };
        StorageRequestContext.Set(sourceHttp, sourceContext);
        ValidateBlobVersionRequest(sourceContext);

        var sourceAuthorization = ProtocolParsing.First(
            destination.Request.Headers,
            "x-ms-copy-source-authorization");
        if (sourceAuthorization is not null)
        {
            RequireFeatureVersion(destinationRequest, new DateOnly(2020, 10, 2), "Copy source authorization");
            if (!sourceAuthorization.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
                throw AzureStorageException.InvalidHeader("x-ms-copy-source-authorization", sourceAuthorization);
            sourceHttp.Request.Headers.Authorization = sourceAuthorization;
        }

        if (sourceAuthorization is null &&
            string.Equals(source.Account, destinationRequest.Account, StringComparison.Ordinal) &&
            destinationRequest.Authorization.Kind == StorageAuthorizationKind.SharedKey)
        {
            sourceContext.Authorization = destinationRequest.Authorization;
        }
        else if (sourceAuthorization is null &&
                 string.Equals(source.Account, destinationRequest.Account, StringComparison.Ordinal) &&
                 destinationRequest.Authorization.Kind == StorageAuthorizationKind.Bearer &&
                 destinationRequest.Authorization.Allows('r') &&
                 !QueryHelpers.ParseQuery(source.Uri.Query).ContainsKey("sig"))
        {
            sourceContext.Authorization = destinationRequest.Authorization;
        }
        else
        {
            var authenticator = sourceScope.ServiceProvider.GetRequiredService<StorageAuthenticator>();
            sourceContext.Authorization = await authenticator.AuthenticateAsync(
                sourceHttp,
                sourceContext,
                cancellationToken);
        }

        if (sourceContext.Authorization.Kind == StorageAuthorizationKind.Anonymous)
        {
            if (requireTagsPermission)
                throw AzureStorageException.AuthorizationFailure();
            var sourceContainer = await service.GetContainerAsync(
                source.Account,
                source.Container,
                includeDeleted: false,
                cancellationToken);
            if (!service.AllowsAnonymousPublicAccess || sourceContainer.PublicAccess is not ("blob" or "container"))
                throw AzureStorageException.AuthorizationFailure();
        }
        else if (!sourceContext.Authorization.Allows('r') ||
                 requireTagsPermission && !sourceContext.Authorization.Allows('t'))
        {
            throw AzureStorageException.AuthorizationFailure();
        }

        var sourceBlob = await service.GetBlobAsync(
            source.Account,
            source.Container,
            source.Blob,
            source.VersionId,
            source.Snapshot,
            false,
            cancellationToken);
        ValidateBlobTypeVersion(destinationRequest, sourceBlob.Kind);
        return sourceBlob;
    }

    private static async Task HandleContainerLeaseAsync(
        HttpContext http,
        StorageRequestContext request,
        BlobService service,
        ContainerRecord container,
        CancellationToken cancellationToken)
    {
        var (action, transition) = ApplyLeaseAction(http.Request, container.Lease, useLegacySemantics: false);
        var returnsProperties = IsServiceVersionAtLeast(request, new DateOnly(2013, 8, 15));
        var updated = await service.SetContainerLeaseAsync(
            container,
            transition.Lease,
            updateProperties: !returnsProperties,
            cancellationToken);
        http.Response.StatusCode = LeaseStatusCode(action);
        if (returnsProperties)
        {
            AzureResponseWriter.AddEntityTag(http.Response, updated.ETag);
            http.Response.Headers.LastModified = updated.LastModified.ToString("R", CultureInfo.InvariantCulture);
        }
        AddLeaseResponseHeaders(http.Response, action, transition);
    }

    private static async Task HandleBlobLeaseAsync(
        HttpContext http,
        StorageRequestContext request,
        BlobService service,
        BlobRecord blob,
        CancellationToken cancellationToken)
    {
        var modernLease = IsServiceVersionAtLeast(request, new DateOnly(2012, 2, 12));
        var (action, transition) = ApplyLeaseAction(http.Request, blob.Lease, useLegacySemantics: !modernLease);
        var updated = await service.SetBlobLeaseAsync(blob, transition.Lease, cancellationToken);
        http.Response.StatusCode = LeaseStatusCode(action);
        if (IsServiceVersionAtLeast(request, new DateOnly(2013, 8, 15)))
        {
            AzureResponseWriter.AddEntityTag(http.Response, updated.ETag);
            http.Response.Headers.LastModified = updated.LastModified.ToString("R", CultureInfo.InvariantCulture);
        }
        AddLeaseResponseHeaders(http.Response, action, transition);
    }

    private static (LeaseAction Action, LeaseTransition Transition) ApplyLeaseAction(
        HttpRequest request,
        LeaseRecord current,
        bool useLegacySemantics)
    {
        var actionValue = ProtocolParsing.First(request.Headers, "x-ms-lease-action")
                          ?? throw AzureStorageException.MissingHeader("x-ms-lease-action");
        if (!Enum.TryParse<LeaseAction>(actionValue, ignoreCase: true, out var action))
            throw AzureStorageException.InvalidHeader("x-ms-lease-action", actionValue);

        if (useLegacySemantics)
        {
            RejectUnsupportedHeader(request, "x-ms-lease-duration");
            RejectUnsupportedHeader(request, "x-ms-lease-break-period");
            RejectUnsupportedHeader(request, "x-ms-proposed-lease-id");
            if (action == LeaseAction.Change)
            {
                throw AzureStorageException.FeatureVersionMismatch(
                    "Changing a blob lease requires service version 2012-02-12 or later.");
            }
        }

        var duration = action == LeaseAction.Acquire
            ? useLegacySemantics
                ? 60
                : ParseLeaseIntegerHeader(request.Headers, "x-ms-lease-duration", required: true)
            : null;
        var breakPeriod = action == LeaseAction.Break
            ? ParseLeaseIntegerHeader(request.Headers, "x-ms-lease-break-period", required: false)
            : null;
        var leases = request.HttpContext.RequestServices.GetRequiredService<LeaseService>();
        return (action, leases.Apply(
            current,
            action,
            duration,
            breakPeriod,
            ProtocolParsing.First(request.Headers, "x-ms-lease-id"),
            ProtocolParsing.First(request.Headers, "x-ms-proposed-lease-id"),
            useLegacySemantics));
    }

    private static void AddLeaseResponseHeaders(
        HttpResponse response,
        LeaseAction action,
        LeaseTransition transition)
    {
        if ((action is LeaseAction.Acquire or LeaseAction.Renew or LeaseAction.Change) &&
            transition.Lease.Id is not null)
        {
            response.Headers["x-ms-lease-id"] = transition.Lease.Id;
        }
        if (transition.RemainingSeconds.HasValue)
        {
            response.Headers["x-ms-lease-time"] =
                transition.RemainingSeconds.Value.ToString(CultureInfo.InvariantCulture);
        }
    }

    private static int LeaseStatusCode(LeaseAction action) => action switch
    {
        LeaseAction.Acquire => StatusCodes.Status201Created,
        LeaseAction.Break => StatusCodes.Status202Accepted,
        _ => StatusCodes.Status200OK
    };

    private static int? ParseLeaseIntegerHeader(
        IHeaderDictionary headers,
        string name,
        bool required)
    {
        var value = ProtocolParsing.First(headers, name);
        if (value is null)
        {
            if (required)
                throw AzureStorageException.MissingHeader(name);
            return null;
        }
        if (!int.TryParse(value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var parsed))
            throw AzureStorageException.InvalidHeader(name, value);
        return parsed;
    }

    private static IReadOnlyList<PageRange> SelectPageRanges(
        StorageRequestContext request,
        BlobRecord blob,
        string? requestedRange)
    {
        if (requestedRange is null)
            return blob.PageRanges;

        var (start, end) = ResolvePageListRange(request, blob, requestedRange);
        if (end < start)
            return [];
        return blob.PageRanges
            .Where(range => range.End >= start && range.Start <= end)
            .Select(range => new PageRange(Math.Max(range.Start, start), Math.Min(range.End, end)))
            .ToArray();
    }

    private static (long Start, long End) ParsePageWriteRange(string value, long length)
    {
        try
        {
            return ProtocolParsing.ParseStorageRange(
                value,
                length,
                allowOpenEnded: false,
                allowEndPastLength: false);
        }
        catch (AzureStorageException exception) when (exception.ErrorCode == "InvalidRange")
        {
            throw AzureStorageException.InvalidPageRange();
        }
    }

    private static (long Start, long End) ResolvePageListRange(
        StorageRequestContext request,
        BlobRecord blob,
        string? requestedRange)
    {
        if (requestedRange is null)
            return blob.Content.Length == 0 ? (0, -1) : (0, blob.Content.Length - 1);
        var (start, end) = ProtocolParsing.ParseStorageRange(
            requestedRange,
            blob.Content.Length,
            allowOpenEnded: IsServiceVersionAtLeast(request, new DateOnly(2011, 8, 18)));
        if (start % 512 != 0 || (end + 1) % 512 != 0)
            throw AzureStorageException.InvalidHeader("x-ms-range", requestedRange);
        return (start, end);
    }

    private static string ParsePreviousSnapshotUrl(
        string value,
        string account,
        string container,
        string blobName)
    {
        if (!Uri.TryCreate(value, UriKind.Absolute, out var uri))
            throw AzureStorageException.InvalidHeader("x-ms-previous-snapshot-url", value);
        var segments = StorageResourcePath.DecodeSegments(uri.AbsolutePath);
        var offset = StorageResourcePath.HostIdentifiesAccount(uri.Host, account)
            ? 0
            : segments.Length > 0 && string.Equals(segments[0], account, StringComparison.Ordinal) ? 1 : 0;
        var (resolvedContainer, resolvedBlob) = StorageResourcePath.ResolveBlob(segments, offset);
        if (!string.Equals(resolvedContainer, container, StringComparison.Ordinal) ||
            !string.Equals(resolvedBlob, blobName, StringComparison.Ordinal))
        {
            throw AzureStorageException.InvalidHeader("x-ms-previous-snapshot-url", value);
        }
        var query = QueryHelpers.ParseQuery(uri.Query);
        var snapshot = query.TryGetValue("snapshot", out var snapshots) ? NullIfEmpty(snapshots.ToString()) : null;
        return snapshot ?? throw AzureStorageException.InvalidHeader("x-ms-previous-snapshot-url", value);
    }

    private static async Task WriteFindByTagsAsync(
        HttpContext http,
        StorageRequestContext request,
        BlobService service,
        AzureResponseWriter writer,
        string? scopedContainer,
        CancellationToken cancellationToken)
    {
        if (!DateOnly.TryParseExact(
                request.ServiceVersion,
                "yyyy-MM-dd",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var serviceVersion) ||
            serviceVersion < new DateOnly(2019, 12, 12))
        {
            throw AzureStorageException.FeatureVersionMismatch(
                "Find Blobs by Tags requires service version 2019-12-12 or later.");
        }

        var expression = http.Request.Query["where"].ToString();
        var filter = BlobTagQuery.ParseFindExpression(expression);
        if (scopedContainer is not null)
        {
            if (filter.Container is not null &&
                !string.Equals(filter.Container, scopedContainer, StringComparison.Ordinal))
            {
                throw AzureStorageException.InvalidQuery("where");
            }
            filter = filter with { Container = scopedContainer };
        }
        var markerValue = http.Request.Query["marker"].ToString();
        var marker = BlobTagQuery.DecodeMarker(request, expression, markerValue);
        if (filter.Container is not null &&
            marker is not null &&
            !string.Equals(filter.Container, marker.Container, StringComparison.Ordinal))
        {
            throw AzureStorageException.InvalidQuery("marker");
        }
        var maxResults = ParseMaxResults(http.Request.Query["maxresults"].ToString(), 5000);
        var page = await service.FindBlobsByTagsPageAsync(
            request.Account,
            filter,
            marker,
            maxResults,
            cancellationToken);
        var nextMarker = page.HasMore && page.Items.Count > 0
            ? BlobTagQuery.EncodeMarker(
                request,
                expression,
                new BlobTagCursor(
                    page.Items[^1].Container,
                    page.Items[^1].Name,
                    page.Items[^1].GenerationId))
            : string.Empty;
        var endpoint = StorageResourcePath.GetServiceEndpoint(http.Request, request.Account);
        await writer.WriteXmlAsync(http, xml =>
        {
            xml.WriteStartElement("EnumerationResults");
            xml.WriteAttributeString("ServiceEndpoint", endpoint);
            xml.WriteElementString("Where", expression);
            xml.WriteStartElement("Blobs");
            foreach (var blob in page.Items)
            {
                xml.WriteStartElement("Blob");
                xml.WriteElementString("Name", blob.Name);
                xml.WriteElementString("ContainerName", blob.Container);
                if (serviceVersion >= new DateOnly(2020, 4, 8))
                {
                    xml.WriteStartElement("Tags");
                    xml.WriteStartElement("TagSet");
                    foreach (var key in filter.Predicates
                                 .Select(predicate => predicate.Key)
                                 .Distinct(StringComparer.Ordinal))
                    {
                        if (!blob.Tags.TryGetValue(key, out var value))
                            continue;
                        xml.WriteStartElement("Tag");
                        xml.WriteElementString("Key", key);
                        xml.WriteElementString("Value", value);
                        xml.WriteEndElement();
                    }
                    xml.WriteEndElement();
                    xml.WriteEndElement();
                }
                xml.WriteEndElement();
            }
            xml.WriteEndElement();
            xml.WriteElementString("NextMarker", nextMarker);
            xml.WriteEndElement();
        }, cancellationToken);
    }

    private static async Task HandleCorsPreflightAsync(
        HttpContext http,
        BlobService service,
        StorageRequestContext request,
        CancellationToken cancellationToken)
    {
        var origin = ProtocolParsing.First(http.Request.Headers, "Origin")
                     ?? throw AzureStorageException.InvalidHeader("Origin");
        var requestedMethod = ProtocolParsing.First(http.Request.Headers, "Access-Control-Request-Method")
                              ?? throw AzureStorageException.InvalidHeader("Access-Control-Request-Method");
        var requestedHeaders = ProtocolParsing.First(http.Request.Headers, "Access-Control-Request-Headers") ?? string.Empty;
        var properties = await service.GetServicePropertiesAsync(request.Account, cancellationToken);
        var rule = properties.Cors.FirstOrDefault(candidate =>
            MatchesCorsOrigin(candidate.AllowedOrigins, origin) &&
            MatchesCsv(candidate.AllowedMethods, requestedMethod) &&
            requestedHeaders.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .All(header => MatchesHeader(candidate.AllowedHeaders, header)));
        if (rule is null)
            throw new AzureStorageException(StatusCodes.Status403Forbidden, "CorsPreflightFailure", "CORS not enabled or no matching rule found for this request.");
        http.Response.Headers.AccessControlAllowOrigin = AllowsAllCorsOrigins(rule.AllowedOrigins) ? "*" : origin;
        http.Response.Headers.AccessControlAllowMethods = requestedMethod;
        http.Response.Headers.AccessControlAllowHeaders = requestedHeaders;
        http.Response.Headers.AccessControlExposeHeaders = rule.ExposedHeaders;
        http.Response.Headers.AccessControlMaxAge = rule.MaxAgeInSeconds.ToString(CultureInfo.InvariantCulture);
        http.Response.Headers.AccessControlAllowCredentials = "true";
    }

    private static async Task ApplyCorsResponseHeadersAsync(
        HttpContext http,
        BlobService service,
        StorageRequestContext request,
        CancellationToken cancellationToken)
    {
        var origin = ProtocolParsing.First(http.Request.Headers, "Origin");
        var properties = await service.GetServicePropertiesAsync(request.Account, cancellationToken);
        var rule = origin is null
            ? properties.Cors.FirstOrDefault(candidate =>
                AllowsAllCorsOrigins(candidate.AllowedOrigins) &&
                MatchesCsv(candidate.AllowedMethods, http.Request.Method))
            : properties.Cors.FirstOrDefault(candidate =>
                MatchesCorsOrigin(candidate.AllowedOrigins, origin) &&
                MatchesCsv(candidate.AllowedMethods, http.Request.Method));
        if (rule is null)
        {
            if (properties.Cors.Count > 0 &&
                (HttpMethods.IsGet(http.Request.Method) || HttpMethods.IsHead(http.Request.Method)))
            {
                http.Response.Headers.Append("Vary", "Origin");
            }
            return;
        }

        var allowsAllOrigins = AllowsAllCorsOrigins(rule.AllowedOrigins);
        http.Response.Headers.AccessControlAllowOrigin = allowsAllOrigins ? "*" : origin;
        http.Response.Headers.AccessControlExposeHeaders = rule.ExposedHeaders;
        if (!allowsAllOrigins &&
            (HttpMethods.IsGet(http.Request.Method) || HttpMethods.IsHead(http.Request.Method)))
        {
            http.Response.Headers.Append("Vary", "Origin");
        }
    }

    private static void ApplySasResponseOverrides(HttpContext http)
    {
        if (StorageRequestContext.Get(http).Authorization.Kind != StorageAuthorizationKind.Sas)
            return;

        ApplyOverride("rscc", "Cache-Control");
        ApplyOverride("rscd", "Content-Disposition");
        ApplyOverride("rsce", "Content-Encoding");
        ApplyOverride("rscl", "Content-Language");
        ApplyOverride("rsct", "Content-Type");
        return;

        void ApplyOverride(string queryName, string headerName)
        {
            if (!http.Request.Query.TryGetValue(queryName, out var values) || values.Count == 0)
                return;
            var value = values[0] ?? string.Empty;
            if (value.Contains('\r', StringComparison.Ordinal) || value.Contains('\n', StringComparison.Ordinal))
                throw AzureStorageException.InvalidQuery(queryName);
            http.Response.Headers[headerName] = value;
        }
    }

    private static async Task AuthorizeBlobReadAsync(
        StorageRequestContext request,
        BlobService service,
        BlobRecord blob,
        CancellationToken cancellationToken)
    {
        if (request.Authorization.Kind != StorageAuthorizationKind.Anonymous)
        {
            Require(request, 'r');
            return;
        }
        var container = await service.GetContainerAsync(blob.Account, blob.Container, false, cancellationToken);
        if (!service.AllowsAnonymousPublicAccess || container.PublicAccess is not ("blob" or "container"))
            throw AzureStorageException.AuthorizationFailure();
    }

    private static Task AuthorizeContainerReadAsync(
        StorageRequestContext request,
        BlobService service,
        ContainerRecord container,
        bool allowContainerPublic)
    {
        if (request.Authorization.Kind != StorageAuthorizationKind.Anonymous)
            Require(request, 'r');
        else if (!service.AllowsAnonymousPublicAccess || !allowContainerPublic || container.PublicAccess != "container")
            throw AzureStorageException.AuthorizationFailure();
        return Task.CompletedTask;
    }

    private static Task AuthorizeContainerListAsync(
        StorageRequestContext request,
        BlobService service,
        ContainerRecord container)
    {
        if (request.Authorization.Kind != StorageAuthorizationKind.Anonymous)
            Require(request, 'l');
        else if (!service.AllowsAnonymousPublicAccess || container.PublicAccess != "container")
            throw AzureStorageException.AuthorizationFailure();
        return Task.CompletedTask;
    }

    private static void Require(StorageRequestContext request, char permission)
    {
        if (!request.Authorization.Allows(permission))
            throw request.Authorization.Kind == StorageAuthorizationKind.Anonymous
                ? AzureStorageException.AuthenticationFailed()
                : AzureStorageException.AuthorizationPermissionMismatch();
    }

    private static void RequireAny(StorageRequestContext request, params char[] permissions)
    {
        if (!permissions.Any(request.Authorization.Allows))
            throw request.Authorization.Kind == StorageAuthorizationKind.Anonymous
                ? AzureStorageException.AuthenticationFailed()
                : AzureStorageException.AuthorizationPermissionMismatch();
    }

    private static void RequireBlockWrite(StorageRequestContext request, bool createsBlob)
    {
        if (request.Authorization.Kind == StorageAuthorizationKind.Sas &&
            createsBlob &&
            IsServiceVersionAtLeast(request, new DateOnly(2026, 4, 6)))
        {
            RequireAny(request, 'c', 'w');
            return;
        }

        Require(request, 'w');
    }

    private static void EvaluateReadConditions(HttpRequest request, BlobRecord blob)
    {
        BlobConditionEvaluator.EvaluateRead(request, blob.ETag, blob.LastModified);
        EvaluateTagCondition(request, blob, "x-ms-if-tags", source: false);
    }

    private static void EvaluateWriteConditions(HttpRequest request, BlobRecord? blob)
    {
        BlobConditionEvaluator.EvaluateWrite(request, blob?.ETag, blob?.LastModified);
        EvaluateTagCondition(request, blob, "x-ms-if-tags", source: false);
    }

    private static void EvaluateBlobTagConditions(
        HttpRequest request,
        StorageRequestContext context,
        BlobRecord blob,
        bool write)
    {
        const string ifMatchName = "x-ms-blob-if-match";
        const string ifNoneMatchName = "x-ms-blob-if-none-match";
        const string ifModifiedName = "x-ms-blob-if-modified-since";
        const string ifUnmodifiedName = "x-ms-blob-if-unmodified-since";
        var ifMatch = ProtocolParsing.First(request.Headers, ifMatchName);
        var ifNoneMatch = ProtocolParsing.First(request.Headers, ifNoneMatchName);
        var hasConditions = ifMatch is not null ||
                            ifNoneMatch is not null ||
                            request.Headers.ContainsKey(ifModifiedName) ||
                            request.Headers.ContainsKey(ifUnmodifiedName);
        if (!hasConditions)
            return;
        if (!IsServiceVersionAtLeast(context, new DateOnly(2025, 11, 5)))
        {
            throw AzureStorageException.FeatureVersionMismatch(
                "Blob tag ETag and date conditions require service version 2025-11-05 or later.");
        }

        if (write)
            BlobConditionEvaluator.EvaluateBlobTagWrite(request, blob.ETag, blob.LastModified);
        else
            BlobConditionEvaluator.EvaluateBlobTagRead(request, blob.ETag, blob.LastModified);
    }

    private static void EvaluatePageSequenceConditions(HttpRequest request, BlobRecord blob)
    {
        var lessThan = TryParseLongHeader(request.Headers, "x-ms-if-sequence-number-lt");
        var lessThanOrEqual = TryParseLongHeader(request.Headers, "x-ms-if-sequence-number-le");
        var equal = TryParseLongHeader(request.Headers, "x-ms-if-sequence-number-eq");
        if (lessThan.HasValue && blob.SequenceNumber >= lessThan.Value ||
            lessThanOrEqual.HasValue && blob.SequenceNumber > lessThanOrEqual.Value ||
            equal.HasValue && blob.SequenceNumber != equal.Value)
        {
            throw new AzureStorageException(
                StatusCodes.Status412PreconditionFailed,
                "SequenceNumberConditionNotMet",
                "The sequence number condition specified was not met.");
        }
    }

    private static void EnsureLease(HttpRequest request, LeaseRecord lease, string resource)
    {
        var leases = request.HttpContext.RequestServices.GetRequiredService<LeaseService>();
        leases.EnsureWriteAccess(
            lease,
            ProtocolParsing.First(request.Headers, "x-ms-lease-id"),
            resource);
    }

    private static void EnsureAsynchronousCopyDestinationLease(
        HttpRequest request,
        BlobRecord? destination)
    {
        if (destination is null)
            return;
        var leases = request.HttpContext.RequestServices.GetRequiredService<LeaseService>();
        var effective = leases.GetEffective(destination.Lease);
        if ((effective.State is LeaseState.Leased or LeaseState.Breaking) &&
            effective.DurationSeconds != -1)
        {
            throw AzureStorageException.InfiniteLeaseDurationRequired();
        }
    }

    private static void EnsureNoPendingCopyDestination(BlobRecord? destination)
    {
        if (destination?.Copy?.Status == "pending")
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "PendingCopyOperation",
                "There is currently a pending copy operation.");
        }
    }

    private static void ValidateOptionalLease(
        HttpRequest request,
        LeaseRecord lease,
        string resource,
        string headerName = "x-ms-lease-id")
    {
        var leases = request.HttpContext.RequestServices.GetRequiredService<LeaseService>();
        leases.ValidateOptionalAccess(
            lease,
            ProtocolParsing.First(request.Headers, headerName),
            resource,
            headerName);
    }

    private static async Task<BlobRecord?> TryGetCurrentBlobAsync(
        BlobService service,
        string account,
        string container,
        string name,
        CancellationToken cancellationToken)
    {
        try
        {
            return await service.GetBlobAsync(account, container, name, null, null, false, cancellationToken);
        }
        catch (AzureStorageException exception) when (exception.ErrorCode == "BlobNotFound")
        {
            return null;
        }
    }

    private static BlobWriteOptions ReadWriteOptions(
        HttpRequest request,
        BlobRecord? fallback,
        bool useStandardProperties = true,
        bool generateContentMd5 = false)
    {
        var (until, locked, legalHold) = ReadImmutabilityHeaders(request);
        ValidateRehydratePriorityVersion(request);
        var encryption = ReadRequestEncryption(request, write: true);
        return new BlobWriteOptions(
            ProtocolParsing.ReadHttpProperties(request.Headers, useStandardProperties: useStandardProperties),
            ProtocolParsing.ReadMetadata(request.Headers),
            ReadTagsHeader(request),
            ReadAccessTier(request, fallback?.AccessTier),
            until,
            locked,
            legalHold,
            encryption.Scope,
            encryption.CustomerProvidedKeySha256,
            encryption.CustomerProvidedKey,
            ProtocolParsing.First(request.Headers, "x-ms-access-tier") is null
                ? fallback?.AccessTierInferred
                : false,
            generateContentMd5);
    }

    private static string? ReadAccessTier(HttpRequest request, string? fallback)
    {
        var tier = ProtocolParsing.First(request.Headers, "x-ms-access-tier");
        if (tier is not null)
            ValidateAccessTierVersion(request, tier);
        return tier ?? fallback;
    }

    private static void ValidateAccessTierVersion(HttpRequest request, string tier)
    {
        if (tier is not ("Hot" or "Cool" or "Cold" or "Smart" or "Archive"))
            throw AzureStorageException.InvalidHeader("x-ms-access-tier", tier);
        var context = StorageRequestContext.Get(request.HttpContext);
        var minimum = tier switch
        {
            "Cold" => new DateOnly(2021, 12, 2),
            "Smart" => new DateOnly(2026, 2, 6),
            _ => new DateOnly(2018, 11, 9)
        };
        if (!IsServiceVersionAtLeast(context, minimum))
        {
            throw AzureStorageException.FeatureVersionMismatch(
                $"The {tier} access tier requires service version {minimum:yyyy-MM-dd} or later.");
        }
    }

    private static void ValidateRehydratePriorityVersion(HttpRequest request)
    {
        if (!request.Headers.ContainsKey("x-ms-rehydrate-priority"))
            return;
        RequireFeatureVersion(
            StorageRequestContext.Get(request.HttpContext),
            new DateOnly(2019, 2, 2),
            "Rehydrate priority");
    }

    private static Dictionary<string, string> ReadTagsHeader(HttpRequest request)
    {
        if (request.Headers.ContainsKey("x-ms-tags"))
        {
            RequireFeatureVersion(
                StorageRequestContext.Get(request.HttpContext),
                new DateOnly(2019, 12, 12),
                "Blob index tags");
            Require(StorageRequestContext.Get(request.HttpContext), 't');
        }
        return ProtocolParsing.ReadTagsHeader(request.Headers);
    }

    private static bool IsServiceVersionAtLeast(StorageRequestContext request, DateOnly minimum) =>
        DateOnly.TryParseExact(
            request.ServiceVersion,
            "yyyy-MM-dd",
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out var version) && version >= minimum;

    private static bool IsArrowListRequest(HttpRequest request, StorageRequestContext context)
    {
        const string arrowContentType = "application/vnd.apache.arrow.stream";
        var accept = request.Headers.Accept.ToString();
        if (string.IsNullOrWhiteSpace(accept))
            return false;

        var mediaTypes = accept.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Select(value => value.Split(';', 2)[0].Trim())
            .ToArray();
        if (mediaTypes.Contains(arrowContentType, StringComparer.OrdinalIgnoreCase))
        {
            RequireFeatureVersion(context, new DateOnly(2026, 6, 6), "Apache Arrow blob listings");
            return true;
        }
        if (mediaTypes.All(value =>
                value.Equals("application/xml", StringComparison.OrdinalIgnoreCase) ||
                value.Equals("*/*", StringComparison.Ordinal)))
        {
            return false;
        }

        throw AzureStorageException.InvalidHeader("Accept", accept);
    }

    private static void RequireFeatureVersion(
        StorageRequestContext request,
        DateOnly minimum,
        string feature)
    {
        if (IsServiceVersionAtLeast(request, minimum))
            return;
        throw AzureStorageException.FeatureVersionMismatch(
            $"{feature} requires service version {minimum:yyyy-MM-dd} or later.");
    }

    private static void ValidatePermanentDeleteRequest(StorageRequestContext request, string deleteType)
    {
        if (!string.Equals(deleteType, "permanent", StringComparison.Ordinal))
            throw AzureStorageException.InvalidQuery("deletetype");
        RequireFeatureVersion(request, new DateOnly(2020, 2, 10), "Permanent Delete Blob");
    }

    internal static void ValidateBlobVersionRequest(StorageRequestContext request)
    {
        if (request.Snapshot is not null)
            RequireFeatureVersion(request, new DateOnly(2009, 9, 19), "Blob snapshots");
        if (request.VersionId is not null)
            RequireFeatureVersion(request, new DateOnly(2019, 12, 12), "Blob versioning");
    }

    private static BlobDeleteSnapshotsOption ReadDeleteSnapshotsOption(
        HttpRequest request,
        bool hasExplicitSnapshotOrVersion)
    {
        const string headerName = "x-ms-delete-snapshots";
        var value = ProtocolParsing.First(request.Headers, headerName);
        if (value is null)
            return BlobDeleteSnapshotsOption.Unspecified;
        if (hasExplicitSnapshotOrVersion)
            throw AzureStorageException.InvalidHeader(headerName, value);
        return value switch
        {
            "include" => BlobDeleteSnapshotsOption.Include,
            "only" => BlobDeleteSnapshotsOption.Only,
            _ => throw AzureStorageException.InvalidHeader(headerName, value)
        };
    }

    private static void ValidateBlobListFeatures(
        StorageRequestContext request,
        IReadOnlySet<string> includes,
        string delimiter)
    {
        var supported = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "snapshots",
            "metadata",
            "uncommittedblobs",
            "copy",
            "deleted",
            "tags",
            "versions",
            "deletedwithversions",
            "immutabilitypolicy",
            "legalhold",
            "permissions"
        };
        if (includes.Any(include => !supported.Contains(include)))
            throw AzureStorageException.InvalidQuery("include");
        if (!IsServiceVersionAtLeast(request, new DateOnly(2009, 9, 19)) &&
            includes.Overlaps(["snapshots", "metadata", "uncommittedblobs"]))
        {
            throw AzureStorageException.FeatureVersionMismatch(
                "The requested listing details require service version 2009-09-19 or later.");
        }
        if (includes.Contains("copy"))
            RequireFeatureVersion(request, new DateOnly(2012, 2, 12), "Listing copy properties");
        if (includes.Contains("deleted"))
            RequireFeatureVersion(request, new DateOnly(2017, 7, 29), "Listing deleted blobs");
        if (includes.Contains("tags"))
            RequireFeatureVersion(request, new DateOnly(2019, 12, 12), "Listing blob index tags");
        if (includes.Contains("versions"))
            RequireFeatureVersion(request, new DateOnly(2019, 12, 12), "Listing blob versions");
        if (includes.Contains("immutabilitypolicy"))
            RequireFeatureVersion(request, new DateOnly(2020, 6, 12), "Listing blob immutability policies");
        if (includes.Contains("legalhold"))
            RequireFeatureVersion(request, new DateOnly(2020, 6, 12), "Listing blob legal holds");
        if (includes.Contains("deletedwithversions"))
            RequireFeatureVersion(request, new DateOnly(2020, 10, 2), "Listing deleted blobs with versions");
        if (includes.Contains("permissions"))
        {
            RequireFeatureVersion(request, new DateOnly(2020, 6, 12), "Listing hierarchical namespace permissions");
            throw AzureStorageException.InvalidQuery("include");
        }
        if (!string.IsNullOrEmpty(delimiter) &&
            includes.Contains("snapshots") &&
            !IsServiceVersionAtLeast(request, new DateOnly(2021, 6, 8)))
        {
            throw AzureStorageException.InvalidQuery("include");
        }
    }

    private static void ValidateContainerListFeatures(
        StorageRequestContext request,
        IReadOnlySet<string> includes)
    {
        var supported = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "metadata",
            "deleted",
            "system"
        };
        if (includes.Any(include => !supported.Contains(include)))
            throw AzureStorageException.InvalidQuery("include");
        if (includes.Contains("metadata"))
            RequireFeatureVersion(request, new DateOnly(2009, 9, 19), "Listing container metadata");
        if (includes.Contains("deleted"))
            RequireFeatureVersion(request, new DateOnly(2019, 12, 12), "Listing deleted containers");
        if (includes.Contains("system"))
            RequireFeatureVersion(request, new DateOnly(2020, 10, 2), "Listing system containers");
    }

    private static void ValidateListedBlobTypes(
        StorageRequestContext request,
        BlobListPage page)
    {
        if ((page.Items.Any(item => item.Blob?.Kind == BlobKind.PageBlob) &&
             !IsServiceVersionAtLeast(request, new DateOnly(2009, 9, 19))) ||
            (page.Items.Any(item => item.Blob?.Kind == BlobKind.AppendBlob) &&
             !IsServiceVersionAtLeast(request, new DateOnly(2015, 2, 21))))
        {
            throw AzureStorageException.FeatureVersionMismatch(
                "The type of a blob in the container is unrecognized by this version.");
        }
    }

    private static void ValidateBlobTypeVersion(
        StorageRequestContext request,
        BlobKind? kind)
    {
        if (kind == BlobKind.PageBlob)
            RequirePageBlobVersion(request);
        if (kind == BlobKind.AppendBlob)
            RequireAppendBlobVersion(request);
    }

    private static void RequirePageBlobVersion(StorageRequestContext request)
    {
        if (!IsServiceVersionAtLeast(request, new DateOnly(2009, 9, 19)))
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "InvalidVersionForPageBlobOperation",
                "All operations on page blobs require at least version 2009-09-19.");
        }
    }

    private static void RequireAppendBlobVersion(StorageRequestContext request)
    {
        if (!IsServiceVersionAtLeast(request, new DateOnly(2015, 2, 21)))
        {
            throw AzureStorageException.FeatureVersionMismatch(
                "The operation for AppendBlob requires at least version 2015-02-21.");
        }
    }

    private static long GetMaximumPutBlobBytes(StorageRequestContext request) =>
        IsServiceVersionAtLeast(request, new DateOnly(2019, 12, 12))
            ? 5_000L * 1024 * 1024
            : IsServiceVersionAtLeast(request, new DateOnly(2016, 5, 31))
                ? 256L * 1024 * 1024
                : 64L * 1024 * 1024;

    private static long GetMaximumPutBlockBytes(StorageRequestContext request) =>
        IsServiceVersionAtLeast(request, new DateOnly(2019, 12, 12))
            ? 4_000L * 1024 * 1024
            : IsServiceVersionAtLeast(request, new DateOnly(2016, 5, 31))
                ? 100L * 1024 * 1024
                : 4L * 1024 * 1024;

    private static long GetMaximumPutBlockFromUrlBytes(StorageRequestContext request) =>
        IsServiceVersionAtLeast(request, new DateOnly(2020, 4, 8))
            ? 4_000L * 1024 * 1024
            : 100L * 1024 * 1024;

    private static long GetMaximumAppendBlockBytes(StorageRequestContext request) =>
        IsServiceVersionAtLeast(request, new DateOnly(2022, 11, 2))
            ? 100L * 1024 * 1024
            : 4L * 1024 * 1024;

    private static void RequireZeroContentLength(HttpRequest request)
    {
        if (request.ContentLength is > 0)
        {
            throw AzureStorageException.InvalidHeader(
                "Content-Length",
                request.ContentLength?.ToString(CultureInfo.InvariantCulture));
        }
        if (ProtocolParsing.First(request.Headers, "x-ms-structured-body") is { } structuredBody)
            throw AzureStorageException.InvalidHeader("x-ms-structured-body", structuredBody);
        if (ProtocolParsing.First(request.Headers, "x-ms-structured-content-length") is { } structuredLength)
            throw AzureStorageException.InvalidHeader("x-ms-structured-content-length", structuredLength);
    }

    private static BlobWriteOptions ReadUrlWriteOptions(
        HttpRequest request,
        UrlSource source,
        bool copySourceTags,
        BlobRecord? destination)
    {
        var (until, locked, legalHold) = ReadImmutabilityHeaders(request);
        ValidateRehydratePriorityVersion(request);
        var encryption = ReadRequestEncryption(request, write: true);
        var copySourceProperties = ReadCopySourceBlobProperties(request);
        var hasReplacementMetadata = request.Headers.Keys.Any(name =>
            name.StartsWith("x-ms-meta-", StringComparison.OrdinalIgnoreCase));
        return new BlobWriteOptions(
            ProtocolParsing.ReadHttpProperties(
                request.Headers,
                copySourceProperties ? source.Http : new BlobHttpProperties()),
            hasReplacementMetadata
                ? ProtocolParsing.ReadMetadata(request.Headers)
                : new Dictionary<string, string>(source.Metadata, StringComparer.OrdinalIgnoreCase),
            copySourceTags
                ? new Dictionary<string, string>(source.Tags, StringComparer.Ordinal)
                : ReadTagsHeader(request),
            ReadAccessTier(request, destination?.AccessTier),
            until,
            locked,
            legalHold,
            encryption.Scope,
            encryption.CustomerProvidedKeySha256,
            encryption.CustomerProvidedKey,
            ProtocolParsing.First(request.Headers, "x-ms-access-tier") is null
                ? destination?.AccessTierInferred
                : false,
            GenerateContentMd5: true);
    }

    private static BlobWriteOptions ReadUrlCopyWriteOptions(
        HttpRequest request,
        UrlSource source,
        bool copySourceTags,
        BlobRecord? destination,
        bool synchronous)
    {
        var (until, locked, legalHold) = ReadImmutabilityHeaders(request);
        ValidateRehydratePriorityVersion(request);
        var encryption = synchronous
            ? ReadRequestEncryption(request, write: true)
            : ReadSignedCopyEncryption(request);
        var hasReplacementMetadata = request.Headers.Keys.Any(name =>
            name.StartsWith("x-ms-meta-", StringComparison.OrdinalIgnoreCase));
        return new BlobWriteOptions(
            source.Http,
            hasReplacementMetadata
                ? ProtocolParsing.ReadMetadata(request.Headers)
                : new Dictionary<string, string>(source.Metadata, StringComparer.OrdinalIgnoreCase),
            copySourceTags
                ? new Dictionary<string, string>(source.Tags, StringComparer.Ordinal)
                : ReadTagsHeader(request),
            ReadAccessTier(request, destination?.AccessTier),
            until,
            locked,
            legalHold,
            encryption.Scope,
            encryption.CustomerProvidedKeySha256,
            encryption.CustomerProvidedKey,
            ProtocolParsing.First(request.Headers, "x-ms-access-tier") is null
                ? destination?.AccessTierInferred
                : false);
    }

    private static bool ReadCopySourceBlobProperties(HttpRequest request)
    {
        const string headerName = "x-ms-copy-source-blob-properties";
        var value = ProtocolParsing.First(request.Headers, headerName);
        if (value is null)
            return true;
        if (bool.TryParse(value, out var parsed))
            return parsed;
        throw AzureStorageException.InvalidHeader(headerName, value);
    }

    private static BlobWriteOptions ReadCopyWriteOptions(
        HttpRequest request,
        BlobRecord source,
        BlobRecord? destination,
        bool copySourceTags = false,
        bool synchronous = false)
    {
        var (until, locked, legalHold) = ReadImmutabilityHeaders(request);
        ValidateRehydratePriorityVersion(request);
        var encryption = synchronous
            ? ReadRequestEncryption(request, write: true)
            : ReadSignedCopyEncryption(request);
        var hasReplacementMetadata = request.Headers.Keys.Any(name =>
            name.StartsWith("x-ms-meta-", StringComparison.OrdinalIgnoreCase));
        return new BlobWriteOptions(
            source.Http,
            hasReplacementMetadata
                ? ProtocolParsing.ReadMetadata(request.Headers)
                : new Dictionary<string, string>(source.Metadata, StringComparer.OrdinalIgnoreCase),
            copySourceTags
                ? new Dictionary<string, string>(source.Tags, StringComparer.Ordinal)
                : ReadTagsHeader(request),
            ReadAccessTier(request, destination?.AccessTier),
            until,
            locked,
            legalHold,
            encryption.Scope,
            encryption.CustomerProvidedKeySha256,
            encryption.CustomerProvidedKey,
            ProtocolParsing.First(request.Headers, "x-ms-access-tier") is null
                ? destination?.AccessTierInferred
                : false);
    }

    private static void ValidateSynchronousCopyEncryption(HttpRequest request)
    {
        if (request.Headers.ContainsKey("x-ms-seal-blob"))
        {
            throw AzureStorageException.InvalidHeader(
                "x-ms-seal-blob",
                ProtocolParsing.First(request.Headers, "x-ms-seal-blob"));
        }
        foreach (var headerName in new[]
                 {
                     "x-ms-encryption-key",
                     "x-ms-encryption-key-sha256",
                     "x-ms-encryption-algorithm"
                 })
        {
            if (request.Headers.ContainsKey(headerName))
                throw AzureStorageException.InvalidHeader(headerName, ProtocolParsing.First(request.Headers, headerName));
        }
        if (ProtocolParsing.First(request.Headers, "x-ms-encryption-scope") is not null ||
            request.Query.ContainsKey("sig") && !string.IsNullOrEmpty(request.Query["ses"]))
        {
            RequireFeatureVersion(
                StorageRequestContext.Get(request.HttpContext),
                new DateOnly(2020, 12, 6),
                "Copy Blob From URL encryption scope");
        }
        if (request.Headers.ContainsKey("x-ms-rehydrate-priority"))
        {
            throw AzureStorageException.InvalidHeader(
                "x-ms-rehydrate-priority",
                ProtocolParsing.First(request.Headers, "x-ms-rehydrate-priority"));
        }
    }

    private static void ValidateAsynchronousCopyEncryption(HttpRequest request)
    {
        foreach (var headerName in new[]
                 {
                     "x-ms-encryption-scope",
                     "x-ms-encryption-key",
                     "x-ms-encryption-key-sha256",
                     "x-ms-encryption-algorithm"
                 })
        {
            if (request.Headers.ContainsKey(headerName))
                throw AzureStorageException.InvalidHeader(headerName, ProtocolParsing.First(request.Headers, headerName));
        }
    }

    private static void ValidateIncrementalCopyHeaders(HttpRequest request)
    {
        foreach (var headerName in new[]
                 {
                     "x-ms-access-tier",
                     "x-ms-copy-source-authorization",
                     "x-ms-copy-source-blob-properties",
                     "x-ms-copy-source-tag-option",
                     "x-ms-immutability-policy-mode",
                     "x-ms-immutability-policy-until-date",
                     "x-ms-legal-hold",
                     "x-ms-rehydrate-priority",
                     "x-ms-seal-blob",
                     "x-ms-source-if-match",
                     "x-ms-source-if-modified-since",
                     "x-ms-source-if-none-match",
                     "x-ms-source-if-tags",
                     "x-ms-source-if-unmodified-since",
                     "x-ms-source-lease-id",
                     "x-ms-tags"
                 })
        {
            RejectUnsupportedHeader(request, headerName);
        }

        var metadataHeader = request.Headers.Keys.FirstOrDefault(name =>
            name.StartsWith("x-ms-meta-", StringComparison.OrdinalIgnoreCase));
        if (metadataHeader is not null)
            RejectUnsupportedHeader(request, metadataHeader);
    }

    private static void RejectUnsupportedHeader(HttpRequest request, string headerName)
    {
        if (request.Headers.ContainsKey(headerName))
        {
            throw AzureStorageException.UnsupportedHeader(
                headerName,
                ProtocolParsing.First(request.Headers, headerName));
        }
    }

    private static bool HasSourceTagCondition(HttpRequest request) =>
        request.Headers.ContainsKey("x-ms-source-if-tags");

    private static bool ReadCopySealDestination(
        HttpRequest request,
        BlobKind sourceKind,
        bool sourceIsSealed)
    {
        const string headerName = "x-ms-seal-blob";
        var value = ProtocolParsing.First(request.Headers, headerName);
        if (value is null)
            return sourceKind == BlobKind.AppendBlob && sourceIsSealed;

        RequireFeatureVersion(
            StorageRequestContext.Get(request.HttpContext),
            new DateOnly(2019, 12, 12),
            "Copy Blob append sealing");
        if (sourceKind != BlobKind.AppendBlob || !bool.TryParse(value, out var shouldSeal))
            throw AzureStorageException.InvalidHeader(headerName, value);
        return shouldSeal;
    }

    private static BlobEncryption ReadSignedCopyEncryption(HttpRequest request) =>
        request.Query.ContainsKey("sig") && !string.IsNullOrEmpty(request.Query["ses"])
            ? ReadRequestEncryption(request, write: true)
            : new BlobEncryption(null, null);

    private static void EnsureDestinationCanBeOverwritten(BlobRecord? destination)
    {
        if (string.Equals(destination?.AccessTier, "Archive", StringComparison.Ordinal))
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "BlobArchived",
                "This operation is not permitted on an archived blob.");
        }
    }

    private static void ValidateCopySourceTier(
        HttpRequest request,
        BlobRecord source,
        bool allowArchivedSource)
    {
        if (!string.Equals(source.AccessTier, "Archive", StringComparison.Ordinal))
            return;
        var destinationTier = ProtocolParsing.First(request.Headers, "x-ms-access-tier");
        if (allowArchivedSource && destinationTier is "Hot" or "Cool" or "Cold" or "Smart")
            return;
        throw new AzureStorageException(
            StatusCodes.Status409Conflict,
            "BlobArchived",
            "An archived copy source requires an explicit online destination access tier.");
    }

    private static void ValidateCopyDestinationType(BlobRecord? destination, BlobKind sourceKind)
    {
        if (destination is null || destination.Kind == sourceKind)
            return;
        throw new AzureStorageException(
            StatusCodes.Status409Conflict,
            "InvalidBlobType",
            "The destination blob type does not match the copy source blob type.");
    }

    private static bool ReadCopySourceTags(HttpRequest request)
    {
        const string headerName = "x-ms-copy-source-tag-option";
        var value = ProtocolParsing.First(request.Headers, headerName);
        if (value is null)
            return false;
        RequireFeatureVersion(
            StorageRequestContext.Get(request.HttpContext),
            new DateOnly(2021, 4, 10),
            "Copy source tags");
        if (value == "REPLACE")
            return false;
        if (value != "COPY")
            throw AzureStorageException.InvalidHeader(headerName, value);
        if (request.Headers.ContainsKey("x-ms-tags"))
        {
            throw new AzureStorageException(
                StatusCodes.Status409Conflict,
                "InvalidHeaderValue",
                "x-ms-tags cannot be specified when x-ms-copy-source-tag-option is COPY.",
                headerName,
                value);
        }
        Require(StorageRequestContext.Get(request.HttpContext), 't');
        return true;
    }

    private static BlobEncryption ReadRequestEncryption(HttpRequest request, bool write)
    {
        var scope = ProtocolParsing.First(request.Headers, "x-ms-encryption-scope");
        var signedScope = request.Query.ContainsKey("sig")
            ? NullIfEmpty(request.Query["ses"].ToString())
            : null;
        if (scope is not null && signedScope is not null && !string.Equals(scope, signedScope, StringComparison.Ordinal))
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "InvalidHeaderValue",
                "The request encryption scope does not match the scope signed by the shared access signature.",
                "x-ms-encryption-scope",
                scope);
        }
        scope ??= signedScope;
        if (scope is { Length: > 256 } ||
            scope is not null && (string.IsNullOrWhiteSpace(scope) || scope.Any(char.IsControl)))
            throw AzureStorageException.InvalidHeader("x-ms-encryption-scope", scope);

        var encodedKey = ProtocolParsing.First(request.Headers, "x-ms-encryption-key");
        var encodedHash = ProtocolParsing.First(request.Headers, "x-ms-encryption-key-sha256");
        var algorithm = ProtocolParsing.First(request.Headers, "x-ms-encryption-algorithm");
        if (scope is not null || encodedKey is not null || encodedHash is not null || algorithm is not null)
        {
            RequireFeatureVersion(
                StorageRequestContext.Get(request.HttpContext),
                new DateOnly(2019, 2, 2),
                "Customer-provided encryption");
        }
        var hasCustomerKeyHeader = encodedKey is not null || encodedHash is not null || algorithm is not null;
        if ((scope is not null || hasCustomerKeyHeader) &&
            (!DateOnly.TryParseExact(
                 StorageRequestContext.Get(request.HttpContext).ServiceVersion,
                 "yyyy-MM-dd",
                 CultureInfo.InvariantCulture,
                 DateTimeStyles.None,
                 out var serviceVersion) || serviceVersion < new DateOnly(2019, 2, 2)))
        {
            throw AzureStorageException.FeatureVersionMismatch(
                "Customer-provided keys and encryption scopes require service version 2019-02-02 or later.");
        }
        if (!hasCustomerKeyHeader)
            return new BlobEncryption(scope, null);
        if (scope is not null)
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "InvalidHeaderValue",
                "A customer-provided key and an encryption scope cannot be specified on the same request.",
                "x-ms-encryption-scope",
                scope);
        }
        if (!request.IsHttps)
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                "Customer-provided encryption keys require HTTPS.");
        }
        if (encodedKey is null)
            throw AzureStorageException.InvalidHeader("x-ms-encryption-key");
        if (write && encodedHash is null)
            throw AzureStorageException.InvalidHeader("x-ms-encryption-key-sha256");
        if (!string.Equals(algorithm, "AES256", StringComparison.Ordinal))
            throw AzureStorageException.InvalidHeader("x-ms-encryption-algorithm", algorithm);

        byte[] key;
        try
        {
            key = Convert.FromBase64String(encodedKey);
        }
        catch (FormatException)
        {
            throw AzureStorageException.InvalidHeader("x-ms-encryption-key");
        }
        try
        {
            if (key.Length != 32)
                throw AzureStorageException.InvalidHeader("x-ms-encryption-key");
            var actualHash = SHA256.HashData(key);
            if (encodedHash is not null)
            {
                byte[] suppliedHash;
                try
                {
                    suppliedHash = Convert.FromBase64String(encodedHash);
                }
                catch (FormatException)
                {
                    throw AzureStorageException.InvalidHeader("x-ms-encryption-key-sha256", encodedHash);
                }
                if (suppliedHash.Length != actualHash.Length ||
                    !CryptographicOperations.FixedTimeEquals(suppliedHash, actualHash))
                {
                    throw AzureStorageException.InvalidHeader("x-ms-encryption-key-sha256", encodedHash);
                }
            }
            request.HttpContext.Response.RegisterForDispose(new SensitiveBufferLease(key));
            return new BlobEncryption(null, Convert.ToBase64String(actualHash), key);
        }
        catch
        {
            CryptographicOperations.ZeroMemory(key);
            throw;
        }
    }

    private static BlobEncryption EnsureCustomerProvidedKey(HttpRequest request, BlobRecord blob, bool write)
    {
        var supplied = ReadRequestEncryption(request, write);
        if (string.Equals(
                supplied.CustomerProvidedKeySha256,
                blob.CustomerProvidedKeySha256,
                StringComparison.Ordinal))
        {
            return supplied;
        }
        if (supplied.CustomerProvidedKey is not null)
            CryptographicOperations.ZeroMemory(supplied.CustomerProvidedKey);
        throw new AzureStorageException(
            StatusCodes.Status409Conflict,
            "CustomerProvidedKeyInUse",
            "The blob is encrypted with a customer-provided key that does not match this request.");
    }

    private static BlobEncryption EncryptionOf(BlobRecord blob) =>
        new(blob.EncryptionScope, blob.CustomerProvidedKeySha256);

    private static void AddEncryptionResponseHeaders(HttpResponse response, BlobEncryption encryption)
    {
        if (!IsServiceVersionAtLeast(
                StorageRequestContext.Get(response.HttpContext),
                new DateOnly(2019, 2, 2)))
        {
            return;
        }
        if (encryption.CustomerProvidedKeySha256 is not null)
            response.Headers["x-ms-encryption-key-sha256"] = encryption.CustomerProvidedKeySha256;
        if (encryption.Scope is not null)
            response.Headers["x-ms-encryption-scope"] = encryption.Scope;
    }

    private static void AddRequestServerEncryptedHeader(HttpResponse response)
    {
        if (IsServiceVersionAtLeast(
                StorageRequestContext.Get(response.HttpContext),
                new DateOnly(2015, 12, 11)))
        {
            response.Headers["x-ms-request-server-encrypted"] = "true";
        }
    }

    private sealed class SensitiveBufferLease(byte[] buffer) : IDisposable
    {
        private byte[]? _buffer = buffer;

        public void Dispose()
        {
            var owned = Interlocked.Exchange(ref _buffer, null);
            if (owned is not null)
                CryptographicOperations.ZeroMemory(owned);
        }
    }

    private static (DateTimeOffset? Until, bool Locked, bool LegalHold) ReadImmutabilityHeaders(HttpRequest request)
    {
        var headers = request.Headers;
        var untilValue = ProtocolParsing.First(headers, "x-ms-immutability-policy-until-date");
        var modeValue = ProtocolParsing.First(headers, "x-ms-immutability-policy-mode");
        var legalHoldValue = ProtocolParsing.First(headers, "x-ms-legal-hold");
        if (untilValue is not null || modeValue is not null || legalHoldValue is not null)
        {
            RequireFeatureVersion(
                StorageRequestContext.Get(request.HttpContext),
                new DateOnly(2020, 6, 12),
                "Blob-level immutability headers");
        }
        if (untilValue is null && modeValue is not null)
            throw AzureStorageException.InvalidHeader("x-ms-immutability-policy-until-date");

        DateTimeOffset? until = null;
        var locked = false;
        if (untilValue is not null)
        {
            if (!DateTimeOffset.TryParse(untilValue, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed))
                throw AzureStorageException.InvalidHeader("x-ms-immutability-policy-until-date", untilValue);
            until = parsed.ToUniversalTime();
            locked = (modeValue ?? "unlocked").ToLowerInvariant() switch
            {
                "locked" => true,
                "unlocked" => false,
                _ => throw AzureStorageException.InvalidHeader("x-ms-immutability-policy-mode", modeValue)
            };
        }

        var legalHold = legalHoldValue is not null &&
                        (bool.TryParse(legalHoldValue, out var parsedLegalHold)
                            ? parsedLegalHold
                            : throw AzureStorageException.InvalidHeader("x-ms-legal-hold", legalHoldValue));
        return (until, locked, legalHold);
    }

    private static void AddImmutabilityHeaders(HttpResponse response, BlobRecord blob)
    {
        if (blob.ImmutabilityUntil.HasValue)
        {
            response.Headers["x-ms-immutability-policy-until-date"] = blob.ImmutabilityUntil.Value.ToString("R", CultureInfo.InvariantCulture);
            response.Headers["x-ms-immutability-policy-mode"] = blob.ImmutabilityLocked ? "locked" : "unlocked";
        }
        response.Headers["x-ms-legal-hold"] = blob.HasLegalHold ? "true" : "false";
    }

    private static void EvaluateCopySourceConditions(HttpRequest request, BlobRecord source)
    {
        BlobConditionEvaluator.EvaluateCopySource(request, source.ETag, source.LastModified);
        var tagCondition = ProtocolParsing.First(request.Headers, "x-ms-source-if-tags");
        if (tagCondition is not null)
            EvaluateTagCondition(request, source, "x-ms-source-if-tags", source: true, requirePermission: false);
    }

    private static void EvaluateTagCondition(
        HttpRequest request,
        BlobRecord? blob,
        string headerName,
        bool source,
        bool requirePermission = true)
    {
        var expression = ProtocolParsing.First(request.Headers, headerName);
        if (expression is null)
            return;

        var context = StorageRequestContext.Get(request.HttpContext);
        if (!DateOnly.TryParseExact(
                context.ServiceVersion,
                "yyyy-MM-dd",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var serviceVersion) || serviceVersion < new DateOnly(2019, 12, 12))
        {
            throw AzureStorageException.FeatureVersionMismatch(
                $"The {headerName} condition requires service version 2019-12-12 or later.",
                headerName,
                expression);
        }

        if (requirePermission && !context.Authorization.Allows('t'))
        {
            throw context.Authorization.Kind == StorageAuthorizationKind.Anonymous
                ? AzureStorageException.AuthenticationFailed()
                : AzureStorageException.AuthorizationPermissionMismatch();
        }
        if (BlobTagCondition.Evaluate(expression, blob?.Tags ?? EmptyBlobTags, headerName))
            return;
        throw source ? SourceConditionNotMet() : AzureStorageException.ConditionNotMet();
    }

    private static AzureStorageException SourceConditionNotMet() =>
        AzureStorageException.SourceConditionNotMet();

    private static string SanitizeCopySource(string sourceValue)
    {
        if (sourceValue.Length > 2048 || !Uri.TryCreate(sourceValue, UriKind.Absolute, out var source))
            throw AzureStorageException.InvalidHeader("x-ms-copy-source");
        var query = Microsoft.AspNetCore.WebUtilities.QueryHelpers.ParseQuery(source.Query);
        var values = query
            .Where(pair => !string.Equals(pair.Key, "sig", StringComparison.OrdinalIgnoreCase))
            .SelectMany(pair => pair.Value.Select(value => new KeyValuePair<string, string?>(pair.Key, value)));
        var builder = new UriBuilder(source)
        {
            Query = QueryString.Create(values).Value?.TrimStart('?') ?? string.Empty
        };
        return builder.Uri.AbsoluteUri;
    }

    private static async Task<TransactionalChecksums> WithIntegrityValidationAsync(
        HttpRequest request,
        Func<Stream, Task> action,
        bool allowStructured = true,
        long maximumBodyBytes = long.MaxValue,
        string expectedMd5HeaderName = "Content-MD5")
    {
        var expectedMd5 = ProtocolParsing.First(request.Headers, expectedMd5HeaderName);
        var expectedCrc64 = ProtocolParsing.First(request.Headers, "x-ms-content-crc64");
        var structuredBody = ProtocolParsing.First(request.Headers, "x-ms-structured-body");
        var structuredContentLength = ProtocolParsing.First(request.Headers, "x-ms-structured-content-length");
        var requestContext = StorageRequestContext.Get(request.HttpContext);
        var options = request.HttpContext.RequestServices.GetRequiredService<IOptions<SavaOptions>>().Value;
        var effectiveMaximumBodyBytes = Math.Min(maximumBodyBytes, options.MaximumRequestBodyBytes);
        var (logicalContentLength, logicalLengthHeader) = GetLogicalRequestContentLength(request);
        if (!logicalContentLength.HasValue)
            throw AzureStorageException.InvalidHeader(logicalLengthHeader);
        if (logicalContentLength.Value > effectiveMaximumBodyBytes)
            throw new RequestBodyTooLargeException(effectiveMaximumBodyBytes);
        if (expectedCrc64 is not null &&
            !IsServiceVersionAtLeast(requestContext, new DateOnly(2019, 2, 2)))
        {
            throw AzureStorageException.FeatureVersionMismatch(
                "Transactional CRC64 checksums require service version 2019-02-02 or later.");
        }
        if ((structuredBody is not null || structuredContentLength is not null) &&
            !IsServiceVersionAtLeast(requestContext, new DateOnly(2025, 1, 5)))
        {
            throw AzureStorageException.FeatureVersionMismatch(
                "Structured request bodies require service version 2025-01-05 or later.");
        }
        if (structuredBody is not null)
        {
            if (!allowStructured)
                throw AzureStorageException.InvalidHeader("x-ms-structured-body", structuredBody);
            if (!string.Equals(structuredBody, StructuredBodyDecoder.ContentType, StringComparison.Ordinal))
                throw AzureStorageException.InvalidHeader("x-ms-structured-body", structuredBody);
            if (expectedMd5 is not null || expectedCrc64 is not null)
            {
                throw new AzureStorageException(
                    StatusCodes.Status400BadRequest,
                    "InvalidHeaderValue",
                    "A structured request body cannot also specify a transactional checksum header.");
            }
            if (!long.TryParse(
                    structuredContentLength,
                    NumberStyles.None,
                    CultureInfo.InvariantCulture,
                    out var decodedLength))
            {
                throw AzureStorageException.InvalidHeader(
                    "x-ms-structured-content-length",
                    structuredContentLength);
            }

            var encodedLength = request.ContentLength
                                ?? throw AzureStorageException.InvalidHeader("Content-Length");
            var structuredPaths = request.HttpContext.RequestServices.GetRequiredService<StoragePaths>();
            var structuredTemporaryPath = Path.Combine(structuredPaths.Staging, $"structured-{Guid.NewGuid():N}.tmp");
            try
            {
                await using var temporary = new FileStream(
                    structuredTemporaryPath,
                    FileMode.CreateNew,
                    FileAccess.ReadWrite,
                    FileShare.None,
                    128 * 1024,
                    FileOptions.Asynchronous | FileOptions.SequentialScan);
                await StructuredBodyDecoder.DecodeAsync(
                    request.Body,
                    temporary,
                    encodedLength,
                    decodedLength,
                    effectiveMaximumBodyBytes,
                    request.HttpContext.RequestAborted);
                temporary.Position = 0;
                using var hashingBody = new TransactionalChecksumReadStream(temporary);
                await action(hashingBody);
                request.HttpContext.Response.Headers["x-ms-structured-body"] = structuredBody;
                return hashingBody.Complete();
            }
            finally
            {
                if (File.Exists(structuredTemporaryPath))
                    File.Delete(structuredTemporaryPath);
            }
        }
        if (structuredContentLength is not null)
        {
            throw AzureStorageException.InvalidHeader(
                "x-ms-structured-content-length",
                structuredContentLength);
        }
        if (expectedMd5 is not null && expectedCrc64 is not null)
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "BothCrc64AndMd5Specified",
                "Both CRC64 and MD5 were specified for the request. Specify only one checksum.");
        }
        if (expectedMd5 is null && expectedCrc64 is null)
        {
            using var hashingBody = new TransactionalChecksumReadStream(request.Body);
            await action(hashingBody);
            return hashingBody.Complete();
        }

        var expected = DecodeChecksum(
            expectedMd5 ?? expectedCrc64!,
            expectedMd5 is null ? 8 : 16,
            expectedMd5 is null ? "x-ms-content-crc64" : expectedMd5HeaderName);
        var paths = request.HttpContext.RequestServices.GetRequiredService<StoragePaths>();
        var temporaryPath = Path.Combine(paths.Staging, $"validated-{Guid.NewGuid():N}.tmp");
        try
        {
            await using var temporary = new FileStream(
                temporaryPath,
                FileMode.CreateNew,
                FileAccess.ReadWrite,
                FileShare.None,
                128 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            using var md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
            var crc64 = new StorageCrc64();
            var buffer = new byte[128 * 1024];
            long length = 0;
            while (true)
            {
                var read = await request.Body.ReadAsync(buffer, request.HttpContext.RequestAborted);
                if (read == 0)
                    break;
                length = checked(length + read);
                if (length > effectiveMaximumBodyBytes)
                    throw new RequestBodyTooLargeException(effectiveMaximumBodyBytes);
                md5.AppendData(buffer, 0, read);
                crc64.Append(buffer.AsSpan(0, read));
                await temporary.WriteAsync(buffer.AsMemory(0, read), request.HttpContext.RequestAborted);
            }

            var checksums = new TransactionalChecksums(md5.GetHashAndReset(), crc64.GetHash());
            var actual = expectedMd5 is null ? checksums.Crc64 : checksums.Md5;
            if (!CryptographicOperations.FixedTimeEquals(expected, actual))
            {
                throw new AzureStorageException(
                    StatusCodes.Status400BadRequest,
                    expectedMd5 is null ? "Crc64Mismatch" : "Md5Mismatch",
                    "The checksum specified in the request did not match the value calculated by the server.");
            }

            temporary.Position = 0;
            await action(temporary);
            return checksums;
        }
        finally
        {
            if (File.Exists(temporaryPath))
                File.Delete(temporaryPath);
        }
    }

    private static (long? Length, string HeaderName) GetLogicalRequestContentLength(HttpRequest request)
    {
        var structuredContentLength = ProtocolParsing.First(request.Headers, "x-ms-structured-content-length");
        if (structuredContentLength is null)
            return (request.ContentLength, "Content-Length");
        if (!long.TryParse(
                structuredContentLength,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var length))
        {
            throw AzureStorageException.InvalidHeader(
                "x-ms-structured-content-length",
                structuredContentLength);
        }
        return (length, "x-ms-structured-content-length");
    }

    private static byte[] DecodeChecksum(string value, int requiredLength, string headerName)
    {
        try
        {
            var bytes = Convert.FromBase64String(value);
            if (bytes.Length == requiredLength)
                return bytes;
        }
        catch (FormatException)
        {
        }
        throw AzureStorageException.InvalidHeader(headerName, value);
    }

    private static void AddTransactionalChecksumHeaders(
        HttpContext http,
        TransactionalChecksums checksums,
        bool sourceChecksum = false)
    {
        var request = StorageRequestContext.Get(http);
        var md5Requested = ProtocolParsing.First(
            http.Request.Headers,
            sourceChecksum ? "x-ms-source-content-md5" : "Content-MD5") is not null;
        if (!IsServiceVersionAtLeast(request, new DateOnly(2019, 2, 2)) || md5Requested)
            http.Response.Headers.ContentMD5 = checksums.Md5Base64;
        if (IsServiceVersionAtLeast(request, new DateOnly(2019, 2, 2)) &&
            (!md5Requested || IsServiceVersionAtLeast(request, new DateOnly(2026, 10, 6))))
        {
            http.Response.Headers["x-ms-content-crc64"] = checksums.Crc64Base64;
        }
    }

    private static void AddFullBlobChecksumHeaders(HttpResponse response, TransactionalChecksums checksums)
    {
        var request = StorageRequestContext.Get(response.HttpContext);
        if (ProtocolParsing.First(response.HttpContext.Request.Headers, "Content-MD5") is not null ||
            IsServiceVersionAtLeast(request, new DateOnly(2012, 2, 12)))
        {
            response.Headers.ContentMD5 = checksums.Md5Base64;
        }
        if (IsServiceVersionAtLeast(request, new DateOnly(2019, 2, 2)))
            response.Headers["x-ms-content-crc64"] = checksums.Crc64Base64;
    }

    private static DateTimeOffset? ParseExpiry(
        IHeaderDictionary headers,
        DateTimeOffset createdAt,
        DateTimeOffset metadataNow)
    {
        var option = ProtocolParsing.First(headers, "x-ms-expiry-option")?.ToLowerInvariant()
                     ?? throw AzureStorageException.InvalidHeader("x-ms-expiry-option");
        var value = ProtocolParsing.First(headers, "x-ms-expiry-time");
        switch (option)
        {
            case "neverexpire":
                if (value is not null)
                    throw AzureStorageException.InvalidHeader("x-ms-expiry-time", value);
                return null;
            case "absolute":
                if (!DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var absolute))
                    throw AzureStorageException.InvalidHeader("x-ms-expiry-time", value);
                return absolute;
            case "relativetonow":
                if (!long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var milliseconds) || milliseconds <= 0)
                    throw AzureStorageException.InvalidHeader("x-ms-expiry-time", value);
                return metadataNow.AddMilliseconds(milliseconds);
            case "relativetocreation":
                if (!long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var fromCreation) || fromCreation <= 0)
                    throw AzureStorageException.InvalidHeader("x-ms-expiry-time", value);
                return createdAt.AddMilliseconds(fromCreation);
            default:
                throw AzureStorageException.InvalidHeader("x-ms-expiry-option", option);
        }
    }

    private static int ParseMaxResults(string value, int defaultValue)
    {
        if (string.IsNullOrEmpty(value))
            return defaultValue;
        if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) || parsed < 1)
            throw AzureStorageException.InvalidQuery("maxresults");
        return Math.Min(parsed, 5000);
    }

    private static int ParsePageRangeMaxResults(string value, bool specified)
    {
        if (!specified)
            return int.MaxValue;
        if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) || parsed < 1)
            throw AzureStorageException.InvalidQuery("maxresults");
        return Math.Min(parsed, 10_000);
    }

    private static int ParsePageRangeMarker(string value, int resultCount)
    {
        if (string.IsNullOrEmpty(value))
            return 0;
        if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) || parsed < 0 || parsed > resultCount)
            throw AzureStorageException.InvalidQuery("marker");
        return parsed;
    }

    private static HashSet<string> SplitCsv(string value) => value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToHashSet(StringComparer.OrdinalIgnoreCase);

    private static string? NullIfEmpty(string value) => string.IsNullOrEmpty(value) ? null : value;

    private static long? TryParseLongHeader(IHeaderDictionary headers, string name)
    {
        var value = ProtocolParsing.First(headers, name);
        if (value is null)
            return null;
        if (!long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed))
            throw AzureStorageException.InvalidHeader(name, value);
        return parsed;
    }

    private static bool MatchesCsv(string csv, string value) => csv.Split(',', StringSplitOptions.TrimEntries).Any(item => item == "*" || string.Equals(item, value, StringComparison.OrdinalIgnoreCase));

    private static bool MatchesCorsOrigin(string csv, string origin) =>
        csv.Split(',', StringSplitOptions.TrimEntries).Any(candidate =>
            candidate == "*" ||
            string.Equals(candidate, origin, StringComparison.Ordinal) ||
            MatchesCorsSubdomain(candidate, origin));

    private static bool MatchesCorsSubdomain(string candidate, string origin)
    {
        var wildcard = candidate.IndexOf("*.", StringComparison.Ordinal);
        if (wildcard < 0 || candidate.IndexOf('*', wildcard + 1) >= 0)
            return false;
        var prefix = candidate[..wildcard];
        var suffix = candidate[(wildcard + 1)..];
        if (!origin.StartsWith(prefix, StringComparison.Ordinal) ||
            !origin.EndsWith(suffix, StringComparison.Ordinal))
        {
            return false;
        }
        var subdomain = origin[prefix.Length..^suffix.Length];
        return subdomain.Length > 0;
    }

    private static bool AllowsAllCorsOrigins(string csv) =>
        csv.Split(',', StringSplitOptions.TrimEntries).Any(candidate => candidate == "*");

    private static bool MatchesHeader(string csv, string header) => csv.Split(',', StringSplitOptions.TrimEntries).Any(item => item == "*" || (item.EndsWith('*') ? header.StartsWith(item[..^1], StringComparison.OrdinalIgnoreCase) : string.Equals(item, header, StringComparison.OrdinalIgnoreCase)));

    private static void EnsureMutableVersion(BlobRecord blob)
    {
        if (!blob.IsCurrent || blob.Snapshot is not null)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "OperationNotAllowedOnArchivedBlob", "The operation is not allowed on the specified immutable blob version.");
    }

    private static AzureStorageException UnsupportedOperation() => new(
        StatusCodes.Status400BadRequest,
        "InvalidQueryParameterValue",
        "The requested operation is not valid for this resource.");

}
