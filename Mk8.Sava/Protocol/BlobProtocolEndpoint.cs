using System.Globalization;
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
    public static async Task HandleAsync(HttpContext http)
    {
        var request = StorageRequestContext.Get(http);
        var service = http.RequestServices.GetRequiredService<BlobService>();
        var writer = http.RequestServices.GetRequiredService<AzureResponseWriter>();
        var cancellationToken = http.RequestAborted;

        if (HttpMethods.IsOptions(http.Request.Method))
        {
            await HandleCorsPreflightAsync(http, service, request, cancellationToken);
            return;
        }

        await ApplyCorsResponseHeadersAsync(http, service, request, cancellationToken);

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
            default:
                throw new ArgumentOutOfRangeException();
        }
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
            if (request.Authorization.Kind != StorageAuthorizationKind.Bearer)
                throw AzureStorageException.AuthorizationFailure();
            var keyRequest = await ProtocolParsing.ReadUserDelegationKeyRequestAsync(http.Request.Body, cancellationToken);
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
            var containers = await service.ListContainersAsync(request.Account, includes.Contains("deleted"), cancellationToken);
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
            var updated = await ProtocolParsing.ReadServicePropertiesAsync(http.Request.Body, current, cancellationToken);
            await service.PutServicePropertiesAsync(request.Account, updated, cancellationToken);
            http.Response.StatusCode = StatusCodes.Status202Accepted;
            return;
        }

        if (comp == "accountinfo" && HttpMethods.IsGet(http.Request.Method))
        {
            Require(request, 'r');
            http.Response.Headers["x-ms-sku-name"] = "Standard_LRS";
            http.Response.Headers["x-ms-account-kind"] = "StorageV2";
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
            await WriteFindByTagsAsync(http, request, service, writer, cancellationToken);
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
            !(HttpMethods.IsGet(http.Request.Method) && comp == "list"))
        {
            throw AzureStorageException.AuthorizationFailure();
        }

        if (HttpMethods.IsPut(http.Request.Method) && string.IsNullOrEmpty(comp))
        {
            RequireAny(request, 'c', 'w');
            var created = await service.CreateContainerAsync(
                request.Account,
                containerName,
                ProtocolParsing.ReadMetadata(http.Request.Headers),
                ProtocolParsing.First(http.Request.Headers, "x-ms-blob-public-access"),
                cancellationToken);
            AzureResponseWriter.AddContainerHeaders(http.Response, created);
            http.Response.StatusCode = StatusCodes.Status201Created;
            return;
        }

        if (comp == "restore" && HttpMethods.IsPut(http.Request.Method))
        {
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

        if (HttpMethods.IsHead(http.Request.Method) && string.IsNullOrEmpty(comp))
        {
            await AuthorizeContainerReadAsync(request, service, container, allowContainerPublic: true);
            EvaluateContainerConditions(http.Request, container);
            AzureResponseWriter.AddContainerHeaders(http.Response, container);
            return;
        }

        if (HttpMethods.IsGet(http.Request.Method) && comp == "list")
        {
            await AuthorizeContainerListAsync(request, service, container);
            var includes = SplitCsv(http.Request.Query["include"].ToString());
            var blobs = await service.ListBlobsAsync(
                request.Account,
                containerName,
                includes.Contains("versions"),
                includes.Contains("snapshots"),
                includes.Contains("deleted") || includes.Contains("deletedwithversions"),
                cancellationToken);
            await writer.WriteBlobsAsync(
                http,
                blobs,
                http.Request.Query["prefix"].ToString(),
                http.Request.Query["delimiter"].ToString(),
                http.Request.Query["marker"].ToString(),
                ParseMaxResults(http.Request.Query["maxresults"].ToString(), 5000),
                includes,
                cancellationToken);
            return;
        }

        if (HttpMethods.IsGet(http.Request.Method) && comp == "metadata")
        {
            await AuthorizeContainerReadAsync(request, service, container, allowContainerPublic: true);
            EvaluateContainerConditions(http.Request, container);
            AzureResponseWriter.AddContainerHeaders(http.Response, container);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "metadata")
        {
            Require(request, 'w');
            EvaluateContainerConditions(http.Request, container);
            EnsureLease(http.Request, container.Lease, "container");
            var updated = await service.SetContainerMetadataAsync(container, ProtocolParsing.ReadMetadata(http.Request.Headers), cancellationToken);
            AzureResponseWriter.AddContainerHeaders(http.Response, updated);
            return;
        }

        if (HttpMethods.IsGet(http.Request.Method) && comp == "acl")
        {
            Require(request, 'r');
            AzureResponseWriter.AddContainerHeaders(http.Response, container);
            await writer.WriteAclAsync(http, container, cancellationToken);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "acl")
        {
            Require(request, 'w');
            EvaluateContainerConditions(http.Request, container);
            EnsureLease(http.Request, container.Lease, "container");
            var policies = http.Request.ContentLength is null or 0
                ? new Dictionary<string, StoredAccessPolicy>(StringComparer.Ordinal)
                : await ProtocolParsing.ReadAclAsync(http.Request.Body, cancellationToken);
            var updated = await service.SetContainerAclAsync(
                container,
                ProtocolParsing.First(http.Request.Headers, "x-ms-blob-public-access"),
                policies,
                cancellationToken);
            AzureResponseWriter.AddContainerHeaders(http.Response, updated);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "lease")
        {
            Require(request, 'w');
            await HandleContainerLeaseAsync(http, service, container, cancellationToken);
            return;
        }

        if (HttpMethods.IsDelete(http.Request.Method) && string.IsNullOrEmpty(comp))
        {
            Require(request, 'd');
            EvaluateContainerConditions(http.Request, container);
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
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "FeatureVersionMismatch",
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
            var segments = subrequest.RawPath.Split('/', StringSplitOptions.None).Skip(1).ToArray();
            var offset = segments.Length >= 3 &&
                         string.Equals(Uri.UnescapeDataString(segments[0]), account, StringComparison.Ordinal)
                ? 1
                : 0;
            if (segments.Length - offset < 2 || string.IsNullOrEmpty(segments[offset]))
                throw InvalidBatchSubrequest("A batch subrequest does not identify a blob.");
            var container = Uri.UnescapeDataString(segments[offset]);
            var blob = string.Join('/', segments.Skip(offset + 1).Select(Uri.UnescapeDataString));
            if (string.IsNullOrEmpty(blob))
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
                query.TryGetValue("versionid", out var version) ? NullIfEmpty(version.ToString()) : null);
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
        inner.Features.Get<IHttpRequestFeature>()!.RawTarget = resolved.Request.RawPath + resolved.Request.QueryString;

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
            var authenticator = outer.RequestServices.GetRequiredService<StorageAuthenticator>();
            subrequestContext.Authorization = await authenticator.AuthenticateAsync(inner, subrequestContext, cancellationToken);
            var blob = await service.GetBlobAsync(
                subrequestContext.Account,
                resolved.Container,
                resolved.Blob,
                resolved.VersionId,
                resolved.Snapshot,
                includeDeleted: false,
                cancellationToken);

            var headers = CreateBatchCommonHeaders(subrequestContext, inner.Request);
            switch (resolved.Request.Kind)
            {
                case BlobBatchOperationKind.Delete:
                    Require(subrequestContext, 'd');
                    EvaluateWriteConditions(inner.Request, blob);
                    EnsureLease(inner.Request, blob.Lease, "blob");
                    var properties = await service.GetServicePropertiesAsync(subrequestContext.Account, cancellationToken);
                    await service.DeleteBlobAsync(
                        blob,
                        ProtocolParsing.First(inner.Request.Headers, "x-ms-delete-snapshots"),
                        cancellationToken);
                    headers["x-ms-delete-type-permanent"] = properties.BlobSoftDeleteEnabled ? "false" : "true";
                    return new BlobBatchSubresponse(
                        StatusCodes.Status202Accepted,
                        headers,
                        [],
                        resolved.Request.ContentId);

                case BlobBatchOperationKind.SetTier:
                    Require(subrequestContext, 'w');
                    EvaluateWriteConditions(inner.Request, blob);
                    var tier = ProtocolParsing.First(inner.Request.Headers, "x-ms-access-tier")
                               ?? throw AzureStorageException.InvalidHeader("x-ms-access-tier");
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

        if (HttpMethods.IsPut(http.Request.Method) && string.IsNullOrEmpty(comp))
        {
            await HandlePutBlobAsync(http, request, service, containerName, blobName, cancellationToken);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "block")
        {
            RequireAny(request, 'w', 'c');
            var current = await TryGetCurrentBlobAsync(service, request.Account, containerName, blobName, cancellationToken);
            EnsureLease(http.Request, current?.Lease ?? LeaseRecord.Available, "blob");
            var blockId = http.Request.Query["blockid"].ToString();
            var copySource = ProtocolParsing.First(http.Request.Headers, "x-ms-copy-source");
            if (copySource is null)
            {
                await WithIntegrityValidationAsync(http.Request, async body =>
                    await service.StageBlockAsync(request.Account, containerName, blobName, blockId, body, cancellationToken));
            }
            else
            {
                var transfers = http.RequestServices.GetRequiredService<UrlTransferClient>();
                await transfers.ReadAsync(
                    http.Request,
                    copySource,
                    ProtocolParsing.First(http.Request.Headers, "x-ms-source-range"),
                    async source =>
                    {
                        await service.StageBlockAsync(request.Account, containerName, blobName, blockId, source.Content, cancellationToken);
                        return true;
                    },
                    cancellationToken);
            }
            http.Response.StatusCode = StatusCodes.Status201Created;
            http.Response.Headers["x-ms-request-server-encrypted"] = "true";
            EchoTransactionalChecksum(http, copySource is not null);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "blocklist")
        {
            RequireAny(request, 'w', 'c');
            var current = await TryGetCurrentBlobAsync(service, request.Account, containerName, blobName, cancellationToken);
            EvaluateWriteConditions(http.Request, current);
            if (current is not null)
                EnsureLease(http.Request, current.Lease, "blob");
            var blockIds = await ProtocolParsing.ReadBlockListAsync(http.Request.Body, cancellationToken);
            var options = ReadWriteOptions(http.Request, current, useStandardContentType: false);
            var committed = await service.CommitBlockListAsync(
                request.Account,
                containerName,
                blobName,
                blockIds,
                options,
                current?.GenerationId,
                current?.Revision,
                cancellationToken);
            AzureResponseWriter.AddBlobHeaders(http.Response, committed);
            http.Response.Headers["x-ms-request-server-encrypted"] = "true";
            http.Response.StatusCode = StatusCodes.Status201Created;
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "appendblock")
        {
            RequireAny(request, 'a', 'w');
            var current = await service.GetBlobAsync(request.Account, containerName, blobName, null, null, false, cancellationToken);
            EvaluateWriteConditions(http.Request, current);
            EnsureLease(http.Request, current.Lease, "blob");
            var expectedPosition = TryParseLongHeader(http.Request.Headers, "x-ms-blob-condition-appendpos");
            var expectedMaximumSize = TryParseLongHeader(http.Request.Headers, "x-ms-blob-condition-maxsize");
            BlobRecord updated = null!;
            var copySource = ProtocolParsing.First(http.Request.Headers, "x-ms-copy-source");
            if (copySource is null)
            {
                await WithIntegrityValidationAsync(http.Request, async body =>
                    updated = await service.AppendBlockAsync(current, body, expectedPosition, expectedMaximumSize, cancellationToken));
            }
            else
            {
                var transfers = http.RequestServices.GetRequiredService<UrlTransferClient>();
                await transfers.ReadAsync(
                    http.Request,
                    copySource,
                    ProtocolParsing.First(http.Request.Headers, "x-ms-source-range"),
                    async source => updated = await service.AppendBlockAsync(
                        current,
                        source.Content,
                        expectedPosition,
                        expectedMaximumSize,
                        cancellationToken),
                    cancellationToken);
            }
            AzureResponseWriter.AddBlobHeaders(http.Response, updated);
            http.Response.Headers["x-ms-blob-append-offset"] = current.Content.Length.ToString(CultureInfo.InvariantCulture);
            http.Response.Headers["x-ms-blob-committed-block-count"] = updated.AppendBlockCount.ToString(CultureInfo.InvariantCulture);
            EchoTransactionalChecksum(http, copySource is not null);
            http.Response.StatusCode = StatusCodes.Status201Created;
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "page")
        {
            Require(request, 'w');
            var current = await service.GetBlobAsync(request.Account, containerName, blobName, null, null, false, cancellationToken);
            EvaluateWriteConditions(http.Request, current);
            EnsureLease(http.Request, current.Lease, "blob");
            var rangeValue = ProtocolParsing.First(http.Request.Headers, "x-ms-range")
                             ?? ProtocolParsing.First(http.Request.Headers, "Range")
                             ?? throw AzureStorageException.InvalidHeader("x-ms-range");
            var (start, end) = ProtocolParsing.ParseRange(rangeValue, current.Content.Length);
            var operation = ProtocolParsing.First(http.Request.Headers, "x-ms-page-write")?.ToLowerInvariant();
            BlobRecord updated;
            if (operation == "clear")
            {
                updated = await service.PutPageAsync(current, start, end, null, clear: true, cancellationToken);
            }
            else if (operation == "update")
            {
                var copySource = ProtocolParsing.First(http.Request.Headers, "x-ms-copy-source");
                updated = null!;
                if (copySource is null)
                {
                    await WithIntegrityValidationAsync(http.Request, async body =>
                        updated = await service.PutPageAsync(current, start, end, body, clear: false, cancellationToken));
                }
                else
                {
                    var transfers = http.RequestServices.GetRequiredService<UrlTransferClient>();
                    await transfers.ReadAsync(
                        http.Request,
                        copySource,
                        ProtocolParsing.First(http.Request.Headers, "x-ms-source-range"),
                        async source => updated = await service.PutPageAsync(
                            current,
                            start,
                            end,
                            source.Content,
                            clear: false,
                            cancellationToken),
                        cancellationToken);
                }
            }
            else
            {
                throw AzureStorageException.InvalidHeader("x-ms-page-write", operation);
            }
            AzureResponseWriter.AddBlobHeaders(http.Response, updated);
            EchoTransactionalChecksum(http, ProtocolParsing.First(http.Request.Headers, "x-ms-copy-source") is not null);
            http.Response.StatusCode = StatusCodes.Status201Created;
            return;
        }

        if (HttpMethods.IsGet(http.Request.Method) && comp == "blocklist")
        {
            Require(request, 'r');
            var current = await TryGetCurrentBlobAsync(service, request.Account, containerName, blobName, cancellationToken);
            var staged = await service.ListStagedBlocksAsync(request.Account, containerName, blobName, cancellationToken);
            var listType = http.Request.Query["blocklisttype"].ToString().ToLowerInvariant();
            if (listType is not ("all" or "committed" or "uncommitted"))
                throw AzureStorageException.InvalidQuery("blocklisttype");
            await writer.WriteBlockListAsync(http, current, staged, listType, cancellationToken);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "undelete")
        {
            Require(request, 'w');
            await service.UndeleteBlobAsync(request.Account, containerName, blobName, cancellationToken);
            return;
        }

        var blob = await service.GetBlobAsync(
            request.Account,
            containerName,
            blobName,
            versionId,
            snapshot,
            includeDeleted: false,
            cancellationToken);

        if (HttpMethods.IsPut(http.Request.Method) && comp == "immutabilitypolicies")
        {
            Require(request, 'i');
            EvaluateWriteConditions(http.Request, blob);
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
            Require(request, 'i');
            EvaluateWriteConditions(http.Request, blob);
            await service.DeleteBlobImmutabilityPolicyAsync(blob, cancellationToken);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "legalhold")
        {
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
            EvaluateWriteConditions(http.Request, blob);
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
            EvaluateReadConditions(http.Request, blob);
            await WriteBlobAsync(http, service, blob, cancellationToken);
            return;
        }

        if (HttpMethods.IsGet(http.Request.Method) && comp == "metadata")
        {
            await AuthorizeBlobReadAsync(request, service, blob, cancellationToken);
            EvaluateReadConditions(http.Request, blob);
            AzureResponseWriter.AddBlobHeaders(http.Response, blob);
            return;
        }

        if (HttpMethods.IsGet(http.Request.Method) && comp == "tags")
        {
            RequireAny(request, 't', 'r');
            EvaluateReadConditions(http.Request, blob);
            await writer.WriteTagsAsync(http, blob.Tags, cancellationToken);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "metadata")
        {
            Require(request, 'w');
            EnsureMutableVersion(blob);
            EvaluateWriteConditions(http.Request, blob);
            EnsureLease(http.Request, blob.Lease, "blob");
            var updated = await service.SetBlobMetadataAsync(blob, ProtocolParsing.ReadMetadata(http.Request.Headers), cancellationToken);
            AzureResponseWriter.AddBlobHeaders(http.Response, updated);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "tags")
        {
            RequireAny(request, 't', 'w');
            EnsureMutableVersion(blob);
            EvaluateWriteConditions(http.Request, blob);
            var tags = await ProtocolParsing.ReadTagsBodyAsync(http.Request.Body, cancellationToken);
            await service.SetBlobTagsAsync(blob, tags, cancellationToken);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "properties")
        {
            Require(request, 'w');
            EnsureMutableVersion(blob);
            EvaluateWriteConditions(http.Request, blob);
            EnsureLease(http.Request, blob.Lease, "blob");
            var resizeTo = TryParseLongHeader(http.Request.Headers, "x-ms-blob-content-length");
            var sequence = TryParseLongHeader(http.Request.Headers, "x-ms-blob-sequence-number");
            var updated = await service.SetBlobPropertiesAsync(
                blob,
                ProtocolParsing.ReadHttpProperties(http.Request.Headers, blob.Http),
                resizeTo,
                sequence,
                ProtocolParsing.First(http.Request.Headers, "x-ms-sequence-number-action"),
                cancellationToken);
            AzureResponseWriter.AddBlobHeaders(http.Response, updated);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "snapshot")
        {
            Require(request, 'w');
            EvaluateWriteConditions(http.Request, blob);
            EnsureLease(http.Request, blob.Lease, "blob");
            var created = await service.CreateSnapshotAsync(blob, cancellationToken);
            http.Response.Headers["x-ms-snapshot"] = created.Snapshot;
            http.Response.Headers.ETag = created.ETag;
            http.Response.Headers.LastModified = created.LastModified.ToString("R", CultureInfo.InvariantCulture);
            http.Response.StatusCode = StatusCodes.Status201Created;
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "seal")
        {
            Require(request, 'w');
            EnsureMutableVersion(blob);
            EvaluateWriteConditions(http.Request, blob);
            EnsureLease(http.Request, blob.Lease, "blob");
            var updated = await service.SealAppendBlobAsync(blob, cancellationToken);
            AzureResponseWriter.AddBlobHeaders(http.Response, updated);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "tier")
        {
            Require(request, 'w');
            EvaluateWriteConditions(http.Request, blob);
            var tier = ProtocolParsing.First(http.Request.Headers, "x-ms-access-tier")
                       ?? throw AzureStorageException.InvalidHeader("x-ms-access-tier");
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
            var expiry = ParseExpiry(http.Request.Headers, metadataNow: DateTimeOffset.UtcNow);
            await service.SetExpiryAsync(blob, expiry, cancellationToken);
            return;
        }

        if (HttpMethods.IsPut(http.Request.Method) && comp == "lease")
        {
            Require(request, 'w');
            EnsureMutableVersion(blob);
            await HandleBlobLeaseAsync(http, service, blob, cancellationToken);
            return;
        }

        if (HttpMethods.IsGet(http.Request.Method) && comp == "pagelist")
        {
            Require(request, 'r');
            if (blob.Kind != BlobKind.PageBlob)
                throw new AzureStorageException(StatusCodes.Status409Conflict, "InvalidBlobType", "The blob type is invalid for this operation.");
            var rangeValue = ProtocolParsing.First(http.Request.Headers, "x-ms-range")
                             ?? ProtocolParsing.First(http.Request.Headers, "Range");
            var ranges = SelectPageRanges(blob, rangeValue);
            AzureResponseWriter.AddBlobHeaders(http.Response, blob);
            http.Response.Headers["x-ms-blob-content-length"] = blob.Content.Length.ToString(CultureInfo.InvariantCulture);
            await writer.WritePageRangesAsync(http, ranges, cancellationToken);
            return;
        }

        if (HttpMethods.IsDelete(http.Request.Method) && string.IsNullOrEmpty(comp))
        {
            Require(request, 'd');
            EvaluateWriteConditions(http.Request, blob);
            EnsureLease(http.Request, blob.Lease, "blob");
            await service.DeleteBlobAsync(blob, ProtocolParsing.First(http.Request.Headers, "x-ms-delete-snapshots"), cancellationToken);
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
        RequireAny(request, current is null ? 'c' : 'w', 'w');
        EvaluateWriteConditions(http.Request, current);
        if (current is not null)
            EnsureLease(http.Request, current.Lease, "blob");

        var copySource = ProtocolParsing.First(http.Request.Headers, "x-ms-copy-source");
        if (copySource is not null)
        {
            var requestedType = ProtocolParsing.First(http.Request.Headers, "x-ms-blob-type");
            if (requestedType == "BlockBlob" || string.Equals(
                    ProtocolParsing.First(http.Request.Headers, "x-ms-requires-sync"),
                    "true",
                    StringComparison.OrdinalIgnoreCase))
            {
                if (requestedType is not null && requestedType != "BlockBlob")
                    throw AzureStorageException.InvalidHeader("x-ms-blob-type", requestedType);
                var transfers = http.RequestServices.GetRequiredService<UrlTransferClient>();
                var uploaded = await transfers.ReadAsync(
                    http.Request,
                    copySource,
                    ProtocolParsing.First(http.Request.Headers, "x-ms-source-range"),
                    async source => await service.PutBlockBlobAsync(
                        request.Account,
                        containerName,
                        blobName,
                        source.Content,
                        ReadUrlWriteOptions(http.Request, source),
                        current?.GenerationId,
                        current?.Revision,
                        cancellationToken),
                    cancellationToken);
                AzureResponseWriter.AddBlobHeaders(http.Response, uploaded);
                http.Response.Headers["x-ms-copy-status"] = "success";
                http.Response.Headers["x-ms-request-server-encrypted"] = "true";
                EchoTransactionalChecksum(http, sourceChecksum: true);
                http.Response.StatusCode = StatusCodes.Status201Created;
                return;
            }

            var publicSource = SanitizeCopySource(copySource);
            BlobRecord copied;
            if (Uri.TryCreate(copySource, UriKind.Absolute, out var copyUri) &&
                string.Equals(copyUri.Host, http.Request.Host.Host, StringComparison.OrdinalIgnoreCase))
            {
                var source = await ResolveCopySourceAsync(request, service, copySource, cancellationToken);
                EvaluateCopySourceConditions(http.Request, source);
                copied = await service.BeginCopyFromBlobAsync(
                    request.Account,
                    containerName,
                    blobName,
                    source,
                    ReadCopyWriteOptions(http.Request, source),
                    publicSource,
                    current?.GenerationId,
                    current?.Revision,
                    cancellationToken);
            }
            else
            {
                var transfers = http.RequestServices.GetRequiredService<UrlTransferClient>();
                copied = await transfers.ReadAsync(
                    http.Request,
                    copySource,
                    sourceRange: null,
                    async source => await service.BeginCopyFromStreamAsync(
                        request.Account,
                        containerName,
                        blobName,
                        source.Content,
                        ReadUrlWriteOptions(http.Request, source),
                        publicSource,
                        current?.GenerationId,
                        current?.Revision,
                        cancellationToken),
                    cancellationToken);
            }
            AzureResponseWriter.AddBlobHeaders(http.Response, copied);
            http.Response.StatusCode = StatusCodes.Status202Accepted;
            return;
        }

        var type = ProtocolParsing.First(http.Request.Headers, "x-ms-blob-type")
                   ?? throw AzureStorageException.InvalidHeader("x-ms-blob-type");
        BlobRecord created;
        switch (type)
        {
            case "BlockBlob":
                created = null!;
                await WithIntegrityValidationAsync(http.Request, async body =>
                    created = await service.PutBlockBlobAsync(
                        request.Account,
                        containerName,
                        blobName,
                        body,
                        ReadWriteOptions(http.Request, current),
                        current?.GenerationId,
                        current?.Revision,
                        cancellationToken));
                break;
            case "AppendBlob":
                if (http.Request.ContentLength is > 0)
                    throw AzureStorageException.InvalidHeader("Content-Length", http.Request.ContentLength.Value.ToString(CultureInfo.InvariantCulture));
                created = await service.CreateAppendBlobAsync(
                    request.Account,
                    containerName,
                    blobName,
                    ReadWriteOptions(http.Request, current),
                    current?.GenerationId,
                    current?.Revision,
                    cancellationToken);
                break;
            case "PageBlob":
                var length = ProtocolParsing.ParseLongHeader(http.Request.Headers, "x-ms-blob-content-length", required: true);
                var sequence = ProtocolParsing.ParseLongHeader(http.Request.Headers, "x-ms-blob-sequence-number", defaultValue: 0);
                created = await service.CreatePageBlobAsync(
                    request.Account,
                    containerName,
                    blobName,
                    length,
                    ReadWriteOptions(http.Request, current),
                    sequence,
                    current?.GenerationId,
                    current?.Revision,
                    cancellationToken);
                break;
            default:
                throw AzureStorageException.InvalidHeader("x-ms-blob-type", type);
        }

        AzureResponseWriter.AddBlobHeaders(http.Response, created);
        EchoTransactionalChecksum(http);
        http.Response.Headers["x-ms-request-server-encrypted"] = "true";
        http.Response.StatusCode = StatusCodes.Status201Created;
    }

    private static async Task WriteBlobAsync(
        HttpContext http,
        BlobService service,
        BlobRecord blob,
        CancellationToken cancellationToken)
    {
        AzureResponseWriter.AddBlobHeaders(http.Response, blob);
        ApplySasResponseOverrides(http);
        long start = 0;
        long end = blob.Content.Length - 1;
        var rangeHeader = ProtocolParsing.First(http.Request.Headers, "x-ms-range")
                          ?? ProtocolParsing.First(http.Request.Headers, "Range");
        if (!string.IsNullOrEmpty(rangeHeader))
        {
            (start, end) = ProtocolParsing.ParseRange(rangeHeader, blob.Content.Length);
            http.Response.StatusCode = StatusCodes.Status206PartialContent;
            http.Response.Headers.ContentRange = $"bytes {start}-{end}/{blob.Content.Length}";
        }

        var length = blob.Content.Length == 0 ? 0 : end - start + 1;
        http.Response.ContentLength = length;
        if (HttpMethods.IsHead(http.Request.Method) || length == 0)
            return;

        var wantMd5 = string.Equals(ProtocolParsing.First(http.Request.Headers, "x-ms-range-get-content-md5"), "true", StringComparison.OrdinalIgnoreCase);
        if (wantMd5)
        {
            if (rangeHeader is null || length > 4 * 1024 * 1024)
                throw AzureStorageException.InvalidHeader("x-ms-range-get-content-md5", "true");
            using var buffer = new MemoryStream((int)length);
            await service.WriteContentAsync(blob, start, length, buffer, cancellationToken);
            var bytes = buffer.ToArray();
            http.Response.Headers.ContentMD5 = Convert.ToBase64String(MD5.HashData(bytes));
            await http.Response.Body.WriteAsync(bytes, cancellationToken);
            return;
        }

        await service.WriteContentAsync(blob, start, length, http.Response.Body, cancellationToken);
    }

    private static async Task<BlobRecord> ResolveCopySourceAsync(
        StorageRequestContext destinationRequest,
        BlobService service,
        string sourceValue,
        CancellationToken cancellationToken)
    {
        if (!Uri.TryCreate(sourceValue, UriKind.Absolute, out var sourceUri))
            throw AzureStorageException.InvalidHeader("x-ms-copy-source", sourceValue);
        var segments = sourceUri.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries).Select(Uri.UnescapeDataString).ToArray();
        var offset = segments.Length > 0 && string.Equals(segments[0], destinationRequest.Account, StringComparison.Ordinal) ? 1 : 0;
        if (segments.Length - offset < 2)
            throw AzureStorageException.InvalidHeader("x-ms-copy-source", sourceValue);
        if (destinationRequest.Authorization.Kind != StorageAuthorizationKind.SharedKey)
            throw AzureStorageException.AuthorizationFailure();
        var container = segments[offset];
        var name = string.Join('/', segments.Skip(offset + 1));
        var query = Microsoft.AspNetCore.WebUtilities.QueryHelpers.ParseQuery(sourceUri.Query);
        return await service.GetBlobAsync(
            destinationRequest.Account,
            container,
            name,
            query.TryGetValue("versionid", out var version) ? version.ToString() : null,
            query.TryGetValue("snapshot", out var snapshot) ? snapshot.ToString() : null,
            false,
            cancellationToken);
    }

    private static async Task HandleContainerLeaseAsync(
        HttpContext http,
        BlobService service,
        ContainerRecord container,
        CancellationToken cancellationToken)
    {
        var (lease, status, remaining) = ApplyLeaseAction(http.Request, container.Lease, isContainer: true);
        await service.SetContainerLeaseAsync(container, lease, cancellationToken);
        http.Response.StatusCode = status;
        if (lease.Id is not null)
            http.Response.Headers["x-ms-lease-id"] = lease.Id;
        if (remaining.HasValue)
            http.Response.Headers["x-ms-lease-time"] = remaining.Value.ToString(CultureInfo.InvariantCulture);
    }

    private static async Task HandleBlobLeaseAsync(
        HttpContext http,
        BlobService service,
        BlobRecord blob,
        CancellationToken cancellationToken)
    {
        var (lease, status, remaining) = ApplyLeaseAction(http.Request, blob.Lease, isContainer: false);
        var updated = await service.SetBlobLeaseAsync(blob, lease, cancellationToken);
        http.Response.StatusCode = status;
        http.Response.Headers.ETag = updated.ETag;
        http.Response.Headers.LastModified = updated.LastModified.ToString("R", CultureInfo.InvariantCulture);
        if (lease.Id is not null)
            http.Response.Headers["x-ms-lease-id"] = lease.Id;
        if (remaining.HasValue)
            http.Response.Headers["x-ms-lease-time"] = remaining.Value.ToString(CultureInfo.InvariantCulture);
    }

    private static (LeaseRecord Lease, int Status, int? Remaining) ApplyLeaseAction(
        HttpRequest request,
        LeaseRecord current,
        bool isContainer)
    {
        current = EffectiveLease(current);
        var action = ProtocolParsing.First(request.Headers, "x-ms-lease-action")?.ToLowerInvariant()
                     ?? throw AzureStorageException.InvalidHeader("x-ms-lease-action");
        var suppliedId = ProtocolParsing.First(request.Headers, "x-ms-lease-id");
        var proposedId = ProtocolParsing.First(request.Headers, "x-ms-proposed-lease-id");
        var now = DateTimeOffset.UtcNow;
        switch (action)
        {
            case "acquire":
                if (current.State == LeaseState.Leased)
                    throw new AzureStorageException(StatusCodes.Status409Conflict, isContainer ? "LeaseAlreadyPresent" : "LeaseAlreadyPresent", "There is already a lease present.");
                var duration = (int)ProtocolParsing.ParseLongHeader(request.Headers, "x-ms-lease-duration", required: true);
                if (duration != -1 && duration is < 15 or > 60)
                    throw AzureStorageException.InvalidHeader("x-ms-lease-duration", duration.ToString(CultureInfo.InvariantCulture));
                var id = proposedId ?? Guid.NewGuid().ToString();
                if (!Guid.TryParse(id, out _))
                    throw AzureStorageException.InvalidHeader("x-ms-proposed-lease-id", id);
                return (new LeaseRecord
                {
                    Id = id,
                    State = LeaseState.Leased,
                    DurationSeconds = duration,
                    AcquiredAt = now,
                    ExpiresAt = duration == -1 ? null : now.AddSeconds(duration)
                }, StatusCodes.Status201Created, null);
            case "renew":
                RequireLeaseId(current, suppliedId);
                return (current with
                {
                    State = LeaseState.Leased,
                    AcquiredAt = now,
                    ExpiresAt = current.DurationSeconds == -1 ? null : now.AddSeconds(current.DurationSeconds ?? 60),
                    BreakEndsAt = null
                }, StatusCodes.Status200OK, null);
            case "change":
                RequireLeaseId(current, suppliedId);
                if (!Guid.TryParse(proposedId, out _))
                    throw AzureStorageException.InvalidHeader("x-ms-proposed-lease-id", proposedId);
                return (current with { Id = proposedId }, StatusCodes.Status200OK, null);
            case "release":
                RequireLeaseId(current, suppliedId);
                return (LeaseRecord.Available, StatusCodes.Status200OK, null);
            case "break":
                if (current.State is LeaseState.Available or LeaseState.Broken)
                    throw new AzureStorageException(StatusCodes.Status409Conflict, "LeaseNotPresentWithLeaseOperation", "There is currently no lease on the resource.");
                var requested = TryParseLongHeader(request.Headers, "x-ms-lease-break-period") is { } seconds ? (int?)seconds : null;
                var remaining = current.ExpiresAt.HasValue ? Math.Max(0, (int)(current.ExpiresAt.Value - now).TotalSeconds) : 60;
                var period = requested.HasValue ? Math.Clamp(requested.Value, 0, Math.Min(60, remaining)) : 0;
                return period == 0
                    ? (new LeaseRecord { State = LeaseState.Broken }, StatusCodes.Status202Accepted, 0)
                    : (current with { State = LeaseState.Breaking, BreakEndsAt = now.AddSeconds(period) }, StatusCodes.Status202Accepted, period);
            default:
                throw AzureStorageException.InvalidHeader("x-ms-lease-action", action);
        }
    }

    private static IReadOnlyList<PageRange> SelectPageRanges(BlobRecord blob, string? requestedRange)
    {
        if (requestedRange is null)
            return blob.PageRanges;

        var (start, end) = ProtocolParsing.ParseRange(requestedRange, blob.Content.Length);
        return blob.PageRanges
            .Where(range => range.End >= start && range.Start <= end)
            .Select(range => new PageRange(Math.Max(range.Start, start), Math.Min(range.End, end)))
            .ToArray();
    }

    private static async Task WriteFindByTagsAsync(
        HttpContext http,
        StorageRequestContext request,
        BlobService service,
        AzureResponseWriter writer,
        CancellationToken cancellationToken)
    {
        var expression = http.Request.Query["where"].ToString();
        var match = System.Text.RegularExpressions.Regex.Match(expression, "^\\s*\"(?<key>[^\"]+)\"\\s*=\\s*'(?<value>[^']*)'\\s*$", System.Text.RegularExpressions.RegexOptions.CultureInvariant);
        if (!match.Success)
            throw AzureStorageException.InvalidQuery("where");
        var containers = await service.ListContainersAsync(request.Account, includeDeleted: false, cancellationToken);
        var matches = new List<BlobRecord>();
        foreach (var container in containers)
        {
            var blobs = await service.ListBlobsAsync(request.Account, container.Name, false, false, false, cancellationToken);
            matches.AddRange(blobs.Where(blob => blob.Tags.TryGetValue(match.Groups["key"].Value, out var value) && value == match.Groups["value"].Value));
        }
        await writer.WriteXmlAsync(http, xml =>
        {
            xml.WriteStartElement("EnumerationResults");
            xml.WriteStartElement("Blobs");
            foreach (var blob in matches)
            {
                xml.WriteStartElement("Blob");
                xml.WriteElementString("Name", blob.Name);
                xml.WriteElementString("ContainerName", blob.Container);
                xml.WriteStartElement("Tags");
                xml.WriteStartElement("TagSet");
                foreach (var (key, value) in blob.Tags)
                {
                    xml.WriteStartElement("Tag");
                    xml.WriteElementString("Key", key);
                    xml.WriteElementString("Value", value);
                    xml.WriteEndElement();
                }
                xml.WriteEndElement();
                xml.WriteEndElement();
                xml.WriteEndElement();
            }
            xml.WriteEndElement();
            xml.WriteElementString("NextMarker", string.Empty);
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
            MatchesCsv(candidate.AllowedOrigins, origin) &&
            MatchesCsv(candidate.AllowedMethods, requestedMethod) &&
            requestedHeaders.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .All(header => MatchesHeader(candidate.AllowedHeaders, header)));
        if (rule is null)
            throw new AzureStorageException(StatusCodes.Status403Forbidden, "CorsPreflightFailure", "CORS not enabled or no matching rule found for this request.");
        http.Response.Headers.AccessControlAllowOrigin = rule.AllowedOrigins.Contains('*') ? "*" : origin;
        http.Response.Headers.AccessControlAllowMethods = requestedMethod;
        http.Response.Headers.AccessControlAllowHeaders = requestedHeaders;
        http.Response.Headers.AccessControlExposeHeaders = rule.ExposedHeaders;
        http.Response.Headers.AccessControlMaxAge = rule.MaxAgeInSeconds.ToString(CultureInfo.InvariantCulture);
    }

    private static async Task ApplyCorsResponseHeadersAsync(
        HttpContext http,
        BlobService service,
        StorageRequestContext request,
        CancellationToken cancellationToken)
    {
        var origin = ProtocolParsing.First(http.Request.Headers, "Origin");
        if (origin is null)
            return;

        var properties = await service.GetServicePropertiesAsync(request.Account, cancellationToken);
        var rule = properties.Cors.FirstOrDefault(candidate =>
            MatchesCsv(candidate.AllowedOrigins, origin) &&
            MatchesCsv(candidate.AllowedMethods, http.Request.Method));
        if (rule is null)
            return;

        http.Response.Headers.AccessControlAllowOrigin = rule.AllowedOrigins.Contains('*') ? "*" : origin;
        http.Response.Headers.AccessControlExposeHeaders = rule.ExposedHeaders;
        http.Response.Headers.Append("Vary", "Origin");
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
                : AzureStorageException.AuthorizationFailure();
    }

    private static void RequireAny(StorageRequestContext request, params char[] permissions)
    {
        if (!permissions.Any(request.Authorization.Allows))
            throw request.Authorization.Kind == StorageAuthorizationKind.Anonymous
                ? AzureStorageException.AuthenticationFailed()
                : AzureStorageException.AuthorizationFailure();
    }

    private static void EvaluateReadConditions(HttpRequest request, BlobRecord blob)
    {
        var ifMatch = ProtocolParsing.First(request.Headers, "If-Match");
        if (!string.IsNullOrEmpty(ifMatch) && !MatchesETag(ifMatch, blob.ETag, requireMatch: true))
            throw AzureStorageException.ConditionNotMet();
        var ifNoneMatch = ProtocolParsing.First(request.Headers, "If-None-Match");
        if (!string.IsNullOrEmpty(ifNoneMatch) && MatchesETag(ifNoneMatch, blob.ETag, requireMatch: true))
            throw new AzureStorageException(StatusCodes.Status304NotModified, "ConditionNotMet", "The condition specified using HTTP conditional header(s) is not met.");
        var ifModified = ParseHttpDate(request.Headers, "If-Modified-Since");
        if (ifModified.HasValue && blob.LastModified <= ifModified.Value.AddSeconds(1))
            throw new AzureStorageException(StatusCodes.Status304NotModified, "ConditionNotMet", "The condition specified using HTTP conditional header(s) is not met.");
        var ifUnmodified = ParseHttpDate(request.Headers, "If-Unmodified-Since");
        if (ifUnmodified.HasValue && blob.LastModified > ifUnmodified.Value.AddSeconds(1))
            throw AzureStorageException.ConditionNotMet();
    }

    private static void EvaluateWriteConditions(HttpRequest request, BlobRecord? blob)
    {
        var ifMatch = ProtocolParsing.First(request.Headers, "If-Match");
        if (!string.IsNullOrEmpty(ifMatch) && (blob is null || !MatchesETag(ifMatch, blob.ETag, true)))
            throw AzureStorageException.ConditionNotMet();
        var ifNoneMatch = ProtocolParsing.First(request.Headers, "If-None-Match");
        if (!string.IsNullOrEmpty(ifNoneMatch) && blob is not null && MatchesETag(ifNoneMatch, blob.ETag, true))
            throw AzureStorageException.ConditionNotMet();
        var ifModified = ParseHttpDate(request.Headers, "If-Modified-Since");
        if (ifModified.HasValue && blob is not null && blob.LastModified <= ifModified.Value.AddSeconds(1))
            throw AzureStorageException.ConditionNotMet();
        var ifUnmodified = ParseHttpDate(request.Headers, "If-Unmodified-Since");
        if (ifUnmodified.HasValue && blob is not null && blob.LastModified > ifUnmodified.Value.AddSeconds(1))
            throw AzureStorageException.ConditionNotMet();
    }

    private static void EvaluateContainerConditions(HttpRequest request, ContainerRecord container)
    {
        var ifModified = ParseHttpDate(request.Headers, "If-Modified-Since");
        if (ifModified.HasValue && container.LastModified <= ifModified.Value.AddSeconds(1))
            throw AzureStorageException.ConditionNotMet();
        var ifUnmodified = ParseHttpDate(request.Headers, "If-Unmodified-Since");
        if (ifUnmodified.HasValue && container.LastModified > ifUnmodified.Value.AddSeconds(1))
            throw AzureStorageException.ConditionNotMet();
    }

    private static void EnsureLease(HttpRequest request, LeaseRecord lease, string resource)
    {
        lease = EffectiveLease(lease);
        var supplied = ProtocolParsing.First(request.Headers, "x-ms-lease-id");
        if (lease.State == LeaseState.Leased && !string.Equals(supplied, lease.Id, StringComparison.Ordinal))
            throw AzureStorageException.LeaseMismatch();
        if (lease.State != LeaseState.Leased && supplied is not null)
            throw new AzureStorageException(StatusCodes.Status412PreconditionFailed, $"LeaseNotPresentWith{CultureInfo.InvariantCulture.TextInfo.ToTitleCase(resource)}Operation", "There is currently no lease on the resource.");
    }

    private static LeaseRecord EffectiveLease(LeaseRecord lease)
    {
        var now = DateTimeOffset.UtcNow;
        if (lease.State == LeaseState.Breaking && lease.BreakEndsAt <= now)
            return new LeaseRecord { State = LeaseState.Broken };
        if (lease.State == LeaseState.Leased && lease.ExpiresAt <= now)
            return lease with { State = LeaseState.Expired, Id = null };
        return lease;
    }

    private static void RequireLeaseId(LeaseRecord current, string? suppliedId)
    {
        if (current.State != LeaseState.Leased)
            throw new AzureStorageException(StatusCodes.Status409Conflict, "LeaseNotPresentWithLeaseOperation", "There is currently no lease on the resource.");
        if (!string.Equals(current.Id, suppliedId, StringComparison.Ordinal))
            throw AzureStorageException.LeaseMismatch();
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
        bool useStandardContentType = true)
    {
        var (until, locked, legalHold) = ReadImmutabilityHeaders(request.Headers);
        return new BlobWriteOptions(
            ProtocolParsing.ReadHttpProperties(request.Headers, fallback?.Http, useStandardContentType),
            ProtocolParsing.ReadMetadata(request.Headers),
            ProtocolParsing.ReadTagsHeader(request.Headers),
            ProtocolParsing.First(request.Headers, "x-ms-access-tier") ?? fallback?.AccessTier,
            until,
            locked,
            legalHold);
    }

    private static BlobWriteOptions ReadUrlWriteOptions(HttpRequest request, UrlSource source)
    {
        var (until, locked, legalHold) = ReadImmutabilityHeaders(request.Headers);
        return new BlobWriteOptions(
            ProtocolParsing.ReadHttpProperties(request.Headers, source.Http),
            ProtocolParsing.ReadMetadata(request.Headers),
            ProtocolParsing.ReadTagsHeader(request.Headers),
            ProtocolParsing.First(request.Headers, "x-ms-access-tier"),
            until,
            locked,
            legalHold);
    }

    private static BlobWriteOptions ReadCopyWriteOptions(HttpRequest request, BlobRecord source)
    {
        var (until, locked, legalHold) = ReadImmutabilityHeaders(request.Headers);
        var hasReplacementMetadata = request.Headers.Keys.Any(name =>
            name.StartsWith("x-ms-meta-", StringComparison.OrdinalIgnoreCase));
        return new BlobWriteOptions(
            ProtocolParsing.ReadHttpProperties(request.Headers, source.Http),
            hasReplacementMetadata
                ? ProtocolParsing.ReadMetadata(request.Headers)
                : new Dictionary<string, string>(source.Metadata, StringComparer.OrdinalIgnoreCase),
            ProtocolParsing.ReadTagsHeader(request.Headers),
            ProtocolParsing.First(request.Headers, "x-ms-access-tier") ?? source.AccessTier,
            until,
            locked,
            legalHold);
    }

    private static (DateTimeOffset? Until, bool Locked, bool LegalHold) ReadImmutabilityHeaders(IHeaderDictionary headers)
    {
        var untilValue = ProtocolParsing.First(headers, "x-ms-immutability-policy-until-date");
        var modeValue = ProtocolParsing.First(headers, "x-ms-immutability-policy-mode");
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

        var legalHoldValue = ProtocolParsing.First(headers, "x-ms-legal-hold");
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
        var ifMatch = ProtocolParsing.First(request.Headers, "x-ms-source-if-match");
        if (!string.IsNullOrEmpty(ifMatch) && !MatchesETag(ifMatch, source.ETag, requireMatch: true))
            throw SourceConditionNotMet();
        var ifNoneMatch = ProtocolParsing.First(request.Headers, "x-ms-source-if-none-match");
        if (!string.IsNullOrEmpty(ifNoneMatch) && MatchesETag(ifNoneMatch, source.ETag, requireMatch: true))
            throw SourceConditionNotMet();
        var ifModified = ParseHttpDate(request.Headers, "x-ms-source-if-modified-since");
        if (ifModified.HasValue && source.LastModified <= ifModified.Value.AddSeconds(1))
            throw SourceConditionNotMet();
        var ifUnmodified = ParseHttpDate(request.Headers, "x-ms-source-if-unmodified-since");
        if (ifUnmodified.HasValue && source.LastModified > ifUnmodified.Value.AddSeconds(1))
            throw SourceConditionNotMet();
        var tagCondition = ProtocolParsing.First(request.Headers, "x-ms-source-if-tags");
        if (tagCondition is not null && !MatchesTagCondition(tagCondition, source.Tags))
            throw SourceConditionNotMet();
    }

    private static bool MatchesTagCondition(string expression, IReadOnlyDictionary<string, string> tags)
    {
        var match = System.Text.RegularExpressions.Regex.Match(
            expression,
            "^\\s*\"(?<key>[^\"]+)\"\\s*=\\s*'(?<value>[^']*)'\\s*$",
            System.Text.RegularExpressions.RegexOptions.CultureInvariant);
        if (!match.Success)
            throw AzureStorageException.InvalidHeader("x-ms-source-if-tags", expression);
        return tags.TryGetValue(match.Groups["key"].Value, out var value) &&
               string.Equals(value, match.Groups["value"].Value, StringComparison.Ordinal);
    }

    private static AzureStorageException SourceConditionNotMet() => new(
        StatusCodes.Status412PreconditionFailed,
        "SourceConditionNotMet",
        "The source condition specified using HTTP conditional header(s) is not met.");

    private static string SanitizeCopySource(string sourceValue)
    {
        if (sourceValue.Length > 2048 || !Uri.TryCreate(sourceValue, UriKind.Absolute, out var source))
            throw AzureStorageException.InvalidHeader("x-ms-copy-source", sourceValue);
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

    private static async Task WithIntegrityValidationAsync(HttpRequest request, Func<Stream, Task> action)
    {
        var expectedMd5 = ProtocolParsing.First(request.Headers, "Content-MD5");
        var expectedCrc64 = ProtocolParsing.First(request.Headers, "x-ms-content-crc64");
        if (expectedMd5 is not null && expectedCrc64 is not null)
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "BothCrc64AndMd5Specified",
                "Both CRC64 and MD5 were specified for the request. Specify only one checksum.");
        }
        if (expectedMd5 is null && expectedCrc64 is null)
        {
            await action(request.Body);
            return;
        }

        var expected = DecodeChecksum(
            expectedMd5 ?? expectedCrc64!,
            expectedMd5 is null ? 8 : 16,
            expectedMd5 is null ? "x-ms-content-crc64" : "Content-MD5");
        var paths = request.HttpContext.RequestServices.GetRequiredService<StoragePaths>();
        var options = request.HttpContext.RequestServices.GetRequiredService<IOptions<SavaOptions>>().Value;
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
            using var md5 = expectedMd5 is null ? null : IncrementalHash.CreateHash(HashAlgorithmName.MD5);
            var crc64 = expectedCrc64 is null ? null : new StorageCrc64();
            var buffer = new byte[128 * 1024];
            long length = 0;
            while (true)
            {
                var read = await request.Body.ReadAsync(buffer, request.HttpContext.RequestAborted);
                if (read == 0)
                    break;
                length = checked(length + read);
                if (length > options.MaximumRequestBodyBytes)
                    throw new RequestBodyTooLargeException(options.MaximumRequestBodyBytes);
                md5?.AppendData(buffer, 0, read);
                crc64?.Append(buffer.AsSpan(0, read));
                await temporary.WriteAsync(buffer.AsMemory(0, read), request.HttpContext.RequestAborted);
            }

            var actual = md5?.GetHashAndReset() ?? crc64!.GetHash();
            if (!CryptographicOperations.FixedTimeEquals(expected, actual))
            {
                throw new AzureStorageException(
                    StatusCodes.Status400BadRequest,
                    expectedMd5 is null ? "Crc64Mismatch" : "Md5Mismatch",
                    "The checksum specified in the request did not match the value calculated by the server.");
            }

            temporary.Position = 0;
            await action(temporary);
        }
        finally
        {
            if (File.Exists(temporaryPath))
                File.Delete(temporaryPath);
        }
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

    private static void EchoTransactionalChecksum(HttpContext http, bool sourceChecksum = false)
    {
        var md5 = ProtocolParsing.First(
            http.Request.Headers,
            sourceChecksum ? "x-ms-source-content-md5" : "Content-MD5");
        var crc64 = ProtocolParsing.First(
            http.Request.Headers,
            sourceChecksum ? "x-ms-source-content-crc64" : "x-ms-content-crc64");
        if (md5 is not null)
            http.Response.Headers.ContentMD5 = md5;
        if (crc64 is not null)
            http.Response.Headers["x-ms-content-crc64"] = crc64;
    }

    private static DateTimeOffset? ParseExpiry(IHeaderDictionary headers, DateTimeOffset metadataNow)
    {
        var option = ProtocolParsing.First(headers, "x-ms-expiry-option")?.ToLowerInvariant()
                     ?? throw AzureStorageException.InvalidHeader("x-ms-expiry-option");
        var value = ProtocolParsing.First(headers, "x-ms-expiry-time")
                    ?? throw AzureStorageException.InvalidHeader("x-ms-expiry-time");
        return option switch
        {
            "neverexpire" => null,
            "absolut" or "absolute" => DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var absolute)
                ? absolute
                : throw AzureStorageException.InvalidHeader("x-ms-expiry-time", value),
            "relativetonow" => long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var milliseconds)
                ? metadataNow.AddMilliseconds(milliseconds)
                : throw AzureStorageException.InvalidHeader("x-ms-expiry-time", value),
            "relativetocreation" => long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var fromCreation)
                ? metadataNow.AddMilliseconds(fromCreation)
                : throw AzureStorageException.InvalidHeader("x-ms-expiry-time", value),
            _ => throw AzureStorageException.InvalidHeader("x-ms-expiry-option", option)
        };
    }

    private static bool MatchesETag(string header, string etag, bool requireMatch) =>
        header.Trim() == "*" || header.Split(',').Select(item => item.Trim()).Any(item => string.Equals(item, etag, StringComparison.Ordinal));

    private static DateTimeOffset? ParseHttpDate(IHeaderDictionary headers, string name)
    {
        var value = ProtocolParsing.First(headers, name);
        if (value is null)
            return null;
        if (!DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed))
            throw AzureStorageException.InvalidHeader(name, value);
        return parsed;
    }

    private static int ParseMaxResults(string value, int defaultValue)
    {
        if (string.IsNullOrEmpty(value))
            return defaultValue;
        if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) || parsed is < 1 or > 5000)
            throw AzureStorageException.InvalidQuery("maxresults");
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
