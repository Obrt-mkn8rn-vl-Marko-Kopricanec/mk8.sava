using System.Globalization;
using System.Text;
using System.Xml;
using Microsoft.Data.Sqlite;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

internal sealed class AzureExceptionMiddleware(RequestDelegate next, ILogger<AzureExceptionMiddleware> logger)
{
    public async Task InvokeAsync(HttpContext context)
    {
        try
        {
            await next(context).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
        {
            context.Abort();
        }
#pragma warning disable CA1031 // The outer HTTP boundary must translate unexpected failures to Azure's InternalError response.
        catch (Exception exception)
        {
            if (!IsKnownStorageException(exception) && logger.IsEnabled(LogLevel.Error))
                StorageLogMessages.StorageOperationFailed(logger, exception, GetRequestId(context));
            await WriteErrorAsync(context, MapException(exception)).ConfigureAwait(false);
        }
#pragma warning restore CA1031
    }

    private static bool IsKnownStorageException(Exception exception) =>
        exception is AzureStorageException or RequestBodyTooLargeException or StorageConcurrencyException or
            StorageImmutabilityException or StoragePendingCopyException or StorageBlobTypeMismatchException or
            StoragePathConflictException or XmlException;

    private static AzureStorageException MapException(Exception exception) => exception switch
    {
        AzureStorageException storage => storage,
        RequestBodyTooLargeException oversized => new AzureStorageException(
            StatusCodes.Status413PayloadTooLarge,
            "RequestBodyTooLarge",
            oversized.Message),
        StorageConcurrencyException => AzureStorageException.ConditionNotMet(),
        StorageImmutabilityException immutable => new AzureStorageException(
            StatusCodes.Status409Conflict,
            immutable.LegalHold ? "BlobImmutableDueToLegalHold" : "BlobImmutableDueToPolicy",
            immutable.Message),
        StoragePendingCopyException pending => new AzureStorageException(
            StatusCodes.Status409Conflict,
            "PendingCopyOperation",
            pending.Message),
        StorageBlobTypeMismatchException mismatch => new AzureStorageException(
            StatusCodes.Status409Conflict,
            "InvalidBlobType",
            mismatch.Message),
        StoragePathConflictException => AzureStorageException.PathAlreadyExists(),
        XmlException => new AzureStorageException(
            StatusCodes.Status400BadRequest,
            "InvalidXmlDocument",
            "The specified XML is not syntactically valid."),
        _ => new AzureStorageException(
            StatusCodes.Status500InternalServerError,
            "InternalError",
            "The server encountered an internal error. Please retry the request.")
    };

    private static async Task WriteErrorAsync(HttpContext context, AzureStorageException exception)
    {
        if (context.Response.HasStarted)
        {
            context.Abort();
            return;
        }

        context.Response.Clear();
        context.Response.StatusCode = exception.StatusCode;
        AddCommonHeaders(context);
        foreach (var header in exception.ResponseHeaders)
            context.Response.Headers[header.Key] = header.Value;
        context.Response.Headers["x-ms-error-code"] = exception.ErrorCode;
        if (exception.StatusCode == StatusCodes.Status304NotModified)
            return;

        if (exception.StatusCode == StatusCodes.Status401Unauthorized)
            context.Response.Headers.WWWAuthenticate = "Bearer resource_id=\"https://storage.azure.com/\"";

        if (HttpMethods.IsHead(context.Request.Method))
            return;

        context.Response.ContentType = "application/xml";

        var builder = new StringBuilder();
        using (var writer = XmlWriter.Create(builder, new XmlWriterSettings
        {
            OmitXmlDeclaration = true,
            Encoding = Encoding.UTF8,
            Indent = false,
            Async = true
        }))
        {
            await writer.WriteStartElementAsync(null, "Error", null).ConfigureAwait(false);
            await writer.WriteElementStringAsync(null, "Code", null, exception.ErrorCode).ConfigureAwait(false);
            await writer.WriteStartElementAsync(null, "Message", null).ConfigureAwait(false);
            await writer.WriteStringAsync(exception.Message).ConfigureAwait(false);
            await writer.WriteStringAsync("\nRequestId:").ConfigureAwait(false);
            await writer.WriteStringAsync(GetRequestId(context)).ConfigureAwait(false);
            await writer.WriteStringAsync("\nTime:").ConfigureAwait(false);
            await writer.WriteStringAsync(DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture)).ConfigureAwait(false);
            await writer.WriteEndElementAsync().ConfigureAwait(false);
            if (exception.HeaderName is not null)
                await writer.WriteElementStringAsync(null, "HeaderName", null, exception.HeaderName).ConfigureAwait(false);
            if (exception.HeaderValue is not null)
                await writer.WriteElementStringAsync(null, "HeaderValue", null, exception.HeaderValue).ConfigureAwait(false);
            foreach (var detail in exception.Details)
                await writer.WriteElementStringAsync(null, detail.Key, null, detail.Value).ConfigureAwait(false);
            await writer.WriteEndElementAsync().ConfigureAwait(false);
        }

        await context.Response.WriteAsync(builder.ToString(), context.RequestAborted).ConfigureAwait(false);
    }

    public static void AddCommonHeaders(HttpContext context)
    {
        context.Response.Headers["x-ms-request-id"] = GetRequestId(context);
        var request = StorageRequestContext.TryGet(context);
        if (request is null ||
            StorageServiceVersions.TryParse(request.ServiceVersion, out var version) &&
            version >= new DateOnly(2009, 9, 19))
        {
            context.Response.Headers["x-ms-version"] = request?.ServiceVersion ?? "2023-11-03";
        }
        context.Response.Headers.Date = DateTimeOffset.UtcNow.ToString("R", CultureInfo.InvariantCulture);
        context.Response.Headers.Server = "mk8.sava/1.0";
        if (context.Request.Headers.TryGetValue("x-ms-client-request-id", out var clientRequestId) &&
            clientRequestId.ToString().Length <= 1024)
        {
            context.Response.Headers["x-ms-client-request-id"] = clientRequestId;
        }
    }

    private static string GetRequestId(HttpContext context) =>
        StorageRequestContext.TryGet(context)?.RequestId
        ?? context.TraceIdentifier.Replace(":", string.Empty, StringComparison.Ordinal);
}
