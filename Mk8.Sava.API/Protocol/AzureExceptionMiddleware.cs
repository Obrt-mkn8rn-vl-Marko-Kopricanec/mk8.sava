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
        catch (AzureStorageException exception)
        {
            await WriteErrorAsync(context, exception).ConfigureAwait(false);
        }
        catch (RequestBodyTooLargeException exception)
        {
            await WriteErrorAsync(context, new AzureStorageException(
                StatusCodes.Status413PayloadTooLarge,
                "RequestBodyTooLarge",
                exception.Message)).ConfigureAwait(false);
        }
        catch (StorageConcurrencyException)
        {
            await WriteErrorAsync(context, AzureStorageException.ConditionNotMet()).ConfigureAwait(false);
        }
        catch (StorageImmutabilityException exception)
        {
            await WriteErrorAsync(context, new AzureStorageException(
                StatusCodes.Status409Conflict,
                exception.LegalHold ? "BlobImmutableDueToLegalHold" : "BlobImmutableDueToPolicy",
                exception.Message)).ConfigureAwait(false);
        }
        catch (StoragePendingCopyException exception)
        {
            await WriteErrorAsync(context, new AzureStorageException(
                StatusCodes.Status409Conflict,
                "PendingCopyOperation",
                exception.Message)).ConfigureAwait(false);
        }
        catch (StorageBlobTypeMismatchException exception)
        {
            await WriteErrorAsync(context, new AzureStorageException(
                StatusCodes.Status409Conflict,
                "InvalidBlobType",
                exception.Message)).ConfigureAwait(false);
        }
        catch (StoragePathConflictException)
        {
            await WriteErrorAsync(context, AzureStorageException.PathAlreadyExists()).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
        {
            context.Abort();
        }
        catch (XmlException)
        {
            await WriteErrorAsync(context, new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "InvalidXmlDocument",
                "The specified XML is not syntactically valid.")).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // The outer HTTP boundary must translate unexpected failures to Azure's InternalError response.
        catch (Exception exception)
        {
            logger.LogError(exception, "Storage operation failed for request {RequestId}.", GetRequestId(context));
            await WriteErrorAsync(context, new AzureStorageException(
                StatusCodes.Status500InternalServerError,
                "InternalError",
                "The server encountered an internal error. Please retry the request.")).ConfigureAwait(false);
        }
#pragma warning restore CA1031
    }

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
        if (exception.StatusCode == StatusCodes.Status304NotModified)
            return;

        context.Response.Headers["x-ms-error-code"] = exception.ErrorCode;
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
            Indent = false
        }))
        {
            writer.WriteStartElement("Error");
            writer.WriteElementString("Code", exception.ErrorCode);
            writer.WriteStartElement("Message");
            writer.WriteString(exception.Message);
            writer.WriteString("\nRequestId:");
            writer.WriteString(GetRequestId(context));
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
