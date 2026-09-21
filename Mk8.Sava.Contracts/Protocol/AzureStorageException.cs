using System.Net;

namespace Mk8.Sava.Protocol;

public sealed class AzureStorageException : Exception
{
    public AzureStorageException(
        int statusCode,
        string errorCode,
        string message,
        string? headerName = null,
        string? headerValue = null,
        IReadOnlyDictionary<string, string>? responseHeaders = null,
        IReadOnlyDictionary<string, string>? details = null)
        : base(message)
    {
        StatusCode = statusCode;
        ErrorCode = errorCode;
        HeaderName = headerName;
        HeaderValue = headerValue;
        ResponseHeaders = responseHeaders ?? EmptyValues;
        Details = details ?? EmptyValues;
    }

    private static readonly IReadOnlyDictionary<string, string> EmptyValues =
        new Dictionary<string, string>(StringComparer.Ordinal);

    public int StatusCode { get; }
    public string ErrorCode { get; }
    public string? HeaderName { get; }
    public string? HeaderValue { get; }
    public IReadOnlyDictionary<string, string> ResponseHeaders { get; }
    public IReadOnlyDictionary<string, string> Details { get; }

    public static AzureStorageException InvalidQuery(string parameter) => new(
        (int)HttpStatusCode.BadRequest,
        "InvalidQueryParameterValue",
        "Value for one of the query parameters specified in the request URI is invalid.",
        parameter);

    public static AzureStorageException InvalidHeader(string header, string? value = null) => new(
        (int)HttpStatusCode.BadRequest,
        "InvalidHeaderValue",
        "The value for one of the HTTP headers is not in the correct format.",
        header,
        value);

    public static AzureStorageException MissingHeader(string header) => new(
        (int)HttpStatusCode.BadRequest,
        "MissingRequiredHeader",
        "An HTTP header that's mandatory for this request isn't specified.",
        header);

    public static AzureStorageException AuthenticationFailed(string detail = "Server failed to authenticate the request.") => new(
        (int)HttpStatusCode.Forbidden,
        "AuthenticationFailed",
        detail);

    public static AzureStorageException AuthorizationFailure() => new(
        (int)HttpStatusCode.Forbidden,
        "AuthorizationFailure",
        "This request is not authorized to perform this operation.");

    public static AzureStorageException AuthorizationPermissionMismatch() => new(
        (int)HttpStatusCode.Forbidden,
        "AuthorizationPermissionMismatch",
        "This request is not authorized to perform this operation using this permission.");

    public static AzureStorageException BearerAuthenticationRequired() => new(
        (int)HttpStatusCode.Unauthorized,
        "AuthenticationFailed",
        "Authentication failed for the supplied bearer token.");

    public static AzureStorageException ContainerNotFound() => new(
        (int)HttpStatusCode.NotFound,
        "ContainerNotFound",
        "The specified container does not exist.");

    public static AzureStorageException BlobNotFound() => new(
        (int)HttpStatusCode.NotFound,
        "BlobNotFound",
        "The specified blob does not exist.");

    public static AzureStorageException ConditionNotMet() => new(
        (int)HttpStatusCode.PreconditionFailed,
        "ConditionNotMet",
        "The condition specified using HTTP conditional header(s) is not met.");

    public static AzureStorageException NotModified() => new(
        (int)HttpStatusCode.NotModified,
        "ConditionNotMet",
        "The condition specified using HTTP conditional header(s) is not met.");

    public static AzureStorageException SourceConditionNotMet() => new(
        (int)HttpStatusCode.PreconditionFailed,
        "SourceConditionNotMet",
        "The source condition specified using HTTP conditional header(s) is not met.");

    public static AzureStorageException MultipleConditionHeadersNotSupported() => new(
        (int)HttpStatusCode.BadRequest,
        "MultipleConditionHeadersNotSupported",
        "Multiple condition headers are not supported.");

    public static AzureStorageException LeaseIdMissing(string resource) => new(
        (int)HttpStatusCode.PreconditionFailed,
        "LeaseIdMissing",
        $"There is currently a lease on the {resource} and no lease ID was specified in the request.");

    public static AzureStorageException LeaseOperationMismatch(string resource) => new(
        (int)HttpStatusCode.PreconditionFailed,
        resource == "container" ? "LeaseIdMismatchWithContainerOperation" : "LeaseIdMismatchWithBlobOperation",
        $"The lease ID specified did not match the lease ID for the {resource}.");

    public static AzureStorageException LeaseNotPresentForOperation(string resource) => new(
        (int)HttpStatusCode.PreconditionFailed,
        resource == "container" ? "LeaseNotPresentWithContainerOperation" : "LeaseNotPresentWithBlobOperation",
        $"A lease ID was specified, but there is currently no active lease on the {resource}.");

    public static AzureStorageException LeaseIdMismatchWithLeaseOperation() => new(
        (int)HttpStatusCode.Conflict,
        "LeaseIdMismatchWithLeaseOperation",
        "The lease ID specified did not match the lease ID for the container or blob.");

    public static AzureStorageException LeaseNotPresentWithLeaseOperation() => new(
        (int)HttpStatusCode.Conflict,
        "LeaseNotPresentWithLeaseOperation",
        "There is currently no lease on the container or blob.");
}
