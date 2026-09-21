namespace Mk8.Sava.Protocol;

public sealed class AzureStorageException : Exception
{
    public AzureStorageException(
        int statusCode,
        string errorCode,
        string message,
        string? headerName = null,
        string? headerValue = null)
        : base(message)
    {
        StatusCode = statusCode;
        ErrorCode = errorCode;
        HeaderName = headerName;
        HeaderValue = headerValue;
    }

    public int StatusCode { get; }
    public string ErrorCode { get; }
    public string? HeaderName { get; }
    public string? HeaderValue { get; }

    public static AzureStorageException InvalidQuery(string parameter) => new(
        StatusCodes.Status400BadRequest,
        "InvalidQueryParameterValue",
        "Value for one of the query parameters specified in the request URI is invalid.",
        parameter);

    public static AzureStorageException InvalidHeader(string header, string? value = null) => new(
        StatusCodes.Status400BadRequest,
        "InvalidHeaderValue",
        "The value for one of the HTTP headers is not in the correct format.",
        header,
        value);

    public static AzureStorageException AuthenticationFailed(string detail = "Server failed to authenticate the request.") => new(
        StatusCodes.Status403Forbidden,
        "AuthenticationFailed",
        detail);

    public static AzureStorageException AuthorizationFailure() => new(
        StatusCodes.Status403Forbidden,
        "AuthorizationFailure",
        "This request is not authorized to perform this operation.");

    public static AzureStorageException BearerAuthenticationRequired() => new(
        StatusCodes.Status401Unauthorized,
        "AuthenticationFailed",
        "Authentication failed for the supplied bearer token.");

    public static AzureStorageException ContainerNotFound() => new(
        StatusCodes.Status404NotFound,
        "ContainerNotFound",
        "The specified container does not exist.");

    public static AzureStorageException BlobNotFound() => new(
        StatusCodes.Status404NotFound,
        "BlobNotFound",
        "The specified blob does not exist.");

    public static AzureStorageException ConditionNotMet() => new(
        StatusCodes.Status412PreconditionFailed,
        "ConditionNotMet",
        "The condition specified using HTTP conditional header(s) is not met.");

    public static AzureStorageException LeaseMismatch() => new(
        StatusCodes.Status412PreconditionFailed,
        "LeaseIdMismatchWithBlobOperation",
        "The lease ID specified did not match the lease ID for the blob.");
}
