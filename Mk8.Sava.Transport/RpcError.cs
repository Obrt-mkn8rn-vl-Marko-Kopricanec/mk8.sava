using Mk8.Sava.Protocol;

namespace Mk8.Sava.Transport;

internal sealed record RpcError(
    int StatusCode, string Code, string Message, string? HeaderName, string? HeaderValue,
    IReadOnlyDictionary<string, string>? ResponseHeaders, IReadOnlyDictionary<string, string>? Details)
{
    internal AzureStorageException ToException() => new(
        StatusCode, Code, Message, HeaderName, HeaderValue, ResponseHeaders, Details);

    internal static RpcError FromException(Exception exception)
    {
        var error = StorageExceptionMapper.Map(exception);
        return new RpcError(error.StatusCode, error.ErrorCode, error.Message, error.HeaderName, error.HeaderValue,
            error.ResponseHeaders, error.Details);
    }
}
