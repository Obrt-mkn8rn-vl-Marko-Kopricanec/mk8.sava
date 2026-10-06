namespace Mk8.Sava.Transport;

internal static class RpcStreamLimits
{
    internal const long MaximumBlobBytes = 50_000L * 4_000 * 1024 * 1024;

    internal static long InputLimit(RpcMethod method, object?[] arguments, long bodyLimit)
    {
        if (string.Equals(method.Method.Name, "CopyBlockBlobFromStreamAsync", StringComparison.Ordinal) ||
            string.Equals(method.Method.Name, "BeginCopyFromStreamAsync", StringComparison.Ordinal))
        {
            var contentLength = ApplicationRpcEndpoint.ReadLongArgument(method, arguments, "contentLength");
            if (contentLength < 0 || contentLength > MaximumBlobBytes)
                throw new InvalidDataException("The application copy-source length is invalid.");
            return contentLength;
        }
        return Math.Min(bodyLimit, MaximumBlobBytes);
    }
}
