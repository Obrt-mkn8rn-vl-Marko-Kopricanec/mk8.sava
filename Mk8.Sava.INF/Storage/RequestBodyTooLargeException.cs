namespace Mk8.Sava.Storage;

#pragma warning disable CA1032 // A valid size-limit exception must carry the configured maximum.
public sealed class RequestBodyTooLargeException(long maximumLength)
#pragma warning restore CA1032
    : Exception($"The request body exceeds the configured limit of {maximumLength} bytes.")
{
    public long MaximumLength { get; } = maximumLength;
}
