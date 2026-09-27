using System.Text.Json;

namespace Mk8.Sava.Protocol;

internal static class BlobQueryResources
{
    public const long BufferedInputBytes = 256 * 1024;

    public static void ValidateInput(long estimatedBytes, long maximumBytes)
    {
        if (estimatedBytes > maximumBytes)
        {
            throw new AzureStorageException(
                503, "ServerBusy", "The query input exceeds this deployment's configured record capacity.",
                responseHeaders: new Dictionary<string, string>(StringComparer.Ordinal) { ["Retry-After"] = "1" });
        }
    }

    public static void ValidateJsonRow(JsonElement element, long retainedBytes, long maximumBytes,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (element.ValueKind == JsonValueKind.Object)
        {
            foreach (var _ in element.EnumerateObject())
            {
                cancellationToken.ThrowIfCancellationRequested();
                retainedBytes += 256;
                ValidateInput(retainedBytes, maximumBytes);
            }
            return;
        }
        var count = element.ValueKind == JsonValueKind.Array ? element.GetArrayLength() : 1;
        ValidateInput(retainedBytes + 256L * count, maximumBytes);
    }
}
