using System.Diagnostics;
using System.Net;
using System.Text.Json;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

internal static class SplitProcessReadinessProbe
{
    private const int MaximumDiagnosticBytes = 4096;

    internal static async Task AssertReadyAsync(HttpClient client, Uri address, Func<string> describeHost)
    {
        var started = Stopwatch.GetTimestamp();
        HttpStatusCode? status = null;
        var body = "not observed";
        try
        {
            // Keep the existing one buffered GET and the caller's original HttpClient timeout.
            // No retry or second probe can turn an unavailable observation into a pass.
            using var response = await client.GetAsync(new Uri(address, "/health/ready")).ConfigureAwait(false);
            status = response.StatusCode;
            if (status != HttpStatusCode.OK)
                body = await DescribeBodyAsync(response.Content).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }
        catch (Exception failure) when (!CatastrophicExceptionPolicy.Contains(failure))
        {
            string host;
            try
            {
                host = describeHost();
            }
            catch (Exception snapshotFailure) when (!CatastrophicExceptionPolicy.Contains(snapshotFailure))
            {
                throw new AggregateException("Gateway readiness and host observation failed.", failure, snapshotFailure);
            }
            var diagnostic = $"Gateway readiness observation failed. Elapsed={Stopwatch.GetElapsedTime(started)}, " +
                $"Status={(status is null ? "not observed" : ((int)status.Value).ToString(System.Globalization.CultureInfo.InvariantCulture))}, " +
                $"Body={body}. Host observations precede fixture cleanup and are sequential, not atomic.\n{host}";
            if (failure is OperationCanceledException canceled)
                throw new OperationCanceledException(diagnostic, canceled, canceled.CancellationToken);
            throw new InvalidOperationException(diagnostic, failure);
        }
    }

    private static async Task<string> DescribeBodyAsync(HttpContent content)
    {
        // GetAsync completed ResponseContentRead above; this borrows the already-buffered body.
        // Read only a bounded prefix plus one byte to distinguish oversized diagnostics.
        var stream = await content.ReadAsStreamAsync().ConfigureAwait(false);
        var buffer = new byte[MaximumDiagnosticBytes + 1];
        var length = 0;
        while (length < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(length)).ConfigureAwait(false);
            if (read == 0)
                break;
            length += read;
        }
        if (length > MaximumDiagnosticBytes)
            return "oversized (fields omitted)";
        try
        {
            using var document = JsonDocument.Parse(buffer.AsMemory(0, length), new JsonDocumentOptions { MaxDepth = 8 });
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
                return "invalid JSON shape (fields omitted)";
            var summary = $"status={State(root, "status", "ready", "unavailable")}, " +
                $"component={State(root, "component", "gateway")}, " +
                $"application={State(root, "application", "ready", "unavailable")}, " +
                $"metadata={State(root, "metadata", "ready", "unavailable")}";
            if (root.TryGetProperty("integrity", out var integrity) && integrity.ValueKind == JsonValueKind.Object)
                summary += $", integrity={State(integrity, "status", "healthy", "corrupt")}";
            return summary;
        }
        catch (JsonException failure) when (!CatastrophicExceptionPolicy.Contains(failure))
        {
            return "invalid JSON (fields omitted)";
        }
    }

    private static string State(JsonElement value, string name, params string[] allowed)
    {
        if (!value.TryGetProperty(name, out var property))
            return "absent";
        return property.ValueKind == JsonValueKind.String && property.GetString() is { } text &&
            allowed.Contains(text, StringComparer.Ordinal) ? text : "unrecognized";
    }
}
