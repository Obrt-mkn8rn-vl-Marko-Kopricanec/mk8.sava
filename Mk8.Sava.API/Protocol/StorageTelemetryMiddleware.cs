using System.Diagnostics;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

public sealed class StorageTelemetryMiddleware(
    RequestDelegate next,
    IStorageTelemetry telemetry,
    ILogger<StorageTelemetryMiddleware> logger)
{
    public async Task InvokeAsync(HttpContext context)
    {
        if (context.Request.Path.StartsWithSegments("/health") ||
            context.Request.Path.StartsWithSegments("/metrics"))
        {
            await next(context);
            return;
        }

        var started = Stopwatch.GetTimestamp();
        try
        {
            await next(context);
        }
        finally
        {
            var elapsed = Stopwatch.GetTimestamp() - started;
            telemetry.RecordRequest(context.Response.StatusCode, elapsed);
            var request = StorageRequestContext.TryGet(context);
            logger.LogInformation(
                "Storage request {RequestId} {Method} {ResourceKind} completed with {StatusCode} in {ElapsedMilliseconds:F3} ms.",
                request?.RequestId ?? context.TraceIdentifier,
                context.Request.Method,
                request?.ResourceKind.ToString() ?? "Unknown",
                context.Response.StatusCode,
                elapsed * 1000d / Stopwatch.Frequency);
        }
    }
}
