using System.Threading.RateLimiting;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Hosting;

internal sealed class GatewayAdmissionMiddleware(RequestDelegate next, GatewayAdmission admission)
{
    public async Task InvokeAsync(HttpContext context)
    {
        if (context.Request.Path.StartsWithSegments("/health", StringComparison.OrdinalIgnoreCase) ||
            context.Request.Path.StartsWithSegments("/metrics", StringComparison.OrdinalIgnoreCase))
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        var scope = context.Features.Get<GatewayRequestAdmissionScope>()
            ?? throw new InvalidOperationException("Gateway telemetry must own the request admission scope.");
        RateLimitLease? permit = await admission.AcquireAsync(context.RequestAborted).ConfigureAwait(false);
        try
        {
            if (!permit.IsAcquired)
                throw new AzureStorageException(503, "ServerBusy", "The server is busy. Please retry the request.");
            scope.Attach(permit);
            permit = null;
        }
        finally
        {
            permit?.Dispose();
        }
        await next(context).ConfigureAwait(false);
    }
}
