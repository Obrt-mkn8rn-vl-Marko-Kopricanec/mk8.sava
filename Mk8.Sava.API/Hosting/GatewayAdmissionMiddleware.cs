using System.Threading.RateLimiting;
using Microsoft.Extensions.Options;
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

        using var permit = await admission.AcquireAsync(context.RequestAborted).ConfigureAwait(false);
        if (!permit.IsAcquired)
            throw new AzureStorageException(503, "ServerBusy", "The server is busy. Please retry the request.");
        await next(context).ConfigureAwait(false);
    }
}
