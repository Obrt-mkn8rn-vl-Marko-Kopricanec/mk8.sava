using System.Threading.RateLimiting;
using System.Globalization;
using Microsoft.Extensions.Options;

namespace Mk8.Sava.Hosting;

internal sealed class GatewayAdmission : IDisposable
{
    private readonly ConcurrencyLimiter _limiter;
    private readonly int _permits;
    private long _rejected;

    public GatewayAdmission(IOptions<GatewayOptions> configuredOptions)
    {
        var options = configuredOptions.Value;
        _permits = options.MaximumConcurrentRequests;
        _limiter = new ConcurrencyLimiter(new ConcurrencyLimiterOptions
        {
            PermitLimit = options.MaximumConcurrentRequests,
            QueueLimit = options.MaximumQueuedRequests,
            QueueProcessingOrder = QueueProcessingOrder.OldestFirst
        });
    }

    public async ValueTask<RateLimitLease> AcquireAsync(CancellationToken cancellationToken)
    {
        var lease = await _limiter.AcquireAsync(1, cancellationToken).ConfigureAwait(false);
        if (!lease.IsAcquired)
            Interlocked.Increment(ref _rejected);
        return lease;
    }

    internal string RenderMetrics()
    {
        var statistics = _limiter.GetStatistics();
        return string.Create(CultureInfo.InvariantCulture, $"""
            # HELP mk8_sava_gateway_active_requests Currently admitted public storage requests.
            # TYPE mk8_sava_gateway_active_requests gauge
            mk8_sava_gateway_active_requests {_permits - (statistics?.CurrentAvailablePermits ?? _permits)}
            # HELP mk8_sava_gateway_queued_requests Public storage requests waiting for Gateway admission.
            # TYPE mk8_sava_gateway_queued_requests gauge
            mk8_sava_gateway_queued_requests {statistics?.CurrentQueuedCount ?? 0}
            # HELP mk8_sava_gateway_rejected_requests_total Gateway admission rejections.
            # TYPE mk8_sava_gateway_rejected_requests_total counter
            mk8_sava_gateway_rejected_requests_total {Interlocked.Read(ref _rejected)}

            """);
    }

    public void Dispose()
    {
        _limiter.Dispose();
        GC.SuppressFinalize(this);
    }
}
