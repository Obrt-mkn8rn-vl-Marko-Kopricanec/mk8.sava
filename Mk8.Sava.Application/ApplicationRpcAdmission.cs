using System.Globalization;
using System.Text;
using System.Threading.RateLimiting;
using Microsoft.Extensions.Options;
using Mk8.Sava.Protocol;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Application;

internal sealed class ApplicationRpcAdmission : IApplicationRpcAdmission, IDisposable
{
    private readonly RpcLaneLimiter _bulk;
    private readonly RpcLaneLimiter _control;
    private readonly TimeSpan _queueTimeout;

    public ApplicationRpcAdmission(IOptions<ApplicationHostingOptions> configuredOptions)
    {
        ArgumentNullException.ThrowIfNull(configuredOptions);
        var options = configuredOptions.Value;
        _bulk = new RpcLaneLimiter(options.MaximumConcurrentRpcRequests, options.MaximumQueuedRpcRequests);
        _control = new RpcLaneLimiter(4, 32);
        _queueTimeout = options.RpcQueueTimeout;
    }

    public async ValueTask<IAsyncDisposable> AcquireAsync(ApplicationRpcLane lane, CancellationToken cancellationToken)
    {
        var limiter = lane switch
        {
            ApplicationRpcLane.Bulk => _bulk,
            ApplicationRpcLane.Control => _control,
            _ => throw new ArgumentOutOfRangeException(nameof(lane), lane, "The Application RPC lane is invalid.")
        };
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(_queueTimeout);
        RateLimitLease lease;
        try
        {
            lease = await limiter.AcquireAsync(deadline.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException exception) when (deadline.IsCancellationRequested &&
            !cancellationToken.IsCancellationRequested && !CatastrophicExceptionPolicy.Contains(exception))
        {
            limiter.RecordRejection();
            throw Busy();
        }
        if (!lease.IsAcquired)
        {
            lease.Dispose();
            limiter.RecordRejection();
            throw Busy();
        }
        return new RpcAdmissionLease(lease);
    }

    internal string RenderMetrics()
    {
        var builder = new StringBuilder(1024);
        builder.Append("# HELP mk8_sava_application_rpc_active_requests Currently admitted private Application RPC requests.\n# TYPE mk8_sava_application_rpc_active_requests gauge\n");
        builder.Append("# HELP mk8_sava_application_rpc_queued_requests Authenticated private RPC requests waiting for Application admission.\n# TYPE mk8_sava_application_rpc_queued_requests gauge\n");
        builder.Append("# HELP mk8_sava_application_rpc_rejected_requests_total Private RPC requests rejected by full queues or queue timeout.\n# TYPE mk8_sava_application_rpc_rejected_requests_total counter\n");
        _bulk.AppendMetrics(builder, "bulk");
        _control.AppendMetrics(builder, "control");
        return builder.ToString();
    }

    public void Dispose()
    {
        _bulk.Dispose();
        _control.Dispose();
        GC.SuppressFinalize(this);
    }

    private static AzureStorageException Busy() => new(
        503, "ServerBusy", "The storage application has reached its RPC admission limit.",
        responseHeaders: new Dictionary<string, string>(StringComparer.Ordinal) { ["Retry-After"] = "1" });

    private sealed class RpcLaneLimiter : IDisposable
    {
        private readonly ConcurrencyLimiter _limiter;
        private readonly int _permits;
        private long _rejected;

        public RpcLaneLimiter(int permits, int queueLimit)
        {
            _permits = permits;
            _limiter = new ConcurrencyLimiter(new ConcurrencyLimiterOptions
            {
                PermitLimit = permits,
                QueueLimit = queueLimit,
                QueueProcessingOrder = QueueProcessingOrder.OldestFirst
            });
        }

        public ValueTask<RateLimitLease> AcquireAsync(CancellationToken cancellationToken) =>
            _limiter.AcquireAsync(1, cancellationToken);

        public void RecordRejection() => Interlocked.Increment(ref _rejected);

        public void AppendMetrics(StringBuilder builder, string lane)
        {
            var statistics = _limiter.GetStatistics();
            AppendValue(builder, "mk8_sava_application_rpc_active_requests", lane,
                _permits - (statistics?.CurrentAvailablePermits ?? _permits));
            AppendValue(builder, "mk8_sava_application_rpc_queued_requests", lane, statistics?.CurrentQueuedCount ?? 0);
            AppendValue(builder, "mk8_sava_application_rpc_rejected_requests_total", lane, Interlocked.Read(ref _rejected));
        }

        private static void AppendValue(StringBuilder builder, string metric, string lane, long value) =>
            builder.Append(metric).Append("{lane=\"").Append(lane).Append("\"} ")
                .Append(value.ToString(CultureInfo.InvariantCulture)).Append('\n');

        public void Dispose()
        {
            _limiter.Dispose();
            GC.SuppressFinalize(this);
        }
    }

    private sealed class RpcAdmissionLease(RateLimitLease lease) : IAsyncDisposable, IDisposable
    {
        private readonly RateLimitLease _lease = lease;
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
                _lease.Dispose();
            GC.SuppressFinalize(this);
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
