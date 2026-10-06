using System.Threading.RateLimiting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Mk8.Sava.Hosting;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class GatewayAdmissionPipelineTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnalyticsRetainsAdmissionAndOperatorsBypassSaturation(bool endpointFails)
    {
        using var admission = CreateAdmission();
        var analytics = new ControlledAnalyticsSink();
        var clock = new ManualDeadlineTimeProvider();
        var telemetry = new StorageTelemetry();
        var pipeline = CreatePipeline(http =>
        {
            if (http.Request.Path.StartsWithSegments("/health", StringComparison.OrdinalIgnoreCase) ||
                http.Request.Path.StartsWithSegments("/metrics", StringComparison.OrdinalIgnoreCase))
            {
                http.Response.StatusCode = StatusCodes.Status200OK;
                return Task.CompletedTask;
            }
            SetStorageContext(http);
            if (endpointFails)
                throw new InvalidOperationException("ordinary endpoint failure");
            http.Response.StatusCode = StatusCodes.Status201Created;
            return Task.CompletedTask;
        }, admission, analytics, telemetry, clock);
        using var firstBody = new MemoryStream();
        var first = CreateContext(firstBody);
        var completion = Record.ExceptionAsync(() => pipeline.InvokeAsync(first));
        Exception? failure;
        try
        {
            var recorded = await analytics.Entered.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(true);
            Assert.Equal(endpointFails ? StatusCodes.Status500InternalServerError : StatusCodes.Status201Created,
                recorded.StatusCode);
            Assert.False(completion.IsCompleted);
            AssertActive(admission, 1);

            using var rejectedBody = new MemoryStream();
            var rejected = CreateContext(rejectedBody);
            await pipeline.InvokeAsync(rejected).ConfigureAwait(true);
            Assert.Equal(StatusCodes.Status503ServiceUnavailable, rejected.Response.StatusCode);
            Assert.Equal("ServerBusy", rejected.Response.Headers["x-ms-error-code"]);
            Assert.Contains("mk8_sava_gateway_rejected_requests_total 1\n", admission.RenderMetrics(), StringComparison.Ordinal);

            await AssertOperatorsBypassAsync(pipeline).ConfigureAwait(true);
            Assert.Equal(1, analytics.Attempts);
            AssertActive(admission, 1);
            var metrics = telemetry.RenderPrometheus();
            Assert.Contains("mk8_sava_http_requests_total 5\n", metrics, StringComparison.Ordinal);
            Assert.Contains(endpointFails
                ? "mk8_sava_http_server_errors_total 2\n"
                : "mk8_sava_http_server_errors_total 1\n", metrics, StringComparison.Ordinal);
        }
        finally
        {
            analytics.Release();
            failure = await completion.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        }
        Assert.Null(failure);
        AssertActive(admission, 0);
    }

    [Fact]
    public async Task OrdinaryAnalyticsFailurePreservesTheCompletedResponseAndReleasesAdmission()
    {
        using var admission = CreateAdmission();
        var analytics = new ControlledAnalyticsSink(new IOException("analytics unavailable"));
        analytics.Release();
        var telemetry = new StorageTelemetry();
        var pipeline = CreatePipeline(CompleteStorageRequestAsync, admission, analytics, telemetry);
        using var body = new MemoryStream();
        var context = CreateContext(body);

        await pipeline.InvokeAsync(context).ConfigureAwait(true);

        Assert.Equal(StatusCodes.Status201Created, context.Response.StatusCode);
        Assert.False(context.Response.Headers.ContainsKey("x-ms-error-code"));
        Assert.Equal(1, analytics.Attempts);
        AssertActive(admission, 0);
        Assert.Contains("mk8_sava_http_server_errors_total 0\n", telemetry.RenderPrometheus(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FatalEndpointOrAnalyticsFailureEscapesAndReleasesAdmission(bool analyticsFails)
    {
#pragma warning disable CA2201 // Inject a reserved fatal graph to verify finalization cannot normalize it or leak the permit.
        var fatal = new InvalidOperationException("injected fatal graph", new OutOfMemoryException());
#pragma warning restore CA2201
        using var admission = CreateAdmission();
        var analytics = new ControlledAnalyticsSink(analyticsFails ? fatal : null);
        var pipeline = CreatePipeline(http =>
        {
            SetStorageContext(http);
            http.Response.StatusCode = StatusCodes.Status201Created;
            return analyticsFails ? Task.CompletedTask : throw fatal;
        }, admission, analytics, timeProvider: new ManualDeadlineTimeProvider());
        using var body = new MemoryStream();
        var context = CreateContext(body);
        var completion = Record.ExceptionAsync(() => pipeline.InvokeAsync(context));
        Exception? escaped;
        try
        {
            await analytics.Entered.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(true);
            AssertActive(admission, 1);
            Assert.False(completion.IsCompleted);
        }
        finally
        {
            analytics.Release();
            escaped = await completion.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        }

        Assert.Same(fatal, escaped);
        Assert.Equal(StatusCodes.Status201Created, context.Response.StatusCode);
        Assert.False(context.Response.Headers.ContainsKey("x-ms-error-code"));
        AssertActive(admission, 0);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnalyticsHasAnIndependentFiveSecondBudgetAndTimeoutReleasesAdmission(bool clientDisconnects)
    {
        using var admission = CreateAdmission();
        using var callerCancellation = new CancellationTokenSource();
        var analytics = new ControlledAnalyticsSink();
        var clock = new ManualDeadlineTimeProvider();
        var pipeline = CreatePipeline(CompleteStorageRequestAsync, admission, analytics, timeProvider: clock);
        using var body = new MemoryStream();
        var context = CreateContext(body);
        context.RequestAborted = callerCancellation.Token;
        var completion = Record.ExceptionAsync(() => pipeline.InvokeAsync(context));
        Exception? failure;
        try
        {
            await analytics.Entered.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(true);
            if (clientDisconnects)
                await callerCancellation.CancelAsync().ConfigureAwait(true);
            Assert.True(analytics.CancellationToken.CanBeCanceled);
            Assert.False(analytics.CancellationToken.IsCancellationRequested);
            Assert.Equal(TimeSpan.FromSeconds(5), clock.DueTime);
            Assert.Equal(Timeout.InfiniteTimeSpan, clock.Period);
            AssertActive(admission, 1);

            clock.Expire();
            failure = await completion.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(true);
            Assert.True(analytics.CancellationToken.IsCancellationRequested);
            Assert.Null(failure);
            AssertActive(admission, 0);
            Assert.Equal(StatusCodes.Status201Created, context.Response.StatusCode);
            Assert.False(context.Response.Headers.ContainsKey("x-ms-error-code"));
        }
        finally
        {
            analytics.Release();
            await completion.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        }
    }

    [Fact]
    public async Task AbortedEndpointStillAttemptsBoundedAnalyticsAndReleasesAdmission()
    {
        using var admission = CreateAdmission();
        using var callerCancellation = new CancellationTokenSource();
        var analytics = new ControlledAnalyticsSink();
        var clock = new ManualDeadlineTimeProvider();
        var pipeline = CreatePipeline(async http =>
        {
            SetStorageContext(http);
            await callerCancellation.CancelAsync().ConfigureAwait(true);
            throw new OperationCanceledException(callerCancellation.Token);
        }, admission, analytics, timeProvider: clock);
        using var body = new MemoryStream();
        var context = CreateContext(body);
        context.RequestAborted = callerCancellation.Token;
        var completion = Record.ExceptionAsync(() => pipeline.InvokeAsync(context));
        try
        {
            await analytics.Entered.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(true);
            Assert.True(callerCancellation.IsCancellationRequested);
            Assert.False(analytics.CancellationToken.IsCancellationRequested);
            AssertActive(admission, 1);
            Assert.False(context.Response.Headers.ContainsKey("x-ms-error-code"));
            Assert.Empty(body.ToArray());

            clock.Expire();
            Assert.Null(await completion.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(true));
            AssertActive(admission, 0);
        }
        finally
        {
            analytics.Release();
            await completion.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        }
    }

    [Fact]
    public async Task FinalMetricsFailureStillReleasesAdmissionBeforeEscaping()
    {
        var failure = new InvalidOperationException("final metrics failed");
        using var admission = CreateAdmission();
        var analytics = new ControlledAnalyticsSink();
        var pipeline = CreatePipeline(CompleteStorageRequestAsync, admission, analytics, new FaultingTelemetry(failure));
        using var body = new MemoryStream();
        var context = CreateContext(body);

        var escaped = await Assert.ThrowsAsync<InvalidOperationException>(() => pipeline.InvokeAsync(context)).ConfigureAwait(true);

        Assert.Same(failure, escaped);
        Assert.Equal(0, analytics.Attempts);
        AssertActive(admission, 0);
    }

    [Fact]
    public async Task MissingOuterTelemetryScopeFailsClosedWithoutAcquiringAdmission()
    {
        using var admission = CreateAdmission();
        var endpointCalls = 0;
        var middleware = new GatewayAdmissionMiddleware(_ =>
        {
            Interlocked.Increment(ref endpointCalls);
            return Task.CompletedTask;
        }, admission);
        var exceptionBoundary = new AzureExceptionMiddleware(middleware.InvokeAsync, NullLogger<AzureExceptionMiddleware>.Instance);
        using var body = new MemoryStream();
        var context = CreateContext(body);

        await exceptionBoundary.InvokeAsync(context).ConfigureAwait(true);

        Assert.Equal(StatusCodes.Status500InternalServerError, context.Response.StatusCode);
        Assert.Equal("InternalError", context.Response.Headers["x-ms-error-code"]);
        Assert.Equal(0, endpointCalls);
        AssertActive(admission, 0);
        Assert.Contains("mk8_sava_gateway_rejected_requests_total 0\n", admission.RenderMetrics(), StringComparison.Ordinal);
    }

    [Fact]
    public void AdmissionScopeDisposesItsPermitOnlyOnce()
    {
        using var permit = new CountingLease();
        using var scope = new GatewayRequestAdmissionScope();
        scope.Attach(permit);

        scope.Dispose();
        scope.Dispose();

        Assert.Equal(1, permit.Disposals);
        Assert.Throws<ObjectDisposedException>(() => scope.Attach(permit));
    }

    private static GatewayAdmission CreateAdmission() => new(Options.Create(new GatewayOptions
    {
        MaximumConcurrentRequests = 1,
        MaximumQueuedRequests = 0
    }));

    private static StorageTelemetryMiddleware CreatePipeline(RequestDelegate endpoint, GatewayAdmission admission,
        ControlledAnalyticsSink analytics, IStorageTelemetry? telemetry = null, TimeProvider? timeProvider = null)
    {
        var admissionMiddleware = new GatewayAdmissionMiddleware(endpoint, admission);
        var exceptionBoundary = new AzureExceptionMiddleware(admissionMiddleware.InvokeAsync, NullLogger<AzureExceptionMiddleware>.Instance);
        return new StorageTelemetryMiddleware(exceptionBoundary.InvokeAsync, telemetry ?? new StorageTelemetry(), analytics,
            timeProvider ?? TimeProvider.System, NullLogger<StorageTelemetryMiddleware>.Instance);
    }

    private static DefaultHttpContext CreateContext(Stream body, string path = "/container/blob")
    {
        var context = new DefaultHttpContext();
        context.Request.Method = HttpMethods.Put;
        context.Request.Scheme = "http";
        context.Request.Host = new HostString("devstoreaccount1.localhost");
        context.Request.Path = path;
        context.Response.Body = body;
        return context;
    }

    private static Task CompleteStorageRequestAsync(HttpContext context)
    {
        SetStorageContext(context);
        context.Response.StatusCode = StatusCodes.Status201Created;
        return Task.CompletedTask;
    }

    private static async Task AssertOperatorsBypassAsync(StorageTelemetryMiddleware pipeline)
    {
        foreach (var path in new[] { "/health/live", "/health/ready", "/metrics" })
        {
            var health = CreateContext(Stream.Null, path);
            health.Request.Method = HttpMethods.Get;
            await pipeline.InvokeAsync(health).ConfigureAwait(false);
            Assert.Equal(StatusCodes.Status200OK, health.Response.StatusCode);
        }
    }

    private static void SetStorageContext(HttpContext context) => StorageRequestContext.Set(context, new StorageRequestContext
    {
        RequestId = Guid.NewGuid().ToString("N"),
        Account = "devstoreaccount1",
        Container = "container",
        Blob = "blob",
        CanonicalResourcePath = "/devstoreaccount1/container/blob",
        ResourceKind = StorageResourceKind.Blob,
        ServiceVersion = "2023-11-03",
        Authorization = StorageAuthorization.Owner
    });

    private static void AssertActive(GatewayAdmission admission, int expected) =>
        Assert.Contains(FormattableString.Invariant($"mk8_sava_gateway_active_requests {expected}\n"),
            admission.RenderMetrics(), StringComparison.Ordinal);

    private sealed class ControlledAnalyticsSink(Exception? failure = null) : IStorageAnalyticsSink
    {
        private readonly TaskCompletionSource<StorageAnalyticsRequest> _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _attempts;

        internal Task<StorageAnalyticsRequest> Entered => _entered.Task;
        internal CancellationToken CancellationToken { get; private set; }
        internal int Attempts => Volatile.Read(ref _attempts);
        internal void Release() => _release.TrySetResult();

        public async Task RecordAsync(StorageAnalyticsRequest request, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _attempts);
            CancellationToken = cancellationToken;
            _entered.TrySetResult(request);
            await _release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            if (failure is not null)
                throw failure;
        }
    }

    private sealed class ManualDeadlineTimeProvider : TimeProvider
    {
        private Action? _expire;
        internal TimeSpan DueTime { get; private set; }
        internal TimeSpan Period { get; private set; }

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new ManualDeadlineTimer(callback, state);
            DueTime = dueTime;
            Period = period;
            _expire = timer.Fire;
            return timer;
        }

        internal void Expire() => (_expire ?? throw new InvalidOperationException("The analytics deadline was not scheduled."))();
    }

    private sealed class ManualDeadlineTimer(TimerCallback callback, object? state) : ITimer
    {
        private int _disposed;

        internal void Fire()
        {
            if (Volatile.Read(ref _disposed) == 0)
                callback(state);
        }

        public bool Change(TimeSpan dueTime, TimeSpan period) => Volatile.Read(ref _disposed) == 0;

        public void Dispose()
        {
            Interlocked.Exchange(ref _disposed, 1);
            GC.SuppressFinalize(this);
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class CountingLease : RateLimitLease
    {
        internal int Disposals { get; private set; }
        public override bool IsAcquired => true;
        public override IEnumerable<string> MetadataNames => [];

        public override bool TryGetMetadata(string metadataName, out object? metadata)
        {
            metadata = null;
            return false;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                Disposals++;
            base.Dispose(disposing);
        }
    }

    private sealed class FaultingTelemetry(Exception failure) : IStorageTelemetry
    {
        public StorageUsageSnapshot Usage => throw new NotSupportedException();
        public StorageIntegritySnapshot Integrity => throw new NotSupportedException();
        public void RecordRequest(int statusCode, long elapsedStopwatchTicks) => throw failure;
        public void RecordMaintenance(StorageMaintenanceResult result, StorageUsageSnapshot usage) => throw new NotSupportedException();
        public void RecordMaintenanceFailure() => throw new NotSupportedException();
        public void RecordIntegrity(StorageIntegritySnapshot integrity) => throw new NotSupportedException();
        public string RenderPrometheus() => throw new NotSupportedException();
    }
}
