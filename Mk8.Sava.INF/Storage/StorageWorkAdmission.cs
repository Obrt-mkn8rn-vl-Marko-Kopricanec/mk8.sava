using System.Globalization;
using System.Text;
using System.Threading.RateLimiting;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Storage;

internal sealed class StorageWorkAdmission : IDisposable
{
    private readonly StorageWorkLimiter _reads;
    private readonly StorageWorkLimiter _writes;
    private readonly StorageWorkLimiter _codecs;
    private readonly StorageWorkLimiter _queries;

    public StorageWorkAdmission(SavaOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _reads = new StorageWorkLimiter(options.MaximumConcurrentStorageReads, options.MaximumQueuedStorageOperations);
        _writes = new StorageWorkLimiter(options.MaximumConcurrentStorageWrites, options.MaximumQueuedStorageOperations);
        _codecs = new StorageWorkLimiter(options.MaximumConcurrentChunkCodecs, options.MaximumQueuedStorageOperations);
        _queries = new StorageWorkLimiter(options.MaximumConcurrentBlobQueries, options.MaximumQueuedStorageOperations);
    }

    public ValueTask<RateLimitLease> AcquireReadAsync(CancellationToken cancellationToken) => _reads.AcquireAsync(cancellationToken);
    public ValueTask<RateLimitLease> AcquireWriteAsync(CancellationToken cancellationToken) => _writes.AcquireAsync(cancellationToken);
    public ValueTask<RateLimitLease> AcquireCodecAsync(CancellationToken cancellationToken) => _codecs.AcquireAsync(cancellationToken);
    public ValueTask<RateLimitLease> AcquireQueryAsync(CancellationToken cancellationToken) => _queries.AcquireAsync(cancellationToken);

    public void Dispose()
    {
        _reads.Dispose();
        _writes.Dispose();
        _codecs.Dispose();
        _queries.Dispose();
        GC.SuppressFinalize(this);
    }

    public string RenderPrometheus()
    {
        var builder = new StringBuilder(2048);
        builder.Append("# HELP mk8_sava_storage_work_active Active storage work permits.\n# TYPE mk8_sava_storage_work_active gauge\n");
        builder.Append("# HELP mk8_sava_storage_work_queued Storage work waiting for a permit.\n# TYPE mk8_sava_storage_work_queued gauge\n");
        builder.Append("# HELP mk8_sava_storage_work_rejected_total Storage work rejected by a full or closed queue.\n# TYPE mk8_sava_storage_work_rejected_total counter\n");
        AppendLane(builder, "reads", _reads);
        AppendLane(builder, "writes", _writes);
        AppendLane(builder, "codecs", _codecs);
        AppendLane(builder, "queries", _queries);
        return builder.ToString();
    }

    private static void AppendLane(StringBuilder builder, string lane, StorageWorkLimiter limiter)
    {
        var statistics = limiter.GetStatistics();
        AppendValue(builder, "mk8_sava_storage_work_active", lane, statistics.Active);
        AppendValue(builder, "mk8_sava_storage_work_queued", lane, statistics.Queued);
        AppendValue(builder, "mk8_sava_storage_work_rejected_total", lane, statistics.Rejected);
    }

    private static void AppendValue(StringBuilder builder, string metric, string lane, long value) =>
        builder.Append(metric).Append("{lane=\"").Append(lane).Append("\"} ")
            .Append(value.ToString(CultureInfo.InvariantCulture)).Append('\n');
}
