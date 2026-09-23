using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace Mk8.Sava.Storage;

public sealed class StorageTelemetry : IStorageTelemetry
{
    private long _requestCount;
    private long _serverErrorCount;
    private long _requestDurationStopwatchTicks;
    private long _maintenancePasses;
    private long _maintenanceFailures;
    private long _completedCopies;
    private long _completedObjectReplications;
    private long _failedObjectReplications;
    private long _removedObjectReplicas;
    private long _completedRehydrations;
    private long _completedSmartTierTransitions;
    private long _expiredBlobs;
    private long _purgedSoftDeletedBlobs;
    private long _purgedSoftDeletedContainers;
    private long _expiredUncommittedBlocks;
    private long _reclaimedChunks;
    private long _reclaimedStagingFiles;
    private long _recompressedChunks;
    private long _recompressionBytesSaved;
    private long _compactedChunkPacks;
    private long _packCompactionBytesSaved;
    private long _lastMaintenanceCompletedUnixSeconds;
    private StorageUsageSnapshot _usage = StorageUsageSnapshot.Empty;
    private StorageIntegritySnapshot _integrity = StorageIntegritySnapshot.Pending;

    public StorageUsageSnapshot Usage => Volatile.Read(ref _usage);

    public StorageIntegritySnapshot Integrity => Volatile.Read(ref _integrity);

    public void RecordRequest(int statusCode, long elapsedStopwatchTicks)
    {
        Interlocked.Increment(ref _requestCount);
        if (statusCode >= 500)
            Interlocked.Increment(ref _serverErrorCount);
        Interlocked.Add(ref _requestDurationStopwatchTicks, elapsedStopwatchTicks);
    }

    public void RecordMaintenance(StorageMaintenanceResult result, StorageUsageSnapshot usage)
    {
        Interlocked.Increment(ref _maintenancePasses);
        Interlocked.Add(ref _completedCopies, result.CompletedCopies);
        Interlocked.Add(ref _completedObjectReplications, result.CompletedObjectReplications);
        Interlocked.Add(ref _failedObjectReplications, result.FailedObjectReplications);
        Interlocked.Add(ref _removedObjectReplicas, result.RemovedObjectReplicas);
        Interlocked.Add(ref _completedRehydrations, result.CompletedRehydrations);
        Interlocked.Add(ref _completedSmartTierTransitions, result.CompletedSmartTierTransitions);
        Interlocked.Add(ref _expiredBlobs, result.ExpiredBlobs);
        Interlocked.Add(ref _purgedSoftDeletedBlobs, result.PurgedSoftDeletedBlobs);
        Interlocked.Add(ref _purgedSoftDeletedContainers, result.PurgedSoftDeletedContainers);
        Interlocked.Add(ref _expiredUncommittedBlocks, result.ExpiredUncommittedBlocks);
        Interlocked.Add(ref _reclaimedChunks, result.ReclaimedChunks);
        Interlocked.Add(ref _reclaimedStagingFiles, result.ReclaimedStagingFiles);
        Interlocked.Add(ref _recompressedChunks, result.RecompressedChunks);
        Interlocked.Add(ref _recompressionBytesSaved, result.RecompressionBytesSaved);
        Interlocked.Add(ref _compactedChunkPacks, result.CompactedChunkPacks);
        Interlocked.Add(ref _packCompactionBytesSaved, result.PackCompactionBytesSaved);
        Interlocked.Exchange(ref _lastMaintenanceCompletedUnixSeconds, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        Volatile.Write(ref _usage, usage);
    }

    public void RecordMaintenanceFailure() => Interlocked.Increment(ref _maintenanceFailures);

    public void RecordIntegrity(StorageIntegritySnapshot integrity) => Volatile.Write(ref _integrity, integrity);

    public string RenderPrometheus()
    {
        var usage = Usage;
        var integrity = Integrity;
        var durationSeconds = Interlocked.Read(ref _requestDurationStopwatchTicks) / (double)Stopwatch.Frequency;
        var builder = new StringBuilder(4096);

        AppendMetric(builder, "mk8_sava_http_requests_total", "Storage protocol requests completed.", Interlocked.Read(ref _requestCount));
        AppendMetric(builder, "mk8_sava_http_server_errors_total", "Storage protocol requests completed with a 5xx response.", Interlocked.Read(ref _serverErrorCount));
        AppendMetric(builder, "mk8_sava_http_request_duration_seconds_sum", "Cumulative storage protocol request duration.", durationSeconds);
        AppendMetric(builder, "mk8_sava_http_request_duration_seconds_count", "Storage protocol requests represented by the duration sum.", Interlocked.Read(ref _requestCount));
        AppendMetric(builder, "mk8_sava_storage_logical_blob_bytes", "Logical bytes referenced by blob records.", usage.LogicalBlobBytes, gauge: true);
        AppendMetric(builder, "mk8_sava_storage_logical_uncommitted_block_bytes", "Logical bytes referenced by uncommitted blocks.", usage.LogicalStagedBlockBytes, gauge: true);
        AppendMetric(builder, "mk8_sava_storage_physical_chunk_bytes", "Serialized file lengths of standalone chunks and chunk packs, not allocated filesystem bytes.", usage.PhysicalChunkBytes, gauge: true);
        AppendMetric(builder, "mk8_sava_storage_staging_bytes", "Serialized file lengths of staging files, not allocated filesystem bytes.", usage.StagingBytes, gauge: true);
        AppendMetric(builder, "mk8_sava_storage_metadata_bytes", "Serialized file lengths of SQLite metadata and journals, not allocated filesystem bytes.", usage.MetadataBytes, gauge: true);
        AppendMetric(builder, "mk8_sava_storage_allocation_available", "Whether a valid allocated-filesystem-byte measurement has been published.", usage.AllocatedRootBytes.HasValue ? 1 : 0, gauge: true);
        if (usage.AllocatedRootBytes is { } allocatedBytes)
            AppendMetric(builder, "mk8_sava_storage_allocated_root_bytes", "Filesystem-allocated bytes under the data root, including metadata, journals, staging, files, directories, and the root lease.", allocatedBytes, gauge: true);
        AppendMetric(builder, "mk8_sava_storage_blob_records", "Blob, version, and snapshot records.", usage.BlobRecordCount, gauge: true);
        AppendMetric(builder, "mk8_sava_storage_uncommitted_blocks", "Uncommitted block records.", usage.StagedBlockCount, gauge: true);
        AppendMetric(builder, "mk8_sava_storage_unique_chunks", "Unique immutable chunk identities across standalone and packed storage.", usage.UniqueChunkCount, gauge: true);
        AppendMetric(builder, "mk8_sava_storage_reachable_chunks", "Chunk identities reachable from metadata.", usage.ReachableChunkCount, gauge: true);
        AppendMetric(builder, "mk8_sava_integrity_checked_chunks", "Reachable chunks checked in the current or last completed integrity cycle.", integrity.CheckedChunks, gauge: true);
        AppendMetric(builder, "mk8_sava_integrity_missing_chunks", "Reachable chunks found missing in the current or last completed integrity cycle.", integrity.MissingChunks, gauge: true);
        AppendMetric(builder, "mk8_sava_integrity_corrupt_chunks", "Reachable chunks that failed authenticated decoding in the current or last completed integrity cycle.", integrity.CorruptChunks, gauge: true);
        AppendMetric(builder, "mk8_sava_integrity_customer_key_chunks", "Reachable customer-key chunks structurally checked but awaiting a caller key for authenticated decoding.", integrity.CustomerKeyChunks, gauge: true);
        AppendMetric(builder, "mk8_sava_integrity_cycle_complete", "Whether the published integrity cycle covered every reachable chunk.", integrity.Complete ? 1 : 0, gauge: true);
        AppendMetric(builder, "mk8_sava_maintenance_passes_total", "Completed maintenance passes.", Interlocked.Read(ref _maintenancePasses));
        AppendMetric(builder, "mk8_sava_maintenance_failures_total", "Failed maintenance passes.", Interlocked.Read(ref _maintenanceFailures));
        AppendMetric(builder, "mk8_sava_maintenance_completed_copies_total", "Asynchronous copies completed by maintenance.", Interlocked.Read(ref _completedCopies));
        AppendMetric(builder, "mk8_sava_maintenance_completed_object_replications_total", "Object-replication copies completed by maintenance.", Interlocked.Read(ref _completedObjectReplications));
        AppendMetric(builder, "mk8_sava_maintenance_failed_object_replications_total", "Object-replication source states first marked failed by maintenance.", Interlocked.Read(ref _failedObjectReplications));
        AppendMetric(builder, "mk8_sava_maintenance_removed_object_replicas_total", "Object replicas removed after their source versions were permanently deleted.", Interlocked.Read(ref _removedObjectReplicas));
        AppendMetric(builder, "mk8_sava_maintenance_completed_rehydrations_total", "Archive rehydrations completed by maintenance.", Interlocked.Read(ref _completedRehydrations));
        AppendMetric(builder, "mk8_sava_maintenance_smart_tier_transitions_total", "Smart-tier capacity transitions completed by maintenance.", Interlocked.Read(ref _completedSmartTierTransitions));
        AppendMetric(builder, "mk8_sava_maintenance_expired_blobs_total", "Expired blobs removed by maintenance.", Interlocked.Read(ref _expiredBlobs));
        AppendMetric(builder, "mk8_sava_maintenance_purged_soft_deleted_blobs_total", "Soft-deleted blob records permanently purged.", Interlocked.Read(ref _purgedSoftDeletedBlobs));
        AppendMetric(builder, "mk8_sava_maintenance_purged_soft_deleted_containers_total", "Soft-deleted containers permanently purged.", Interlocked.Read(ref _purgedSoftDeletedContainers));
        AppendMetric(builder, "mk8_sava_maintenance_expired_uncommitted_blocks_total", "Expired uncommitted blocks removed.", Interlocked.Read(ref _expiredUncommittedBlocks));
        AppendMetric(builder, "mk8_sava_maintenance_reclaimed_chunks_total", "Unreachable standalone files or packed chunk locators reclaimed.", Interlocked.Read(ref _reclaimedChunks));
        AppendMetric(builder, "mk8_sava_maintenance_reclaimed_staging_files_total", "Crash-abandoned staging files reclaimed.", Interlocked.Read(ref _reclaimedStagingFiles));
        AppendMetric(builder, "mk8_sava_maintenance_recompressed_chunks_total", "Reachable chunks atomically replaced by a smaller verified representation.", Interlocked.Read(ref _recompressedChunks));
        AppendMetric(builder, "mk8_sava_maintenance_recompression_bytes_saved_total", "Serialized chunk-file bytes removed by verified background recompression.", Interlocked.Read(ref _recompressionBytesSaved));
        AppendMetric(builder, "mk8_sava_maintenance_compacted_chunk_packs_total", "Immutable small-chunk packs compacted or unregistered crash remnants reclaimed.", Interlocked.Read(ref _compactedChunkPacks));
        AppendMetric(builder, "mk8_sava_maintenance_pack_compaction_bytes_saved_total", "Pack-file bytes removed by verified compaction or crash recovery.", Interlocked.Read(ref _packCompactionBytesSaved));
        AppendMetric(builder, "mk8_sava_maintenance_last_completed_timestamp_seconds", "Unix timestamp of the last completed maintenance pass.", Interlocked.Read(ref _lastMaintenanceCompletedUnixSeconds), gauge: true);
        return builder.ToString();
    }

    private static void AppendMetric(StringBuilder builder, string name, string help, long value, bool gauge = false)
    {
        builder.Append("# HELP ").Append(name).Append(' ').AppendLine(help);
        builder.Append("# TYPE ").Append(name).Append(gauge ? " gauge\n" : " counter\n");
        builder.Append(name).Append(' ').Append(value.ToString(CultureInfo.InvariantCulture)).Append('\n');
    }

    private static void AppendMetric(StringBuilder builder, string name, string help, double value)
    {
        builder.Append("# HELP ").Append(name).Append(' ').AppendLine(help);
        builder.Append("# TYPE ").Append(name).Append(" counter\n");
        builder.Append(name).Append(' ').Append(value.ToString("R", CultureInfo.InvariantCulture)).Append('\n');
    }
}
