using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

internal sealed partial class AzureResponseWriter
{
    internal const string ArrowStreamContentType = "application/vnd.apache.arrow.stream";

    private static readonly TimestampType BlobListTimestampType = new(TimeUnit.Second, (string?)null);
    private static readonly MapType BlobListMapType = new(StringType.Default, StringType.Default);

    private static async Task WriteArrowBlobsAsync(
        HttpContext context,
        BlobListPage listing,
        string nextMarker,
        IReadOnlySet<string> includes,
        bool hierarchicalNamespace,
        bool lastAccessTimeTracking,
        CancellationToken cancellationToken)
    {
        var fields = new List<Field>();
        var arrays = new List<IArrowArray>();
        var items = listing.Items;

        AddStringColumn(fields, arrays, "Name", items, entry => entry.Name, nullable: false, required: true);
        AddTimestampColumn(fields, arrays, "Creation-Time", items, entry => entry.Blob?.CreatedAt);
        AddTimestampColumn(fields, arrays, "Last-Modified", items, entry => entry.Blob?.LastModified);
        AddStringColumn(fields, arrays, "BlobType", items, entry => entry.Blob is { } blob
            ? BlobType(blob.Kind)
            : entry.IsUncommitted ? "BlockBlob" : null);
        AddStringColumn(
            fields,
            arrays,
            "ResourceType",
            items,
            entry => entry.Prefix is not null ? "blobprefix" : "blob",
            nullable: false,
            required: true);
        AddStringColumn(fields, arrays, "Etag", items, entry => entry.Blob?.ETag);
        if (hierarchicalNamespace && includes.Contains("permissions"))
        {
            AddStringColumn(fields, arrays, "Owner", items, entry => entry.Blob?.Owner);
            AddStringColumn(fields, arrays, "Group", items, entry => entry.Blob?.Group);
            AddStringColumn(fields, arrays, "Permissions", items, entry => entry.Blob?.Permissions);
            AddStringColumn(
                fields,
                arrays,
                "Acl",
                items,
                entry => entry.Blob?.Acl);
        }
        AddUInt64Column(fields, arrays, "Content-Length", items, entry =>
            entry.Blob is { } blob
                ? checked((ulong)blob.Content.Length)
                : entry.IsUncommitted ? 0UL : null);
        AddStringColumn(fields, arrays, "Content-Type", items, entry => entry.Blob?.Http.ContentType);
        AddStringColumn(fields, arrays, "Content-Encoding", items, entry => entry.Blob?.Http.ContentEncoding);
        AddStringColumn(fields, arrays, "Content-Language", items, entry => entry.Blob?.Http.ContentLanguage);
        AddStringColumn(fields, arrays, "Content-MD5", items, entry => entry.Blob?.Http.ContentMd5);
        AddStringColumn(fields, arrays, "Content-Disposition", items, entry => entry.Blob?.Http.ContentDisposition);
        AddStringColumn(fields, arrays, "Cache-Control", items, entry => entry.Blob?.Http.CacheControl);
        AddUInt64Column(fields, arrays, "x-ms-blob-sequence-number", items, entry =>
            entry.Blob is { Kind: BlobKind.PageBlob } blob ? checked((ulong)blob.SequenceNumber) : null);
        AddStringColumn(fields, arrays, "AccessTier", items, entry =>
            entry.Blob is { Kind: BlobKind.BlockBlob } blob ? blob.AccessTier : null);
        AddBooleanColumn(fields, arrays, "AccessTierInferred", items, entry =>
            entry.Blob is { Kind: BlobKind.BlockBlob, AccessTierInferred: true } ? true : null);
        AddTimestampColumn(fields, arrays, "AccessTierChangeTime", items, entry =>
            entry.Blob is { Kind: BlobKind.BlockBlob } blob ? blob.AccessTierChangedAt : null);
        AddStringColumn(fields, arrays, "SmartAccessTier", items, entry =>
            entry.Blob is { Kind: BlobKind.BlockBlob, AccessTier: "Smart" } blob
                ? blob.SmartAccessTier
                : null);
        AddStringColumn(fields, arrays, "LeaseState", items, entry =>
            entry.Blob is { Snapshot: null, IsDeleted: false } blob ? LeaseStateValue(blob.Lease) : null);
        AddStringColumn(fields, arrays, "LeaseStatus", items, entry =>
            entry.Blob is { Snapshot: null, IsDeleted: false } blob ? LeaseStatus(blob.Lease) : null);
        AddStringColumn(fields, arrays, "LeaseDuration", items, entry =>
            entry.Blob is { Snapshot: null, IsDeleted: false, Lease.State: LeaseState.Leased } blob
                ? blob.Lease.DurationSeconds == -1 ? "infinite" : "fixed"
                : null);
        AddBooleanColumn(fields, arrays, "IncrementalCopy", items, entry =>
            entry.Blob?.IsIncrementalCopy == true ? true : null);
        AddBooleanColumn(fields, arrays, "ServerEncrypted", items, entry => entry.Blob is null ? null : true);
        AddStringColumn(fields, arrays, "CustomerProvidedKeySha256", items, entry =>
            entry.Blob?.CustomerProvidedKeySha256);
        AddStringColumn(fields, arrays, "EncryptionScope", items, entry => entry.Blob?.EncryptionScope);
        AddStringColumn(fields, arrays, "RehydratePriority", items, entry => entry.Blob?.RehydratePriority);
        AddBooleanColumn(fields, arrays, "Sealed", items, entry =>
            entry.Blob is { Kind: BlobKind.AppendBlob } blob ? blob.IsSealed : null);
        AddStringColumn(fields, arrays, "ArchiveStatus", items, entry => entry.Blob?.ArchiveStatus);
        AddStringColumn(fields, arrays, "CopyId", items, entry =>
            includes.Contains("copy") ? entry.Blob?.Copy?.Id : null);
        AddStringColumn(fields, arrays, "CopyStatus", items, entry =>
            includes.Contains("copy") ? entry.Blob?.Copy?.Status : null);
        AddStringColumn(fields, arrays, "CopySource", items, entry =>
            includes.Contains("copy") ? entry.Blob?.Copy?.Source : null);
        AddStringColumn(fields, arrays, "CopyProgress", items, entry =>
            includes.Contains("copy") && entry.Blob?.Copy is { } copy
                ? $"{copy.BytesCopied}/{copy.TotalBytes}"
                : null);
        AddTimestampColumn(fields, arrays, "CopyCompletionTime", items, entry =>
            includes.Contains("copy") ? entry.Blob?.Copy?.CompletedAt : null);
        AddStringColumn(fields, arrays, "CopyStatusDescription", items, entry =>
            includes.Contains("copy") ? entry.Blob?.Copy?.Description : null);
        AddStringColumn(fields, arrays, "CopyDestinationSnapshot", items, entry =>
            string.Equals(entry.Blob?.Copy?.Status, "success", StringComparison.Ordinal)
                ? entry.Blob?.CopyDestinationSnapshot
                : null);
        AddTimestampColumn(fields, arrays, "ImmutabilityPolicyUntilDate", items, entry =>
            includes.Contains("immutabilitypolicy") ? entry.Blob?.ImmutabilityUntil : null);
        AddStringColumn(fields, arrays, "ImmutabilityPolicyMode", items, entry =>
            includes.Contains("immutabilitypolicy") && entry.Blob?.ImmutabilityUntil is not null
                ? entry.Blob.ImmutabilityLocked ? "locked" : "unlocked"
                : null);
        AddStringColumn(fields, arrays, "VersionId", items, entry =>
            includes.Contains("versions") ? entry.Blob?.VersionId : null);
        AddBooleanColumn(fields, arrays, "IsCurrentVersion", items, entry =>
            includes.Contains("versions") && entry.Blob?.VersionId is not null
                ? entry.Blob.IsCurrent
                : null);
        AddStringColumn(fields, arrays, "Snapshot", items, entry => entry.Blob?.Snapshot);
        AddBooleanColumn(fields, arrays, "LegalHold", items, entry =>
            includes.Contains("legalhold") && entry.Blob is { } blob ? blob.HasLegalHold : null);
        AddBooleanColumn(fields, arrays, "Deleted", items, entry =>
            entry.Blob?.IsDeleted == true ? true : null);
        AddTimestampColumn(fields, arrays, "DeletedTime", items, entry => entry.Blob?.DeletedAt);
        AddUInt64Column(fields, arrays, "RemainingRetentionDays", items, entry =>
            entry.Blob?.DeleteRetentionUntil is { } retentionUntil
                ? checked((ulong)RemainingRetentionDays(retentionUntil))
                : null);
        AddTimestampColumn(fields, arrays, "LastAccessTime", items, entry =>
            lastAccessTimeTracking ? entry.Blob?.LastAccessedAt : null);
        AddMapColumn(fields, arrays, "Tags", items, entry =>
            includes.Contains("tags") && entry.Blob is { Tags.Count: > 0 } blob ? blob.Tags : null);
        AddMapColumn(fields, arrays, "OrMetadata", items, entry =>
            entry.Blob is { Kind: BlobKind.BlockBlob, ObjectReplicationStatuses.Count: > 0 } blob
                ? blob.ObjectReplicationStatuses.ToDictionary(
                    pair => $"or-{pair.Key}",
                    pair => pair.Value.Status,
                    StringComparer.Ordinal)
                : null);
        AddStringColumn(fields, arrays, "OrsPolicySourceBlob", items, entry =>
            entry.Blob is { Kind: BlobKind.BlockBlob } blob
                ? blob.ObjectReplicationDestinationPolicyId
                : null);
        AddUInt64Column(fields, arrays, "TagCount", items, entry =>
            entry.Blob is { Tags.Count: > 0 } blob ? checked((ulong)blob.Tags.Count) : null);
        AddMapColumn(fields, arrays, "Metadata", items, entry =>
        {
            if (!includes.Contains("metadata") || entry.Blob is not { } blob)
                return null;
            return blob.CustomerProvidedKeySha256 is not null && blob.Metadata.Count > 0
                ? null
                : blob.Metadata;
        });
        AddBooleanColumn(fields, arrays, "Encrypted", items, entry =>
            includes.Contains("metadata") &&
            entry.Blob is { CustomerProvidedKeySha256: not null, Metadata.Count: > 0 }
                ? true
                : null);

        var schema = new Schema(
            fields,
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["NumberOfRecords"] = items.Count.ToString(CultureInfo.InvariantCulture),
                ["NextMarker"] = nextMarker
            });
        using var batch = new RecordBatch(schema, arrays, items.Count);
        context.Response.ContentType = ArrowStreamContentType;
        using var writer = new ArrowStreamWriter(context.Response.Body, schema, leaveOpen: true);
        await writer.WriteStartAsync(cancellationToken).ConfigureAwait(false);
        await writer.WriteRecordBatchAsync(batch, cancellationToken).ConfigureAwait(false);
        await writer.WriteEndAsync(cancellationToken).ConfigureAwait(false);
    }

    private static void AddStringColumn(
        ICollection<Field> fields,
        ICollection<IArrowArray> arrays,
        string name,
        IReadOnlyList<BlobListEntry> items,
        Func<BlobListEntry, string?> select,
        bool nullable = true,
        bool required = false)
    {
        var values = items.Select(select).ToArray();
        if (!required && values.All(value => value is null))
            return;
        var builder = new StringArray.Builder().Reserve(values.Length);
        foreach (var value in values)
        {
            if (value is null)
                builder.AppendNull();
            else
                builder.Append(value);
        }
        fields.Add(new Field(name, StringType.Default, nullable));
        arrays.Add(builder.Build());
    }

    private static void AddTimestampColumn(
        ICollection<Field> fields,
        ICollection<IArrowArray> arrays,
        string name,
        IReadOnlyList<BlobListEntry> items,
        Func<BlobListEntry, DateTimeOffset?> select)
    {
        var values = items.Select(select).ToArray();
        if (values.All(value => value is null))
            return;
        var builder = new TimestampArray.Builder(BlobListTimestampType).Reserve(values.Length);
        foreach (var value in values)
        {
            if (value.HasValue)
                builder.Append(value.Value);
            else
                builder.AppendNull();
        }
        fields.Add(new Field(name, BlobListTimestampType, nullable: true));
        arrays.Add(builder.Build());
    }

    private static void AddUInt64Column(
        ICollection<Field> fields,
        ICollection<IArrowArray> arrays,
        string name,
        IReadOnlyList<BlobListEntry> items,
        Func<BlobListEntry, ulong?> select)
    {
        var values = items.Select(select).ToArray();
        if (values.All(value => value is null))
            return;
        var builder = new UInt64Array.Builder().Reserve(values.Length);
        foreach (var value in values)
            builder.Append(value);
        fields.Add(new Field(name, UInt64Type.Default, nullable: true));
        arrays.Add(builder.Build());
    }

    private static void AddBooleanColumn(
        ICollection<Field> fields,
        ICollection<IArrowArray> arrays,
        string name,
        IReadOnlyList<BlobListEntry> items,
        Func<BlobListEntry, bool?> select)
    {
        var values = items.Select(select).ToArray();
        if (values.All(value => value is null))
            return;
        var builder = new BooleanArray.Builder().Reserve(values.Length);
        foreach (var value in values)
            builder.NullableAppend(value);
        fields.Add(new Field(name, BooleanType.Default, nullable: true));
        arrays.Add(builder.Build());
    }

    private static void AddMapColumn(
        ICollection<Field> fields,
        ICollection<IArrowArray> arrays,
        string name,
        IReadOnlyList<BlobListEntry> items,
        Func<BlobListEntry, IReadOnlyDictionary<string, string>?> select)
    {
        var values = items.Select(select).ToArray();
        if (values.All(value => value is null))
            return;
        var builder = new MapArray.Builder(BlobListMapType).Reserve(values.Length);
        var keys = (StringArray.Builder)builder.KeyBuilder;
        var mapValues = (StringArray.Builder)builder.ValueBuilder;
        foreach (var value in values)
        {
            if (value is null)
            {
                builder.AppendNull();
                continue;
            }

            builder.Append();
            foreach (var pair in value.OrderBy(pair => pair.Key, StringComparer.Ordinal))
            {
                keys.Append(pair.Key);
                mapValues.Append(pair.Value);
            }
        }
        fields.Add(new Field(name, BlobListMapType, nullable: true));
        arrays.Add(builder.Build());
    }
}
