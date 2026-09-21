using System.Globalization;
using System.Text;
using System.Xml;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

public sealed class AzureResponseWriter
{
    public async Task WriteXmlAsync(HttpContext context, Action<XmlWriter> write, CancellationToken cancellationToken)
    {
        var builder = new StringBuilder();
        using (var writer = XmlWriter.Create(builder, new XmlWriterSettings
        {
            Encoding = Encoding.UTF8,
            OmitXmlDeclaration = true,
            Indent = false
        }))
        {
            write(writer);
        }

        context.Response.ContentType = "application/xml";
        await context.Response.WriteAsync(builder.ToString(), cancellationToken);
    }

    public Task WriteContainersAsync(
        HttpContext context,
        IReadOnlyList<ContainerRecord> containers,
        string prefix,
        string marker,
        int maxResults,
        bool includeMetadata,
        bool includeDeleted,
        CancellationToken cancellationToken)
    {
        var filtered = containers
            .Where(container => container.Name.StartsWith(prefix, StringComparison.Ordinal))
            .Where(container => string.IsNullOrEmpty(marker) || string.CompareOrdinal(container.Name, marker) > 0)
            .Take(maxResults + 1)
            .ToArray();
        var page = filtered.Take(maxResults).ToArray();
        var nextMarker = filtered.Length > maxResults ? page[^1].Name : string.Empty;
        var endpoint = $"{context.Request.Scheme}://{context.Request.Host}/{StorageRequestContext.Get(context).Account}";

        return WriteXmlAsync(context, writer =>
        {
            writer.WriteStartElement("EnumerationResults");
            writer.WriteAttributeString("ServiceEndpoint", endpoint);
            writer.WriteElementString("Prefix", prefix);
            writer.WriteElementString("Marker", marker);
            writer.WriteElementString("MaxResults", maxResults.ToString(CultureInfo.InvariantCulture));
            writer.WriteStartElement("Containers");
            foreach (var container in page)
            {
                writer.WriteStartElement("Container");
                writer.WriteElementString("Name", container.Name);
                writer.WriteStartElement("Properties");
                writer.WriteElementString("Last-Modified", container.LastModified.ToString("R", CultureInfo.InvariantCulture));
                writer.WriteElementString("Etag", container.ETag);
                if (includeDeleted)
                {
                    writer.WriteElementString("Deleted", container.DeletedAt.HasValue ? "true" : "false");
                    if (container.DeletedVersion is not null)
                        writer.WriteElementString("Version", container.DeletedVersion);
                    WriteOptional(writer, "DeletedTime", container.DeletedAt?.ToString("R", CultureInfo.InvariantCulture));
                    if (container.DeleteRetentionUntil.HasValue)
                    {
                        writer.WriteElementString(
                            "RemainingRetentionDays",
                            RemainingRetentionDays(container.DeleteRetentionUntil.Value).ToString(CultureInfo.InvariantCulture));
                    }
                }
                writer.WriteEndElement();
                if (includeMetadata)
                    WriteMetadata(writer, container.Metadata);
                writer.WriteEndElement();
            }
            writer.WriteEndElement();
            writer.WriteElementString("NextMarker", nextMarker);
            writer.WriteEndElement();
        }, cancellationToken);
    }

    public Task WriteBlobsAsync(
        HttpContext context,
        IReadOnlyList<BlobRecord> blobs,
        string prefix,
        string delimiter,
        string marker,
        int maxResults,
        IReadOnlySet<string> includes,
        CancellationToken cancellationToken)
    {
        var entries = new List<(BlobRecord? Blob, string? Prefix)>();
        var seenPrefixes = new HashSet<string>(StringComparer.Ordinal);
        foreach (var blob in blobs.Where(item => item.Name.StartsWith(prefix, StringComparison.Ordinal)))
        {
            if (!string.IsNullOrEmpty(marker) && string.CompareOrdinal(blob.Name, marker) <= 0)
                continue;
            if (!string.IsNullOrEmpty(delimiter))
            {
                var remainder = blob.Name[prefix.Length..];
                var delimiterIndex = remainder.IndexOf(delimiter, StringComparison.Ordinal);
                if (delimiterIndex >= 0)
                {
                    var commonPrefix = prefix + remainder[..(delimiterIndex + delimiter.Length)];
                    if (seenPrefixes.Add(commonPrefix))
                        entries.Add((null, commonPrefix));
                    continue;
                }
            }
            entries.Add((blob, null));
        }

        entries.Sort((left, right) => string.CompareOrdinal(left.Blob?.Name ?? left.Prefix, right.Blob?.Name ?? right.Prefix));
        var selected = entries.Take(maxResults + 1).ToArray();
        var page = selected.Take(maxResults).ToArray();
        var nextMarker = selected.Length > maxResults ? page[^1].Blob?.Name ?? page[^1].Prefix ?? string.Empty : string.Empty;
        var request = StorageRequestContext.Get(context);
        var endpoint = $"{context.Request.Scheme}://{context.Request.Host}/{request.Account}";

        return WriteXmlAsync(context, writer =>
        {
            writer.WriteStartElement("EnumerationResults");
            writer.WriteAttributeString("ServiceEndpoint", endpoint);
            writer.WriteAttributeString("ContainerName", request.Container);
            writer.WriteElementString("Prefix", prefix);
            writer.WriteElementString("Marker", marker);
            if (!string.IsNullOrEmpty(delimiter))
                writer.WriteElementString("Delimiter", delimiter);
            writer.WriteElementString("MaxResults", maxResults.ToString(CultureInfo.InvariantCulture));
            writer.WriteStartElement("Blobs");
            foreach (var entry in page)
            {
                if (entry.Prefix is not null)
                {
                    writer.WriteStartElement("BlobPrefix");
                    writer.WriteElementString("Name", entry.Prefix);
                    writer.WriteEndElement();
                    continue;
                }

                var blob = entry.Blob!;
                writer.WriteStartElement("Blob");
                writer.WriteElementString("Name", blob.Name);
                if (blob.Snapshot is not null)
                    writer.WriteElementString("Snapshot", blob.Snapshot);
                if (includes.Contains("versions") && blob.VersionId is not null)
                {
                    writer.WriteElementString("VersionId", blob.VersionId);
                    writer.WriteElementString("IsCurrentVersion", blob.IsCurrent ? "true" : "false");
                }
                if (includes.Contains("deleted"))
                    writer.WriteElementString("Deleted", blob.IsDeleted ? "true" : "false");
                writer.WriteStartElement("Properties");
                writer.WriteElementString("Creation-Time", blob.CreatedAt.ToString("R", CultureInfo.InvariantCulture));
                writer.WriteElementString("Last-Modified", blob.LastModified.ToString("R", CultureInfo.InvariantCulture));
                writer.WriteElementString("Etag", blob.ETag);
                writer.WriteElementString("Content-Length", blob.Content.Length.ToString(CultureInfo.InvariantCulture));
                writer.WriteElementString("Content-Type", blob.Http.ContentType);
                WriteOptional(writer, "Content-Encoding", blob.Http.ContentEncoding);
                WriteOptional(writer, "Content-Language", blob.Http.ContentLanguage);
                WriteOptional(writer, "Content-MD5", blob.Http.ContentMd5);
                WriteOptional(writer, "Cache-Control", blob.Http.CacheControl);
                WriteOptional(writer, "Content-Disposition", blob.Http.ContentDisposition);
                writer.WriteElementString("BlobType", BlobType(blob.Kind));
                writer.WriteElementString("AccessTier", blob.AccessTier);
                WriteOptional(writer, "CustomerProvidedKeySha256", blob.CustomerProvidedKeySha256);
                WriteOptional(writer, "EncryptionScope", blob.EncryptionScope);
                WriteOptional(writer, "ArchiveStatus", blob.ArchiveStatus);
                WriteOptional(writer, "RehydratePriority", blob.RehydratePriority);
                WriteOptional(writer, "AccessTierChangeTime", blob.AccessTierChangedAt?.ToString("R", CultureInfo.InvariantCulture));
                WriteOptional(writer, "Expiry-Time", blob.ExpiresAt?.ToString("R", CultureInfo.InvariantCulture));
                if (blob.IsDeleted)
                {
                    WriteOptional(writer, "DeletedTime", blob.DeletedAt?.ToString("R", CultureInfo.InvariantCulture));
                    if (blob.DeleteRetentionUntil.HasValue)
                    {
                        writer.WriteElementString(
                            "RemainingRetentionDays",
                            RemainingRetentionDays(blob.DeleteRetentionUntil.Value).ToString(CultureInfo.InvariantCulture));
                    }
                }
                writer.WriteElementString("LeaseStatus", LeaseStatus(blob.Lease));
                writer.WriteElementString("LeaseState", LeaseStateValue(blob.Lease));
                if (blob.Kind == Storage.BlobKind.AppendBlob)
                    writer.WriteElementString("CommittedBlockCount", blob.AppendBlockCount.ToString(CultureInfo.InvariantCulture));
                if (includes.Contains("tags"))
                    writer.WriteElementString("TagCount", blob.Tags.Count.ToString(CultureInfo.InvariantCulture));
                if (includes.Contains("immutabilitypolicy") && blob.ImmutabilityUntil.HasValue)
                {
                    writer.WriteElementString("ImmutabilityPolicyUntilDate", blob.ImmutabilityUntil.Value.ToString("R", CultureInfo.InvariantCulture));
                    writer.WriteElementString("ImmutabilityPolicyMode", blob.ImmutabilityLocked ? "locked" : "unlocked");
                }
                if (includes.Contains("legalhold"))
                    writer.WriteElementString("LegalHold", blob.HasLegalHold ? "true" : "false");
                if (blob.Copy is not null)
                {
                    writer.WriteElementString("CopyId", blob.Copy.Id);
                    writer.WriteElementString("CopySource", blob.Copy.Source);
                    writer.WriteElementString("CopyStatus", blob.Copy.Status);
                    writer.WriteElementString("CopyProgress", $"{blob.Copy.BytesCopied}/{blob.Copy.TotalBytes}");
                    WriteOptional(writer, "CopyCompletionTime", blob.Copy.CompletedAt?.ToString("R", CultureInfo.InvariantCulture));
                    WriteOptional(writer, "CopyStatusDescription", blob.Copy.Description);
                }
                if (blob.IsIncrementalCopy)
                    writer.WriteElementString("IncrementalCopy", "true");
                if (blob.Copy?.Status == "success")
                    WriteOptional(writer, "DestinationSnapshot", blob.CopyDestinationSnapshot);
                writer.WriteEndElement();
                if (includes.Contains("metadata"))
                    WriteMetadata(writer, blob.Metadata, blob.CustomerProvidedKeySha256 is not null);
                if (includes.Contains("tags"))
                    WriteTags(writer, blob.Tags);
                writer.WriteEndElement();
            }
            writer.WriteEndElement();
            writer.WriteElementString("NextMarker", nextMarker);
            writer.WriteEndElement();
        }, cancellationToken);
    }

    public Task WriteTagsAsync(HttpContext context, IReadOnlyDictionary<string, string> tags, CancellationToken cancellationToken) =>
        WriteXmlAsync(context, writer =>
        {
            writer.WriteStartElement("Tags");
            WriteTagSet(writer, tags);
            writer.WriteEndElement();
        }, cancellationToken);

    public Task WriteBlockListAsync(
        HttpContext context,
        BlobRecord? blob,
        IReadOnlyList<StagedBlockRecord> staged,
        string listType,
        CancellationToken cancellationToken) =>
        WriteXmlAsync(context, writer =>
        {
            writer.WriteStartElement("BlockList");
            if (listType is "all" or "committed")
            {
                writer.WriteStartElement("CommittedBlocks");
                if (blob is not null)
                {
                    foreach (var block in blob.CommittedBlocks)
                    {
                        writer.WriteStartElement("Block");
                        writer.WriteElementString("Name", block.Id);
                        writer.WriteElementString("Size", block.Content.Length.ToString(CultureInfo.InvariantCulture));
                        writer.WriteEndElement();
                    }
                }
                writer.WriteEndElement();
            }
            if (listType is "all" or "uncommitted")
            {
                writer.WriteStartElement("UncommittedBlocks");
                foreach (var block in staged)
                {
                    writer.WriteStartElement("Block");
                    writer.WriteElementString("Name", block.BlockId);
                    writer.WriteElementString("Size", block.Content.Length.ToString(CultureInfo.InvariantCulture));
                    writer.WriteEndElement();
                }
                writer.WriteEndElement();
            }
            writer.WriteEndElement();
        }, cancellationToken);

    public Task WritePageRangesAsync(
        HttpContext context,
        IReadOnlyList<PageRange> ranges,
        IReadOnlyList<PageRange> clearRanges,
        string? nextMarker,
        CancellationToken cancellationToken) =>
        WriteXmlAsync(context, writer =>
        {
            writer.WriteStartElement("PageList");
            foreach (var range in ranges)
            {
                writer.WriteStartElement("PageRange");
                writer.WriteElementString("Start", range.Start.ToString(CultureInfo.InvariantCulture));
                writer.WriteElementString("End", range.End.ToString(CultureInfo.InvariantCulture));
                writer.WriteEndElement();
            }
            foreach (var range in clearRanges)
            {
                writer.WriteStartElement("ClearRange");
                writer.WriteElementString("Start", range.Start.ToString(CultureInfo.InvariantCulture));
                writer.WriteElementString("End", range.End.ToString(CultureInfo.InvariantCulture));
                writer.WriteEndElement();
            }
            if (nextMarker is not null)
                writer.WriteElementString("NextMarker", nextMarker);
            writer.WriteEndElement();
        }, cancellationToken);

    public Task WriteAclAsync(HttpContext context, ContainerRecord container, CancellationToken cancellationToken) =>
        WriteXmlAsync(context, writer =>
        {
            writer.WriteStartElement("SignedIdentifiers");
            foreach (var (id, policy) in container.AccessPolicies.OrderBy(item => item.Key, StringComparer.Ordinal))
            {
                writer.WriteStartElement("SignedIdentifier");
                writer.WriteElementString("Id", id);
                writer.WriteStartElement("AccessPolicy");
                WriteOptional(writer, "Start", policy.StartsAt?.UtcDateTime.ToString("O", CultureInfo.InvariantCulture));
                WriteOptional(writer, "Expiry", policy.ExpiresAt?.UtcDateTime.ToString("O", CultureInfo.InvariantCulture));
                writer.WriteElementString("Permission", policy.Permission);
                writer.WriteEndElement();
                writer.WriteEndElement();
            }
            writer.WriteEndElement();
        }, cancellationToken);

    public Task WriteServicePropertiesAsync(HttpContext context, ServiceProperties properties, CancellationToken cancellationToken) =>
        WriteXmlAsync(context, writer =>
        {
            writer.WriteStartElement("StorageServiceProperties");
            writer.WriteStartElement("Cors");
            foreach (var rule in properties.Cors)
            {
                writer.WriteStartElement("CorsRule");
                writer.WriteElementString("AllowedOrigins", rule.AllowedOrigins);
                writer.WriteElementString("AllowedMethods", rule.AllowedMethods);
                writer.WriteElementString("AllowedHeaders", rule.AllowedHeaders);
                writer.WriteElementString("ExposedHeaders", rule.ExposedHeaders);
                writer.WriteElementString("MaxAgeInSeconds", rule.MaxAgeInSeconds.ToString(CultureInfo.InvariantCulture));
                writer.WriteEndElement();
            }
            writer.WriteEndElement();
            writer.WriteStartElement("DefaultServiceVersion");
            writer.WriteString(properties.DefaultServiceVersion ?? string.Empty);
            writer.WriteEndElement();
            WriteRetentionPolicy(writer, "DeleteRetentionPolicy", properties.BlobSoftDeleteEnabled, properties.BlobSoftDeleteRetentionDays);
            WriteRetentionPolicy(writer, "ContainerDeleteRetentionPolicy", properties.ContainerSoftDeleteEnabled, properties.ContainerSoftDeleteRetentionDays);
            writer.WriteElementString("IsVersioningEnabled", properties.VersioningEnabled ? "true" : "false");
            writer.WriteStartElement("StaticWebsite");
            writer.WriteElementString("Enabled", properties.StaticWebsite.Enabled ? "true" : "false");
            WriteOptional(writer, "IndexDocument", properties.StaticWebsite.IndexDocument);
            WriteOptional(writer, "ErrorDocument404Path", properties.StaticWebsite.ErrorDocument404Path);
            writer.WriteEndElement();
            writer.WriteEndElement();
        }, cancellationToken);

    public Task WriteUserDelegationKeyAsync(
        HttpContext context,
        UserDelegationKey key,
        CancellationToken cancellationToken) =>
        WriteXmlAsync(context, writer =>
        {
            writer.WriteStartElement("UserDelegationKey");
            writer.WriteElementString("SignedOid", key.SignedObjectId);
            writer.WriteElementString("SignedTid", key.SignedTenantId);
            writer.WriteElementString("SignedStart", key.SignedStart);
            writer.WriteElementString("SignedExpiry", key.SignedExpiry);
            writer.WriteElementString("SignedService", key.SignedService);
            writer.WriteElementString("SignedVersion", key.SignedVersion);
            WriteOptional(writer, "SignedDelegatedUserTid", key.SignedDelegatedUserTenantId);
            writer.WriteElementString("Value", key.Value);
            writer.WriteEndElement();
        }, cancellationToken);

    public static void AddContainerHeaders(HttpResponse response, ContainerRecord container)
    {
        response.Headers.ETag = container.ETag;
        response.Headers.LastModified = container.LastModified.ToString("R", CultureInfo.InvariantCulture);
        if (container.Lease.State != Storage.LeaseState.Available)
        {
            response.Headers["x-ms-lease-status"] = LeaseStatus(container.Lease);
            response.Headers["x-ms-lease-state"] = LeaseStateValue(container.Lease);
            response.Headers["x-ms-lease-duration"] = container.Lease.DurationSeconds == -1 ? "infinite" : "fixed";
        }
        foreach (var (name, value) in container.Metadata)
            response.Headers[$"x-ms-meta-{name}"] = value;
        if (container.PublicAccess is not null)
            response.Headers["x-ms-blob-public-access"] = container.PublicAccess;
    }

    public static void AddBlobHeaders(HttpResponse response, BlobRecord blob)
    {
        response.Headers.ETag = blob.ETag;
        response.Headers.LastModified = blob.LastModified.ToString("R", CultureInfo.InvariantCulture);
        response.Headers["x-ms-creation-time"] = blob.CreatedAt.ToString("R", CultureInfo.InvariantCulture);
        response.Headers["x-ms-blob-type"] = BlobType(blob.Kind);
        response.Headers["x-ms-server-encrypted"] = "true";
        SetOptional(response.Headers, "x-ms-encryption-key-sha256", blob.CustomerProvidedKeySha256);
        SetOptional(response.Headers, "x-ms-encryption-scope", blob.EncryptionScope);
        response.Headers["x-ms-access-tier"] = blob.AccessTier;
        SetOptional(response.Headers, "x-ms-archive-status", blob.ArchiveStatus);
        SetOptional(response.Headers, "x-ms-rehydrate-priority", blob.RehydratePriority);
        SetOptional(response.Headers, "x-ms-access-tier-change-time", blob.AccessTierChangedAt?.ToString("R", CultureInfo.InvariantCulture));
        SetOptional(response.Headers, "x-ms-expiry-time", blob.ExpiresAt?.ToString("R", CultureInfo.InvariantCulture));
        response.Headers["x-ms-lease-status"] = LeaseStatus(blob.Lease);
        response.Headers["x-ms-lease-state"] = LeaseStateValue(blob.Lease);
        response.Headers["Accept-Ranges"] = "bytes";
        response.ContentType = blob.Http.ContentType;
        SetOptional(response.Headers, "Content-Encoding", blob.Http.ContentEncoding);
        SetOptional(response.Headers, "Content-Language", blob.Http.ContentLanguage);
        SetOptional(response.Headers, "Cache-Control", blob.Http.CacheControl);
        SetOptional(response.Headers, "Content-Disposition", blob.Http.ContentDisposition);
        SetOptional(response.Headers, "Content-MD5", blob.Http.ContentMd5);
        foreach (var (name, value) in blob.Metadata)
            response.Headers[$"x-ms-meta-{name}"] = value;
        if (blob.Tags.Count > 0)
            response.Headers["x-ms-tag-count"] = blob.Tags.Count.ToString(CultureInfo.InvariantCulture);
        if (blob.VersionId is not null)
            response.Headers["x-ms-version-id"] = blob.VersionId;
        if (blob.Snapshot is not null)
            response.Headers["x-ms-snapshot"] = blob.Snapshot;
        if (blob.Kind == Storage.BlobKind.PageBlob)
            response.Headers["x-ms-blob-sequence-number"] = blob.SequenceNumber.ToString(CultureInfo.InvariantCulture);
        if (blob.ImmutabilityUntil.HasValue)
        {
            response.Headers["x-ms-immutability-policy-until-date"] = blob.ImmutabilityUntil.Value.ToString("R", CultureInfo.InvariantCulture);
            response.Headers["x-ms-immutability-policy-mode"] = blob.ImmutabilityLocked ? "locked" : "unlocked";
        }
        response.Headers["x-ms-legal-hold"] = blob.HasLegalHold ? "true" : "false";
        if (blob.Kind == Storage.BlobKind.AppendBlob)
        {
            response.Headers["x-ms-blob-committed-block-count"] = blob.AppendBlockCount.ToString(CultureInfo.InvariantCulture);
            response.Headers["x-ms-blob-sealed"] = blob.IsSealed ? "true" : "false";
        }
        if (blob.Copy is not null)
        {
            response.Headers["x-ms-copy-id"] = blob.Copy.Id;
            response.Headers["x-ms-copy-source"] = blob.Copy.Source;
            response.Headers["x-ms-copy-status"] = blob.Copy.Status;
            response.Headers["x-ms-copy-progress"] = $"{blob.Copy.BytesCopied}/{blob.Copy.TotalBytes}";
            if (blob.Copy.CompletedAt.HasValue)
                response.Headers["x-ms-copy-completion-time"] = blob.Copy.CompletedAt.Value.ToString("R", CultureInfo.InvariantCulture);
            SetOptional(response.Headers, "x-ms-copy-status-description", blob.Copy.Description);
        }
        if (blob.IsIncrementalCopy)
            response.Headers["x-ms-incremental-copy"] = "true";
        if (blob.Copy?.Status == "success")
            SetOptional(response.Headers, "x-ms-copy-destination-snapshot", blob.CopyDestinationSnapshot);
    }

    private static void WriteMetadata(
        XmlWriter writer,
        IReadOnlyDictionary<string, string> metadata,
        bool encrypted = false)
    {
        writer.WriteStartElement("Metadata");
        if (encrypted && metadata.Count > 0)
            writer.WriteAttributeString("Encrypted", "true");
        if (!encrypted)
        {
            foreach (var (name, value) in metadata)
                writer.WriteElementString(XmlConvert.EncodeLocalName(name), value);
        }
        writer.WriteEndElement();
    }

    private static void WriteTags(XmlWriter writer, IReadOnlyDictionary<string, string> tags)
    {
        writer.WriteStartElement("Tags");
        WriteTagSet(writer, tags);
        writer.WriteEndElement();
    }

    private static void WriteTagSet(XmlWriter writer, IReadOnlyDictionary<string, string> tags)
    {
        writer.WriteStartElement("TagSet");
        foreach (var (key, value) in tags)
        {
            writer.WriteStartElement("Tag");
            writer.WriteElementString("Key", key);
            writer.WriteElementString("Value", value);
            writer.WriteEndElement();
        }
        writer.WriteEndElement();
    }

    private static void WriteRetentionPolicy(XmlWriter writer, string name, bool enabled, int days)
    {
        writer.WriteStartElement(name);
        writer.WriteElementString("Enabled", enabled ? "true" : "false");
        if (enabled)
            writer.WriteElementString("Days", days.ToString(CultureInfo.InvariantCulture));
        writer.WriteEndElement();
    }

    private static void WriteOptional(XmlWriter writer, string name, string? value)
    {
        if (value is not null)
            writer.WriteElementString(name, value);
    }

    private static void SetOptional(IHeaderDictionary headers, string name, string? value)
    {
        if (value is not null)
            headers[name] = value;
    }

    private static int RemainingRetentionDays(DateTimeOffset retentionUntil) =>
        Math.Max(0, (int)Math.Ceiling((retentionUntil - DateTimeOffset.UtcNow).TotalDays));

    private static string BlobType(Storage.BlobKind kind) => kind switch
    {
        Storage.BlobKind.BlockBlob => "BlockBlob",
        Storage.BlobKind.AppendBlob => "AppendBlob",
        Storage.BlobKind.PageBlob => "PageBlob",
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static string LeaseStatus(LeaseRecord lease) => lease.State == Storage.LeaseState.Leased ? "locked" : "unlocked";

    private static string LeaseStateValue(LeaseRecord lease) => lease.State.ToString().ToLowerInvariant();
}
