using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Xml;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

internal static partial class AzureResponseWriter
{
    private const string LegacyBlobMarkerPrefix = "mk8s1.";
    private const string BlobMarkerPrefix = "mk8s2.";

    private static string ProjectHnsIdentity(HttpContext context, string objectId)
    {
        if (!bool.TryParse(context.Request.Headers["x-ms-upn"], out var projectUpn) ||
            !projectUpn ||
string.Equals(objectId, "$superuser", StringComparison.Ordinal))
            return objectId;

        var principals = context.RequestServices
            .GetRequiredService<IOptions<SavaOptions>>().Value.BearerAuthentication.Principals;
        return principals.TryGetValue(objectId, out var principal) &&
               !string.IsNullOrWhiteSpace(principal.UserPrincipalName)
            ? principal.UserPrincipalName
            : objectId;
    }

    public static async Task WriteXmlAsync(HttpContext context, Action<XmlWriter> write, CancellationToken cancellationToken)
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
        await context.Response.WriteAsync(builder.ToString(), cancellationToken).ConfigureAwait(false);
    }

    internal static Task WriteContainersAsync(
        HttpContext context,
        ContainerListPage page,
        string prefix,
        string marker,
        int maxResults,
        bool includeMetadata,
        bool includeDeleted,
        CancellationToken cancellationToken)
    {
        var request = StorageRequestContext.Get(context);
        var nextMarker = page.HasMore && page.Items.Count > 0 ? page.Items[^1].Name : string.Empty;
        var endpoint = StorageResourcePath.GetServiceEndpoint(context.Request, request.Account);
        var usesModernEndpointShape = IsServiceVersionAtLeast(request, new DateOnly(2013, 8, 15));

        return WriteXmlAsync(context, writer =>
        {
            writer.WriteStartElement("EnumerationResults");
            writer.WriteAttributeString(usesModernEndpointShape ? "ServiceEndpoint" : "AccountName", endpoint);
            if (context.Request.Query.ContainsKey("prefix"))
                writer.WriteElementString("Prefix", prefix);
            if (context.Request.Query.ContainsKey("marker"))
                writer.WriteElementString("Marker", marker);
            if (context.Request.Query.ContainsKey("maxresults"))
                writer.WriteElementString("MaxResults", maxResults.ToString(CultureInfo.InvariantCulture));
            writer.WriteStartElement("Containers");
            foreach (var container in page.Items)
                WriteContainerElement(writer, context, request, container, includeMetadata, includeDeleted, usesModernEndpointShape);
            writer.WriteEndElement();
            writer.WriteElementString("NextMarker", nextMarker);
            writer.WriteEndElement();
        }, cancellationToken);
    }

    private static void WriteContainerElement(
        XmlWriter writer,
        HttpContext context,
        StorageRequestContext request,
        ContainerRecord container,
        bool includeMetadata,
        bool includeDeleted,
        bool usesModernEndpointShape)
    {
        writer.WriteStartElement("Container");
        writer.WriteElementString("Name", container.Name);
        if (!usesModernEndpointShape)
        {
            writer.WriteElementString(
                "Url",
                StorageResourcePath.GetContainerEndpoint(context.Request, request.Account, container.Name));
        }
        var isDeleted = includeDeleted && container.DeletedAt.HasValue;
        if (isDeleted)
        {
            WriteOptional(writer, "Version", container.DeletedVersion);
            writer.WriteElementString("Deleted", "true");
        }
        WriteContainerProperties(writer, request, container, isDeleted);
        if (includeMetadata)
            WriteMetadata(writer, container.Metadata);
        writer.WriteEndElement();
    }

    private static void WriteContainerProperties(
        XmlWriter writer,
        StorageRequestContext request,
        ContainerRecord container,
        bool isDeleted)
    {
        writer.WriteStartElement("Properties");
        writer.WriteElementString(
            IsServiceVersionAtLeast(request, new DateOnly(2009, 9, 19)) ? "Last-Modified" : "LastModified",
            container.LastModified.ToString("R", CultureInfo.InvariantCulture));
        writer.WriteElementString("Etag", FormatEntityTag(request, container.ETag));
        if (!isDeleted && IsServiceVersionAtLeast(request, new DateOnly(2012, 2, 12)))
        {
            writer.WriteElementString("LeaseStatus", LeaseStatus(container.Lease));
            writer.WriteElementString("LeaseState", LeaseStateValue(container.Lease));
            if (container.Lease.State == Storage.LeaseState.Leased)
            {
                writer.WriteElementString(
                    "LeaseDuration",
                    container.Lease.DurationSeconds == -1 ? "infinite" : "fixed");
            }
        }
        if (!isDeleted && IsServiceVersionAtLeast(request, new DateOnly(2016, 5, 31)))
            WriteOptional(writer, "PublicAccess", container.PublicAccess);
        if (!isDeleted && IsServiceVersionAtLeast(request, new DateOnly(2017, 11, 9)))
        {
            writer.WriteElementString("HasImmutabilityPolicy", container.ImmutabilityUntil.HasValue ? "true" : "false");
            writer.WriteElementString("HasLegalHold", container.HasLegalHold ? "true" : "false");
        }
        if (!isDeleted && container.DefaultEncryptionScope is not null &&
            IsServiceVersionAtLeast(request, new DateOnly(2019, 7, 7)))
        {
            writer.WriteElementString("DefaultEncryptionScope", container.DefaultEncryptionScope);
            writer.WriteElementString(
                "DenyEncryptionScopeOverride",
                container.PreventEncryptionScopeOverride ? "true" : "false");
        }
        if (!isDeleted && IsServiceVersionAtLeast(request, new DateOnly(2020, 10, 2)))
        {
            writer.WriteElementString(
                "ImmutableStorageWithVersioningEnabled",
                container.ImmutableStorageWithVersioningEnabled ? "true" : "false");
        }
        if (isDeleted)
        {
            WriteOptional(writer, "DeletedTime", container.DeletedAt?.ToString("R", CultureInfo.InvariantCulture));
            if (container.DeleteRetentionUntil.HasValue)
            {
                writer.WriteElementString(
                    "RemainingRetentionDays",
                    RemainingRetentionDays(container.DeleteRetentionUntil.Value).ToString(CultureInfo.InvariantCulture));
            }
        }
        writer.WriteEndElement();
    }

    internal static Task WriteBlobsAsync(
        HttpContext context,
        BlobListPage listing,
        string prefix,
        string startFrom,
        string endBefore,
        string delimiter,
        string marker,
        int maxResults,
        IReadOnlySet<string> includes,
        bool arrow,
        bool hierarchicalNamespace,
        bool lastAccessTimeTracking,
        CancellationToken cancellationToken)
    {
        var request = StorageRequestContext.Get(context);
        var listingScope = CreateBlobListingScope(
            request,
            prefix,
            startFrom,
            endBefore,
            delimiter,
            context.Request.Query["showonly"].ToString(),
            includes);
        var nextMarker = listing.HasMore && listing.Items.Count > 0
            ? EncodeBlobMarker(listing.Items[^1].Cursor, listingScope)
            : string.Empty;
        if (arrow)
        {
            return WriteArrowBlobsAsync(
                context,
                listing,
                nextMarker,
                includes,
                hierarchicalNamespace,
                lastAccessTimeTracking,
                cancellationToken);
        }
        var usesModernEndpointShape = IsServiceVersionAtLeast(request, new DateOnly(2013, 8, 15));
        var usesModernPropertyShape = IsServiceVersionAtLeast(request, new DateOnly(2009, 9, 19));

        return WriteXmlAsync(context, writer =>
        {
            WriteBlobListingEnvelopeStartXml(
                writer,
                context,
                request,
                prefix,
                delimiter,
                marker,
                maxResults,
                usesModernEndpointShape);
            writer.WriteStartElement("Blobs");
            foreach (var entry in listing.Items)
            {
                WriteBlobListEntryXml(
                    writer,
                    context,
                    request,
                    entry,
                    includes,
                    usesModernEndpointShape,
                    usesModernPropertyShape,
                    hierarchicalNamespace,
                    lastAccessTimeTracking);
            }
            writer.WriteEndElement();
            writer.WriteElementString("NextMarker", nextMarker);
            writer.WriteEndElement();
        }, cancellationToken);
    }

    private static void WriteBlobListingEnvelopeStartXml(
        XmlWriter writer,
        HttpContext context,
        StorageRequestContext request,
        string prefix,
        string delimiter,
        string marker,
        int maxResults,
        bool usesModernEndpointShape)
    {
        writer.WriteStartElement("EnumerationResults");
        if (usesModernEndpointShape)
        {
            writer.WriteAttributeString(
                "ServiceEndpoint",
                StorageResourcePath.GetServiceEndpoint(context.Request, request.Account));
            writer.WriteAttributeString("ContainerName", request.Container);
        }
        else
        {
            writer.WriteAttributeString(
                "ContainerName",
                StorageResourcePath.GetContainerEndpoint(context.Request, request.Account, request.Container!));
        }
        if (context.Request.Query.ContainsKey("prefix"))
            writer.WriteElementString("Prefix", prefix);
        if (context.Request.Query.ContainsKey("marker"))
            writer.WriteElementString("Marker", marker);
        if (context.Request.Query.ContainsKey("delimiter"))
            writer.WriteElementString("Delimiter", delimiter);
        if (context.Request.Query.ContainsKey("maxresults"))
            writer.WriteElementString("MaxResults", maxResults.ToString(CultureInfo.InvariantCulture));
    }

    private static void WriteBlobListEntryXml(
        XmlWriter writer,
        HttpContext context,
        StorageRequestContext request,
        BlobListEntry entry,
        IReadOnlySet<string> includes,
        bool usesModernEndpointShape,
        bool usesModernPropertyShape,
        bool hierarchicalNamespace,
        bool lastAccessTimeTracking)
    {
        if (entry.Prefix is not null)
        {
            WriteBlobPrefixXml(writer, context, request, entry, includes, hierarchicalNamespace);
            return;
        }
        if (entry.IsUncommitted)
        {
            WriteUncommittedBlobXml(writer, context, request, entry, usesModernEndpointShape);
            return;
        }

        var blob = entry.Blob!;
        WriteListedBlobIdentityXml(
            writer,
            context,
            request,
            blob,
            includes,
            usesModernEndpointShape,
            hierarchicalNamespace);
        if (!usesModernPropertyShape)
        {
            WriteLegacyBlobProperties(writer, request, blob);
            writer.WriteEndElement();
            return;
        }
        writer.WriteStartElement("Properties");
        WriteListedBlobCorePropertiesXml(writer, context, request, blob, includes, hierarchicalNamespace);
        WriteListedBlobTierAndEncryptionXml(writer, request, blob, hierarchicalNamespace, lastAccessTimeTracking);
        WriteListedBlobStateXml(writer, request, blob);
        WriteListedBlobRetentionAndCopyXml(writer, request, blob, includes);
        writer.WriteEndElement();
        WriteListedBlobIncludedXml(writer, request, blob, includes);
        writer.WriteEndElement();
    }

    private static void WriteBlobPrefixXml(
        XmlWriter writer,
        HttpContext context,
        StorageRequestContext request,
        BlobListEntry entry,
        IReadOnlySet<string> includes,
        bool hierarchicalNamespace)
    {
        writer.WriteStartElement("BlobPrefix");
        writer.WriteElementString("Name", entry.Prefix);
        if (hierarchicalNamespace &&
            IsServiceVersionAtLeast(request, new DateOnly(2020, 6, 12)))
        {
            var directory = entry.Blob;
            writer.WriteStartElement("Properties");
            if (directory is not null)
            {
                if (IsServiceVersionAtLeast(request, new DateOnly(2017, 11, 9)))
                {
                    writer.WriteElementString(
                        "Creation-Time",
                        directory.CreatedAt.ToString("R", CultureInfo.InvariantCulture));
                }
                writer.WriteElementString(
                    "Last-Modified",
                    directory.LastModified.ToString("R", CultureInfo.InvariantCulture));
                writer.WriteElementString("Etag", FormatEntityTag(request, directory.ETag));
                if (includes.Contains("permissions"))
                {
                    writer.WriteElementString("Owner", ProjectHnsIdentity(context, directory.Owner));
                    writer.WriteElementString("Group", ProjectHnsIdentity(context, directory.Group));
                    writer.WriteElementString("Permissions", directory.Permissions);
                    writer.WriteElementString("Acl", directory.Acl);
                }
            }
            if (IsServiceVersionAtLeast(request, new DateOnly(2020, 10, 2)))
                writer.WriteElementString("ResourceType", "directory");
            if (directory is null &&
                string.Equals(
                    context.Request.Query["showonly"],
                    "deleted",
                    StringComparison.Ordinal) &&
                IsServiceVersionAtLeast(request, new DateOnly(2021, 6, 8)))
            {
                writer.WriteElementString("Placeholder", "true");
            }
            writer.WriteElementString("Content-Length", "0");
            writer.WriteElementString("BlobType", "BlockBlob");
            if (IsServiceVersionAtLeast(request, new DateOnly(2015, 12, 11)))
                writer.WriteElementString("ServerEncrypted", "true");
            writer.WriteEndElement();
        }
        writer.WriteEndElement();
    }

    private static void WriteUncommittedBlobXml(
        XmlWriter writer,
        HttpContext context,
        StorageRequestContext request,
        BlobListEntry entry,
        bool usesModernEndpointShape)
    {
        writer.WriteStartElement("Blob");
        writer.WriteElementString("Name", entry.Name);
        if (!usesModernEndpointShape)
        {
            writer.WriteElementString(
                "Url",
                StorageResourcePath.GetBlobEndpoint(
                    context.Request,
                    request.Account,
                    request.Container!,
                    entry.Name));
        }
        writer.WriteStartElement("Properties");
        writer.WriteElementString("Content-Length", "0");
        writer.WriteElementString("BlobType", "BlockBlob");
        writer.WriteEndElement();
        writer.WriteEndElement();
    }

    private static void WriteListedBlobIdentityXml(
        XmlWriter writer,
        HttpContext context,
        StorageRequestContext request,
        BlobRecord blob,
        IReadOnlySet<string> includes,
        bool usesModernEndpointShape,
        bool hierarchicalNamespace)
    {
        writer.WriteStartElement("Blob");
        writer.WriteElementString("Name", blob.Name);
        if (!usesModernEndpointShape)
        {
            writer.WriteElementString(
                "Url",
                StorageResourcePath.GetBlobEndpoint(
                    context.Request,
                    request.Account,
                    request.Container!,
                    blob.Name));
        }
        if (blob.Snapshot is not null)
            writer.WriteElementString("Snapshot", blob.Snapshot);
        if (includes.Contains("versions") && blob.VersionId is not null)
        {
            writer.WriteElementString("VersionId", blob.VersionId);
            writer.WriteElementString("IsCurrentVersion", blob.IsCurrent ? "true" : "false");
        }
        if (blob.IsDeleted &&
            (hierarchicalNamespace ||
             includes.Contains("deleted") ||
             includes.Contains("deletedwithversions")))
        {
            writer.WriteElementString("Deleted", "true");
        }
        if (hierarchicalNamespace &&
            blob.IsDeleted &&
            blob.DeletionId.HasValue &&
            IsServiceVersionAtLeast(request, new DateOnly(2020, 8, 4)))
        {
            writer.WriteElementString(
                "DeletionId",
                blob.DeletionId.Value.ToString(CultureInfo.InvariantCulture));
        }
    }

    private static void WriteListedBlobCorePropertiesXml(
        XmlWriter writer,
        HttpContext context,
        StorageRequestContext request,
        BlobRecord blob,
        IReadOnlySet<string> includes,
        bool hierarchicalNamespace)
    {
        if (IsServiceVersionAtLeast(request, new DateOnly(2017, 11, 9)))
            writer.WriteElementString("Creation-Time", blob.CreatedAt.ToString("R", CultureInfo.InvariantCulture));
        writer.WriteElementString("Last-Modified", blob.LastModified.ToString("R", CultureInfo.InvariantCulture));
        writer.WriteElementString("Etag", FormatEntityTag(request, blob.ETag));
        if (hierarchicalNamespace && includes.Contains("permissions"))
        {
            writer.WriteElementString("Owner", ProjectHnsIdentity(context, blob.Owner));
            writer.WriteElementString("Group", ProjectHnsIdentity(context, blob.Group));
            writer.WriteElementString("Permissions", blob.Permissions);
            writer.WriteElementString("Acl", blob.Acl);
        }
        if (hierarchicalNamespace && IsServiceVersionAtLeast(request, new DateOnly(2020, 10, 2)))
            writer.WriteElementString("ResourceType", blob.IsDirectory ? "directory" : "file");
        writer.WriteElementString("Content-Length", blob.Content.Length.ToString(CultureInfo.InvariantCulture));
        writer.WriteElementString("Content-Type", blob.Http.ContentType);
        writer.WriteElementString("Content-Encoding", blob.Http.ContentEncoding ?? string.Empty);
        writer.WriteElementString("Content-Language", blob.Http.ContentLanguage ?? string.Empty);
        WriteOptional(writer, "Content-MD5", blob.Http.ContentMd5);
        writer.WriteElementString("Cache-Control", blob.Http.CacheControl ?? string.Empty);
        if (IsServiceVersionAtLeast(request, new DateOnly(2013, 8, 15)))
            writer.WriteElementString("Content-Disposition", blob.Http.ContentDisposition ?? string.Empty);
        writer.WriteElementString("BlobType", BlobType(blob.Kind));
    }

    private static void WriteListedBlobTierAndEncryptionXml(
        XmlWriter writer,
        StorageRequestContext request,
        BlobRecord blob,
        bool hierarchicalNamespace,
        bool lastAccessTimeTracking)
    {
        if (blob.Kind == Storage.BlobKind.BlockBlob &&
            IsServiceVersionAtLeast(request, new DateOnly(2017, 4, 17)))
        {
            writer.WriteElementString("AccessTier", blob.AccessTier);
            if (blob.AccessTierInferred)
                writer.WriteElementString("AccessTierInferred", "true");
            if (string.Equals(blob.AccessTier, "Smart", StringComparison.Ordinal) &&
                IsServiceVersionAtLeast(request, new DateOnly(2026, 2, 6)))
            {
                WriteOptional(writer, "SmartAccessTier", blob.SmartAccessTier);
            }
            WriteOptional(writer, "ArchiveStatus", blob.ArchiveStatus);
            WriteOptional(writer, "AccessTierChangeTime", blob.AccessTierChangedAt?.ToString("R", CultureInfo.InvariantCulture));
        }
        if (IsServiceVersionAtLeast(request, new DateOnly(2015, 12, 11)))
            writer.WriteElementString("ServerEncrypted", "true");
        if (IsServiceVersionAtLeast(request, new DateOnly(2019, 2, 2)))
        {
            WriteOptional(writer, "CustomerProvidedKeySha256", blob.CustomerProvidedKeySha256);
            WriteOptional(writer, "EncryptionScope", blob.EncryptionScope);
        }
        if (hierarchicalNamespace && IsServiceVersionAtLeast(request, new DateOnly(2021, 6, 8)))
            WriteOptional(writer, "EncryptionContext", blob.EncryptionContext);
        if (IsServiceVersionAtLeast(request, new DateOnly(2019, 12, 12)))
            WriteOptional(writer, "RehydratePriority", blob.RehydratePriority);
        if (lastAccessTimeTracking &&
            IsServiceVersionAtLeast(request, new DateOnly(2020, 2, 10)))
        {
            WriteOptional(
                writer,
                "LastAccessTime",
                blob.LastAccessedAt?.ToString("R", CultureInfo.InvariantCulture));
        }
        if (hierarchicalNamespace && IsServiceVersionAtLeast(request, new DateOnly(2020, 2, 10)))
            WriteOptional(writer, "Expiry-Time", blob.ExpiresAt?.ToString("R", CultureInfo.InvariantCulture));
    }

    private static void WriteListedBlobStateXml(XmlWriter writer, StorageRequestContext request, BlobRecord blob)
    {
        if (blob.IsDeleted && IsServiceVersionAtLeast(request, new DateOnly(2017, 7, 29)))
        {
            WriteOptional(writer, "DeletedTime", blob.DeletedAt?.ToString("R", CultureInfo.InvariantCulture));
            if (blob.DeleteRetentionUntil.HasValue)
            {
                writer.WriteElementString(
                    "RemainingRetentionDays",
                    RemainingRetentionDays(blob.DeleteRetentionUntil.Value).ToString(CultureInfo.InvariantCulture));
            }
        }
        if (blob.Snapshot is null &&
            !blob.IsDeleted &&
            IsServiceVersionAtLeast(request, new DateOnly(2009, 9, 19)))
        {
            writer.WriteElementString("LeaseStatus", LeaseStatus(blob.Lease));
            if (IsServiceVersionAtLeast(request, new DateOnly(2012, 2, 12)))
            {
                writer.WriteElementString("LeaseState", LeaseStateValue(blob.Lease));
                if (blob.Lease.State == Storage.LeaseState.Leased)
                {
                    writer.WriteElementString(
                        "LeaseDuration",
                        blob.Lease.DurationSeconds == -1 ? "infinite" : "fixed");
                }
            }
        }
        if (blob.Kind == Storage.BlobKind.PageBlob)
        {
            writer.WriteElementString(
                "x-ms-blob-sequence-number",
                blob.SequenceNumber.ToString(CultureInfo.InvariantCulture));
        }
        if (blob.Kind == Storage.BlobKind.AppendBlob)
        {
            writer.WriteElementString("CommittedBlockCount", blob.AppendBlockCount.ToString(CultureInfo.InvariantCulture));
            if (blob.IsSealed && IsServiceVersionAtLeast(request, new DateOnly(2019, 12, 12)))
                writer.WriteElementString("Sealed", "true");
        }
        if (blob.Tags.Count > 0 && IsServiceVersionAtLeast(request, new DateOnly(2019, 12, 12)))
            writer.WriteElementString("TagCount", blob.Tags.Count.ToString(CultureInfo.InvariantCulture));
    }

    private static void WriteListedBlobRetentionAndCopyXml(
        XmlWriter writer,
        StorageRequestContext request,
        BlobRecord blob,
        IReadOnlySet<string> includes)
    {
        if (includes.Contains("immutabilitypolicy") && blob.ImmutabilityUntil.HasValue)
        {
            writer.WriteElementString("ImmutabilityPolicyUntilDate", blob.ImmutabilityUntil.Value.ToString("R", CultureInfo.InvariantCulture));
            writer.WriteElementString("ImmutabilityPolicyMode", blob.ImmutabilityLocked ? "locked" : "unlocked");
        }
        if (includes.Contains("legalhold"))
            writer.WriteElementString("LegalHold", blob.HasLegalHold ? "true" : "false");
        if (includes.Contains("copy") &&
            blob.Copy is not null &&
            IsServiceVersionAtLeast(request, new DateOnly(2012, 2, 12)))
        {
            writer.WriteElementString("CopyId", blob.Copy.Id);
            writer.WriteElementString("CopySource", blob.Copy.Source);
            writer.WriteElementString("CopyStatus", blob.Copy.Status);
            writer.WriteElementString("CopyProgress", $"{blob.Copy.BytesCopied}/{blob.Copy.TotalBytes}");
            WriteOptional(writer, "CopyCompletionTime", blob.Copy.CompletedAt?.ToString("R", CultureInfo.InvariantCulture));
            WriteOptional(writer, "CopyStatusDescription", blob.Copy.Description);
        }
        if (blob.IsIncrementalCopy && IsServiceVersionAtLeast(request, new DateOnly(2016, 5, 31)))
            writer.WriteElementString("IncrementalCopy", "true");
        if (string.Equals(blob.Copy?.Status, "success", StringComparison.Ordinal) && IsServiceVersionAtLeast(request, new DateOnly(2016, 5, 31)))
            WriteOptional(writer, "DestinationSnapshot", blob.CopyDestinationSnapshot);
    }

    private static void WriteListedBlobIncludedXml(
        XmlWriter writer,
        StorageRequestContext request,
        BlobRecord blob,
        IReadOnlySet<string> includes)
    {
        if (includes.Contains("metadata"))
            WriteMetadata(writer, blob.Metadata, blob.CustomerProvidedKeySha256 is not null);
        if (includes.Contains("tags") && blob.Tags.Count > 0)
            WriteTags(writer, blob.Tags);
        if (blob.Kind == Storage.BlobKind.BlockBlob &&
            blob.ObjectReplicationStatuses.Count > 0 &&
            IsServiceVersionAtLeast(request, new DateOnly(2019, 12, 12)))
        {
            writer.WriteStartElement("OrMetadata");
            foreach (var status in blob.ObjectReplicationStatuses.OrderBy(
                         pair => pair.Key,
                         StringComparer.Ordinal))
            {
                writer.WriteElementString($"or-{status.Key}", status.Value.Status);
            }
            writer.WriteEndElement();
        }
    }

    public static Task WriteTagsAsync(HttpContext context, IReadOnlyDictionary<string, string> tags, CancellationToken cancellationToken) =>
        WriteXmlAsync(context, writer =>
        {
            writer.WriteStartElement("Tags");
            WriteTagSet(writer, tags);
            writer.WriteEndElement();
        }, cancellationToken);

    public static Task WriteBlockListAsync(
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

    public static Task WritePageRangesAsync(
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

    public static Task WriteAclAsync(HttpContext context, ContainerRecord container, CancellationToken cancellationToken) =>
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

    public static Task WriteServicePropertiesAsync(HttpContext context, ServiceProperties properties, CancellationToken cancellationToken)
    {
        var request = StorageRequestContext.Get(context);
        return WriteXmlAsync(context, writer =>
        {
            writer.WriteStartElement("StorageServiceProperties");
            WriteAnalyticsLogging(writer, properties.Logging);
            if (IsServiceVersionAtLeast(request, new DateOnly(2013, 8, 15)))
            {
                WriteAnalyticsMetrics(writer, "HourMetrics", properties.HourMetrics);
                WriteAnalyticsMetrics(writer, "MinuteMetrics", properties.MinuteMetrics);
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
            }
            else
            {
                WriteAnalyticsMetrics(writer, "Metrics", properties.HourMetrics);
            }
            writer.WriteStartElement("DefaultServiceVersion");
            writer.WriteString(properties.DefaultServiceVersion ?? string.Empty);
            writer.WriteEndElement();
            if (IsServiceVersionAtLeast(request, new DateOnly(2017, 7, 29)))
                WriteRetentionPolicy(
                    writer,
                    "DeleteRetentionPolicy",
                    properties.BlobSoftDeleteEnabled,
                    properties.BlobSoftDeleteRetentionDays,
                    IsServiceVersionAtLeast(request, new DateOnly(2020, 2, 10))
                        ? properties.BlobPermanentDeleteEnabled
                        : null);
            if (IsServiceVersionAtLeast(request, new DateOnly(2018, 3, 28)))
            {
                writer.WriteStartElement("StaticWebsite");
                writer.WriteElementString("Enabled", properties.StaticWebsite.Enabled ? "true" : "false");
                WriteOptional(writer, "IndexDocument", properties.StaticWebsite.IndexDocument);
                if (IsServiceVersionAtLeast(request, new DateOnly(2019, 12, 12)))
                    WriteOptional(writer, "DefaultIndexDocumentPath", properties.StaticWebsite.DefaultIndexDocumentPath);
                WriteOptional(writer, "ErrorDocument404Path", properties.StaticWebsite.ErrorDocument404Path);
                writer.WriteEndElement();
            }
            writer.WriteEndElement();
        }, cancellationToken);
    }

    public static Task WriteUserDelegationKeyAsync(
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
        AddEntityTag(response, container.ETag);
        response.Headers.LastModified = container.LastModified.ToString("R", CultureInfo.InvariantCulture);
    }

    public static void AddContainerMetadataHeaders(HttpResponse response, ContainerRecord container)
    {
        AddContainerHeaders(response, container);
        foreach (var (name, value) in container.Metadata)
            response.Headers[$"x-ms-meta-{name}"] = value;
    }

    public static void AddContainerPropertiesHeaders(HttpResponse response, ContainerRecord container)
    {
        AddContainerMetadataHeaders(response, container);
        var request = StorageRequestContext.Get(response.HttpContext);
        if (IsServiceVersionAtLeast(request, new DateOnly(2012, 2, 12)))
        {
            response.Headers["x-ms-lease-status"] = LeaseStatus(container.Lease);
            response.Headers["x-ms-lease-state"] = LeaseStateValue(container.Lease);
            if (container.Lease.State == Storage.LeaseState.Leased)
                response.Headers["x-ms-lease-duration"] = container.Lease.DurationSeconds == -1 ? "infinite" : "fixed";
        }
        AddContainerPublicAccessHeader(response, container);
        if (IsServiceVersionAtLeast(request, new DateOnly(2017, 11, 9)))
        {
            response.Headers["x-ms-has-immutability-policy"] = "false";
            response.Headers["x-ms-has-legal-hold"] = "false";
        }
        if (container.DefaultEncryptionScope is not null &&
            IsServiceVersionAtLeast(request, new DateOnly(2019, 7, 7)))
        {
            response.Headers["x-ms-default-encryption-scope"] = container.DefaultEncryptionScope;
            response.Headers["x-ms-deny-encryption-scope-override"] =
                container.PreventEncryptionScopeOverride ? "true" : "false";
        }
        if (IsServiceVersionAtLeast(request, new DateOnly(2020, 10, 2)))
        {
            response.Headers["x-ms-immutable-storage-with-versioning-enabled"] =
                container.ImmutableStorageWithVersioningEnabled ? "true" : "false";
        }
    }

    public static void AddContainerAccessPolicyHeaders(HttpResponse response, ContainerRecord container)
    {
        AddContainerHeaders(response, container);
        AddContainerPublicAccessHeader(response, container);
    }

    public static void AddBlobMetadataHeaders(HttpResponse response, BlobRecord blob)
    {
        AddBlobEntityHeaders(response, blob);
        foreach (var (name, value) in blob.Metadata)
            response.Headers[$"x-ms-meta-{name}"] = value;
    }

    public static void AddBlobEntityHeaders(HttpResponse response, BlobRecord blob)
    {
        AddEntityTag(response, blob.ETag);
        response.Headers.LastModified = blob.LastModified.ToString("R", CultureInfo.InvariantCulture);
    }

    public static void AddBlobWriteHeaders(HttpResponse response, BlobRecord blob)
    {
        AddBlobEntityHeaders(response, blob);
        AddBlobVersionHeader(response, blob);
    }

    public static void AddBlobCopyHeaders(HttpResponse response, BlobRecord blob, bool includeVersion)
    {
        AddBlobEntityHeaders(response, blob);
        if (blob.Copy is not null)
        {
            response.Headers["x-ms-copy-id"] = blob.Copy.Id;
            response.Headers["x-ms-copy-status"] = blob.Copy.Status;
        }
        if (includeVersion)
            AddBlobVersionHeader(response, blob);
    }

    public static void AddPageBlobWriteHeaders(HttpResponse response, BlobRecord blob)
    {
        AddBlobEntityHeaders(response, blob);
        response.Headers["x-ms-blob-sequence-number"] = blob.SequenceNumber.ToString(CultureInfo.InvariantCulture);
    }

    public static void AddAppendBlobSealHeaders(HttpResponse response, BlobRecord blob)
    {
        AddBlobEntityHeaders(response, blob);
        response.Headers["x-ms-blob-sealed"] = blob.IsSealed ? "true" : "false";
    }

    public static void AddBlobQueryHeaders(HttpResponse response, BlobRecord blob)
    {
        var request = StorageRequestContext.Get(response.HttpContext);
        AddBlobEntityHeaders(response, blob);
        SetOptional(response.Headers, "Content-Encoding", blob.Http.ContentEncoding);
        SetOptional(response.Headers, "Content-Language", blob.Http.ContentLanguage);
        SetOptional(response.Headers, "Cache-Control", blob.Http.CacheControl);
        if (IsServiceVersionAtLeast(request, new DateOnly(2013, 8, 15)))
            SetOptional(response.Headers, "Content-Disposition", blob.Http.ContentDisposition);
        response.Headers["x-ms-blob-type"] = BlobType(blob.Kind);
        if (blob.Kind == Storage.BlobKind.AppendBlob)
            response.Headers["x-ms-blob-committed-block-count"] = blob.AppendBlockCount.ToString(CultureInfo.InvariantCulture);
        if (IsServiceVersionAtLeast(request, new DateOnly(2015, 12, 11)))
            response.Headers["x-ms-server-encrypted"] = "true";
    }

    private static void AddContainerPublicAccessHeader(HttpResponse response, ContainerRecord container)
    {
        if (container.PublicAccess is not null)
            response.Headers["x-ms-blob-public-access"] = container.PublicAccess;
    }

    private static void AddBlobVersionHeader(HttpResponse response, BlobRecord blob)
    {
        if (blob.VersionId is not null &&
            IsServiceVersionAtLeast(StorageRequestContext.Get(response.HttpContext), new DateOnly(2019, 12, 12)))
        {
            response.Headers["x-ms-version-id"] = blob.VersionId;
        }
    }

    public static void AddBlobHeaders(
        HttpResponse response,
        BlobRecord blob,
        bool hierarchicalNamespace = false,
        bool lastAccessTimeTracking = false)
    {
        var request = StorageRequestContext.Get(response.HttpContext);
        AddBlobIdentityAndEncryptionHeaders(response, blob, request, hierarchicalNamespace);
        AddBlobTierAndReplicationHeaders(response, blob, request, hierarchicalNamespace, lastAccessTimeTracking);
        AddBlobHttpAndLeaseHeaders(response, blob, request);
        AddBlobStateAndCopyHeaders(response, blob, request);
    }

    private static void AddBlobIdentityAndEncryptionHeaders(
        HttpResponse response,
        BlobRecord blob,
        StorageRequestContext request,
        bool hierarchicalNamespace)
    {
        AddBlobEntityHeaders(response, blob);
        if (IsServiceVersionAtLeast(request, new DateOnly(2017, 11, 9)))
            response.Headers["x-ms-creation-time"] = blob.CreatedAt.ToString("R", CultureInfo.InvariantCulture);
        if (hierarchicalNamespace && IsServiceVersionAtLeast(request, new DateOnly(2020, 6, 12)))
        {
            response.Headers["x-ms-owner"] = ProjectHnsIdentity(response.HttpContext, blob.Owner);
            response.Headers["x-ms-group"] = ProjectHnsIdentity(response.HttpContext, blob.Group);
            response.Headers["x-ms-permissions"] = blob.Permissions;
        }
        if (hierarchicalNamespace && IsServiceVersionAtLeast(request, new DateOnly(2020, 10, 2)))
            response.Headers["x-ms-resource-type"] = blob.IsDirectory ? "directory" : "file";
        if (hierarchicalNamespace && IsServiceVersionAtLeast(request, new DateOnly(2023, 11, 3)))
        {
            response.Headers["x-ms-acl"] = blob.Acl;
        }
        response.Headers["x-ms-blob-type"] = BlobType(blob.Kind);
        if (IsServiceVersionAtLeast(request, new DateOnly(2015, 12, 11)))
            response.Headers["x-ms-server-encrypted"] = "true";
        if (IsServiceVersionAtLeast(request, new DateOnly(2019, 2, 2)))
        {
            SetOptional(response.Headers, "x-ms-encryption-key-sha256", blob.CustomerProvidedKeySha256);
            SetOptional(response.Headers, "x-ms-encryption-scope", blob.EncryptionScope);
        }
        if (hierarchicalNamespace && IsServiceVersionAtLeast(request, new DateOnly(2021, 8, 6)))
            SetOptional(response.Headers, "x-ms-encryption-context", blob.EncryptionContext);
    }

    private static void AddBlobTierAndReplicationHeaders(
        HttpResponse response,
        BlobRecord blob,
        StorageRequestContext request,
        bool hierarchicalNamespace,
        bool lastAccessTimeTracking)
    {
        if (blob.Kind == Storage.BlobKind.BlockBlob &&
            IsServiceVersionAtLeast(request, new DateOnly(2017, 4, 17)))
        {
            response.Headers["x-ms-access-tier"] = blob.AccessTier;
            if (blob.AccessTierInferred)
                response.Headers["x-ms-access-tier-inferred"] = "true";
            if (string.Equals(blob.AccessTier, "Smart", StringComparison.Ordinal) &&
                IsServiceVersionAtLeast(request, new DateOnly(2026, 2, 6)))
            {
                SetOptional(response.Headers, "x-ms-smart-access-tier", blob.SmartAccessTier);
            }
            SetOptional(response.Headers, "x-ms-archive-status", blob.ArchiveStatus);
            SetOptional(response.Headers, "x-ms-access-tier-change-time", blob.AccessTierChangedAt?.ToString("R", CultureInfo.InvariantCulture));
        }
        if (IsServiceVersionAtLeast(request, new DateOnly(2019, 12, 12)))
        {
            SetOptional(response.Headers, "x-ms-rehydrate-priority", blob.RehydratePriority);
            if (blob.Kind == Storage.BlobKind.BlockBlob)
            {
                SetOptional(
                    response.Headers,
                    "x-ms-or-policy-id",
                    blob.ObjectReplicationDestinationPolicyId);
                foreach (var status in blob.ObjectReplicationStatuses.OrderBy(
                             pair => pair.Key,
                             StringComparer.Ordinal))
                {
                    response.Headers[$"x-ms-or-{status.Key}"] = status.Value.Status;
                }
            }
        }
        if (lastAccessTimeTracking && IsServiceVersionAtLeast(request, new DateOnly(2020, 2, 10)))
            SetOptional(response.Headers, "x-ms-last-access-time", blob.LastAccessedAt?.ToString("R", CultureInfo.InvariantCulture));
        if (hierarchicalNamespace && IsServiceVersionAtLeast(request, new DateOnly(2020, 2, 10)))
            SetOptional(response.Headers, "x-ms-expiry-time", blob.ExpiresAt?.ToString("R", CultureInfo.InvariantCulture));
    }

    private static void AddBlobHttpAndLeaseHeaders(
        HttpResponse response,
        BlobRecord blob,
        StorageRequestContext request)
    {
        response.Headers["x-ms-lease-status"] = LeaseStatus(blob.Lease);
        if (IsServiceVersionAtLeast(request, new DateOnly(2012, 2, 12)))
        {
            response.Headers["x-ms-lease-state"] = LeaseStateValue(blob.Lease);
            if (blob.Lease.State == Storage.LeaseState.Leased)
                response.Headers["x-ms-lease-duration"] = blob.Lease.DurationSeconds == -1 ? "infinite" : "fixed";
        }
        if (IsServiceVersionAtLeast(request, new DateOnly(2011, 8, 18)))
            response.Headers["Accept-Ranges"] = "bytes";
        response.ContentType = blob.Http.ContentType;
        SetOptional(response.Headers, "Content-Encoding", blob.Http.ContentEncoding);
        SetOptional(response.Headers, "Content-Language", blob.Http.ContentLanguage);
        SetOptional(response.Headers, "Cache-Control", blob.Http.CacheControl);
        if (IsServiceVersionAtLeast(request, new DateOnly(2013, 8, 15)))
            SetOptional(response.Headers, "Content-Disposition", blob.Http.ContentDisposition);
        var isRangedGet = HttpMethods.IsGet(response.HttpContext.Request.Method) &&
                          (response.HttpContext.Request.Headers.ContainsKey("x-ms-range") ||
                           response.HttpContext.Request.Headers.ContainsKey("Range"));
        if (isRangedGet)
        {
            if (IsServiceVersionAtLeast(request, new DateOnly(2016, 5, 31)))
                SetOptional(response.Headers, "x-ms-blob-content-md5", blob.Http.ContentMd5);
        }
        else
        {
            SetOptional(response.Headers, "Content-MD5", blob.Http.ContentMd5);
        }
    }

    private static void AddBlobStateAndCopyHeaders(
        HttpResponse response,
        BlobRecord blob,
        StorageRequestContext request)
    {
        foreach (var (name, value) in blob.Metadata)
            response.Headers[$"x-ms-meta-{name}"] = value;
        if (blob.Tags.Count > 0 && IsServiceVersionAtLeast(request, new DateOnly(2019, 12, 12)))
            response.Headers["x-ms-tag-count"] = blob.Tags.Count.ToString(CultureInfo.InvariantCulture);
        if (blob.VersionId is not null && IsServiceVersionAtLeast(request, new DateOnly(2019, 12, 12)))
        {
            response.Headers["x-ms-version-id"] = blob.VersionId;
            response.Headers["x-ms-is-current-version"] = blob.IsCurrent ? "true" : "false";
        }
        if (blob.Snapshot is not null)
            response.Headers["x-ms-snapshot"] = blob.Snapshot;
        if (blob.Kind == Storage.BlobKind.PageBlob)
            response.Headers["x-ms-blob-sequence-number"] = blob.SequenceNumber.ToString(CultureInfo.InvariantCulture);
        if (blob.ImmutabilityUntil.HasValue && IsServiceVersionAtLeast(request, new DateOnly(2020, 6, 12)))
        {
            response.Headers["x-ms-immutability-policy-until-date"] = blob.ImmutabilityUntil.Value.ToString("R", CultureInfo.InvariantCulture);
            response.Headers["x-ms-immutability-policy-mode"] = blob.ImmutabilityLocked ? "locked" : "unlocked";
        }
        if (IsServiceVersionAtLeast(request, new DateOnly(2020, 6, 12)))
            response.Headers["x-ms-legal-hold"] = blob.HasLegalHold ? "true" : "false";
        if (blob.Kind == Storage.BlobKind.AppendBlob)
        {
            response.Headers["x-ms-blob-committed-block-count"] = blob.AppendBlockCount.ToString(CultureInfo.InvariantCulture);
            if (IsServiceVersionAtLeast(request, new DateOnly(2019, 12, 12)))
                response.Headers["x-ms-blob-sealed"] = blob.IsSealed ? "true" : "false";
        }
        if (blob.Copy is not null && IsServiceVersionAtLeast(request, new DateOnly(2012, 2, 12)))
        {
            response.Headers["x-ms-copy-id"] = blob.Copy.Id;
            response.Headers["x-ms-copy-source"] = blob.Copy.Source;
            response.Headers["x-ms-copy-status"] = blob.Copy.Status;
            response.Headers["x-ms-copy-progress"] = $"{blob.Copy.BytesCopied}/{blob.Copy.TotalBytes}";
            if (blob.Copy.CompletedAt.HasValue)
                response.Headers["x-ms-copy-completion-time"] = blob.Copy.CompletedAt.Value.ToString("R", CultureInfo.InvariantCulture);
            SetOptional(response.Headers, "x-ms-copy-status-description", blob.Copy.Description);
        }
        if (blob.IsIncrementalCopy && IsServiceVersionAtLeast(request, new DateOnly(2016, 5, 31)))
            response.Headers["x-ms-incremental-copy"] = "true";
        if (string.Equals(blob.Copy?.Status, "success", StringComparison.Ordinal) && IsServiceVersionAtLeast(request, new DateOnly(2016, 5, 31)))
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

    private static void WriteLegacyBlobProperties(
        XmlWriter writer,
        StorageRequestContext request,
        BlobRecord blob)
    {
        writer.WriteElementString("LastModified", blob.LastModified.ToString("R", CultureInfo.InvariantCulture));
        writer.WriteElementString("Etag", FormatEntityTag(request, blob.ETag));
        writer.WriteElementString("Size", blob.Content.Length.ToString(CultureInfo.InvariantCulture));
        writer.WriteElementString("ContentType", blob.Http.ContentType);
        writer.WriteElementString("ContentEncoding", blob.Http.ContentEncoding ?? string.Empty);
        writer.WriteElementString("ContentLanguage", blob.Http.ContentLanguage ?? string.Empty);
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

    private static void WriteAnalyticsLogging(XmlWriter writer, StorageAnalyticsLogging logging)
    {
        writer.WriteStartElement("Logging");
        writer.WriteElementString("Version", logging.Version);
        writer.WriteElementString("Delete", logging.Delete ? "true" : "false");
        writer.WriteElementString("Read", logging.Read ? "true" : "false");
        writer.WriteElementString("Write", logging.Write ? "true" : "false");
        WriteAnalyticsRetentionPolicy(writer, logging.RetentionPolicy);
        writer.WriteEndElement();
    }

    private static void WriteAnalyticsMetrics(XmlWriter writer, string name, StorageAnalyticsMetrics metrics)
    {
        writer.WriteStartElement(name);
        writer.WriteElementString("Version", metrics.Version);
        writer.WriteElementString("Enabled", metrics.Enabled ? "true" : "false");
        if (metrics.IncludeApis.HasValue)
            writer.WriteElementString("IncludeAPIs", metrics.IncludeApis.Value ? "true" : "false");
        WriteAnalyticsRetentionPolicy(writer, metrics.RetentionPolicy);
        writer.WriteEndElement();
    }

    private static void WriteAnalyticsRetentionPolicy(
        XmlWriter writer,
        StorageAnalyticsRetentionPolicy retentionPolicy)
    {
        writer.WriteStartElement("RetentionPolicy");
        writer.WriteElementString("Enabled", retentionPolicy.Enabled ? "true" : "false");
        if (retentionPolicy.Days.HasValue)
            writer.WriteElementString("Days", retentionPolicy.Days.Value.ToString(CultureInfo.InvariantCulture));
        writer.WriteEndElement();
    }

    private static void WriteRetentionPolicy(
        XmlWriter writer,
        string name,
        bool enabled,
        int days,
        bool? allowPermanentDelete)
    {
        writer.WriteStartElement(name);
        writer.WriteElementString("Enabled", enabled ? "true" : "false");
        if (enabled)
            writer.WriteElementString("Days", days.ToString(CultureInfo.InvariantCulture));
        if (allowPermanentDelete.HasValue)
            writer.WriteElementString("AllowPermanentDelete", allowPermanentDelete.Value ? "true" : "false");
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

    private static bool IsServiceVersionAtLeast(StorageRequestContext request, DateOnly minimum) =>
        DateOnly.TryParseExact(
            request.ServiceVersion,
            "yyyy-MM-dd",
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out var version) && version >= minimum;

    internal static void AddEntityTag(HttpResponse response, string etag) =>
        response.Headers.ETag = FormatEntityTag(StorageRequestContext.Get(response.HttpContext), etag);

    private static string FormatEntityTag(StorageRequestContext request, string etag)
    {
        var unquoted = etag.Length >= 2 && etag[0] == '"' && etag[^1] == '"'
            ? etag[1..^1]
            : etag;
        return IsServiceVersionAtLeast(request, new DateOnly(2011, 8, 18))
            ? $"\"{unquoted}\""
            : unquoted;
    }

    private static string CreateBlobListingScope(
        StorageRequestContext request,
        string prefix,
        string startFrom,
        string endBefore,
        string delimiter,
        string showOnly,
        IReadOnlySet<string> includes)
    {
        var scopeParts = new List<string>
        {
            request.Account,
            request.Container ?? string.Empty,
            request.ServiceVersion,
            prefix,
            startFrom
        };
        if (!string.IsNullOrEmpty(endBefore))
            scopeParts.Add(endBefore);
        scopeParts.Add(delimiter);
        scopeParts.Add(showOnly);
        scopeParts.Add(string.Join(',', includes.Order(StringComparer.OrdinalIgnoreCase)));
        var value = string.Join('\n', scopeParts);
        return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(value)).AsSpan(0, 12));
    }

    internal static BlobListingMarker DecodeBlobMarker(
        HttpContext context,
        string prefix,
        string startFrom,
        string endBefore,
        string delimiter,
        string showOnly,
        IReadOnlySet<string> includes,
        string marker)
    {
        if (string.IsNullOrEmpty(marker))
            return new BlobListingMarker(null, 0);

        var expectedScope = CreateBlobListingScope(
            StorageRequestContext.Get(context),
            prefix,
            startFrom,
            endBefore,
            delimiter,
            showOnly,
            includes);
        if (marker.StartsWith(BlobMarkerPrefix, StringComparison.Ordinal))
        {
            var scopeSeparator = marker.LastIndexOf('.');
            if (scopeSeparator <= BlobMarkerPrefix.Length ||
                !string.Equals(marker[(scopeSeparator + 1)..], expectedScope, StringComparison.Ordinal))
            {
                throw AzureStorageException.InvalidQuery("marker");
            }

            try
            {
                var bytes = WebEncoders.Base64UrlDecode(
                    marker[BlobMarkerPrefix.Length..scopeSeparator]);
                var cursor = JsonSerializer.Deserialize<BlobListCursor>(bytes);
                if (!IsValidBlobCursor(cursor))
                    throw AzureStorageException.InvalidQuery("marker");
                return new BlobListingMarker(cursor, 0);
            }
            catch (Exception exception) when (exception is FormatException or JsonException)
            {
                throw AzureStorageException.InvalidQuery("marker");
            }
        }

        if (!marker.StartsWith(LegacyBlobMarkerPrefix, StringComparison.Ordinal))
        {
            return new BlobListingMarker(
                new BlobListCursor(marker, true, false, 3, string.Empty, string.Empty),
                0);
        }

        var legacySeparator = marker.IndexOf('.', LegacyBlobMarkerPrefix.Length);
        if (legacySeparator < 0 ||
            !int.TryParse(
                marker.AsSpan(
                    LegacyBlobMarkerPrefix.Length,
                    legacySeparator - LegacyBlobMarkerPrefix.Length),
                NumberStyles.None,
                CultureInfo.InvariantCulture,
            out var offset) ||
            offset < 0 ||
            !string.Equals(marker[(legacySeparator + 1)..], expectedScope, StringComparison.Ordinal))
        {
            throw AzureStorageException.InvalidQuery("marker");
        }
        return new BlobListingMarker(null, offset);
    }

    private static bool IsValidBlobCursor(BlobListCursor? cursor)
    {
        if (cursor is null || cursor.NameComplete || string.IsNullOrEmpty(cursor.Name))
            return false;
        if (cursor.IsPrefix)
        {
            return cursor.Rank == -1 &&
                   cursor.OrderedId.Length == 0 &&
                   cursor.GenerationId.Length == 0;
        }
        if (cursor.Rank == -1)
            return cursor.GenerationId.Length == 0 && cursor.OrderedId.Length == 0;
        return cursor.Rank is >= 0 and <= 3 &&
               cursor.GenerationId.Length > 0 &&
               (cursor.Rank is not (1 or 3) || cursor.OrderedId.Length > 0);
    }

    private static string EncodeBlobMarker(BlobListCursor cursor, string scope) =>
        $"{BlobMarkerPrefix}{WebEncoders.Base64UrlEncode(JsonSerializer.SerializeToUtf8Bytes(cursor))}.{scope}";

    private static string BlobType(Storage.BlobKind kind) => kind switch
    {
        Storage.BlobKind.BlockBlob => "BlockBlob",
        Storage.BlobKind.AppendBlob => "AppendBlob",
        Storage.BlobKind.PageBlob => "PageBlob",
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static string LeaseStatus(LeaseRecord lease) =>
        lease.State is Storage.LeaseState.Leased or Storage.LeaseState.Breaking ? "locked" : "unlocked";

    private static string LeaseStateValue(LeaseRecord lease) => lease.State.ToString().ToRequiredLowerInvariant();

}
