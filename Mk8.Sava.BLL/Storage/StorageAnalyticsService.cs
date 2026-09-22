using System.Globalization;
using System.Net;
using System.Text;

namespace Mk8.Sava.Storage;

public sealed class StorageAnalyticsService(
    MetadataStore metadata,
    ChunkStore chunks) : IStorageAnalyticsSink
{
    public const string LogsContainerName = "$logs";

    private static readonly BlobEncryption Unencrypted = new(null, null);

    public async Task RecordAsync(
        StorageAnalyticsRequest request,
        CancellationToken cancellationToken)
    {
        var properties = await metadata.GetServicePropertiesAsync(request.Account, cancellationToken);
        var logging = properties.Logging;
        if (!IsEnabled(logging, request.Category) || !ShouldLog(request))
            return;

        await EnsureContainerAsync(request.Account, cancellationToken);
        var bytes = Encoding.UTF8.GetBytes(FormatRecord(logging.Version, request));
        await using var source = new MemoryStream(bytes, writable: false);
        using var content = await chunks.StorePinnedAsync(
            request.Account,
            Unencrypted,
            source,
            cancellationToken);

        var completedAt = request.CompletedAt.ToUniversalTime();
        var prefix = $"blob/{completedAt:yyyy/MM/dd/HH}00/";
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var lastName = await metadata.GetLastBlobNameAsync(
                request.Account,
                LogsContainerName,
                prefix,
                cancellationToken);
            var counter = NextCounter(prefix, lastName);
            var now = metadata.GetUtcNow();
            var proposed = new BlobRecord
            {
                Account = request.Account,
                Container = LogsContainerName,
                Name = $"{prefix}{counter:D6}.log",
                GenerationId = Guid.NewGuid().ToString("N"),
                Revision = MetadataStore.NewRevision(),
                IsCurrent = true,
                Kind = BlobKind.BlockBlob,
                Content = content.Manifest,
                ETag = MetadataStore.NewETag(),
                CreatedAt = now,
                LastModified = now,
                Metadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["LogType"] = CategoryName(request.Category),
                    ["StartTime"] = FormatMetadataTime(request.StartedAt),
                    ["EndTime"] = FormatMetadataTime(request.CompletedAt),
                    ["LogVersion"] = logging.Version
                },
                Http = new BlobHttpProperties { ContentType = "application/octet-stream" },
                AccessTier = "Hot",
                AccessTierInferred = true
            };

            try
            {
                _ = await metadata.PublishBlobAsync(proposed, null, null, cancellationToken);
                return;
            }
            catch (StorageConcurrencyException)
            {
                // Another request allocated this hourly counter. Re-read the tail and retry.
            }
        }
    }

    public async Task EnsureContainerAsync(string account, CancellationToken cancellationToken)
    {
        while (true)
        {
            var existing = await metadata.GetContainerAsync(
                account,
                LogsContainerName,
                includeDeleted: true,
                cancellationToken);
            if (existing is { DeletedAt: null })
                return;

            var now = metadata.GetUtcNow();
            if (existing is null)
            {
                var container = new ContainerRecord
                {
                    Account = account,
                    Name = LogsContainerName,
                    Revision = MetadataStore.NewRevision(),
                    ETag = MetadataStore.NewETag(),
                    CreatedAt = now,
                    LastModified = now
                };
                if (await metadata.TryCreateContainerAsync(container, cancellationToken))
                    return;
                continue;
            }

            var restored = existing with
            {
                Revision = MetadataStore.NewRevision(),
                ETag = MetadataStore.NewETag(),
                LastModified = now,
                DeletedAt = null,
                DeleteRetentionUntil = null,
                DeletedVersion = null,
                Lease = LeaseRecord.Available
            };
            try
            {
                await metadata.PutContainerAsync(restored, existing.Revision, cancellationToken);
                return;
            }
            catch (StorageConcurrencyException)
            {
                // Re-read a concurrently created or restored service container.
            }
        }
    }

    private static bool IsEnabled(
        StorageAnalyticsLogging logging,
        StorageAnalyticsOperationCategory category) => category switch
        {
            StorageAnalyticsOperationCategory.Read => logging.Read,
            StorageAnalyticsOperationCategory.Write => logging.Write,
            StorageAnalyticsOperationCategory.Delete => logging.Delete,
            _ => false
        };

    private static bool ShouldLog(StorageAnalyticsRequest request) =>
        !string.Equals(request.AuthenticationType, "anonymous", StringComparison.Ordinal) ||
        request.StatusCode is >= 200 and < 300 or StatusCodes.Status304NotModified or >= 500;

    private static int NextCounter(string prefix, string? lastName)
    {
        if (lastName is null ||
            !lastName.StartsWith(prefix, StringComparison.Ordinal) ||
            !lastName.EndsWith(".log", StringComparison.Ordinal) ||
            !int.TryParse(
                lastName.AsSpan(prefix.Length, lastName.Length - prefix.Length - ".log".Length),
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var lastCounter))
        {
            return 0;
        }
        return checked(lastCounter + 1);
    }

    private static string FormatRecord(string version, StorageAnalyticsRequest request)
    {
        var fields = new List<string>(version == "2.0" ? 38 : 30)
        {
            version,
            request.StartedAt.UtcDateTime.ToString("O", CultureInfo.InvariantCulture),
            request.Operation,
            request.RequestStatus,
            request.StatusCode.ToString(CultureInfo.InvariantCulture),
            request.EndToEndLatencyMilliseconds.ToString(CultureInfo.InvariantCulture),
            request.ServerLatencyMilliseconds.ToString(CultureInfo.InvariantCulture),
            request.AuthenticationType,
            request.RequesterAccountName ?? string.Empty,
            request.Account,
            "blob",
            Quote(request.RequestUrl),
            Quote(request.RequestedObjectKey),
            request.RequestId,
            "0",
            request.RequesterIpAddress ?? string.Empty,
            request.RequestVersion,
            request.RequestHeaderSize.ToString(CultureInfo.InvariantCulture),
            request.RequestPacketSize.ToString(CultureInfo.InvariantCulture),
            request.ResponseHeaderSize.ToString(CultureInfo.InvariantCulture),
            request.ResponsePacketSize.ToString(CultureInfo.InvariantCulture),
            request.RequestContentLength.ToString(CultureInfo.InvariantCulture),
            Quote(request.RequestMd5),
            Quote(request.ServerMd5),
            Quote(request.ETag),
            request.LastModified ?? string.Empty,
            Quote(request.Conditions),
            Quote(request.UserAgent),
            Quote(request.Referrer),
            Quote(request.ClientRequestId)
        };
        if (version == "2.0")
        {
            fields.Add(Quote(request.UserObjectId));
            fields.Add(Quote(request.TenantId));
            fields.Add(Quote(request.ApplicationId));
            fields.Add(Quote(request.Audience));
            fields.Add(Quote(request.Issuer));
            fields.Add(Quote(request.UserPrincipalName));
            fields.Add(string.Empty);
            fields.Add(string.Empty);
        }
        return string.Join(';', fields) + '\n';
    }

    private static string Quote(string? value) => string.IsNullOrEmpty(value)
        ? string.Empty
        : $"\"{WebUtility.HtmlEncode(value)}\"";

    private static string CategoryName(StorageAnalyticsOperationCategory category) => category switch
    {
        StorageAnalyticsOperationCategory.Read => "read",
        StorageAnalyticsOperationCategory.Write => "write",
        StorageAnalyticsOperationCategory.Delete => "delete",
        _ => throw new ArgumentOutOfRangeException(nameof(category))
    };

    private static string FormatMetadataTime(DateTimeOffset value) =>
        value.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
}
