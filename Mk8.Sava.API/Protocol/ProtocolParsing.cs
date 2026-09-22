using System.Globalization;
using System.Net;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

internal sealed record UserDelegationKeyRequest(
    DateTimeOffset StartsAt,
    DateTimeOffset ExpiresAt,
    string? DelegatedUserTenantId);

internal static class ProtocolParsing
{
    private const long MaximumBlockListXmlCharacters = 8L * 1024 * 1024;
    private const int MaximumMetadataBytes = 8 * 1024;
    public const long MaximumBlockListBodyBytes = MaximumBlockListXmlCharacters * sizeof(uint) + 4;

    public static Dictionary<string, string> ReadMetadata(IHeaderDictionary headers)
    {
        var metadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var header in headers)
        {
            if (!header.Key.StartsWith("x-ms-meta-", StringComparison.OrdinalIgnoreCase))
                continue;
            var name = header.Key[10..];
            if (string.IsNullOrEmpty(name))
            {
                throw new AzureStorageException(
                    StatusCodes.Status400BadRequest,
                    "EmptyMetadataKey",
                    "The key for one of the metadata key-value pairs is empty.");
            }
            if (!IsMetadataName(name) || header.Value.Count > 1)
                throw InvalidMetadata();
            var value = header.Value.ToString();
            if (value.Any(character => character > 127 || character == 127 || character < 32 && character != '\t'))
                throw InvalidMetadata();
            if (!metadata.TryAdd(name, value))
                throw InvalidMetadata();
        }
        if (metadata.Sum(pair => pair.Key.Length + pair.Value.Length) > MaximumMetadataBytes)
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "MetadataTooLarge",
                "The size of the specified metadata exceeds the maximum size permitted.");
        }
        return metadata;
    }

    private static bool IsMetadataName(string name) =>
        (name[0] is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or '_') &&
        name.Skip(1).All(character =>
            character is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or >= '0' and <= '9' or '_');

    private static AzureStorageException InvalidMetadata() => new(
        StatusCodes.Status400BadRequest,
        "InvalidMetadata",
        "The metadata specified is invalid. It has characters that are not permitted.");

    public static BlobHttpProperties ReadHttpProperties(
        IHeaderDictionary headers,
        BlobHttpProperties? fallback = null,
        bool useStandardProperties = true)
    {
        fallback ??= new BlobHttpProperties();
        const string customContentMd5Header = "x-ms-blob-content-md5";
        const string standardContentMd5Header = "Content-MD5";
        var contentMd5 = ReadHttpProperty(
            headers,
            customContentMd5Header,
            standardContentMd5Header,
            useStandardProperties,
            fallback.ContentMd5);
        if (contentMd5 is not null)
        {
            var contentMd5Header = headers.ContainsKey(customContentMd5Header)
                ? customContentMd5Header
                : useStandardProperties && headers.ContainsKey(standardContentMd5Header)
                    ? standardContentMd5Header
                    : customContentMd5Header;
            ValidateContentMd5(contentMd5, contentMd5Header);
        }

        return new BlobHttpProperties
        {
            ContentType = ReadHttpProperty(
                              headers,
                              "x-ms-blob-content-type",
                              "Content-Type",
                              useStandardProperties,
                              fallback.ContentType)
                          ?? "application/octet-stream",
            ContentEncoding = ReadHttpProperty(
                headers,
                "x-ms-blob-content-encoding",
                "Content-Encoding",
                useStandardProperties,
                fallback.ContentEncoding),
            ContentLanguage = ReadHttpProperty(
                headers,
                "x-ms-blob-content-language",
                "Content-Language",
                useStandardProperties,
                fallback.ContentLanguage),
            CacheControl = ReadHttpProperty(
                headers,
                "x-ms-blob-cache-control",
                "Cache-Control",
                useStandardProperties,
                fallback.CacheControl),
            ContentDisposition = ReadHttpProperty(
                headers,
                "x-ms-blob-content-disposition",
                standardName: null,
                useStandardProperties,
                fallback.ContentDisposition),
            ContentMd5 = contentMd5
        };
    }

    public static bool HasBlobHttpPropertyHeaders(IHeaderDictionary headers) =>
        headers.ContainsKey("x-ms-blob-cache-control") ||
        headers.ContainsKey("x-ms-blob-content-type") ||
        headers.ContainsKey("x-ms-blob-content-md5") ||
        headers.ContainsKey("x-ms-blob-content-encoding") ||
        headers.ContainsKey("x-ms-blob-content-language") ||
        headers.ContainsKey("x-ms-blob-content-disposition");

    private static string? ReadHttpProperty(
        IHeaderDictionary headers,
        string customName,
        string? standardName,
        bool useStandardProperties,
        string? fallback)
    {
        if (headers.TryGetValue(customName, out var customValues))
            return NullIfEmpty(customValues.ToString());
        if (useStandardProperties &&
            standardName is not null &&
            headers.TryGetValue(standardName, out var standardValues))
        {
            return NullIfEmpty(standardValues.ToString());
        }
        return fallback;
    }

    private static void ValidateContentMd5(string value, string headerName)
    {
        try
        {
            if (Convert.FromBase64String(value).Length == 16)
                return;
        }
        catch (FormatException)
        {
        }
        throw AzureStorageException.InvalidHeader(headerName, value);
    }

    public static Dictionary<string, string> ReadTagsHeader(IHeaderDictionary headers)
    {
        var value = First(headers, "x-ms-tags");
        if (string.IsNullOrEmpty(value))
            return new Dictionary<string, string>(StringComparer.Ordinal);
        var tags = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var pair in value.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var separator = pair.IndexOf('=');
            if (separator <= 0)
                throw AzureStorageException.InvalidHeader("x-ms-tags", value);
            var key = WebUtility.UrlDecode(pair[..separator]);
            var tagValue = WebUtility.UrlDecode(pair[(separator + 1)..]);
            ValidateTag(key, tagValue);
            if (!tags.TryAdd(key, tagValue))
                throw AzureStorageException.InvalidHeader("x-ms-tags", value);
        }
        if (tags.Count > 10)
            throw AzureStorageException.InvalidHeader("x-ms-tags", value);
        return tags;
    }

    public static async Task<Dictionary<string, string>> ReadTagsBodyAsync(Stream body, CancellationToken cancellationToken)
    {
        using var reader = CreateXmlReader(body);
        var document = await XDocument.LoadAsync(reader, LoadOptions.None, cancellationToken);
        var tags = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var tag in document.Descendants().Where(element => element.Name.LocalName == "Tag"))
        {
            var key = ChildValue(tag, "Key");
            var value = ChildValue(tag, "Value");
            if (key is null || value is null)
                throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", "The specified XML is not syntactically valid.");
            ValidateTag(key, value);
            if (!tags.TryAdd(key, value))
                throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", "Blob tag keys must be unique.");
        }
        if (tags.Count > 10)
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "TagsTooLarge", "The number of blob tags exceeds the permitted limit.");
        return tags;
    }

    public static async Task<IReadOnlyList<BlockListEntry>> ReadBlockListAsync(Stream body, CancellationToken cancellationToken)
    {
        using var reader = CreateXmlReader(body, MaximumBlockListXmlCharacters);
        if (await reader.MoveToContentAsync() != XmlNodeType.Element || reader.LocalName != "BlockList")
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", "The specified XML is not syntactically valid.");

        var blocks = new List<BlockListEntry>();
        var rootDepth = reader.Depth;
        if (reader.IsEmptyElement)
        {
            await reader.ReadAsync();
            await EnsureEndOfXmlDocumentAsync(reader, cancellationToken);
            return blocks;
        }

        await reader.ReadAsync();
        while (!reader.EOF)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (reader.NodeType is XmlNodeType.Whitespace or XmlNodeType.SignificantWhitespace)
            {
                await reader.ReadAsync();
                continue;
            }
            if (reader.NodeType == XmlNodeType.EndElement &&
                reader.Depth == rootDepth &&
                reader.LocalName == "BlockList")
            {
                await reader.ReadAsync();
                await EnsureEndOfXmlDocumentAsync(reader, cancellationToken);
                return blocks;
            }
            if (reader.NodeType != XmlNodeType.Element || reader.Depth != rootDepth + 1)
                throw InvalidBlockListXml();

            var mode = reader.LocalName switch
            {
                "Latest" => BlockListMode.Latest,
                "Committed" => BlockListMode.Committed,
                "Uncommitted" => BlockListMode.Uncommitted,
                _ => throw InvalidBlockListXml()
            };
            if (blocks.Count == BlobServiceLimits.MaximumCommittedBlockCount)
            {
                throw new AzureStorageException(
                    StatusCodes.Status409Conflict,
                    "BlockCountExceedsLimit",
                    "The block list may not contain more than 50,000 blocks.");
            }

            string blockId;
            try
            {
                blockId = await reader.ReadElementContentAsStringAsync();
            }
            catch (InvalidOperationException)
            {
                throw InvalidBlockListXml();
            }
            blocks.Add(new BlockListEntry(blockId, mode));
        }

        throw InvalidBlockListXml();
    }

    public static async Task<Dictionary<string, StoredAccessPolicy>> ReadAclAsync(Stream body, CancellationToken cancellationToken)
    {
        using var reader = CreateXmlReader(body);
        var document = await XDocument.LoadAsync(reader, LoadOptions.None, cancellationToken);
        var policies = new Dictionary<string, StoredAccessPolicy>(StringComparer.Ordinal);
        if (document.Root?.Name.LocalName != "SignedIdentifiers")
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", "The specified access policy XML is invalid.");
        foreach (var identifier in document.Root.Elements().Where(element => element.Name.LocalName == "SignedIdentifier"))
        {
            var id = ChildValue(identifier, "Id");
            var accessPolicy = identifier.Elements().FirstOrDefault(element => element.Name.LocalName == "AccessPolicy");
            var permission = accessPolicy is null ? string.Empty : ChildValue(accessPolicy, "Permission") ?? string.Empty;
            var start = ParseDate(accessPolicy is null ? string.Empty : ChildValue(accessPolicy, "Start") ?? string.Empty, "Start");
            var expiry = ParseDate(accessPolicy is null ? string.Empty : ChildValue(accessPolicy, "Expiry") ?? string.Empty, "Expiry");
            if (string.IsNullOrEmpty(id) || id.Length > 64 || !policies.TryAdd(id, new StoredAccessPolicy
            {
                StartsAt = start,
                ExpiresAt = expiry,
                Permission = permission
            }))
            {
                throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", "The specified access policy XML is invalid.");
            }
        }
        if (policies.Count > 5)
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", "A container can have at most five stored access policies.");
        return policies;
    }

    public static async Task<UserDelegationKeyRequest> ReadUserDelegationKeyRequestAsync(
        Stream body,
        string serviceVersion,
        CancellationToken cancellationToken)
    {
        using var reader = CreateXmlReader(body);
        var document = await XDocument.LoadAsync(reader, LoadOptions.None, cancellationToken);
        if (document.Root?.Name.LocalName != "KeyInfo")
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", "The specified XML is not syntactically valid.");

        var root = document.Root;
        ValidateUniqueChildren(root);
        ValidateKnownChildren(root, "Start", "Expiry", "DelegatedUserTid");
        var startsAt = ParseRequiredDate(ChildValue(root, "Start"), "Start");
        var expiresAt = ParseRequiredDate(ChildValue(root, "Expiry"), "Expiry");
        var delegatedTenant = ChildValue(root, "DelegatedUserTid");
        if (!string.IsNullOrEmpty(delegatedTenant) && !Guid.TryParse(delegatedTenant, out _))
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", "The DelegatedUserTid value is invalid.");
        if (!string.IsNullOrEmpty(delegatedTenant) &&
            (!StorageServiceVersions.TryParse(serviceVersion, out var version) ||
             version < new DateOnly(2025, 7, 5)))
        {
            throw AzureStorageException.FeatureVersionMismatch(
                "DelegatedUserTid requires service version 2025-07-05 or later.");
        }
        return new UserDelegationKeyRequest(startsAt, expiresAt, NullIfEmpty(delegatedTenant));
    }

    public static async Task<ServiceProperties> ReadServicePropertiesAsync(
        Stream body,
        ServiceProperties current,
        string serviceVersion,
        CancellationToken cancellationToken)
    {
        using var reader = CreateXmlReader(body);
        var document = await XDocument.LoadAsync(reader, LoadOptions.None, cancellationToken);
        var root = document.Root;
        if (root?.Name.LocalName != "StorageServiceProperties")
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", "The specified XML is not syntactically valid.");

        if (!DateOnly.TryParseExact(
                serviceVersion,
                "yyyy-MM-dd",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var version))
        {
            throw AzureStorageException.InvalidHeader("x-ms-version", serviceVersion);
        }

        ValidateUniqueChildren(root);
        ValidateKnownChildren(
            root,
            "Logging",
            "Metrics",
            "HourMetrics",
            "MinuteMetrics",
            "Cors",
            "DefaultServiceVersion",
            "DeleteRetentionPolicy",
            "StaticWebsite");

        if (!root.Elements().Any())
            throw InvalidServicePropertiesXml("At least one service property must be specified.");

        var modernAnalytics = version >= new DateOnly(2013, 8, 15);
        var loggingElement = Child(root, "Logging");
        var legacyMetricsElement = Child(root, "Metrics");
        var hourMetricsElement = Child(root, "HourMetrics");
        var minuteMetricsElement = Child(root, "MinuteMetrics");
        if (modernAnalytics && legacyMetricsElement is not null ||
            !modernAnalytics && (hourMetricsElement is not null || minuteMetricsElement is not null))
        {
            throw InvalidServicePropertiesXml("The metrics element is not valid for the requested service version.");
        }
        if (!modernAnalytics && (loggingElement is null || legacyMetricsElement is null))
            throw InvalidServicePropertiesXml("Logging and Metrics are required for this service version.");

        var logging = loggingElement is null ? current.Logging : ReadAnalyticsLogging(loggingElement);
        var hourMetrics = modernAnalytics
            ? hourMetricsElement is null ? current.HourMetrics : ReadAnalyticsMetrics(hourMetricsElement)
            : ReadAnalyticsMetrics(legacyMetricsElement!);
        var minuteMetrics = minuteMetricsElement is null
            ? current.MinuteMetrics
            : ReadAnalyticsMetrics(minuteMetricsElement);

        var corsElement = Child(root, "Cors");
        var cors = corsElement is null ? current.Cors : [];
        if (corsElement is not null)
        {
            RequireServicePropertiesVersion(version, new DateOnly(2013, 8, 15), "Cors");
            ValidateKnownChildren(corsElement, "CorsRule");
            foreach (var rule in corsElement.Elements())
            {
                ValidateUniqueChildren(rule);
                ValidateKnownChildren(
                    rule,
                    "AllowedOrigins",
                    "AllowedMethods",
                    "AllowedHeaders",
                    "ExposedHeaders",
                    "MaxAgeInSeconds");
                cors.Add(new CorsRule
                {
                    AllowedOrigins = RequiredText(rule, "AllowedOrigins"),
                    AllowedMethods = RequiredText(rule, "AllowedMethods"),
                    AllowedHeaders = RequiredText(rule, "AllowedHeaders"),
                    ExposedHeaders = RequiredText(rule, "ExposedHeaders"),
                    MaxAgeInSeconds = ParseInt(RequiredText(rule, "MaxAgeInSeconds"), "MaxAgeInSeconds")
                });
            }
        }
        if (cors.Count > 5)
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", "A maximum of five CORS rules is supported.");
        if (corsElement is not null)
            ValidateCorsRules(cors);

        var defaultServiceVersion = OptionalText(root, "DefaultServiceVersion");
        if (defaultServiceVersion is not null)
        {
            RequireServicePropertiesVersion(version, new DateOnly(2011, 8, 18), "DefaultServiceVersion");
            if (!string.IsNullOrEmpty(defaultServiceVersion) &&
                !StorageServiceVersions.TryParse(defaultServiceVersion, out _))
            {
                throw InvalidServicePropertiesXml("The DefaultServiceVersion value is invalid.");
            }
        }
        var deleteRetentionPolicy = Child(root, "DeleteRetentionPolicy");
        if (deleteRetentionPolicy is not null)
            RequireServicePropertiesVersion(version, new DateOnly(2017, 7, 29), "DeleteRetentionPolicy");
        if (deleteRetentionPolicy is not null && Child(deleteRetentionPolicy, "AllowPermanentDelete") is not null)
            RequireServicePropertiesVersion(version, new DateOnly(2020, 2, 10), "AllowPermanentDelete");

        var deletePolicy = ReadRetentionPolicy(
            root,
            "DeleteRetentionPolicy",
            current.BlobSoftDeleteEnabled,
            current.BlobSoftDeleteRetentionDays,
            current.BlobPermanentDeleteEnabled,
            supportsPermanentDelete: true);
        var website = Child(root, "StaticWebsite");
        var staticWebsite = current.StaticWebsite;
        if (website is not null)
        {
            RequireServicePropertiesVersion(version, new DateOnly(2018, 3, 28), "StaticWebsite");
            ValidateUniqueChildren(website);
            ValidateKnownChildren(
                website,
                "Enabled",
                "IndexDocument",
                "DefaultIndexDocumentPath",
                "ErrorDocument404Path");
            var indexDocument = NullIfEmpty(OptionalText(website, "IndexDocument"));
            var defaultIndexDocumentPath = NullIfEmpty(OptionalText(website, "DefaultIndexDocumentPath"));
            if (defaultIndexDocumentPath is not null)
                RequireServicePropertiesVersion(version, new DateOnly(2019, 12, 12), "DefaultIndexDocumentPath");
            if (indexDocument is not null && defaultIndexDocumentPath is not null)
            {
                throw new AzureStorageException(
                    StatusCodes.Status400BadRequest,
                    "InvalidXmlDocument",
                    "IndexDocument and DefaultIndexDocumentPath are mutually exclusive.");
            }
            staticWebsite = new StaticWebsiteProperties
            {
                Enabled = ParseBool(RequiredText(website, "Enabled"), false),
                IndexDocument = indexDocument,
                DefaultIndexDocumentPath = defaultIndexDocumentPath,
                ErrorDocument404Path = NullIfEmpty(OptionalText(website, "ErrorDocument404Path"))
            };
        }

        return current with
        {
            Logging = logging,
            HourMetrics = hourMetrics,
            MinuteMetrics = minuteMetrics,
            Cors = cors,
            DefaultServiceVersion = defaultServiceVersion ?? current.DefaultServiceVersion,
            BlobSoftDeleteEnabled = deletePolicy.Enabled,
            BlobSoftDeleteRetentionDays = deletePolicy.Days,
            BlobPermanentDeleteEnabled = deletePolicy.AllowPermanentDelete,
            StaticWebsite = staticWebsite
        };
    }

    public static (long Start, long End) ParseRange(string value, long length)
    {
        if (!value.StartsWith("bytes=", StringComparison.OrdinalIgnoreCase) || value.Contains(',', StringComparison.Ordinal))
            throw new AzureStorageException(StatusCodes.Status416RangeNotSatisfiable, "InvalidRange", "The range specified is invalid for the current size of the resource.");
        var components = value[6..].Split('-', 2);
        if (components.Length != 2)
            throw new AzureStorageException(StatusCodes.Status416RangeNotSatisfiable, "InvalidRange", "The range specified is invalid for the current size of the resource.");

        long start;
        long end;
        if (string.IsNullOrEmpty(components[0]))
        {
            if (!long.TryParse(components[1], NumberStyles.None, CultureInfo.InvariantCulture, out var suffix) || suffix <= 0)
                throw InvalidRange();
            start = Math.Max(0, length - suffix);
            end = length - 1;
        }
        else
        {
            if (!long.TryParse(components[0], NumberStyles.None, CultureInfo.InvariantCulture, out start) || start < 0)
                throw InvalidRange();
            if (string.IsNullOrEmpty(components[1]))
                end = length - 1;
            else if (!long.TryParse(components[1], NumberStyles.None, CultureInfo.InvariantCulture, out end))
                throw InvalidRange();
        }

        if (length == 0 || start >= length || end < start)
            throw InvalidRange();
        return (start, Math.Min(end, length - 1));
    }

    public static (long Start, long End) ParseStorageRange(
        string value,
        long length,
        bool allowOpenEnded,
        bool allowEndPastLength = true)
    {
        if (!value.StartsWith("bytes=", StringComparison.OrdinalIgnoreCase) ||
            value.Contains(',', StringComparison.Ordinal))
        {
            throw InvalidRange();
        }

        var components = value[6..].Split('-', 2);
        if (components.Length != 2 ||
            string.IsNullOrEmpty(components[0]) ||
            !long.TryParse(components[0], NumberStyles.None, CultureInfo.InvariantCulture, out var start) ||
            start < 0)
        {
            throw InvalidRange();
        }

        long end;
        if (string.IsNullOrEmpty(components[1]))
        {
            if (!allowOpenEnded)
                throw InvalidRange();
            end = length - 1;
        }
        else if (!long.TryParse(components[1], NumberStyles.None, CultureInfo.InvariantCulture, out end))
        {
            throw InvalidRange();
        }

        if (length == 0 ||
            start >= length ||
            end < start ||
            !allowEndPastLength && end >= length)
        {
            throw InvalidRange();
        }
        return (start, Math.Min(end, length - 1));
    }

    public static long ParseLongHeader(IHeaderDictionary headers, string name, bool required = false, long? defaultValue = null)
    {
        var value = First(headers, name);
        if (string.IsNullOrEmpty(value))
        {
            if (required)
                throw AzureStorageException.InvalidHeader(name);
            return defaultValue ?? 0;
        }
        if (!long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed))
            throw AzureStorageException.InvalidHeader(name, value);
        return parsed;
    }

    public static string? First(IHeaderDictionary headers, string name) =>
        headers.TryGetValue(name, out var values) && values.Count > 0 ? values[0] : null;

    public static XmlReader CreateXmlReader(Stream stream) => CreateXmlReader(stream, 4L * 1024 * 1024);

    private static XmlReader CreateXmlReader(Stream stream, long maximumCharacters) => XmlReader.Create(stream, new XmlReaderSettings
    {
        Async = true,
        DtdProcessing = DtdProcessing.Prohibit,
        XmlResolver = null,
        MaxCharactersInDocument = maximumCharacters,
        IgnoreComments = true,
        IgnoreProcessingInstructions = true
    });

    private static async Task EnsureEndOfXmlDocumentAsync(XmlReader reader, CancellationToken cancellationToken)
    {
        while (!reader.EOF)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (reader.NodeType is not (XmlNodeType.Whitespace or XmlNodeType.SignificantWhitespace or XmlNodeType.None))
                throw InvalidBlockListXml();
            await reader.ReadAsync();
        }
    }

    private static AzureStorageException InvalidBlockListXml() => new(
        StatusCodes.Status400BadRequest,
        "InvalidXmlDocument",
        "The specified XML is not syntactically valid.");

    private static void ValidateTag(string key, string value)
    {
        if (!IsValidTagComponent(key, allowEmpty: false) ||
            !IsValidTagComponent(value, allowEmpty: true))
        {
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidTag", "The specified blob tag is invalid.");
        }
    }

    internal static bool IsValidTagComponent(string value, bool allowEmpty)
    {
        var maximum = allowEmpty ? 256 : 128;
        if (value.Length > maximum || !allowEmpty && value.Length == 0)
            return false;
        return value.All(character =>
            char.IsAsciiLetterOrDigit(character) ||
            character is ' ' or '+' or '-' or '.' or ':' or '=' or '_' or '/');
    }

    private static DateTimeOffset? ParseDate(string value, string field) =>
        string.IsNullOrEmpty(value)
            ? null
            : DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed)
                ? parsed
                : throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", $"The {field} value is invalid.");

    private static DateTimeOffset ParseRequiredDate(string? value, string field) =>
        !string.IsNullOrEmpty(value) && DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed)
            ? parsed.ToUniversalTime()
            : throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", $"The {field} value is invalid.");

    private static StorageAnalyticsLogging ReadAnalyticsLogging(XElement element)
    {
        ValidateUniqueChildren(element);
        ValidateKnownChildren(element, "Version", "Delete", "Read", "Write", "RetentionPolicy");
        var version = RequiredText(element, "Version");
        if (version != "1.0")
            throw InvalidServicePropertiesXml("The Storage Analytics version is invalid.");
        return new StorageAnalyticsLogging
        {
            Version = version,
            Delete = ParseBool(RequiredText(element, "Delete"), false),
            Read = ParseBool(RequiredText(element, "Read"), false),
            Write = ParseBool(RequiredText(element, "Write"), false),
            RetentionPolicy = ReadAnalyticsRetentionPolicy(element)
        };
    }

    private static StorageAnalyticsMetrics ReadAnalyticsMetrics(XElement element)
    {
        ValidateUniqueChildren(element);
        ValidateKnownChildren(element, "Version", "Enabled", "IncludeAPIs", "RetentionPolicy");
        var version = RequiredText(element, "Version");
        if (version != "1.0")
            throw InvalidServicePropertiesXml("The Storage Analytics version is invalid.");
        var enabled = ParseBool(RequiredText(element, "Enabled"), false);
        var includeApisValue = OptionalText(element, "IncludeAPIs");
        if (enabled && includeApisValue is null)
            throw InvalidServicePropertiesXml("The IncludeAPIs element is required when metrics are enabled.");
        return new StorageAnalyticsMetrics
        {
            Version = version,
            Enabled = enabled,
            IncludeApis = includeApisValue is null ? null : ParseBool(includeApisValue, false),
            RetentionPolicy = ReadAnalyticsRetentionPolicy(element)
        };
    }

    private static StorageAnalyticsRetentionPolicy ReadAnalyticsRetentionPolicy(XElement parent)
    {
        var policy = Child(parent, "RetentionPolicy")
                     ?? throw InvalidServicePropertiesXml("The RetentionPolicy element is required.");
        ValidateUniqueChildren(policy);
        ValidateKnownChildren(policy, "Enabled", "Days");
        var enabled = ParseBool(RequiredText(policy, "Enabled"), false);
        var days = OptionalText(policy, "Days") is { } value ? ParseInt(value, "Days") : (int?)null;
        if (enabled && days is null)
            throw InvalidServicePropertiesXml("Retention Days is required when retention is enabled.");
        if (days is not null and (< 1 or > 365))
            throw InvalidServicePropertiesXml("Retention days must be between 1 and 365.");
        return new StorageAnalyticsRetentionPolicy { Enabled = enabled, Days = days };
    }

    private static void ValidateCorsRules(IReadOnlyList<CorsRule> rules)
    {
        const int maximumSettingsBytes = 2 * 1024;
        var settingsBytes = 0;
        var originCount = 0;
        var literalHeaderCount = 0;
        var prefixedHeaderCount = 0;
        var allowedMethods = new HashSet<string>(
            ["DELETE", "GET", "HEAD", "MERGE", "PATCH", "POST", "OPTIONS", "PUT"],
            StringComparer.Ordinal);

        foreach (var rule in rules)
        {
            settingsBytes += Encoding.UTF8.GetByteCount(rule.AllowedOrigins);
            settingsBytes += Encoding.UTF8.GetByteCount(rule.AllowedMethods);
            settingsBytes += Encoding.UTF8.GetByteCount(rule.AllowedHeaders);
            settingsBytes += Encoding.UTF8.GetByteCount(rule.ExposedHeaders);
            settingsBytes += Encoding.UTF8.GetByteCount(rule.MaxAgeInSeconds.ToString(CultureInfo.InvariantCulture));
            if (settingsBytes > maximumSettingsBytes)
                throw InvalidServicePropertiesXml("CORS rule settings cannot exceed 2 KiB.");
            if (rule.MaxAgeInSeconds < 0)
                throw InvalidServicePropertiesXml("CORS MaxAgeInSeconds cannot be negative.");

            var origins = SplitRequiredCorsList(rule.AllowedOrigins, "AllowedOrigins");
            originCount += origins.Length;
            if (originCount > 64 || origins.Any(origin => origin.Length > 256 || !IsValidCorsOrigin(origin)))
                throw InvalidServicePropertiesXml("The CORS allowed origins are invalid.");

            var methods = SplitRequiredCorsList(rule.AllowedMethods, "AllowedMethods");
            if (methods.Distinct(StringComparer.Ordinal).Count() != methods.Length ||
                methods.Any(method => !allowedMethods.Contains(method)))
            {
                throw InvalidServicePropertiesXml("The CORS allowed methods are invalid.");
            }

            CountCorsHeaders(rule.AllowedHeaders, ref literalHeaderCount, ref prefixedHeaderCount);
            CountCorsHeaders(rule.ExposedHeaders, ref literalHeaderCount, ref prefixedHeaderCount);
            if (literalHeaderCount > 64 || prefixedHeaderCount > 2)
                throw InvalidServicePropertiesXml("The CORS header limits were exceeded.");
        }
    }

    private static string[] SplitRequiredCorsList(string value, string field)
    {
        var values = value.Split(',', StringSplitOptions.TrimEntries);
        if (values.Length == 0 || values.Any(string.IsNullOrEmpty))
            throw InvalidServicePropertiesXml($"The CORS {field} value is invalid.");
        return values;
    }

    private static void CountCorsHeaders(string value, ref int literalCount, ref int prefixedCount)
    {
        if (value.Length == 0)
            return;
        foreach (var header in value.Split(',', StringSplitOptions.TrimEntries))
        {
            if (header.Length == 0 || header.Length > 256 || header.Count(character => character == '*') > 1 ||
                header.Contains('*') && !header.EndsWith('*'))
            {
                throw InvalidServicePropertiesXml("A CORS header value is invalid.");
            }
            if (header.EndsWith('*'))
                prefixedCount++;
            else
                literalCount++;
        }
    }

    private static bool IsValidCorsOrigin(string origin)
    {
        if (origin == "*")
            return true;
        var wildcardCount = origin.Count(character => character == '*');
        if (wildcardCount > 1)
            return false;
        var normalized = wildcardCount == 0
            ? origin
            : origin.Replace("://*.", "://cors-wildcard.", StringComparison.Ordinal);
        if (wildcardCount == 1 && normalized == origin)
            return false;
        return Uri.TryCreate(normalized, UriKind.Absolute, out var uri) &&
               uri.Scheme is "http" or "https" &&
               string.IsNullOrEmpty(uri.UserInfo) &&
               uri.AbsolutePath == "/" &&
               string.IsNullOrEmpty(uri.Query) &&
               string.IsNullOrEmpty(uri.Fragment);
    }

    private static (bool Enabled, int Days, bool AllowPermanentDelete) ReadRetentionPolicy(
        XElement root,
        string name,
        bool currentEnabled,
        int currentDays,
        bool currentAllowPermanentDelete,
        bool supportsPermanentDelete)
    {
        if (Child(root, name) is not { } policy)
            return (currentEnabled, currentDays, currentAllowPermanentDelete);
        ValidateUniqueChildren(policy);
        if (supportsPermanentDelete)
            ValidateKnownChildren(policy, "Enabled", "Days", "AllowPermanentDelete");
        else
            ValidateKnownChildren(policy, "Enabled", "Days");
        var enabled = ParseBool(RequiredText(policy, "Enabled"), false);
        var days = OptionalText(policy, "Days") is { } text ? ParseInt(text, "Days") : currentDays;
        var allowPermanentDelete = supportsPermanentDelete && Child(policy, "AllowPermanentDelete") is not null
            ? ParseBool(RequiredText(policy, "AllowPermanentDelete"), false)
            : false;
        if (enabled && Child(policy, "Days") is null)
            throw InvalidServicePropertiesXml("Retention Days is required when retention is enabled.");
        if (enabled && days is < 1 or > 365)
            throw InvalidServicePropertiesXml("Retention days must be between 1 and 365.");
        return (enabled, days, allowPermanentDelete);
    }

    private static void RequireServicePropertiesVersion(DateOnly version, DateOnly minimum, string feature)
    {
        if (version < minimum)
        {
            throw AzureStorageException.FeatureVersionMismatch(
                $"{feature} requires service version {minimum:yyyy-MM-dd} or later.");
        }
    }

    private static void ValidateUniqueChildren(XElement element)
    {
        if (element.Elements().GroupBy(child => child.Name.LocalName, StringComparer.Ordinal).Any(group => group.Skip(1).Any()))
            throw InvalidServicePropertiesXml("The specified XML contains duplicate elements.");
    }

    private static void ValidateKnownChildren(XElement element, params string[] names)
    {
        var known = names.ToHashSet(StringComparer.Ordinal);
        if (element.Elements().Any(child => !known.Contains(child.Name.LocalName)))
            throw InvalidServicePropertiesXml("The specified XML contains an unsupported element.");
    }

    private static AzureStorageException InvalidServicePropertiesXml(string message) =>
        new(StatusCodes.Status400BadRequest, "InvalidXmlDocument", message);

    private static bool ParseBool(string? value, bool defaultValue) => value?.ToLowerInvariant() switch
    {
        null or "" => defaultValue,
        "true" => true,
        "false" => false,
        _ => throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", "A Boolean XML value is invalid.")
    };

    private static int ParseInt(string value, string field) =>
        int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", $"The {field} value is invalid.");

    private static string RequiredText(XElement parent, string name) =>
        OptionalText(parent, name) ?? throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", $"The {name} element is required.");

    private static string? OptionalText(XElement parent, string name) => Child(parent, name)?.Value;

    private static XElement? Child(XElement parent, string name) =>
        parent.Elements().FirstOrDefault(element => element.Name.LocalName == name);

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;

    private static string? ChildValue(XElement parent, string name) =>
        parent.Elements().FirstOrDefault(element => element.Name.LocalName == name)?.Value;

    private static AzureStorageException InvalidRange() => new(
        StatusCodes.Status416RangeNotSatisfiable,
        "InvalidRange",
        "The range specified is invalid for the current size of the resource.");

}
