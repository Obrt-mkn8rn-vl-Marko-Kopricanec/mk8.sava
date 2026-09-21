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
    public const long MaximumBlockListBodyBytes = MaximumBlockListXmlCharacters * sizeof(uint) + 4;

    public static Dictionary<string, string> ReadMetadata(IHeaderDictionary headers)
    {
        var metadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var header in headers)
        {
            if (!header.Key.StartsWith("x-ms-meta-", StringComparison.OrdinalIgnoreCase))
                continue;
            var name = header.Key[10..];
            if (string.IsNullOrEmpty(name) || name.Any(character => character > 127 || char.IsControl(character)))
                throw AzureStorageException.InvalidHeader(header.Key, header.Value.ToString());
            var value = header.Value.ToString();
            if (value.Any(character => character is '\r' or '\n'))
                throw AzureStorageException.InvalidHeader(header.Key, value);
            metadata[name] = value;
        }
        return metadata;
    }

    public static BlobHttpProperties ReadHttpProperties(
        IHeaderDictionary headers,
        BlobHttpProperties? fallback = null,
        bool useStandardContentType = true)
    {
        fallback ??= new BlobHttpProperties();
        return new BlobHttpProperties
        {
            ContentType = First(headers, "x-ms-blob-content-type")
                          ?? (useStandardContentType ? First(headers, "Content-Type") : null)
                          ?? fallback.ContentType,
            ContentEncoding = First(headers, "x-ms-blob-content-encoding") ?? fallback.ContentEncoding,
            ContentLanguage = First(headers, "x-ms-blob-content-language") ?? fallback.ContentLanguage,
            CacheControl = First(headers, "x-ms-blob-cache-control") ?? fallback.CacheControl,
            ContentDisposition = First(headers, "x-ms-blob-content-disposition") ?? fallback.ContentDisposition,
            ContentMd5 = First(headers, "x-ms-blob-content-md5") ?? fallback.ContentMd5
        };
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
        CancellationToken cancellationToken)
    {
        using var reader = CreateXmlReader(body);
        var document = await XDocument.LoadAsync(reader, LoadOptions.None, cancellationToken);
        if (document.Root?.Name.LocalName != "KeyInfo")
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", "The specified XML is not syntactically valid.");

        var startsAt = ParseRequiredDate(ChildValue(document.Root, "Start"), "Start");
        var expiresAt = ParseRequiredDate(ChildValue(document.Root, "Expiry"), "Expiry");
        var delegatedTenant = ChildValue(document.Root, "DelegatedUserTid");
        if (!string.IsNullOrEmpty(delegatedTenant) && !Guid.TryParse(delegatedTenant, out _))
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", "The DelegatedUserTid value is invalid.");
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
            "ContainerDeleteRetentionPolicy",
            "IsVersioningEnabled",
            "StaticWebsite");

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

        if (Child(root, "DefaultServiceVersion") is not null)
            RequireServicePropertiesVersion(version, new DateOnly(2011, 8, 18), "DefaultServiceVersion");
        if (Child(root, "DeleteRetentionPolicy") is not null)
            RequireServicePropertiesVersion(version, new DateOnly(2017, 7, 29), "DeleteRetentionPolicy");
        if (Child(root, "ContainerDeleteRetentionPolicy") is not null)
            RequireServicePropertiesVersion(version, new DateOnly(2019, 12, 12), "ContainerDeleteRetentionPolicy");
        if (Child(root, "IsVersioningEnabled") is not null)
            RequireServicePropertiesVersion(version, new DateOnly(2019, 12, 12), "IsVersioningEnabled");

        var deletePolicy = ReadRetentionPolicy(root, "DeleteRetentionPolicy", current.BlobSoftDeleteEnabled, current.BlobSoftDeleteRetentionDays);
        var containerPolicy = ReadRetentionPolicy(root, "ContainerDeleteRetentionPolicy", current.ContainerSoftDeleteEnabled, current.ContainerSoftDeleteRetentionDays);
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
            DefaultServiceVersion = OptionalText(root, "DefaultServiceVersion") ?? current.DefaultServiceVersion,
            BlobSoftDeleteEnabled = deletePolicy.Enabled,
            BlobSoftDeleteRetentionDays = deletePolicy.Days,
            ContainerSoftDeleteEnabled = containerPolicy.Enabled,
            ContainerSoftDeleteRetentionDays = containerPolicy.Days,
            VersioningEnabled = Child(root, "IsVersioningEnabled") is null
                ? current.VersioningEnabled
                : ParseBool(RequiredText(root, "IsVersioningEnabled"), current.VersioningEnabled),
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

    private static (bool Enabled, int Days) ReadRetentionPolicy(XElement root, string name, bool currentEnabled, int currentDays)
    {
        if (Child(root, name) is not { } policy)
            return (currentEnabled, currentDays);
        ValidateUniqueChildren(policy);
        ValidateKnownChildren(policy, "Enabled", "Days");
        var enabled = ParseBool(RequiredText(policy, "Enabled"), false);
        var days = OptionalText(policy, "Days") is { } text ? ParseInt(text, "Days") : currentDays;
        if (enabled && Child(policy, "Days") is null)
            throw InvalidServicePropertiesXml("Retention Days is required when retention is enabled.");
        if (enabled && days is < 1 or > 365)
            throw InvalidServicePropertiesXml("Retention days must be between 1 and 365.");
        return (enabled, days);
    }

    private static void RequireServicePropertiesVersion(DateOnly version, DateOnly minimum, string feature)
    {
        if (version < minimum)
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "FeatureVersionMismatch",
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
