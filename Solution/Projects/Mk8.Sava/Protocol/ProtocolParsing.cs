using System.Globalization;
using System.Net;
using System.Text;
using System.Xml;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

internal static class ProtocolParsing
{
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

    public static BlobHttpProperties ReadHttpProperties(IHeaderDictionary headers, BlobHttpProperties? fallback = null)
    {
        fallback ??= new BlobHttpProperties();
        return new BlobHttpProperties
        {
            ContentType = First(headers, "x-ms-blob-content-type") ?? First(headers, "Content-Type") ?? fallback.ContentType,
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
        var tags = new Dictionary<string, string>(StringComparer.Ordinal);
        while (await reader.ReadAsync())
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (reader.NodeType != XmlNodeType.Element || reader.LocalName != "Tag")
                continue;
            using var subtree = reader.ReadSubtree();
            string? key = null;
            string? value = null;
            while (await subtree.ReadAsync())
            {
                if (subtree.NodeType != XmlNodeType.Element)
                    continue;
                if (subtree.LocalName == "Key")
                    key = await subtree.ReadElementContentAsStringAsync();
                else if (subtree.LocalName == "Value")
                    value = await subtree.ReadElementContentAsStringAsync();
            }
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
        using var reader = CreateXmlReader(body);
        var blocks = new List<BlockListEntry>();
        while (await reader.ReadAsync())
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (reader.NodeType == XmlNodeType.Element && reader.LocalName is "Latest" or "Committed" or "Uncommitted")
            {
                var mode = reader.LocalName switch
                {
                    "Latest" => BlockListMode.Latest,
                    "Committed" => BlockListMode.Committed,
                    _ => BlockListMode.Uncommitted
                };
                blocks.Add(new BlockListEntry(await reader.ReadElementContentAsStringAsync(), mode));
            }
        }
        return blocks;
    }

    public static async Task<Dictionary<string, StoredAccessPolicy>> ReadAclAsync(Stream body, CancellationToken cancellationToken)
    {
        using var reader = CreateXmlReader(body);
        var policies = new Dictionary<string, StoredAccessPolicy>(StringComparer.Ordinal);
        while (await reader.ReadAsync())
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (reader.NodeType != XmlNodeType.Element || reader.LocalName != "SignedIdentifier")
                continue;
            using var subtree = reader.ReadSubtree();
            string? id = null;
            string permission = string.Empty;
            DateTimeOffset? start = null;
            DateTimeOffset? expiry = null;
            while (await subtree.ReadAsync())
            {
                if (subtree.NodeType != XmlNodeType.Element)
                    continue;
                switch (subtree.LocalName)
                {
                    case "Id": id = await subtree.ReadElementContentAsStringAsync(); break;
                    case "Permission": permission = await subtree.ReadElementContentAsStringAsync(); break;
                    case "Start": start = ParseDate(await subtree.ReadElementContentAsStringAsync(), "Start"); break;
                    case "Expiry": expiry = ParseDate(await subtree.ReadElementContentAsStringAsync(), "Expiry"); break;
                }
            }
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

    public static async Task<ServiceProperties> ReadServicePropertiesAsync(
        Stream body,
        ServiceProperties current,
        CancellationToken cancellationToken)
    {
        var document = new XmlDocument { XmlResolver = null };
        using var reader = CreateXmlReader(body);
        document.Load(reader);
        cancellationToken.ThrowIfCancellationRequested();
        var root = document.DocumentElement;
        if (root?.LocalName != "StorageServiceProperties")
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", "The specified XML is not syntactically valid.");

        var cors = new List<CorsRule>();
        foreach (XmlElement rule in root.SelectNodes("*[local-name()='Cors']/*[local-name()='CorsRule']") ?? EmptyNodes.Instance)
        {
            cors.Add(new CorsRule
            {
                AllowedOrigins = RequiredText(rule, "AllowedOrigins"),
                AllowedMethods = RequiredText(rule, "AllowedMethods"),
                AllowedHeaders = RequiredText(rule, "AllowedHeaders"),
                ExposedHeaders = RequiredText(rule, "ExposedHeaders"),
                MaxAgeInSeconds = ParseInt(RequiredText(rule, "MaxAgeInSeconds"), "MaxAgeInSeconds")
            });
        }

        var deletePolicy = ReadRetentionPolicy(root, "DeleteRetentionPolicy", current.BlobSoftDeleteEnabled, current.BlobSoftDeleteRetentionDays);
        var containerPolicy = ReadRetentionPolicy(root, "ContainerDeleteRetentionPolicy", current.ContainerSoftDeleteEnabled, current.ContainerSoftDeleteRetentionDays);
        var website = root.SelectSingleNode("*[local-name()='StaticWebsite']") as XmlElement;
        return current with
        {
            Cors = cors,
            DefaultServiceVersion = OptionalText(root, "DefaultServiceVersion") ?? current.DefaultServiceVersion,
            BlobSoftDeleteEnabled = deletePolicy.Enabled,
            BlobSoftDeleteRetentionDays = deletePolicy.Days,
            ContainerSoftDeleteEnabled = containerPolicy.Enabled,
            ContainerSoftDeleteRetentionDays = containerPolicy.Days,
            VersioningEnabled = ParseBool(OptionalText(root, "IsVersioningEnabled"), current.VersioningEnabled),
            StaticWebsite = website is null ? current.StaticWebsite : new StaticWebsiteProperties
            {
                Enabled = ParseBool(OptionalText(website, "Enabled"), false),
                IndexDocument = OptionalText(website, "IndexDocument"),
                ErrorDocument404Path = OptionalText(website, "ErrorDocument404Path")
            }
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

    public static XmlReader CreateXmlReader(Stream stream) => XmlReader.Create(stream, new XmlReaderSettings
    {
        Async = true,
        DtdProcessing = DtdProcessing.Prohibit,
        XmlResolver = null,
        MaxCharactersInDocument = 4 * 1024 * 1024,
        IgnoreComments = true,
        IgnoreProcessingInstructions = true
    });

    private static void ValidateTag(string key, string value)
    {
        if (key.Length is < 1 or > 128 || value.Length > 256 || key.Any(char.IsControl) || value.Any(char.IsControl))
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidTag", "The specified blob tag is invalid.");
    }

    private static DateTimeOffset? ParseDate(string value, string field) =>
        string.IsNullOrEmpty(value)
            ? null
            : DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed)
                ? parsed
                : throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", $"The {field} value is invalid.");

    private static (bool Enabled, int Days) ReadRetentionPolicy(XmlElement root, string name, bool currentEnabled, int currentDays)
    {
        if (root.SelectSingleNode($"*[local-name()='{name}']") is not XmlElement policy)
            return (currentEnabled, currentDays);
        var enabled = ParseBool(OptionalText(policy, "Enabled"), false);
        var days = OptionalText(policy, "Days") is { } text ? ParseInt(text, "Days") : currentDays;
        if (enabled && days is < 1 or > 365)
            throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", "Retention days must be between 1 and 365.");
        return (enabled, days);
    }

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

    private static string RequiredText(XmlElement parent, string name) =>
        OptionalText(parent, name) ?? throw new AzureStorageException(StatusCodes.Status400BadRequest, "InvalidXmlDocument", $"The {name} element is required.");

    private static string? OptionalText(XmlElement parent, string name) =>
        (parent.SelectSingleNode($"*[local-name()='{name}']") as XmlElement)?.InnerText;

    private static AzureStorageException InvalidRange() => new(
        StatusCodes.Status416RangeNotSatisfiable,
        "InvalidRange",
        "The range specified is invalid for the current size of the resource.");

    private sealed class EmptyNodes : XmlNodeList
    {
        public static EmptyNodes Instance { get; } = new();
        public override XmlNode? Item(int index) => null;
        public override System.Collections.IEnumerator GetEnumerator() => Array.Empty<XmlNode>().GetEnumerator();
        public override int Count => 0;
    }
}
