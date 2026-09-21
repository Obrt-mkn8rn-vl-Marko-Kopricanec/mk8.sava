using System.Globalization;
using Microsoft.Extensions.Primitives;

namespace Mk8.Sava.Protocol;

internal static class BlobConditionEvaluator
{
    private static readonly ConditionHeaderNames StandardHeaders = new(
        "If-Match",
        "If-None-Match",
        "If-Modified-Since",
        "If-Unmodified-Since");

    private static readonly ConditionHeaderNames CopySourceHeaders = new(
        "x-ms-source-if-match",
        "x-ms-source-if-none-match",
        "x-ms-source-if-modified-since",
        "x-ms-source-if-unmodified-since");

    private static readonly ConditionHeaderNames BlobTagHeaders = new(
        "x-ms-blob-if-match",
        "x-ms-blob-if-none-match",
        "x-ms-blob-if-modified-since",
        "x-ms-blob-if-unmodified-since");

    public static void EvaluateRead(
        HttpRequest request,
        string etag,
        DateTimeOffset lastModified)
    {
        var conditions = ReadConditions(request.Headers, StandardHeaders);
        if (!conditions.HasAny)
            return;

        if (IsServiceVersionAtLeast(request, new DateOnly(2013, 8, 15)))
        {
            EvaluateModernRead(
                conditions,
                etag,
                lastModified,
                AzureStorageException.ConditionNotMet,
                AzureStorageException.NotModified);
            return;
        }

        EvaluateLegacy(
            conditions,
            etag,
            lastModified,
            resourceExists: true,
            writeFailure: false,
            AzureStorageException.ConditionNotMet,
            AzureStorageException.NotModified);
    }

    public static void EvaluateWrite(
        HttpRequest request,
        string? etag,
        DateTimeOffset? lastModified)
    {
        var conditions = ReadConditions(request.Headers, StandardHeaders);
        EvaluateLegacy(
            conditions,
            etag,
            lastModified,
            resourceExists: etag is not null,
            writeFailure: true,
            AzureStorageException.ConditionNotMet,
            AzureStorageException.NotModified);
    }

    public static void EvaluateCopySource(
        HttpRequest request,
        string etag,
        DateTimeOffset lastModified)
    {
        var conditions = ReadConditions(request.Headers, CopySourceHeaders);
        if (!conditions.HasAny)
            return;

        EvaluateModernRead(
            conditions,
            etag,
            lastModified,
            AzureStorageException.SourceConditionNotMet,
            AzureStorageException.SourceConditionNotMet);
    }

    public static void EvaluateBlobTagRead(
        HttpRequest request,
        string etag,
        DateTimeOffset lastModified)
    {
        var conditions = ReadConditions(request.Headers, BlobTagHeaders);
        if (!conditions.HasAny)
            return;
        EvaluateModernRead(
            conditions,
            etag,
            lastModified,
            AzureStorageException.ConditionNotMet,
            AzureStorageException.NotModified);
    }

    public static void EvaluateBlobTagWrite(
        HttpRequest request,
        string etag,
        DateTimeOffset lastModified)
    {
        var conditions = ReadConditions(request.Headers, BlobTagHeaders);
        EvaluateLegacy(
            conditions,
            etag,
            lastModified,
            resourceExists: true,
            writeFailure: true,
            AzureStorageException.ConditionNotMet,
            AzureStorageException.NotModified);
    }

    public static void EvaluateContainerWrite(
        HttpRequest request,
        DateTimeOffset lastModified,
        bool supportsIfUnmodifiedSince)
    {
        var conditions = new Conditions(
            [],
            [],
            ReadDate(request.Headers, StandardHeaders.IfModifiedSince),
            supportsIfUnmodifiedSince
                ? ReadDate(request.Headers, StandardHeaders.IfUnmodifiedSince)
                : null);
        EvaluateLegacy(
            conditions,
            etag: null,
            lastModified,
            resourceExists: true,
            writeFailure: true,
            AzureStorageException.ConditionNotMet,
            AzureStorageException.NotModified);
    }

    public static void EvaluateIfUnmodifiedSince(
        HttpRequest request,
        DateTimeOffset lastModified)
    {
        var condition = ReadDate(request.Headers, StandardHeaders.IfUnmodifiedSince);
        if (condition.HasValue && ToWholeSeconds(lastModified) > ToWholeSeconds(condition.Value))
            throw AzureStorageException.ConditionNotMet();
    }

    public static void EvaluateAccessTierDeleteConditions(
        HttpRequest request,
        DateTimeOffset? accessTierChangedAt)
    {
        const string ifModifiedName = "x-ms-access-tier-if-modified-since";
        const string ifUnmodifiedName = "x-ms-access-tier-if-unmodified-since";
        var hasModified = request.Headers.ContainsKey(ifModifiedName);
        var hasUnmodified = request.Headers.ContainsKey(ifUnmodifiedName);
        if (!hasModified && !hasUnmodified)
            return;
        if (!IsServiceVersionAtLeast(request, new DateOnly(2025, 5, 5)))
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "FeatureVersionMismatch",
                "Access-tier conditional headers require service version 2025-05-05 or later.");
        }
        if (hasModified && hasUnmodified)
            throw AzureStorageException.MultipleConditionHeadersNotSupported();

        if (!accessTierChangedAt.HasValue)
            return;
        var modified = ReadDate(request.Headers, ifModifiedName);
        if (modified.HasValue && ToWholeSeconds(accessTierChangedAt.Value) <= ToWholeSeconds(modified.Value))
            throw AzureStorageException.ConditionNotMet();
        var unmodified = ReadDate(request.Headers, ifUnmodifiedName);
        if (unmodified.HasValue && ToWholeSeconds(accessTierChangedAt.Value) > ToWholeSeconds(unmodified.Value))
            throw AzureStorageException.ConditionNotMet();
    }

    private static void EvaluateModernRead(
        Conditions conditions,
        string etag,
        DateTimeOffset lastModified,
        Func<AzureStorageException> preconditionFailure,
        Func<AzureStorageException> notModifiedFailure)
    {
        var resourceTime = ToWholeSeconds(lastModified);
        if (conditions.IfMatch.Count > 0 && !conditions.IfMatch.Any(candidate => Matches(candidate, etag)))
            throw preconditionFailure();
        if (conditions.IfUnmodifiedSince.HasValue &&
            resourceTime > ToWholeSeconds(conditions.IfUnmodifiedSince.Value))
        {
            throw preconditionFailure();
        }

        var hasIfNoneMatch = conditions.IfNoneMatch.Count > 0;
        var hasIfModifiedSince = conditions.IfModifiedSince.HasValue;
        if (!hasIfNoneMatch && !hasIfModifiedSince)
            return;

        var ifNoneMatchPasses = hasIfNoneMatch &&
                                conditions.IfNoneMatch.All(candidate => !Matches(candidate, etag));
        var ifModifiedSincePasses = hasIfModifiedSince &&
                                    resourceTime > ToWholeSeconds(conditions.IfModifiedSince!.Value);
        if (!ifNoneMatchPasses && !ifModifiedSincePasses)
            throw notModifiedFailure();
    }

    private static void EvaluateLegacy(
        Conditions conditions,
        string? etag,
        DateTimeOffset? lastModified,
        bool resourceExists,
        bool writeFailure,
        Func<AzureStorageException> preconditionFailure,
        Func<AzureStorageException> notModifiedFailure)
    {
        ValidateLegacyCombination(conditions);
        if (!conditions.HasAny)
            return;

        if (conditions.IfNoneMatch.Count > 0)
        {
            if (resourceExists && conditions.IfNoneMatch.Any(candidate => Matches(candidate, etag!)))
                throw writeFailure ? preconditionFailure() : notModifiedFailure();
            return;
        }

        if (conditions.IfMatch.Count > 0)
        {
            if (!resourceExists || !conditions.IfMatch.Any(candidate => Matches(candidate, etag!)))
                throw preconditionFailure();
            return;
        }

        if (!resourceExists || !lastModified.HasValue)
            return;
        var resourceTime = ToWholeSeconds(lastModified.Value);
        if (conditions.IfModifiedSince.HasValue)
        {
            if (resourceTime <= ToWholeSeconds(conditions.IfModifiedSince.Value))
                throw writeFailure ? preconditionFailure() : notModifiedFailure();
            return;
        }
        if (conditions.IfUnmodifiedSince.HasValue &&
            resourceTime > ToWholeSeconds(conditions.IfUnmodifiedSince.Value))
        {
            throw preconditionFailure();
        }
    }

    private static void ValidateLegacyCombination(Conditions conditions)
    {
        if (conditions.IfMatch.Count > 1 || conditions.IfNoneMatch.Count > 1)
            throw AzureStorageException.MultipleConditionHeadersNotSupported();

        var ifMatch = conditions.IfMatch.Count > 0;
        var ifNoneMatch = conditions.IfNoneMatch.Count > 0;
        var ifModified = conditions.IfModifiedSince.HasValue;
        var ifUnmodified = conditions.IfUnmodifiedSince.HasValue;
        var count = Convert.ToInt32(ifMatch) +
                    Convert.ToInt32(ifNoneMatch) +
                    Convert.ToInt32(ifModified) +
                    Convert.ToInt32(ifUnmodified);
        if (count <= 1)
            return;
        if (count == 2 &&
            (ifNoneMatch && ifModified || ifMatch && ifUnmodified))
        {
            return;
        }
        throw AzureStorageException.MultipleConditionHeadersNotSupported();
    }

    private static Conditions ReadConditions(
        IHeaderDictionary headers,
        ConditionHeaderNames names) => new(
        ReadEtags(headers, names.IfMatch),
        ReadEtags(headers, names.IfNoneMatch),
        ReadDate(headers, names.IfModifiedSince),
        ReadDate(headers, names.IfUnmodifiedSince));

    private static IReadOnlyList<string> ReadEtags(IHeaderDictionary headers, string name)
    {
        if (!headers.TryGetValue(name, out var values))
            return [];

        var etags = new List<string>();
        foreach (var value in values)
        {
            if (value is null)
                throw AzureStorageException.InvalidHeader(name);
            ParseEtags(value, name, etags);
        }
        if (etags.Count == 0)
            throw AzureStorageException.InvalidHeader(name);
        return etags;
    }

    private static void ParseEtags(string value, string name, List<string> destination)
    {
        var start = 0;
        var quoted = false;
        for (var index = 0; index <= value.Length; index++)
        {
            if (index < value.Length && value[index] == '"')
                quoted = !quoted;
            if (index < value.Length && (value[index] != ',' || quoted))
                continue;

            var etag = value[start..index].Trim();
            if (etag.Length == 0)
                throw AzureStorageException.InvalidHeader(name, value);
            destination.Add(etag);
            start = index + 1;
        }
        if (quoted)
            throw AzureStorageException.InvalidHeader(name, value);
    }

    private static DateTimeOffset? ReadDate(IHeaderDictionary headers, string name)
    {
        if (!headers.TryGetValue(name, out StringValues values))
            return null;
        if (values.Count != 1)
            throw AzureStorageException.MultipleConditionHeadersNotSupported();
        var value = values[0];
        if (value is null || !DateTimeOffset.TryParseExact(
                value,
                "R",
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                out var parsed))
        {
            throw AzureStorageException.InvalidHeader(name, value);
        }
        return parsed;
    }

    private static bool Matches(string candidate, string etag) =>
        candidate == "*" || string.Equals(candidate, etag, StringComparison.Ordinal);

    private static DateTimeOffset ToWholeSeconds(DateTimeOffset value)
    {
        var utc = value.ToUniversalTime();
        return new DateTimeOffset(
            utc.Ticks - utc.Ticks % TimeSpan.TicksPerSecond,
            TimeSpan.Zero);
    }

    private static bool IsServiceVersionAtLeast(HttpRequest request, DateOnly minimum) =>
        DateOnly.TryParseExact(
            StorageRequestContext.Get(request.HttpContext).ServiceVersion,
            "yyyy-MM-dd",
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out var version) && version >= minimum;

    private sealed record ConditionHeaderNames(
        string IfMatch,
        string IfNoneMatch,
        string IfModifiedSince,
        string IfUnmodifiedSince);

    private sealed record Conditions(
        IReadOnlyList<string> IfMatch,
        IReadOnlyList<string> IfNoneMatch,
        DateTimeOffset? IfModifiedSince,
        DateTimeOffset? IfUnmodifiedSince)
    {
        public bool HasAny =>
            IfMatch.Count > 0 ||
            IfNoneMatch.Count > 0 ||
            IfModifiedSince.HasValue ||
            IfUnmodifiedSince.HasValue;
    }
}
