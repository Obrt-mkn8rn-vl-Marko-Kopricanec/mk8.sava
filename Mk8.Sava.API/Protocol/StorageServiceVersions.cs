using System.Globalization;

namespace Mk8.Sava.Protocol;

internal static class StorageServiceVersions
{
    public const string AnonymousGeneralPurposeFallback = "2009-09-19";

    private static readonly HashSet<string> Supported = new(StringComparer.Ordinal)
    {
        "2008-10-27",
        "2009-04-14",
        "2009-07-17",
        "2009-09-19",
        "2011-08-18",
        "2012-02-12",
        "2013-08-15",
        "2014-02-14",
        "2015-02-21",
        "2015-04-05",
        "2015-07-08",
        "2015-12-11",
        "2016-05-31",
        "2017-04-17",
        "2017-07-29",
        "2017-11-09",
        "2018-03-28",
        "2018-11-09",
        "2019-02-02",
        "2019-07-07",
        "2019-12-12",
        "2020-02-10",
        "2020-04-08",
        "2020-06-12",
        "2020-08-04",
        "2020-10-02",
        "2020-12-06",
        "2021-02-12",
        "2021-04-10",
        "2021-06-08",
        "2021-08-06",
        "2021-10-04",
        "2021-12-02",
        "2022-11-02",
        "2023-01-03",
        "2023-05-03",
        "2023-08-03",
        "2023-11-03",
        "2024-02-04",
        "2024-05-04",
        "2024-08-04",
        "2024-11-04",
        "2025-01-05",
        "2025-05-05",
        "2025-07-05",
        "2025-11-05",
        "2026-02-06",
        "2026-04-06",
        "2026-06-06",
        "2026-10-06"
    };

    public static bool TryParse(string value, out DateOnly version)
    {
        if (!Supported.Contains(value))
        {
            version = default;
            return false;
        }

        return DateOnly.TryParseExact(
            value,
            "yyyy-MM-dd",
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out version);
    }

    public static string RequireHeader(string value)
    {
        if (!TryParse(value, out _))
            throw AzureStorageException.InvalidHeader("x-ms-version", value);
        return value;
    }

    public static string RequireApiVersion(string value)
    {
        if (!TryParse(value, out _))
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "InvalidQueryParameterValue",
                "Value for one of the query parameters specified in the request URI is invalid.",
                details: new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["QueryParameterName"] = "api-version",
                    ["QueryParameterValue"] = value
                });
        }
        return value;
    }
}
