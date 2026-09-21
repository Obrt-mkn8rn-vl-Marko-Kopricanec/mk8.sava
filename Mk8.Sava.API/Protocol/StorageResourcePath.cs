using Microsoft.AspNetCore.Http.Features;

namespace Mk8.Sava.Protocol;

internal static class StorageResourcePath
{
    public static string GetEscapedPath(HttpRequest request)
    {
        if (TryGetRawEscapedPath(request, out var escapedPath))
            return escapedPath;

        return request.Path.ToUriComponent();
    }

    public static IReadOnlyList<string> GetSignaturePathCandidates(HttpRequest request)
    {
        var primary = GetEscapedPath(request);
        if (TryGetRawEscapedPath(request, out _) ||
            request.Path.Value is not { } decodedPath ||
            !decodedPath.Contains('%', StringComparison.Ordinal))
        {
            return [primary];
        }

        var forcedPercentEscaping = new PathString(decodedPath.Replace("%", "%25", StringComparison.Ordinal))
            .ToUriComponent();
        return string.Equals(primary, forcedPercentEscaping, StringComparison.Ordinal)
            ? [primary]
            : [primary, forcedPercentEscaping];
    }

    public static string[] DecodeRequestSegments(HttpRequest request)
    {
        if (TryGetRawEscapedPath(request, out var escapedPath))
            return DecodeSegments(escapedPath);

        return SplitPath(request.Path.Value ?? "/");
    }

    private static bool TryGetRawEscapedPath(HttpRequest request, out string escapedPath)
    {
        var rawTarget = request.HttpContext.Features.Get<IHttpRequestFeature>()?.RawTarget;
        if (!string.IsNullOrEmpty(rawTarget))
        {
            if (!rawTarget.StartsWith("/", StringComparison.Ordinal) &&
                Uri.TryCreate(rawTarget, UriKind.Absolute, out var absolute))
            {
                escapedPath = absolute.AbsolutePath;
                return true;
            }
            var queryOffset = rawTarget.IndexOf('?');
            escapedPath = queryOffset < 0 ? rawTarget : rawTarget[..queryOffset];
            return true;
        }

        escapedPath = string.Empty;
        return false;
    }

    public static string[] DecodeSegments(string escapedPath)
    {
        return SplitPath(escapedPath)
            .Select(Uri.UnescapeDataString)
            .ToArray();
    }

    private static string[] SplitPath(string path)
    {
        var normalized = path.StartsWith("/", StringComparison.Ordinal) ? path[1..] : path;
        return normalized.Length == 0 ? [] : normalized.Split('/', StringSplitOptions.None);
    }

    public static bool HostIdentifiesAccount(string host, string account) =>
        string.Equals(host.Split('.', 2)[0], account, StringComparison.OrdinalIgnoreCase);

    public static (string Container, string Blob) ResolveBlob(string[] segments, int offset)
    {
        var remaining = segments.Length - offset;
        if (remaining <= 0)
            return (string.Empty, string.Empty);
        if (remaining == 1)
            return ("$root", segments[offset]);
        return (segments[offset], string.Join('/', segments.Skip(offset + 1)));
    }
}
