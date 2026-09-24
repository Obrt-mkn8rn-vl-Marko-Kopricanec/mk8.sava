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
        var candidates = new List<string>(4);
        AddCandidate(primary);
        if (!TryGetRawEscapedPath(request, out _) &&
            request.Path.Value is { } decodedPath &&
            decodedPath.Contains('%', StringComparison.Ordinal))
        {
            var forcedPercentEscaping = new PathString(decodedPath.Replace("%", "%25", StringComparison.Ordinal))
                .ToUriComponent();
            AddCandidate(forcedPercentEscaping);
        }
        return candidates;

        void AddCandidate(string path)
        {
            if (!candidates.Contains(path, StringComparer.Ordinal))
                candidates.Add(path);
            // The SDK signs the encoded URI path, but an HTTP transport can
            // expose an encoded ! as a literal request-target character.
            var escapedExclamation = path.Replace("!", "%21", StringComparison.Ordinal);
            if (!candidates.Contains(escapedExclamation, StringComparer.Ordinal))
                candidates.Add(escapedExclamation);
        }
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
            if (!rawTarget.StartsWith('/') &&
                Uri.TryCreate(rawTarget, UriKind.Absolute, out var absolute))
            {
                escapedPath = absolute.AbsolutePath;
                return true;
            }
            var queryOffset = rawTarget.IndexOf('?', StringComparison.Ordinal);
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
        var normalized = path.StartsWith('/') ? path[1..] : path;
        return normalized.Length == 0 ? [] : normalized.Split('/', StringSplitOptions.None);
    }

    public static bool HostIdentifiesAccount(string host, string account) =>
        string.Equals(host.Split('.', 2)[0], account, StringComparison.OrdinalIgnoreCase);

    public static string GetServiceEndpoint(HttpRequest request, string account)
    {
        var origin = $"{request.Scheme}://{request.Host}";
        return HostIdentifiesAccount(request.Host.Host, account)
            ? $"{origin}/"
            : $"{origin}/{EscapePathSegment(account)}/";
    }

    public static string GetContainerEndpoint(HttpRequest request, string account, string container) =>
        GetServiceEndpoint(request, account) + EscapePathSegment(container);

    public static string GetBlobEndpoint(
        HttpRequest request,
        string account,
        string container,
        string blob)
    {
        var escapedBlob = string.Join('/', blob.Split('/').Select(EscapePathSegment));
        return string.Equals(container, "$root", StringComparison.Ordinal)
            ? GetServiceEndpoint(request, account) + escapedBlob
            : $"{GetContainerEndpoint(request, account, container)}/{escapedBlob}";
    }

    public static (string Container, string Blob) ResolveBlob(string[] segments, int offset)
    {
        var remaining = segments.Length - offset;
        if (remaining <= 0)
            return (string.Empty, string.Empty);
        if (remaining == 1)
            return ("$root", segments[offset]);
        return (segments[offset], string.Join('/', segments.Skip(offset + 1)));
    }

    private static string EscapePathSegment(string value) => Uri.EscapeDataString(value);
}
