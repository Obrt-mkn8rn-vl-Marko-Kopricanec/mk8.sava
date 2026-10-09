using Microsoft.AspNetCore.Http.Features;

namespace Mk8.Sava.Protocol;

internal static class RequestBodyLimits
{
    public static void PrepareOrdinaryBlockBlob(HttpRequest request, long maximumContentLength)
    {
        var feature = request.HttpContext.Features.Get<IHttpMaxRequestBodySizeFeature>();
        if (feature is not { IsReadOnly: false } ||
            request.Headers.ContainsKey("x-ms-copy-source") ||
            request.Headers.ContainsKey("x-ms-structured-body") ||
            request.Headers.ContainsKey("x-ms-structured-content-length") ||
            !string.Equals(ProtocolParsing.First(request.Headers, "x-ms-blob-type"), "BlockBlob", StringComparison.Ordinal))
        {
            return;
        }
        if (request.ContentLength is not { } contentLength || contentLength < 0 || contentLength > maximumContentLength)
            return;

        // The caller has checked write authorization and supplied the lesser logical/version budget.
        // Permit Kestrel's existing rejection drain without reading or admitting this body into storage.
        feature.MaxRequestBodySize = contentLength;
    }

    public static void Apply(HttpRequest request, long maximumContentLength, bool structured)
    {
        var feature = request.HttpContext.Features.Get<IHttpMaxRequestBodySizeFeature>();
        if (feature is not { IsReadOnly: false })
            return;

        // Upload authorization and logical/version limits are checked before reading.
        // XSM permits up to ushort.MaxValue segments, not just our encoder's layout.
        var overhead = structured ? StructuredBodyDecoder.MaximumEncodingOverhead : 0;
        feature.MaxRequestBodySize = maximumContentLength > long.MaxValue - overhead
            ? long.MaxValue
            : maximumContentLength + overhead;
    }
}
