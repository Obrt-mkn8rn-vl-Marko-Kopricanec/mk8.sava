using Microsoft.AspNetCore.Http.Features;

namespace Mk8.Sava.Protocol;

internal static class RequestBodyLimits
{
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
