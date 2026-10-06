using System.Reflection;
using Mk8.Sava.Application;

namespace Mk8.Sava.Transport;

internal static class RpcControlLane
{
    internal const int MaximumFrameBytes = 4096;
    internal const long MaximumRequestBytes = 4 + MaximumFrameBytes + 4 + 32;
    internal static readonly TimeSpan MaximumRequestTimeout = TimeSpan.FromSeconds(5);
    private static readonly MethodInfo[] Operations =
    [
        typeof(IApplicationReadSessions).GetMethod(nameof(IApplicationReadSessions.TouchAsync), [typeof(string), typeof(CancellationToken)])!,
        typeof(IApplicationReadSessions).GetMethod(nameof(IApplicationReadSessions.CloseAsync), [typeof(string), typeof(CancellationToken)])!,
        typeof(IApplicationReadiness).GetMethod(nameof(IApplicationReadiness.GetAsync), [typeof(CancellationToken)])!,
    ];

    internal static bool IsAllowed(RpcContract contract, RpcMethod method) =>
        contract.Type == method.Method.DeclaringType && Operations.Contains(method.Method) &&
        method.InputIndex < 0 && method.OutputIndex < 0 && method.PageSourceIndex < 0;

    internal static Uri GetEndpoint(Uri endpoint, ApplicationRpcLane lane) => lane == ApplicationRpcLane.Control
        ? new Uri(endpoint.AbsoluteUri.TrimEnd('/') + "/control", UriKind.Absolute) : endpoint;

    internal static TimeSpan RequestTimeout(TimeSpan configured, ApplicationRpcLane lane) =>
        lane == ApplicationRpcLane.Control && configured > MaximumRequestTimeout ? MaximumRequestTimeout : configured;

    internal static int FrameLimit(int configured, ApplicationRpcLane lane) =>
        lane == ApplicationRpcLane.Control ? Math.Min(configured, MaximumFrameBytes) : configured;
}
