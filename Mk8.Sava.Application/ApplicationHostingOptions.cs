using System.ComponentModel.DataAnnotations;

namespace Mk8.Sava.Application;

internal sealed class ApplicationHostingOptions
{
    public const string SectionName = "ApplicationHosting";
    public string CertificateFile { get; init; } = string.Empty;
    public string? CertificatePassword { get; init; }
    [Range(1, 65536)]
    public int MaximumReadSessions { get; init; } = 256;
    public TimeSpan ReadSessionIdleTimeout { get; init; } = TimeSpan.FromMinutes(2);
    [Range(1, 256)]
    public int MaximumConcurrentRpcRequests { get; init; } = 16;
    [Range(0, 4096)]
    public int MaximumQueuedRpcRequests { get; init; } = 128;
    public TimeSpan RpcQueueTimeout { get; init; } = TimeSpan.FromSeconds(10);
}
