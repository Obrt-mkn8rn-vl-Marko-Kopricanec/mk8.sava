using System.ComponentModel.DataAnnotations;

namespace Mk8.Sava.Hosting;

public sealed class GatewayOptions
{
    public const string SectionName = "Gateway";

    [Required]
    public string StagingPath { get; init; } = ".gateway-staging";

    [Range(1, 256)]
    public int MaximumConcurrentRequests { get; init; } = 32;

    [Range(0, 4096)]
    public int MaximumQueuedRequests { get; init; } = 128;

    [Range(1L, long.MaxValue)]
    public long MaximumStagingBytes { get; init; } = 10L * 1024 * 1024 * 1024;
}
