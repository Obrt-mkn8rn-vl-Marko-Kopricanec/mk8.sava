namespace Mk8.Sava.Configuration;

public sealed class ObjectReplicationPolicyOptions
{
    public string PolicyId { get; init; } = string.Empty;
    public string SourceAccount { get; init; } = string.Empty;
    public string DestinationAccount { get; init; } = string.Empty;
    public DateTimeOffset? EnabledAt { get; init; }
    public List<ObjectReplicationRuleOptions> Rules { get; init; } = [];
}
