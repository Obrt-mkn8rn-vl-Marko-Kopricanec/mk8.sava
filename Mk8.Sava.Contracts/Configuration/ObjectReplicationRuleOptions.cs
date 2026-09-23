namespace Mk8.Sava.Configuration;

public sealed class ObjectReplicationRuleOptions
{
    public string RuleId { get; init; } = string.Empty;
    public string SourceContainer { get; init; } = string.Empty;
    public string DestinationContainer { get; init; } = string.Empty;
    public IList<string> PrefixMatch { get; init; } = [];
    public DateTimeOffset? MinimumCreationTime { get; init; }
    public bool ReplicateBlobTags { get; init; }
}
