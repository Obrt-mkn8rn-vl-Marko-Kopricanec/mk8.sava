namespace Mk8.Sava.Storage;

internal sealed record ObjectReplicationState
{
    public required string PolicyId { get; init; }
    public required string RuleId { get; init; }
    public required string SourceGenerationId { get; init; }
    public required string SourceAccount { get; init; }
    public required string SourceContainer { get; init; }
    public required string SourceName { get; init; }
    public required string DestinationAccount { get; init; }
    public required string DestinationContainer { get; init; }
    public string? DestinationGenerationId { get; init; }
    public required string SourceFingerprint { get; init; }
    public required string Status { get; init; }
    public required DateTimeOffset UpdatedAt { get; init; }
}
