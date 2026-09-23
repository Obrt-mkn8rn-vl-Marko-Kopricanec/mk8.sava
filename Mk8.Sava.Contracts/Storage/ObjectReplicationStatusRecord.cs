namespace Mk8.Sava.Storage;

public sealed record ObjectReplicationStatusRecord
{
    public required string Status { get; init; }
    public required string SourceFingerprint { get; init; }
}
