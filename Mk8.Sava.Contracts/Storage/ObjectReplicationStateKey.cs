namespace Mk8.Sava.Storage;

internal readonly record struct ObjectReplicationStateKey(
    string PolicyId,
    string RuleId,
    string SourceGenerationId);
