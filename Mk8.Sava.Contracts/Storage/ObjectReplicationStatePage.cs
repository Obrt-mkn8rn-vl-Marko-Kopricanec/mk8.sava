namespace Mk8.Sava.Storage;

internal sealed record ObjectReplicationStatePage(
    IReadOnlyList<ObjectReplicationState> Items,
    bool HasMore);
