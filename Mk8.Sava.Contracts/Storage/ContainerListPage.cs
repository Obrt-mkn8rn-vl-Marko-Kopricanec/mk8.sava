namespace Mk8.Sava.Storage;

public sealed record ContainerListPage(IReadOnlyList<ContainerRecord> Items, bool HasMore);
