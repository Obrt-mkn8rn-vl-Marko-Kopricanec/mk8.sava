namespace Mk8.Sava.Storage;

internal sealed record ContainerListPage(IReadOnlyList<ContainerRecord> Items, bool HasMore);
