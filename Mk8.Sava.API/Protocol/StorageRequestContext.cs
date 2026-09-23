namespace Mk8.Sava.Protocol;

internal sealed record StorageRequestContext
{
    private const string ItemKey = "Mk8.Sava.RequestContext";

    public required string RequestId { get; init; }
    public required string Account { get; init; }
    public string? Container { get; init; }
    public string? Blob { get; init; }
    public string? Snapshot { get; init; }
    public string? VersionId { get; init; }
    public required StorageResourceKind ResourceKind { get; init; }
    public required string CanonicalResourcePath { get; init; }
    public required string ServiceVersion { get; init; }
    public required StorageAuthorization Authorization { get; set; }

    public static StorageRequestContext Get(HttpContext context) =>
        TryGet(context) ?? throw new InvalidOperationException("The storage request context has not been initialized.");

    public static StorageRequestContext? TryGet(HttpContext context) =>
        context.Items.TryGetValue(ItemKey, out var value) ? value as StorageRequestContext : null;

    public static void Set(HttpContext context, StorageRequestContext value) => context.Items[ItemKey] = value;
}
