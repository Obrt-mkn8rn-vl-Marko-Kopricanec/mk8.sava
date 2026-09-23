namespace Mk8.Sava.Storage;

public sealed class StoredContent(
    ContentManifest manifest,
    IDisposable pin,
    string? contentMd5 = null) : IDisposable
{
    public ContentManifest Manifest { get; } = manifest;
    public string? ContentMd5 { get; } = contentMd5;

    public void Dispose() => pin.Dispose();
}
