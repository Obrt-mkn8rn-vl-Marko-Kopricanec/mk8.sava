namespace Mk8.Sava.Storage;

public sealed record StaticWebsiteProperties
{
    public bool Enabled { get; init; }
    public string? IndexDocument { get; init; }
    public string? DefaultIndexDocumentPath { get; init; }
    public string? ErrorDocument404Path { get; init; }
}
