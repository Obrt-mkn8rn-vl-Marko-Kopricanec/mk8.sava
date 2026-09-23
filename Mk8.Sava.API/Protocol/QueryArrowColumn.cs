namespace Mk8.Sava.Protocol;

internal sealed record QueryArrowColumn(
    BlobQueryArrowFieldKind Kind,
    string Name,
    int Precision,
    int Scale);
