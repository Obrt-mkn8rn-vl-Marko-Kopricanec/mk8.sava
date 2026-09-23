namespace Mk8.Sava.Protocol;

internal sealed record BlobQueryTextFormat(
    BlobQueryFormatKind Kind,
    string ColumnSeparator,
    char Quote,
    string RecordSeparator,
    char Escape,
    bool HasHeaders,
    IReadOnlyList<QueryArrowColumn> ArrowSchema);
