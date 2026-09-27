using Parquet.Schema;

namespace Mk8.Sava.Protocol;

internal sealed record ParquetQueryInput(DataField[] Fields, string[] Names, int MetadataBytes, BlobQueryDataException? Error);
