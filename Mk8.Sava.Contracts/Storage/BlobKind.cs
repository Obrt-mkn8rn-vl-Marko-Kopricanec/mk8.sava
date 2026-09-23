using System.Text.Json.Serialization;

namespace Mk8.Sava.Storage;

[JsonConverter(typeof(JsonStringEnumConverter<BlobKind>))]
public enum BlobKind
{
    BlockBlob,
    AppendBlob,
    PageBlob
}
