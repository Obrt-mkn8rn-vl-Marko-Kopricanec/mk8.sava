using System.Text.Json;
using System.Text.Json.Serialization;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Transport;

internal sealed class RpcContentManifestConverter : JsonConverter<ContentManifest>
{
    public override ContentManifest Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var document = JsonDocument.ParseValue(ref reader);
        var value = document.RootElement;
        var domain = value.GetProperty(nameof(ContentManifest.Domain)).GetString();
        var digest = value.GetProperty(nameof(ContentManifest.Sha256)).GetString();
        var length = value.GetProperty(nameof(ContentManifest.Length)).GetInt64();
        var chunks = value.GetProperty(nameof(ContentManifest.Chunks));
        if (domain is null || digest is null || length < 0 || chunks.ValueKind != JsonValueKind.Array || chunks.GetArrayLength() != 0)
            throw new JsonException("The application content descriptor is invalid.");
        // Resource-bearing arguments are resolved again inside the Application.
        // A Gateway never receives or supplies physical chunk references.
        return new ContentManifest(domain, length, digest, []);
    }

    public override void Write(Utf8JsonWriter writer, ContentManifest value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString(nameof(ContentManifest.Domain), value.Domain);
        writer.WriteNumber(nameof(ContentManifest.Length), value.Length);
        writer.WriteString(nameof(ContentManifest.Sha256), value.Sha256);
        writer.WriteStartArray(nameof(ContentManifest.Chunks));
        writer.WriteEndArray();
        writer.WriteEndObject();
    }
}
