using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Transport;

internal static class RpcJson
{
    private static readonly HashSet<string> GatewayUnusedBlobProperties = new(StringComparer.Ordinal)
    {
        nameof(BlobRecord.PendingCopyContent), nameof(BlobRecord.PendingCopyCommittedBlocks),
        nameof(BlobRecord.PendingCopyAppendBlockCount), nameof(BlobRecord.PendingCopyIsSealed),
        nameof(BlobRecord.PendingCopyPageRanges), nameof(BlobRecord.PageMutationRanges),
        nameof(BlobRecord.PageMutationSequence), nameof(BlobRecord.PageBlobIncarnationId),
        nameof(BlobRecord.IncrementalCopySourceIncarnationId),
    };

    internal static JsonSerializerOptions Options { get; } = CreateOptions();

    internal static bool IsGatewayUnusedProperty(Type owner, string name) =>
        owner == typeof(BlobRecord) && GatewayUnusedBlobProperties.Contains(name);

    private static JsonSerializerOptions CreateOptions()
    {
        var resolver = new DefaultJsonTypeInfoResolver();
        resolver.Modifiers.Add(HideStorageHistory);
        return new JsonSerializerOptions
        {
            IncludeFields = true,
            MaxDepth = 64,
            TypeInfoResolver = resolver,
            Converters = { new RpcContentManifestConverter() },
        };
    }

    private static void HideStorageHistory(JsonTypeInfo typeInfo)
    {
        for (var index = typeInfo.Properties.Count - 1; index >= 0; index--)
        {
            if (IsGatewayUnusedProperty(typeInfo.Type, typeInfo.Properties[index].Name))
                typeInfo.Properties.RemoveAt(index);
        }
    }
}
