using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.WebUtilities;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class BlobContinuationShapeTests
{
    [Theory]
    [InlineData(true, -1, "OrderedId", false)]
    [InlineData(true, -1, "GenerationId", false)]
    [InlineData(true, -1, "OrderedId", true)]
    [InlineData(true, -1, "GenerationId", true)]
    [InlineData(false, -1, "OrderedId", false)]
    [InlineData(false, -1, "GenerationId", false)]
    [InlineData(false, -1, "OrderedId", true)]
    [InlineData(false, -1, "GenerationId", true)]
    [InlineData(false, 0, "OrderedId", false)]
    [InlineData(false, 0, "GenerationId", false)]
    [InlineData(false, 0, "OrderedId", true)]
    [InlineData(false, 0, "GenerationId", true)]
    [InlineData(false, 1, "OrderedId", false)]
    [InlineData(false, 1, "GenerationId", false)]
    [InlineData(false, 1, "OrderedId", true)]
    [InlineData(false, 1, "GenerationId", true)]
    public void NullAndOmittedIdentityFieldsAreInvalidMarkers(bool prefix, int rank, string field, bool omitted)
    {
        var payload = Assert.IsType<JsonObject>(JsonSerializer.SerializeToNode(CreateCursor(prefix, rank)));
        if (omitted)
            Assert.True(payload.Remove(field));
        else
            payload[field] = null;
        var marker = Encode(payload);
        var refusal = Assert.Throws<AzureStorageException>(() => Decode(marker));
        Assert.Equal(400, refusal.StatusCode);
        Assert.Equal("InvalidQueryParameterValue", refusal.ErrorCode);
        Assert.Equal("marker", refusal.HeaderName);
    }

    [Theory]
    [InlineData(true, -1)]
    [InlineData(false, -1)]
    [InlineData(false, 0)]
    [InlineData(false, 1)]
    [InlineData(false, 2)]
    [InlineData(false, 3)]
    [InlineData(false, 4)]
    public void CompleteIdentityFieldsRetainValidPaginationShapes(bool prefix, int rank)
    {
        var expected = CreateCursor(prefix, rank);
        var payload = Assert.IsType<JsonObject>(JsonSerializer.SerializeToNode(expected));
        Assert.Equal(expected, Decode(Encode(payload)).Cursor);
    }

    private static BlobListCursor CreateCursor(bool prefix, int rank) => new("present.bin", NameComplete: false, prefix, rank,
        rank is 1 or 3 ? "ordered" : string.Empty, rank >= 0 ? "generation" : string.Empty);

    private static string Encode(JsonObject payload)
    {
        // Keep the original list scope intact: malformed identity fields, not a
        // mismatched scope, must be responsible for the invalid-marker response.
        var scope = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(
            "fixtureaccount\nfixturecontainer\n2023-11-03\n\n\n\n\n")).AsSpan(0, 12));
        return "mk8s2." + WebEncoders.Base64UrlEncode(JsonSerializer.SerializeToUtf8Bytes(payload)) + "." + scope;
    }

    private static BlobListingMarker Decode(string marker)
    {
        var context = new DefaultHttpContext();
        StorageRequestContext.Set(context, new StorageRequestContext
        {
            RequestId = "fixture",
            Account = "fixtureaccount",
            Container = "fixturecontainer",
            ResourceKind = StorageResourceKind.Container,
            CanonicalResourcePath = "/fixtureaccount/fixturecontainer",
            ServiceVersion = "2023-11-03",
            Authorization = StorageAuthorization.Owner,
        });
        return AzureResponseWriter.DecodeBlobMarker(context, string.Empty, string.Empty, string.Empty,
            string.Empty, string.Empty, new HashSet<string>(StringComparer.Ordinal), marker);
    }
}
