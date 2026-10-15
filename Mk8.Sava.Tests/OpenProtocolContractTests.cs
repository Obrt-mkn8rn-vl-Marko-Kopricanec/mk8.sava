using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class OpenProtocolContractTests
{
    [Theory]
    [InlineData("put")]
    [InlineData("metadata")]
    [InlineData("blocks")]
    public async Task RepeatedClockPublicationsAllocateDistinctReadableVersions(string operation)
    {
        var clock = new FixedClock(DateTimeOffset.UtcNow);
        var factory = new SavaWebApplicationFactory(clock,
            new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);
        await using var lifetime = factory.ConfigureAwait(false);
        using var transport = new HttpClientTransport(factory.Server.CreateHandler());
        var owner = CreateOwner(transport);
        var container = owner.GetBlobContainerClient("same-tick-versions");
        await container.CreateAsync().ConfigureAwait(true);
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var properties = await metadata.GetServicePropertiesAsync(SavaWebApplicationFactory.AccountName,
            CancellationToken.None).ConfigureAwait(true);
        await metadata.PutServicePropertiesAsync(SavaWebApplicationFactory.AccountName,
            properties with { VersioningEnabled = true }, CancellationToken.None).ConfigureAwait(true);
        var blob = container.GetBlobClient("versions.bin");
        await blob.UploadAsync(BinaryData.FromString("initial bytes")).ConfigureAwait(true);
        var versions = new List<(string Id, string Content)>
        {
            ((await blob.GetPropertiesAsync().ConfigureAwait(true)).Value.VersionId, "initial bytes"),
        };
        for (var index = 0; index < 2; index++)
        {
            var content = await PublishSameTickAsync(blob, operation, index).ConfigureAwait(true);
            var current = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value;
            Assert.Equal(clock.GetUtcNow(), (await GetRecordAsync(metadata, blob).ConfigureAwait(true)).LastModified);
            versions.Add((current.VersionId, content));
        }
        Assert.Equal(3, versions.Select(item => item.Id).Distinct(StringComparer.Ordinal).Count());
        foreach (var (id, content) in versions)
            Assert.Equal(content, (await blob.WithVersion(id).DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        var listed = new List<BlobItem>();
        await foreach (var item in container.GetBlobsAsync(traits: BlobTraits.None, states: BlobStates.Version,
                           prefix: null, cancellationToken: CancellationToken.None).ConfigureAwait(true))
        {
            listed.Add(item);
        }
        Assert.Equal(3, listed.Count);
    }

    private static async Task<string> PublishSameTickAsync(BlobClient blob, string operation, int index)
    {
        var content = "replacement " + index.ToString(CultureInfo.InvariantCulture);
        if (string.Equals(operation, "metadata", StringComparison.Ordinal))
        {
            await blob.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["revision"] = content }).ConfigureAwait(false);
            return "initial bytes";
        }
        if (string.Equals(operation, "blocks", StringComparison.Ordinal))
        {
            var block = blob.GetParentBlobContainerClient().GetBlockBlobClient(blob.Name);
            var id = Convert.ToBase64String("fixed-block"u8);
            var body = new MemoryStream(Encoding.UTF8.GetBytes(content), writable: false);
            await using var bodyLifetime = body.ConfigureAwait(false);
            await block.StageBlockAsync(id, body).ConfigureAwait(false);
            await block.CommitBlockListAsync([id]).ConfigureAwait(false);
        }
        else
        {
            await blob.UploadAsync(BinaryData.FromString(content), overwrite: true).ConfigureAwait(false);
        }
        return content;
    }

    [Theory]
    [InlineData("modified-false", false, 412)]
    [InlineData("modified-false", true, 412)]
    [InlineData("unmodified-false", false, 412)]
    [InlineData("unmodified-false", true, 412)]
    [InlineData("modified-true", false, 202)]
    [InlineData("modified-true", true, 202)]
    [InlineData("unmodified-true", false, 202)]
    [InlineData("unmodified-true", true, 202)]
    [InlineData("old-version", false, 409)]
    [InlineData("old-version", true, 409)]
    [InlineData("malformed", false, 400)]
    [InlineData("malformed", true, 400)]
    [InlineData("both", false, 400)]
    [InlineData("both", true, 400)]
    public async Task BatchAndDirectDeleteHonorTheSameTierConditions(string condition, bool batch, int status)
    {
        ArgumentNullException.ThrowIfNull(condition);
        var factory = CreateFactory();
        await using var lifetime = factory.ConfigureAwait(false);
        using var sdkTransport = new HttpClientTransport(factory.Server.CreateHandler());
        var owner = CreateOwner(sdkTransport);
        var container = owner.GetBlobContainerClient("tier-delete-conditions");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("retained.bin");
        await blob.UploadAsync(BinaryData.FromString("tier condition preservation")).ConfigureAwait(true);
        await blob.SetAccessTierAsync(AccessTier.Cool).ConfigureAwait(true);
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var before = await GetRecordAsync(metadata, blob).ConfigureAwait(true);
        var headers = TierHeaders(condition);
        using var client = new HttpClient(factory.Server.CreateHandler());
        if (batch)
        {
            var payload = await SendDeleteBatchAsync(client, container, blob, headers).ConfigureAwait(true);
            Assert.Contains("HTTP/1.1 " + status.ToString(CultureInfo.InvariantCulture), payload, StringComparison.Ordinal);
            if (status != 202)
                Assert.Contains(TierError(condition), payload, StringComparison.Ordinal);
        }
        else
        {
            using var request = new HttpRequestMessage(HttpMethod.Delete, blob.GenerateSasUri(
                BlobSasPermissions.Delete, DateTimeOffset.UtcNow.AddMinutes(5)));
            foreach (var header in headers)
                request.Headers.TryAddWithoutValidation(header.Key, header.Value);
            using var response = await client.SendAsync(request).ConfigureAwait(true);
            Assert.Equal(status, (int)response.StatusCode);
            if (status != 202)
                Assert.Equal(TierError(condition), Assert.Single(response.Headers.GetValues("x-ms-error-code")));
        }
        if (status == 202)
        {
            Assert.False((await blob.ExistsAsync().ConfigureAwait(true)).Value);
        }
        else
        {
            Assert.Equal(JsonSerializer.Serialize(before), JsonSerializer.Serialize(await GetRecordAsync(metadata, blob).ConfigureAwait(true)));
            Assert.Equal("tier condition preservation", (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        }
    }

    private static Dictionary<string, string> TierHeaders(string condition)
    {
        var headers = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["x-ms-version"] = string.Equals(condition, "old-version", StringComparison.Ordinal) ? "2023-11-03" : "2026-06-06",
        };
        var unmodified = condition.StartsWith("unmodified", StringComparison.Ordinal);
        var future = string.Equals(condition, "modified-false", StringComparison.Ordinal) ||
                     string.Equals(condition, "unmodified-true", StringComparison.Ordinal);
        headers[unmodified ? "x-ms-access-tier-if-unmodified-since" : "x-ms-access-tier-if-modified-since"] =
            string.Equals(condition, "malformed", StringComparison.Ordinal) ? "invalid date" :
            DateTimeOffset.UtcNow.AddDays(future ? 1 : -1).ToString("R", CultureInfo.InvariantCulture);
        if (string.Equals(condition, "both", StringComparison.Ordinal))
            headers["x-ms-access-tier-if-unmodified-since"] = DateTimeOffset.UtcNow.ToString("R", CultureInfo.InvariantCulture);
        return headers;
    }

    private static string TierError(string condition) => condition switch
    {
        "old-version" => "FeatureVersionMismatch",
        "malformed" => "InvalidHeaderValue",
        "both" => "MultipleConditionHeadersNotSupported",
        _ => "ConditionNotMet",
    };

    private static async Task<string> SendDeleteBatchAsync(HttpClient client, BlobContainerClient container,
        BlobClient blob, Dictionary<string, string> headers)
    {
        const string boundary = "tier_condition_batch";
        var target = blob.GenerateSasUri(BlobSasPermissions.Delete, DateTimeOffset.UtcNow.AddMinutes(5));
        // Batch version belongs to the envelope; nested x-ms-version is refused.
        var innerHeaders = string.Concat(headers
            .Where(header => !string.Equals(header.Key, "x-ms-version", StringComparison.OrdinalIgnoreCase))
            .Select(header => header.Key + ": " + header.Value + "\r\n"));
        var payload = "--" + boundary + "\r\nContent-Type: application/http\r\nContent-Transfer-Encoding: binary\r\n\r\n" +
                      "DELETE " + target.PathAndQuery + " HTTP/1.1\r\n" + innerHeaders + "\r\n--" + boundary + "--\r\n";
        var endpoint = container.GenerateSasUri(BlobContainerSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5));
        using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(endpoint + "&restype=container&comp=batch"))
        {
            Content = new ByteArrayContent(Encoding.Latin1.GetBytes(payload)),
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", headers["x-ms-version"]);
        request.Content.Headers.ContentType = MediaTypeHeaderValue.Parse("multipart/mixed; boundary=" + boundary);
        using var response = await client.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        return await response.Content.ReadAsStringAsync().ConfigureAwait(false);
    }

    [Theory]
    [InlineData("RelativeToNow", false, true)]
    [InlineData("RelativeToCreation", false, true)]
    [InlineData("RelativeToNow", true, true)]
    [InlineData("RelativeToNow", false, false)]
    [InlineData("RelativeToCreation", false, false)]
    [InlineData("RelativeToNow", true, false)]
    public async Task RelativeExpiryRejectsOverflowAndPreservesState(string option, bool creation, bool overflow)
    {
        var clock = new FixedClock(DateTimeOffset.UtcNow);
        var factory = new SavaWebApplicationFactory(clock, new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true",
        }, disableMaintenance: true);
        await using var lifetime = factory.ConfigureAwait(false);
        using var sdkTransport = new HttpClientTransport(factory.Server.CreateHandler());
        var container = CreateOwner(sdkTransport).GetBlobContainerClient("expiry-ranges");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("retained.bin");
        await blob.UploadAsync(BinaryData.FromString("unchanged expiry bytes")).ConfigureAwait(true);
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var before = await GetRecordAsync(metadata, blob).ConfigureAwait(true);
        var milliseconds = overflow ? long.MaxValue :
            (DateTimeOffset.MaxValue.UtcTicks - clock.GetUtcNow().UtcTicks) / TimeSpan.TicksPerMillisecond;
        using var client = new HttpClient(factory.Server.CreateHandler());
        using var request = ExpiryRequest(blob, option, creation, milliseconds);
        using var response = await client.SendAsync(request).ConfigureAwait(true);
        Assert.Equal(overflow ? HttpStatusCode.BadRequest : creation ? HttpStatusCode.Created : HttpStatusCode.OK, response.StatusCode);
        var after = await GetRecordAsync(metadata, blob).ConfigureAwait(true);
        if (overflow)
        {
            Assert.Equal("InvalidHeaderValue", Assert.Single(response.Headers.GetValues("x-ms-error-code")));
            Assert.Equal(JsonSerializer.Serialize(before), JsonSerializer.Serialize(after));
            Assert.Equal("unchanged expiry bytes", (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        }
        else
        {
            Assert.Equal(clock.GetUtcNow().AddMilliseconds(milliseconds), after.ExpiresAt);
        }
    }

    private static HttpRequestMessage ExpiryRequest(BlobClient blob, string option, bool creation, long milliseconds)
    {
        var target = blob.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5));
        var request = new HttpRequestMessage(HttpMethod.Put, creation ? target : new Uri(target + "&comp=expiry"))
        {
            Content = new ByteArrayContent(creation ? "new expiry bytes"u8.ToArray() : []),
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
        if (creation)
            request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
        request.Headers.TryAddWithoutValidation("x-ms-expiry-option", option);
        request.Headers.TryAddWithoutValidation("x-ms-expiry-time", milliseconds.ToString(CultureInfo.InvariantCulture));
        return request;
    }

    [Theory]
    [InlineData(0, false)]
    [InlineData(1, false)]
    [InlineData(0, true)]
    [InlineData(1, true)]
    public async Task ArchiveContentAdmissionIncludesEmptyGetButNotProperties(int length, bool archive)
    {
        var factory = CreateFactory();
        await using var lifetime = factory.ConfigureAwait(false);
        using var sdkTransport = new HttpClientTransport(factory.Server.CreateHandler());
        var container = CreateOwner(sdkTransport).GetBlobContainerClient("archive-content-admission");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("content.bin");
        await blob.UploadAsync(BinaryData.FromBytes(new byte[length])).ConfigureAwait(true);
        if (archive)
            await blob.SetAccessTierAsync(AccessTier.Archive).ConfigureAwait(true);
        var properties = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(length, properties.ContentLength);
        Assert.Equal(archive ? AccessTier.Archive : AccessTier.Hot, properties.AccessTier);
        using var client = new HttpClient(factory.Server.CreateHandler());
        using var response = await client.GetAsync(blob.GenerateSasUri(BlobSasPermissions.Read,
            DateTimeOffset.UtcNow.AddMinutes(5))).ConfigureAwait(true);
        Assert.Equal(archive ? HttpStatusCode.Conflict : HttpStatusCode.OK, response.StatusCode);
        if (archive)
            Assert.Equal("BlobArchived", Assert.Single(response.Headers.GetValues("x-ms-error-code")));
        else
            Assert.Equal(new byte[length], await response.Content.ReadAsByteArrayAsync().ConfigureAwait(true));
        Assert.Equal(properties.ETag, (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value.ETag);
    }

    private static SavaWebApplicationFactory CreateFactory() =>
        new(TimeProvider.System, new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);

    private static BlobServiceClient CreateOwner(HttpClientTransport transport) =>
        new(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });

    private static async Task<BlobRecord> GetRecordAsync(MetadataStore metadata, BlobClient blob) =>
        Assert.IsType<BlobRecord>(await metadata.GetBlobAsync(SavaWebApplicationFactory.AccountName,
            blob.BlobContainerName, blob.Name, versionId: null, snapshot: null, includeDeleted: false,
            CancellationToken.None).ConfigureAwait(false));

    private sealed class FixedClock(DateTimeOffset value) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => value;
    }
}
