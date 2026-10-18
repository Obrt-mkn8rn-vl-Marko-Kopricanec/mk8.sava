using System.Text.Json;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class DataAccessRevisionTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task AccessTrackingCannotSubstituteAChangedSource(bool smartTier, bool overwrite)
    {
        var clock = new AccessClock(DateTimeOffset.UtcNow);
        var configuration = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:LastAccessTimeTrackingEnabled"] = "true",
        };
        var factory = new SavaWebApplicationFactory(clock, configuration, disableMaintenance: true);
        await using var lifetime = factory.ConfigureAwait(false);
        using var transport = new HttpClientTransport(factory.Server.CreateHandler());
        var owner = new BlobServiceClient(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });
        var container = owner.GetBlobContainerClient("access-revision");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("source.bin");
        var options = new BlobUploadOptions { AccessTier = smartTier ? new AccessTier("Smart") : AccessTier.Hot };
        await blob.UploadAsync(BinaryData.FromString("authorized content"), options).ConfigureAwait(true);
        var service = factory.Services.GetRequiredService<BlobService>();
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var captured = await GetRecordAsync(metadata).ConfigureAwait(true);
        clock.Advance(TimeSpan.FromDays(1));
        if (overwrite)
            await blob.UploadAsync(BinaryData.FromString("replacement content"), options).ConfigureAwait(true);
        else
            await blob.SetTagsAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["admission"] = "changed" }).ConfigureAwait(true);
        var changed = await GetRecordAsync(metadata).ConfigureAwait(true);

        var error = await Assert.ThrowsAsync<StorageConcurrencyException>(() =>
            service.RecordDataAccessAsync(captured, CancellationToken.None)).ConfigureAwait(true);

        Assert.Equal(412, StorageExceptionMapper.Map(error).StatusCode);
        Assert.Equal(JsonSerializer.Serialize(changed), JsonSerializer.Serialize(await GetRecordAsync(metadata).ConfigureAwait(true)));
        var fresh = await service.RecordDataAccessAsync(changed, CancellationToken.None).ConfigureAwait(true);
        Assert.Equal(changed.GenerationId, fresh.GenerationId);
        Assert.Equal(changed.ETag, fresh.ETag);
        Assert.Equal(changed.Content.Sha256, fresh.Content.Sha256);
        Assert.Equal(clock.GetUtcNow(), fresh.LastAccessedAt);
        Assert.Equal(overwrite ? "replacement content" : "authorized content",
            (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    private static async Task<BlobRecord> GetRecordAsync(MetadataStore metadata) =>
        Assert.IsType<BlobRecord>(await metadata.GetBlobAsync(SavaWebApplicationFactory.AccountName,
            "access-revision", "source.bin", versionId: null, snapshot: null, includeDeleted: false,
            CancellationToken.None).ConfigureAwait(false));

    private sealed class AccessClock(DateTimeOffset now) : TimeProvider
    {
        private long _ticks = now.UtcTicks;
        public override DateTimeOffset GetUtcNow() => new(Interlocked.Read(ref _ticks), TimeSpan.Zero);
        internal void Advance(TimeSpan amount) => Interlocked.Add(ref _ticks, amount.Ticks);
    }
}
