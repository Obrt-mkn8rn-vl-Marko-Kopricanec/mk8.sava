using System.Net;
using System.Text;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;

namespace Mk8.Sava.Tests;

public sealed class AzureRehydrationPriorityTests
{
    [Fact]
    public async Task ArchivedCopyPublishesArchiveStateAndSupportsPriorityUpgrade()
    {
        var clock = new AdjustableTimeProvider(new DateTimeOffset(2026, 9, 22, 12, 0, 0, TimeSpan.Zero));
        await using var application = new SavaWebApplicationFactory(
            clock,
            new Dictionary<string, string?>
            {
                ["Sava:AsyncCopyCompletionDelay"] = "00:00:02",
                ["Sava:StandardRehydrationDelay"] = "00:00:05",
                ["Sava:HighPriorityRehydrationDelay"] = "00:00:01",
                ["Sava:MaintenanceScanInterval"] = "1.00:00:00"
            });
        await application.InitializeAsync();
        var service = CreateClient(application);
        var container = service.GetBlobContainerClient($"copy-rehydrate-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var payload = "archived copy payload"u8.ToArray();
        var source = container.GetBlockBlobClient("source.bin");
        await source.UploadAsync(BinaryData.FromBytes(payload).ToStream());
        await source.SetAccessTierAsync(AccessTier.Archive);

        var destination = container.GetBlockBlobClient("destination.bin");
        var copy = await destination.StartCopyFromUriAsync(
            source.Uri,
            new BlobCopyFromUriOptions
            {
                AccessTier = AccessTier.Hot,
                RehydratePriority = RehydratePriority.Standard
            });

        var pendingCopy = (await destination.GetPropertiesAsync()).Value;
        Assert.Equal(CopyStatus.Pending, pendingCopy.CopyStatus);
        Assert.Equal(AccessTier.Archive, pendingCopy.AccessTier);
        Assert.Equal("rehydrate-pending-to-hot", pendingCopy.ArchiveStatus);
        Assert.Equal("Standard", pendingCopy.RehydratePriority);

        clock.Advance(TimeSpan.FromSeconds(2));
        var copiedButOffline = (await destination.GetPropertiesAsync()).Value;
        Assert.Equal(CopyStatus.Success, copiedButOffline.CopyStatus);
        Assert.Equal(AccessTier.Archive, copiedButOffline.AccessTier);
        Assert.Equal("rehydrate-pending-to-hot", copiedButOffline.ArchiveStatus);
        Assert.Equal("Standard", copiedButOffline.RehydratePriority);
        var offline = await Assert.ThrowsAsync<Azure.RequestFailedException>(() =>
            destination.DownloadContentAsync());
        Assert.Equal("BlobArchived", offline.ErrorCode);

        var upgraded = await destination.SetAccessTierAsync(
            AccessTier.Hot,
            rehydratePriority: RehydratePriority.High);
        Assert.Equal(202, upgraded.Status);
        Assert.Equal(
            "High",
            (await destination.GetPropertiesAsync()).Value.RehydratePriority);

        clock.Advance(TimeSpan.FromSeconds(1));
        var online = (await destination.GetPropertiesAsync()).Value;
        Assert.Equal(CopyStatus.Success, online.CopyStatus);
        Assert.Equal(AccessTier.Hot, online.AccessTier);
        Assert.Null(online.ArchiveStatus);
        Assert.Null(online.RehydratePriority);
        Assert.Equal(payload, (await destination.DownloadContentAsync()).Value.Content.ToArray());

        var defaultPriority = container.GetBlockBlobClient("default-priority.bin");
        var defaultCopy = await defaultPriority.StartCopyFromUriAsync(
            source.Uri,
            new BlobCopyFromUriOptions { AccessTier = AccessTier.Cool });
        var defaultPending = (await defaultPriority.GetPropertiesAsync()).Value;
        Assert.Equal(AccessTier.Archive, defaultPending.AccessTier);
        Assert.Equal("rehydrate-pending-to-cool", defaultPending.ArchiveStatus);
        Assert.Equal("Standard", defaultPending.RehydratePriority);

        await defaultPriority.AbortCopyFromUriAsync(defaultCopy.Id);
        var aborted = (await defaultPriority.GetPropertiesAsync()).Value;
        Assert.Equal(CopyStatus.Aborted, aborted.CopyStatus);
        Assert.Equal(AccessTier.Cool, aborted.AccessTier);
        Assert.Null(aborted.ArchiveStatus);
        Assert.Null(aborted.RehydratePriority);
    }

    [Fact]
    public async Task Pre20200612SetTierCannotUpgradeAnExistingPriority()
    {
        var clock = new AdjustableTimeProvider(new DateTimeOffset(2026, 9, 22, 13, 0, 0, TimeSpan.Zero));
        await using var application = new SavaWebApplicationFactory(
            clock,
            new Dictionary<string, string?>
            {
                ["Sava:StandardRehydrationDelay"] = "00:00:05",
                ["Sava:HighPriorityRehydrationDelay"] = "00:00:01",
                ["Sava:MaintenanceScanInterval"] = "1.00:00:00"
            });
        await application.InitializeAsync();
        var service = CreateClient(application);
        var container = service.GetBlobContainerClient($"tier-version-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlockBlobClient("archived.bin");
        await blob.UploadAsync(BinaryData.FromString("versioned tier payload").ToStream());
        await blob.SetAccessTierAsync(AccessTier.Archive);
        using var transport = new HttpClient(application.Server.CreateHandler());
        var tierUri = AppendQuery(
            blob.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddHours(1)),
            "comp=tier");

        using (var start = TierRequest(tierUri, "2019-12-12", "Standard"))
        using (var response = await transport.SendAsync(start))
            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        clock.Advance(TimeSpan.FromSeconds(1));
        using (var ignoredUpgrade = TierRequest(tierUri, "2019-12-12", "High"))
        using (var response = await transport.SendAsync(ignoredUpgrade))
            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        var unchanged = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal(AccessTier.Archive, unchanged.AccessTier);
        Assert.Equal("Standard", unchanged.RehydratePriority);

        clock.Advance(TimeSpan.FromSeconds(4));
        var online = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal(AccessTier.Hot, online.AccessTier);
        Assert.Null(online.RehydratePriority);
    }

    [Fact]
    public async Task RehydratePriorityIsRejectedOutsideSupportedOperationsAndVersions()
    {
        await using var application = new SavaWebApplicationFactory();
        await application.InitializeAsync();
        var service = CreateClient(application);
        var container = service.GetBlobContainerClient($"priority-headers-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var source = container.GetBlockBlobClient("source.bin");
        await source.UploadAsync(BinaryData.FromString("copy source").ToStream());
        var sourceUri = source.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddHours(1));
        using var transport = new HttpClient(application.Server.CreateHandler());

        var putDestination = container.GetBlockBlobClient("put.bin");
        using (var put = new HttpRequestMessage(HttpMethod.Put, WriteUri(putDestination))
        {
            Content = new ByteArrayContent("must not publish"u8.ToArray())
        })
        {
            put.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            put.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            put.Headers.TryAddWithoutValidation("x-ms-rehydrate-priority", "High");
            await AssertErrorAsync(transport, put, HttpStatusCode.BadRequest, "UnsupportedHeader");
        }
        Assert.False((await putDestination.ExistsAsync()).Value);

        var blockListDestination = container.GetBlockBlobClient("block-list.bin");
        await blockListDestination.UploadAsync(BinaryData.FromString("original").ToStream());
        using (var blockList = new HttpRequestMessage(
                   HttpMethod.Put,
                   AppendQuery(WriteUri(blockListDestination), "comp=blocklist"))
        {
            Content = new StringContent("<BlockList />", Encoding.UTF8, "application/xml")
        })
        {
            blockList.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            blockList.Headers.TryAddWithoutValidation("x-ms-rehydrate-priority", "High");
            await AssertErrorAsync(transport, blockList, HttpStatusCode.BadRequest, "UnsupportedHeader");
        }
        Assert.Equal(
            "original",
            (await blockListDestination.DownloadContentAsync()).Value.Content.ToString());

        var putFromUrlDestination = container.GetBlockBlobClient("put-from-url.bin");
        using (var putFromUrl = CopyRequest(
                   putFromUrlDestination,
                   new Uri("https://unreachable.invalid/source"),
                   "2023-11-03",
                   "High"))
        {
            putFromUrl.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            await AssertErrorAsync(transport, putFromUrl, HttpStatusCode.BadRequest, "UnsupportedHeader");
        }
        Assert.False((await putFromUrlDestination.ExistsAsync()).Value);

        var synchronousDestination = container.GetBlockBlobClient("synchronous.bin");
        using (var synchronous = CopyRequest(
                   synchronousDestination,
                   new Uri("https://unreachable.invalid/source"),
                   "2023-11-03",
                   "High"))
        {
            synchronous.Headers.TryAddWithoutValidation("x-ms-requires-sync", "true");
            await AssertErrorAsync(transport, synchronous, HttpStatusCode.BadRequest, "UnsupportedHeader");
        }
        Assert.False((await synchronousDestination.ExistsAsync()).Value);

        var oldVersionDestination = container.GetBlockBlobClient("old-version.bin");
        using (var oldVersion = CopyRequest(
                   oldVersionDestination,
                   sourceUri,
                   "2018-11-09",
                   "High"))
        {
            await AssertErrorAsync(transport, oldVersion, HttpStatusCode.Conflict, "FeatureVersionMismatch");
        }
        Assert.False((await oldVersionDestination.ExistsAsync()).Value);

        var invalidDestination = container.GetBlockBlobClient("invalid.bin");
        using (var invalid = CopyRequest(
                   invalidDestination,
                   sourceUri,
                   "2023-11-03",
                   "Urgent"))
        {
            await AssertErrorAsync(transport, invalid, HttpStatusCode.BadRequest, "InvalidHeaderValue");
        }
        Assert.False((await invalidDestination.ExistsAsync()).Value);

        var onlineCopyDestination = container.GetBlockBlobClient("online-copy.bin");
        using (var onlineCopy = CopyRequest(
                   onlineCopyDestination,
                   sourceUri,
                   "2023-11-03",
                   "High"))
        using (var response = await transport.SendAsync(onlineCopy))
            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Null((await onlineCopyDestination.GetPropertiesAsync()).Value.RehydratePriority);
    }

    private static HttpRequestMessage TierRequest(Uri uri, string version, string priority)
    {
        var request = new HttpRequestMessage(HttpMethod.Put, uri)
        {
            Content = new ByteArrayContent([])
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        request.Headers.TryAddWithoutValidation("x-ms-access-tier", "Hot");
        request.Headers.TryAddWithoutValidation("x-ms-rehydrate-priority", priority);
        return request;
    }

    private static HttpRequestMessage CopyRequest(
        BlockBlobClient destination,
        Uri source,
        string version,
        string priority)
    {
        var request = new HttpRequestMessage(HttpMethod.Put, WriteUri(destination))
        {
            Content = new ByteArrayContent([])
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        request.Headers.TryAddWithoutValidation("x-ms-copy-source", source.AbsoluteUri);
        request.Headers.TryAddWithoutValidation("x-ms-rehydrate-priority", priority);
        return request;
    }

    private static async Task AssertErrorAsync(
        HttpClient transport,
        HttpRequestMessage request,
        HttpStatusCode status,
        string errorCode)
    {
        using var response = await transport.SendAsync(request);
        Assert.Equal(status, response.StatusCode);
        Assert.Equal(errorCode, response.Headers.GetValues("x-ms-error-code").Single());
    }

    private static Uri WriteUri(BlobBaseClient blob) => blob.GenerateSasUri(
        BlobSasPermissions.All,
        DateTimeOffset.UtcNow.AddHours(1));

    private static Uri AppendQuery(Uri uri, string query)
    {
        var builder = new UriBuilder(uri)
        {
            Query = string.IsNullOrEmpty(uri.Query)
                ? query
                : uri.Query.TrimStart('?') + "&" + query
        };
        return builder.Uri;
    }

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory application)
    {
        var transportClient = new HttpClient(application.Server.CreateHandler())
        {
            BaseAddress = new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost")
        };
        return new BlobServiceClient(
            transportClient.BaseAddress,
            new StorageSharedKeyCredential(
                SavaWebApplicationFactory.AccountName,
                SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions
            {
                Transport = new HttpClientTransport(transportClient),
                Retry = { MaxRetries = 0 }
            });
    }

    private sealed class AdjustableTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private long _utcTicks = utcNow.UtcDateTime.Ticks;

        public override DateTimeOffset GetUtcNow() =>
            new(Interlocked.Read(ref _utcTicks), TimeSpan.Zero);

        public void Advance(TimeSpan value) =>
            Interlocked.Add(ref _utcTicks, value.Ticks);
    }
}
