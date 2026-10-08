using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;
using System.Net;

namespace Mk8.Sava.Tests;

public sealed partial class AzureSdkCompatibilityTests
{
    private static SavaWebApplicationFactory CreateArchiveClockApplication(AdjustableTimeProvider clock) =>
        new(clock, new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            // Clock advancement is explicit; maintenance may observe the same
            // clock but cannot move a rehydration deadline on its own.
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });

    private static void AssertArchiveClockConfiguration(
        SavaWebApplicationFactory application, AdjustableTimeProvider clock)
    {
        var options = application.Services.GetRequiredService<IOptions<SavaOptions>>().Value;
        Assert.Equal(TimeSpan.FromSeconds(5), options.StandardRehydrationDelay);
        Assert.Equal(TimeSpan.FromMilliseconds(200), options.HighPriorityRehydrationDelay);
        Assert.Equal(clock.GetUtcNow(), application.Services.GetRequiredService<MetadataStore>().GetUtcNow());
    }

    [Theory]
    [InlineData("2019-12-12", "Standard", 5000)]
    [InlineData("2020-06-12", "High", 200)]
    public async Task ArchivePriorityVersionControlsPendingDeadline(
        string serviceVersion, string effectivePriority, int completionMilliseconds)
    {
        var clock = new AdjustableTimeProvider(DateTimeOffset.UtcNow);
        await using var application = CreateArchiveClockApplication(clock);
        await application.InitializeAsync().ConfigureAwait(true);
        AssertArchiveClockConfiguration(application, clock);
        await AssertArchivePriorityVersionAsync(application, clock, serviceVersion,
            effectivePriority, completionMilliseconds).ConfigureAwait(true);
    }

    private static async Task AssertArchivePriorityVersionAsync(
        SavaWebApplicationFactory application, AdjustableTimeProvider clock,
        string serviceVersion, string effectivePriority, int completionMilliseconds)
    {
        var container = CreateClient(application).GetBlobContainerClient($"archive-version-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("pending.bin");
        var bytes = "priority version boundary"u8.ToArray();
        await blob.UploadAsync(BinaryData.FromBytes(bytes)).ConfigureAwait(false);
        await blob.SetAccessTierAsync(AccessTier.Archive).ConfigureAwait(false);
        Assert.Equal(202, (await blob.SetAccessTierAsync(
            AccessTier.Hot, rehydratePriority: RehydratePriority.Standard).ConfigureAwait(false)).Status);

        using var transport = new HttpClient(application.Server.CreateHandler());
        await SetArchivePriorityRestAsync(transport, blob, serviceVersion, "High").ConfigureAwait(false);
        await AssertArchiveHeadersAsync(transport, blob, "Archive", effectivePriority).ConfigureAwait(false);
        // Neither a repeated request nor an attempted downgrade may postpone
        // the first admitted deadline. This is observable at the exact boundary.
        clock.Advance(TimeSpan.FromMilliseconds(100));
        await SetArchivePriorityRestAsync(transport, blob, serviceVersion, "Standard").ConfigureAwait(false);
        clock.Advance(TimeSpan.FromMilliseconds(completionMilliseconds - 100) - TimeSpan.FromTicks(1));
        await AssertArchiveHeadersAsync(transport, blob, "Archive", effectivePriority).ConfigureAwait(false);
        clock.Advance(TimeSpan.FromTicks(1));
        await AssertArchiveHeadersAsync(transport, blob, "Hot", priority: null).ConfigureAwait(false);
        Assert.Equal(bytes, (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ArchiveCompletionClearsPriorityWhenObservationFollowsDeadline(bool maintenanceCompletes)
    {
        var clock = new AdjustableTimeProvider(DateTimeOffset.UtcNow);
        await using var application = CreateArchiveClockApplication(clock);
        await application.InitializeAsync().ConfigureAwait(true);
        AssertArchiveClockConfiguration(application, clock);
        var container = CreateClient(application).GetBlobContainerClient($"archive-late-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("late.bin");
        var bytes = "late observation is not pending"u8.ToArray();
        await blob.UploadAsync(BinaryData.FromBytes(bytes)).ConfigureAwait(true);
        await blob.SetAccessTierAsync(AccessTier.Archive).ConfigureAwait(true);
        Assert.Equal(202, (await blob.SetAccessTierAsync(
            AccessTier.Hot, rehydratePriority: RehydratePriority.High).ConfigureAwait(true)).Status);
        using var transport = new HttpClient(application.Server.CreateHandler());
        await AssertArchiveHeadersAsync(transport, blob, "Archive", "High").ConfigureAwait(true);

        clock.Advance(TimeSpan.FromMilliseconds(200));
        if (maintenanceCompletes)
        {
            var result = await application.Services.GetRequiredService<BlobService>()
                .RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(true);
            Assert.Equal(1, result.CompletedRehydrations);
        }
        var online = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(AccessTier.Hot, online.AccessTier);
        Assert.Null(online.ArchiveStatus);
        Assert.Null(online.RehydratePriority);
        await AssertArchiveHeadersAsync(transport, blob, "Hot", priority: null).ConfigureAwait(true);
        Assert.Equal(bytes, (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());
    }

    private static async Task SetArchivePriorityRestAsync(
        HttpClient transport, BlobClient blob, string serviceVersion, string priority)
    {
        var uri = AppendQuery(blob.GenerateSasUri(
            BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)), "comp=tier");
        using var request = new HttpRequestMessage(HttpMethod.Put, uri);
        request.Headers.Add("x-ms-version", serviceVersion);
        request.Headers.Add("x-ms-access-tier", "Hot");
        request.Headers.Add("x-ms-rehydrate-priority", priority);
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    }

    private static async Task AssertArchiveHeadersAsync(
        HttpClient transport, BlobClient blob, string tier, string? priority)
    {
        var uri = blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        using var request = new HttpRequestMessage(HttpMethod.Head, uri);
        request.Headers.Add("x-ms-version", "2020-06-12");
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(tier, response.Headers.GetValues("x-ms-access-tier").Single());
        if (priority is not null)
        {
            Assert.Equal("rehydrate-pending-to-hot", response.Headers.GetValues("x-ms-archive-status").Single());
            Assert.Equal(priority, response.Headers.GetValues("x-ms-rehydrate-priority").Single());
        }
        else
        {
            Assert.False(response.Headers.Contains("x-ms-archive-status"));
            Assert.False(response.Headers.Contains("x-ms-rehydrate-priority"));
        }
    }
}
