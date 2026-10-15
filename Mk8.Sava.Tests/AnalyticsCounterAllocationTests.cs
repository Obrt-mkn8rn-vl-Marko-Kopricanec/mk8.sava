using System.Globalization;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class AnalyticsCounterAllocationTests
{
    private const string Account = SavaWebApplicationFactory.AccountName;
    private const string Prefix = "blob/2026/10/15/1600/";

    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    public async Task HourlyAllocationCrossesSixDigitRolloverWithoutLosingRecords(int writers)
    {
        var factory = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);
        await using var lifetime = factory.ConfigureAwait(false);
        var analytics = factory.Services.GetRequiredService<StorageAnalyticsService>();
        var service = factory.Services.GetRequiredService<BlobService>();
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        await analytics.EnsureContainerAsync(Account, CancellationToken.None).ConfigureAwait(true);
        await SeedAsync(service, "999999").ConfigureAwait(true);
        await SeedAsync(service, "1000000").ConfigureAwait(true);
        var properties = await metadata.GetServicePropertiesAsync(Account, CancellationToken.None).ConfigureAwait(true);
        await metadata.PutServicePropertiesAsync(Account, properties with
        {
            Logging = new StorageAnalyticsLogging { Write = true },
        }, CancellationToken.None).ConfigureAwait(true);
        var requests = Enumerable.Range(0, writers).Select(CreateRequest).ToArray();
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // Every actual producer is registered while waiting on the shared gate.
        // There is no observation timeout that could abandon these operations.
        var operations = requests.Select(async request =>
        {
            await release.Task.WaitAsync(cancellation.Token).ConfigureAwait(false);
            await analytics.RecordAsync(request, cancellation.Token).ConfigureAwait(false);
        }).ToArray();
        release.SetResult();
        await Task.WhenAll(operations).ConfigureAwait(true);
        var records = await metadata.ListBlobsAsync(Account, StorageAnalyticsService.LogsContainerName,
            includeVersions: false, includeSnapshots: false, includeDeleted: false, CancellationToken.None).ConfigureAwait(true);
        Assert.Equal(writers + 2, records.Count);
        foreach (var suffix in new[] { "999999", "1000000" })
        {
            var retained = Assert.Single(records, record => string.Equals(record.Name, Prefix + suffix + ".log", StringComparison.Ordinal));
            await AssertContentAsync(factory, retained, Encoding.UTF8.GetBytes("sentinel " + suffix)).ConfigureAwait(true);
        }
        for (var index = 0; index < writers; index++)
        {
            Assert.Contains(records, record => string.Equals(record.Name,
                Prefix + (1000001 + index).ToString("D6", CultureInfo.InvariantCulture) + ".log", StringComparison.Ordinal));
        }
        var published = records.Where(record => !record.Name.EndsWith("/999999.log", StringComparison.Ordinal) &&
                                                !record.Name.EndsWith("/1000000.log", StringComparison.Ordinal)).ToArray();
        foreach (var request in requests)
        {
            var expected = Encoding.UTF8.GetBytes(StorageAnalyticsService.FormatRecord("1.0", request));
            var record = Assert.Single(published, item => string.Equals(item.Content.Sha256,
                Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(expected)), StringComparison.Ordinal));
            await AssertContentAsync(factory, record, expected).ConfigureAwait(true);
        }
    }

    private static async Task SeedAsync(BlobService service, string suffix)
    {
        var body = new MemoryStream(Encoding.UTF8.GetBytes("sentinel " + suffix), writable: false);
        await using var bodyLifetime = body.ConfigureAwait(false);
        _ = await service.PutBlockBlobAsync(Account, StorageAnalyticsService.LogsContainerName, Prefix + suffix + ".log",
            body, new BlobWriteOptions(new BlobHttpProperties(), new Dictionary<string, string>(StringComparer.Ordinal)),
            LeaseRecord.Available, expectedGeneration: null, expectedRevision: null, CancellationToken.None).ConfigureAwait(false);
    }

    private static async Task AssertContentAsync(SavaWebApplicationFactory factory, BlobRecord record, byte[] expected)
    {
        var destination = new MemoryStream();
        await using var destinationLifetime = destination.ConfigureAwait(false);
        await factory.Services.GetRequiredService<ChunkStore>().WriteRangeAsync(record.Content,
            new BlobEncryption(Scope: null, CustomerProvidedKeySha256: null), 0, record.Content.Length,
            destination, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(expected, destination.ToArray());
    }

    private static StorageAnalyticsRequest CreateRequest(int index) => new()
    {
        Account = Account,
        StartedAt = new DateTimeOffset(2026, 10, 15, 16, 0, 0, TimeSpan.Zero),
        CompletedAt = new DateTimeOffset(2026, 10, 15, 16, 0, 1, TimeSpan.Zero),
        Operation = "PutBlob",
        Category = StorageAnalyticsOperationCategory.Write,
        RequestStatus = "Success",
        StatusCode = 201,
        EndToEndLatencyMilliseconds = 1,
        ServerLatencyMilliseconds = 1,
        AuthenticationType = "sas",
        RequestUrl = "https://account.example.test/container/blob",
        RequestedObjectKey = "/account/container/blob",
        RequestId = "rollover-" + index.ToString(CultureInfo.InvariantCulture),
        RequestVersion = "2026-06-06",
        RequestHeaderSize = 0,
        RequestPacketSize = 0,
        ResponseHeaderSize = 0,
        ResponsePacketSize = 0,
        RequestContentLength = 0,
    };
}
