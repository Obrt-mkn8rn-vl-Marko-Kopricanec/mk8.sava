using System.Security.Cryptography;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageUsageSamplingTests
{
    [Fact]
    public async Task PhysicalInventoryRespectsPerPassBudgetAndPublishesOnlyCompleteSamples()
    {
        var application = new SavaWebApplicationFactory(
            Path.Combine(Path.GetTempPath(), $"mk8-sava-usage-budget-{Guid.NewGuid():N}"),
            new NullStorageFaultInjector(),
            analyticsSink: null,
            configurationOverrides: new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sava:MaintenanceScanInterval"] = "01:00:00",
                ["Sava:PhysicalUsageScanInterval"] = "01:00:00",
                ["Sava:PhysicalUsageEntriesPerMaintenancePass"] = "2"
            },
            deleteDataPath: true,
            disableMaintenance: true);
        try
        {
            await application.InitializeAsync();
            var fixturePath = Path.Combine(application.DataPath, "inventory-fixture");
            Directory.CreateDirectory(fixturePath);
            for (var index = 0; index < 20; index++)
                await File.WriteAllBytesAsync(Path.Combine(fixturePath, $"{index:D3}.bin"), new byte[4096]);

            var service = application.Services.GetRequiredService<BlobService>();
            var chunks = application.Services.GetRequiredService<ChunkStore>();
            var telemetry = application.Services.GetRequiredService<StorageTelemetry>();
            await service.RunMaintenanceAsync(CancellationToken.None);
            Assert.True(chunks.IsPhysicalUsageScanInProgress);
            Assert.InRange(chunks.PhysicalUsageScanStepsLastPass, 1, 2);
            Assert.Equal(0, telemetry.Usage.PhysicalScanUnixSeconds);
            Assert.Equal(0, telemetry.Usage.PhysicalChunkBytes);

            for (var pass = 0; pass < 100 && chunks.IsPhysicalUsageScanInProgress; pass++)
            {
                await service.RunMaintenanceAsync(CancellationToken.None);
                Assert.InRange(chunks.PhysicalUsageScanStepsLastPass, 1, 2);
            }

            Assert.False(chunks.IsPhysicalUsageScanInProgress);
            Assert.True(telemetry.Usage.PhysicalScanUnixSeconds > 0);
            Assert.Equal(chunks.MeasurePhysicalUsage().ChunkBytes, telemetry.Usage.PhysicalChunkBytes);
            if (OperatingSystem.IsLinux())
                Assert.True(telemetry.Usage.AllocatedRootBytes >= 20 * 4096);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Theory]
    [InlineData("01:00:00", false)]
    [InlineData("00:00:00.001", true)]
    public async Task PhysicalInventoryRunsOnItsOwnCadenceWhileLogicalInventoryStaysCurrent(
        string scanInterval,
        bool expectRefresh)
    {
        var application = new SavaWebApplicationFactory(
            Path.Combine(Path.GetTempPath(), $"mk8-sava-usage-sample-{Guid.NewGuid():N}"),
            new NullStorageFaultInjector(),
            analyticsSink: null,
            configurationOverrides: new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sava:MaintenanceScanInterval"] = "01:00:00",
                ["Sava:PhysicalUsageScanInterval"] = scanInterval,
                ["Sava:EnableSmallChunkPacking"] = "false"
            },
            deleteDataPath: true,
            disableMaintenance: true);
        try
        {
            await application.InitializeAsync();
            var service = application.Services.GetRequiredService<BlobService>();
            var telemetry = application.Services.GetRequiredService<StorageTelemetry>();
            await service.RunMaintenanceAsync(CancellationToken.None);
            var before = telemetry.Usage;
            Assert.Equal(0, before.PhysicalChunkBytes);
            Assert.True(before.PhysicalScanUnixSeconds > 0);

            var account = SavaWebApplicationFactory.AccountName;
            var endpoint = new Uri($"http://{account}.localhost");
            var client = new BlobServiceClient(
                endpoint,
                new StorageSharedKeyCredential(account, SavaWebApplicationFactory.AccountKey),
                new BlobClientOptions
                {
                    Transport = new HttpClientTransport(new HttpClient(application.Server.CreateHandler())
                    {
                        BaseAddress = endpoint
                    }),
                    Retry = { MaxRetries = 0 }
                });
            var container = client.GetBlobContainerClient($"usage-sample-{Guid.NewGuid():N}");
            await container.CreateAsync();
            var content = RandomNumberGenerator.GetBytes(8192);
            await container.GetBlobClient("stored.bin").UploadAsync(BinaryData.FromBytes(content));

            if (expectRefresh)
                await Task.Delay(TimeSpan.FromMilliseconds(20));
            await service.RunMaintenanceAsync(CancellationToken.None);
            var after = telemetry.Usage;
            Assert.Equal(content.Length, after.LogicalBlobBytes);
            Assert.True(after.ReachableChunkCount > 0);
            Assert.Equal(expectRefresh, after.PhysicalChunkBytes > 0);
            Assert.Equal(expectRefresh, after.UniqueChunkCount > 0);
            if (expectRefresh)
                Assert.True(after.PhysicalScanUnixSeconds >= before.PhysicalScanUnixSeconds);
            else
                Assert.Equal(before.PhysicalScanUnixSeconds, after.PhysicalScanUnixSeconds);
            Assert.Contains(
                $"mk8_sava_storage_physical_last_scan_timestamp_seconds {after.PhysicalScanUnixSeconds}",
                telemetry.RenderPrometheus(),
                StringComparison.Ordinal);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }
}
