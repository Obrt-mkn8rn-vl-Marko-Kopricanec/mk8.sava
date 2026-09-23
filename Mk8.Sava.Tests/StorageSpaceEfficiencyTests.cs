using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageSpaceEfficiencyTests
{
    [Fact]
    public async Task PackedSmallChunksDoNotAllocateUnusedHashDirectories()
    {
        await using var application = new SavaWebApplicationFactory(new Dictionary<string, string?>
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });
        await application.InitializeAsync();
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
        var container = client.GetBlobContainerClient($"packed-directories-{Guid.NewGuid():N}");
        await container.CreateAsync();

        for (var index = 0; index < 32; index++)
        {
            var bytes = new byte[80];
            new Random(0x6100 + index).NextBytes(bytes);
            await container.GetBlobClient($"small-{index}.bin").UploadAsync(BinaryData.FromBytes(bytes));
        }

        var metadata = application.Services.GetRequiredService<MetadataStore>();
        Assert.Equal(32, metadata.CountPackedChunks());
        Assert.Empty(Directory.EnumerateDirectories(
            Path.Combine(application.DataPath, "chunks"),
            "*",
            SearchOption.AllDirectories));
        Assert.NotEmpty(Directory.EnumerateFiles(
            Path.Combine(application.DataPath, "packs"),
            "*.pack",
            SearchOption.AllDirectories));
    }

    [Fact]
    public async Task ContentDefinedChunksRemainSharedAfterAnEarlyInsertion()
    {
        await using var application = new SavaWebApplicationFactory(new Dictionary<string, string?>
        {
            ["Sava:EnableSmallChunkPacking"] = "false",
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });
        await application.InitializeAsync();
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
        var container = client.GetBlobContainerClient($"shifted-{Guid.NewGuid():N}");
        await container.CreateAsync();

        var original = new byte[512 * 1024];
        new Random(0x5A8A).NextBytes(original);
        var inserted = new byte[3 * 1024];
        new Random(0x5A8B).NextBytes(inserted);
        var shifted = new byte[original.Length + inserted.Length];
        original.AsSpan(0, 2048).CopyTo(shifted);
        inserted.CopyTo(shifted, 2048);
        original.AsSpan(2048).CopyTo(shifted.AsSpan(2048 + inserted.Length));

        await container.GetBlobClient("original.bin").UploadAsync(BinaryData.FromBytes(original));
        var metadata = application.Services.GetRequiredService<MetadataStore>();
        var inventoryBefore = await metadata.GetStorageInventoryAsync(CancellationToken.None);
        await container.GetBlobClient("shifted.bin").UploadAsync(BinaryData.FromBytes(shifted));
        var inventoryAfter = await metadata.GetStorageInventoryAsync(CancellationToken.None);

        var blobService = application.Services.GetRequiredService<BlobService>();
        var first = await blobService.GetBlobAsync(
            account, container.Name, "original.bin", null, null, false, CancellationToken.None);
        var second = await blobService.GetBlobAsync(
            account, container.Name, "shifted.bin", null, null, false, CancellationToken.None);
        var originalChunks = first.Content.Chunks.Select(chunk => chunk.Id).ToHashSet(StringComparer.Ordinal);
        var shiftedChunks = second.Content.Chunks.Select(chunk => chunk.Id).ToHashSet(StringComparer.Ordinal);
        var shared = originalChunks.Intersect(shiftedChunks, StringComparer.Ordinal).Count();
        Assert.True(originalChunks.Count >= 20);
        Assert.True(shared >= originalChunks.Count / 2,
            $"Only {shared} of {originalChunks.Count} original chunk identities were reused after an early insertion.");
        Assert.True(
            inventoryAfter.ReachableChunkIds.Count - inventoryBefore.ReachableChunkIds.Count <= originalChunks.Count / 2,
            "An early insertion caused too many new physical chunk identities.");
        Assert.Equal(original, (await container.GetBlobClient("original.bin").DownloadContentAsync())
            .Value.Content.ToArray());
        Assert.Equal(shifted, (await container.GetBlobClient("shifted.bin").DownloadContentAsync())
            .Value.Content.ToArray());
    }
}
