using System.Security.Cryptography;
using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageEnospcHarnessTests
{
    private const string DataPathVariable = "MK8_SAVA_ENOSPC_DATA_PATH";

    [Fact]
    public async Task ExhaustedFilesystemDoesNotPublishPartialUploadOrLoseEarlierBlob()
    {
        var dataPath = Environment.GetEnvironmentVariable(DataPathVariable);
        if (string.IsNullOrWhiteSpace(dataPath))
            return;

        dataPath = Path.GetFullPath(dataPath);
        var mountRoot = Path.GetDirectoryName(dataPath)
                        ?? throw new InvalidOperationException("The ENOSPC data path has no mount root.");
        if (Path.GetFileName(dataPath) != "data" ||
            !Path.GetFileName(mountRoot).StartsWith("mk8-sava-enospc-", StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The ENOSPC test requires an isolated mk8-sava-enospc-* mount.");
        }

        var stableBytes = RandomNumberGenerator.GetBytes(32 * 1024);
        var attemptedBytes = RandomNumberGenerator.GetBytes(512 * 1024);
        const string containerName = "enospc-harness";
        var configuration = new Dictionary<string, string?>
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00",
            ["Sava:EnableSmallChunkPacking"] = "false"
        };
        var fillerPath = Path.Combine(mountRoot, "filler.bin");

        var first = new SavaWebApplicationFactory(
            dataPath,
            new NullStorageFaultInjector(),
            analyticsSink: null,
            configurationOverrides: configuration,
            deleteDataPath: false,
            disableMaintenance: true);
        try
        {
            await first.InitializeAsync();
            var container = CreateClient(first).GetBlobContainerClient(containerName);
            await container.CreateAsync();
            var stable = container.GetBlobClient("stable.bin");
            var attempted = container.GetBlobClient("interrupted.bin");
            await stable.UploadAsync(BinaryData.FromBytes(stableBytes));
            var priorChunkCount = CountStandaloneChunks(dataPath);

            FillUntilNoSpace(fillerPath);
            var failure = await Assert.ThrowsAsync<RequestFailedException>(() =>
                attempted.UploadAsync(BinaryData.FromBytes(attemptedBytes)));
            Assert.Equal(500, failure.Status);
            Assert.True(CountStandaloneChunks(dataPath) > priorChunkCount);
            File.Delete(fillerPath);

            Assert.False((await attempted.ExistsAsync()).Value);
            Assert.Equal(stableBytes, (await stable.DownloadContentAsync()).Value.Content.ToArray());
        }
        finally
        {
            await first.DisposeAsync();
        }

        var restarted = new SavaWebApplicationFactory(
            dataPath,
            new NullStorageFaultInjector(),
            analyticsSink: null,
            configurationOverrides: configuration,
            deleteDataPath: false,
            disableMaintenance: true);
        try
        {
            await restarted.InitializeAsync();
            var container = CreateClient(restarted).GetBlobContainerClient(containerName);
            Assert.Equal(stableBytes,
                (await container.GetBlobClient("stable.bin").DownloadContentAsync()).Value.Content.ToArray());
            var attempted = container.GetBlobClient("interrupted.bin");
            Assert.False((await attempted.ExistsAsync()).Value);
            Assert.True(await restarted.Services.GetRequiredService<BlobService>()
                .CollectGarbageAsync(CancellationToken.None) > 0);
            await attempted.UploadAsync(BinaryData.FromBytes(attemptedBytes));
            Assert.Equal(attemptedBytes, (await attempted.DownloadContentAsync()).Value.Content.ToArray());
        }
        finally
        {
            await restarted.DisposeAsync();
        }
    }

    private static void FillUntilNoSpace(string path)
    {
        var bytes = new byte[1024 * 1024];
        IOException? noSpace = null;
        using (var filler = new FileStream(
                   path,
                   FileMode.CreateNew,
                   FileAccess.ReadWrite,
                   FileShare.None,
                   bufferSize: 4096,
                   FileOptions.WriteThrough))
        {
            while (noSpace is null)
            {
                try
                {
                    filler.Write(bytes);
                }
                catch (IOException exception)
                {
                    noSpace = exception;
                }
            }
        }

        Assert.Contains("No space left on device", noSpace.Message, StringComparison.OrdinalIgnoreCase);
        using var release = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        Assert.True(release.Length >= 8 * 1024 * 1024);
        release.SetLength(release.Length - 256 * 1024);
        release.Flush(flushToDisk: true);
    }

    private static int CountStandaloneChunks(string dataPath) =>
        Directory.EnumerateFiles(Path.Combine(dataPath, "chunks"), "*.chunk", SearchOption.AllDirectories).Count();

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory application)
    {
        var endpoint = new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost");
        return new BlobServiceClient(
            endpoint,
            new StorageSharedKeyCredential(
                SavaWebApplicationFactory.AccountName,
                SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions
            {
                Transport = new HttpClientTransport(new HttpClient(application.Server.CreateHandler())
                {
                    BaseAddress = endpoint
                }),
                Retry = { MaxRetries = 0 }
            });
    }
}
