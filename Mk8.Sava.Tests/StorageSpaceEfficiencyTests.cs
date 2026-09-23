using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageSpaceEfficiencyTests
{
    [Fact]
    public async Task LegacyMsavaMarkerCannotProveEqualityOrChangeClientBytes()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });
        await using var applicationDisposal1 = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var container = CreateClient(application)
            .GetBlobContainerClient($"msava-marker-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var originalBytes = RandomNumberGenerator.GetBytes(32 * 1024);
        var declaredHash = Convert.ToHexStringLower(SHA256.HashData(originalBytes));
        var marker = $"MSAVA:v1:{declaredHash}:42:1760000000";
        var markerBytes = Encoding.ASCII.GetBytes(marker);
        var markedBytes = new byte[originalBytes.Length + markerBytes.Length];
        originalBytes.CopyTo(markedBytes, 0);
        markerBytes.CopyTo(markedBytes, originalBytes.Length);

        var original = container.GetBlobClient("original.bin");
        var marked = container.GetBlobClient("marked.bin");
        await original.UploadAsync(BinaryData.FromBytes(originalBytes));
        await marked.UploadAsync(BinaryData.FromBytes(markedBytes), new BlobUploadOptions
        {
            Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["MsavaSignature"] = marker }
        });

        Assert.Equal(originalBytes, (await original.DownloadContentAsync()).Value.Content.ToArray());
        Assert.Equal(markedBytes, (await marked.DownloadContentAsync()).Value.Content.ToArray());
        Assert.Equal(marker, (await marked.GetPropertiesAsync()).Value.Metadata["MsavaSignature"]);
        var service = application.Services.GetRequiredService<BlobService>();
        var originalRecord = await service.GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            container.Name,
            original.Name,
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None);
        var markedRecord = await service.GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            container.Name,
            marked.Name,
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None);
        Assert.NotEqual(originalRecord.Content.Sha256, markedRecord.Content.Sha256, StringComparer.Ordinal);
        Assert.Equal(markedBytes.Length, markedRecord.Content.Length);
    }

    [Fact]
    public async Task PhysicalUsageInventoryDoesNotFollowExternalOrCyclicStorageLinks()
    {
        if (!OperatingSystem.IsLinux())
            return;

        var external = Path.Combine(Path.GetTempPath(), $"mk8-sava-usage-external-{Guid.NewGuid():N}");
        Directory.CreateDirectory(external);
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:EnableSmallChunkPacking"] = "false",
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });
        await using var applicationDisposal2 = application.ConfigureAwait(false);
        string? chunkLink = null;
        string? packLink = null;
        string? cycleLink = null;
        try
        {
            await application.InitializeAsync();
            var chunks = application.Services.GetRequiredService<ChunkStore>();
            var content = new byte[80];
            DeterministicTestBytes.Fill(0x7600, content);
            using var pinned = await chunks.StorePinnedAsync(
                SavaWebApplicationFactory.AccountName,
                new BlobEncryption(Scope: null, CustomerProvidedKeySha256: null),
                new MemoryStream(content, writable: false),
                CancellationToken.None);
            var before = chunks.MeasurePhysicalUsage();
            Assert.True(before.ChunkBytes > 0);
            Assert.Equal(1, before.ChunkCount);
            await File.WriteAllBytesAsync(Path.Combine(external, "foreign.chunk"), new byte[8192]);
            await File.WriteAllBytesAsync(Path.Combine(external, "foreign.pack"), new byte[8192]);
            chunkLink = Path.Combine(application.DataPath, "chunks", "outside");
            packLink = Path.Combine(application.DataPath, "packs", "outside");
            cycleLink = Path.Combine(application.DataPath, "chunks", "cycle");
            Directory.CreateSymbolicLink(chunkLink, external);
            Directory.CreateSymbolicLink(packLink, external);
            Directory.CreateSymbolicLink(cycleLink, Path.Combine(application.DataPath, "chunks"));

            var after = chunks.MeasurePhysicalUsage();
            Assert.Equal(before.ChunkBytes, after.ChunkBytes);
            Assert.Equal(before.ChunkCount, after.ChunkCount);

            StoragePhysicalUsage? budgeted = null;
            for (var pass = 0; pass < 100 && budgeted is null; pass++)
            {
                budgeted = chunks.ScanPhysicalUsageBatch(2);
                Assert.InRange(chunks.PhysicalUsageScanStepsLastPass, 1, 2);
            }
            Assert.NotNull(budgeted);
            Assert.Equal(after.ChunkBytes, budgeted.ChunkBytes);
            Assert.Equal(after.ChunkCount, budgeted.ChunkCount);
        }
        finally
        {
            if (chunkLink is not null && Directory.Exists(chunkLink))
                Directory.Delete(chunkLink);
            if (packLink is not null && Directory.Exists(packLink))
                Directory.Delete(packLink);
            if (cycleLink is not null && Directory.Exists(cycleLink))
                Directory.Delete(cycleLink);
            if (Directory.Exists(external))
                Directory.Delete(external, recursive: true);
        }
    }

    [Fact]
    public async Task LinuxAllocationMeterMatchesFilesystemBlocksWithoutFollowingExternalLinks()
    {
        if (!OperatingSystem.IsLinux())
            return;

        var root = Path.Combine(Path.GetTempPath(), $"mk8-sava-allocated-{Guid.NewGuid():N}");
        var external = Path.Combine(Path.GetTempPath(), $"mk8-sava-allocated-external-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        Directory.CreateDirectory(external);
        var link = Path.Combine(root, "external-link");
        try
        {
            var nested = Path.Combine(root, "nested");
            Directory.CreateDirectory(nested);
            var smallFile = Path.Combine(nested, "small.bin");
            await File.WriteAllBytesAsync(smallFile, new byte[80]);
            await File.WriteAllBytesAsync(Path.Combine(nested, "sparse.bin"), new byte[8192]);
            await File.WriteAllBytesAsync(Path.Combine(external, "large.bin"), new byte[4 * 1024 * 1024]);
            Directory.CreateSymbolicLink(link, external);
            var hardLink = new ProcessStartInfo("ln") { RedirectStandardError = true };
            hardLink.ArgumentList.Add(smallFile);
            hardLink.ArgumentList.Add(Path.Combine(root, "small-hard-link.bin"));
            using (var linkProcess = Process.Start(hardLink)!)
            {
                var linkError = await linkProcess.StandardError.ReadToEndAsync();
                await linkProcess.WaitForExitAsync();
                Assert.True(linkProcess.ExitCode == 0, linkError);
            }

            var measured = StorageAllocationMeter.MeasureRoot(root);
            Assert.NotNull(measured);
            using var process = new Process
            {
                StartInfo = new ProcessStartInfo("du")
                {
                    RedirectStandardOutput = true,
                    RedirectStandardError = true
                }
            };
            process.StartInfo.ArgumentList.Add("-s");
            process.StartInfo.ArgumentList.Add("--block-size=1");
            process.StartInfo.ArgumentList.Add("--");
            process.StartInfo.ArgumentList.Add(root);
            process.Start();
            var output = await process.StandardOutput.ReadToEndAsync();
            var error = await process.StandardError.ReadToEndAsync();
            await process.WaitForExitAsync();
            Assert.True(process.ExitCode == 0, error);
            var separator = output.IndexOf('\t');
            Assert.True(separator > 0, output);
            Assert.Equal(
                long.Parse(output.AsSpan(0, separator), CultureInfo.InvariantCulture),
                measured.Value);
        }
        finally
        {
            if (Directory.Exists(link))
                Directory.Delete(link);
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
            if (Directory.Exists(external))
                Directory.Delete(external, recursive: true);
        }
    }

    [Fact]
    public async Task StartupPrunesLegacyEmptyHashDirectoriesWithoutFollowingSymlinksOrRemovingPackedContent()
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-legacy-chunk-dirs-{Guid.NewGuid():N}");
        var externalPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-external-dir-{Guid.NewGuid():N}");
        var containerName = $"legacy-directories-{Guid.NewGuid():N}";
        var configuration = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        };
        var bytes = new byte[80];
        DeterministicTestBytes.Fill(0x6400, bytes);

        try
        {
            {
                var first = new SavaWebApplicationFactory(dataPath, configuration, deleteDataPath: false);
                await using (first.ConfigureAwait(false))
                {
                    await first.InitializeAsync();
                    await CreateClient(first).GetBlobContainerClient(containerName)
                        .CreateIfNotExistsAsync();
                    await CreateClient(first).GetBlobContainerClient(containerName)
                        .GetBlobClient("packed.bin").UploadAsync(BinaryData.FromBytes(bytes));
                    Assert.Empty(Directory.EnumerateDirectories(
                        Path.Combine(dataPath, "chunks"),
                        "*",
                        SearchOption.AllDirectories));
                }
            }

            var chunksRoot = Path.Combine(dataPath, "chunks");
            var emptyLeaf = Path.Combine(chunksRoot, "legacy", "domain", "hash");
            var occupied = Path.Combine(chunksRoot, "occupied");
            Directory.CreateDirectory(emptyLeaf);
            Directory.CreateDirectory(occupied);
            await File.WriteAllBytesAsync(Path.Combine(occupied, "keep.bin"), [7]);

            if (OperatingSystem.IsLinux())
            {
                Directory.CreateDirectory(Path.Combine(externalPath, "outside-empty"));
                Directory.CreateSymbolicLink(Path.Combine(chunksRoot, "linked"), externalPath);
            }

            {
                var restarted = new SavaWebApplicationFactory(dataPath, configuration, deleteDataPath: false);
                await using (restarted.ConfigureAwait(false))
                {
                    await restarted.InitializeAsync();
                    Assert.False(Directory.Exists(emptyLeaf));
                    Assert.False(Directory.Exists(Path.Combine(chunksRoot, "legacy")));
                    Assert.True(File.Exists(Path.Combine(occupied, "keep.bin")));
                    if (OperatingSystem.IsLinux())
                    {
                        Assert.True(Directory.Exists(Path.Combine(externalPath, "outside-empty")));
                        Assert.True(Directory.Exists(Path.Combine(chunksRoot, "linked")));
                    }

                    var downloaded = await CreateClient(restarted)
                        .GetBlobContainerClient(containerName)
                        .GetBlobClient("packed.bin")
                        .DownloadContentAsync();
                    Assert.Equal(bytes, downloaded.Value.Content.ToArray());
                }
            }
        }
        finally
        {
            var link = Path.Combine(dataPath, "chunks", "linked");
            if (OperatingSystem.IsLinux() && Directory.Exists(link))
                Directory.Delete(link);
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, recursive: true);
            if (Directory.Exists(externalPath))
                Directory.Delete(externalPath, recursive: true);
        }
    }

    [Fact]
    public async Task GarbageCollectionReclaimsEmptyStandaloneChunkDirectoriesAndCanRecreateThem()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:EnableSmallChunkPacking"] = "false",
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });
        await using var applicationDisposal3 = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var service = application.Services.GetRequiredService<BlobService>();
        var encryption = new BlobEncryption(Scope: null, CustomerProvidedKeySha256: null);
        var firstBytes = new byte[80];
        DeterministicTestBytes.Fill(0x6200, firstBytes);

        for (var index = 0; index < 32; index++)
        {
            var bytes = new byte[80];
            DeterministicTestBytes.Fill(0x6200 + index, bytes);
            using var stored = await chunks.StorePinnedAsync(
                SavaWebApplicationFactory.AccountName,
                encryption,
                new MemoryStream(bytes, writable: false),
                CancellationToken.None);
            Assert.Single(stored.Manifest.Chunks);
        }

        var root = Path.Combine(application.DataPath, "chunks");
        Assert.NotEmpty(Directory.EnumerateDirectories(root, "*", SearchOption.AllDirectories));
        Assert.Equal(32, await service.CollectGarbageAsync(CancellationToken.None));
        Assert.Empty(Directory.EnumerateFiles(root, "*.chunk", SearchOption.AllDirectories));
        Assert.Empty(Directory.EnumerateDirectories(root, "*", SearchOption.AllDirectories));

        using (var recreated = await chunks.StorePinnedAsync(
                   SavaWebApplicationFactory.AccountName,
                   encryption,
                   new MemoryStream(firstBytes, writable: false),
                   CancellationToken.None))
        {
            using var output = new MemoryStream();
            await chunks.WriteRangeAsync(
                recreated.Manifest,
                encryption,
                0,
                firstBytes.Length,
                output,
                CancellationToken.None);
            Assert.Equal(firstBytes, output.ToArray());
        }

        Assert.Equal(1, await service.CollectGarbageAsync(CancellationToken.None));
        Assert.Empty(Directory.EnumerateDirectories(root, "*", SearchOption.AllDirectories));
    }

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory application)
    {
        var account = SavaWebApplicationFactory.AccountName;
        var endpoint = new Uri($"http://{account}.localhost");
        return new BlobServiceClient(
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
    }

    [Fact]
    public async Task ConcurrentStandalonePublicationAndDirectoryPruningKeepPinnedBytesReadable()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:EnableSmallChunkPacking"] = "false",
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });
        await using var applicationDisposal4 = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var service = application.Services.GetRequiredService<BlobService>();
        var encryption = new BlobEncryption(Scope: null, CustomerProvidedKeySha256: null);
        using var stop = new CancellationTokenSource();
        var sweeper = Task.Run(async () =>
        {
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    await service.CollectGarbageAsync(stop.Token).ConfigureAwait(false);
                    await Task.Yield();
                }
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
            }
        });

        try
        {
            await Task.WhenAll(Enumerable.Range(0, 48).Select(async index =>
            {
                var bytes = new byte[80];
                DeterministicTestBytes.Fill(0x6300 + index, bytes);
                using var stored = await chunks.StorePinnedAsync(
                    SavaWebApplicationFactory.AccountName,
                    encryption,
                    new MemoryStream(bytes, writable: false),
                    CancellationToken.None).ConfigureAwait(false);
                using var output = new MemoryStream();
                await chunks.WriteRangeAsync(
                    stored.Manifest,
                    encryption,
                    0,
                    bytes.Length,
                    output,
                    CancellationToken.None).ConfigureAwait(false);
                Assert.Equal(bytes, output.ToArray());
            }));
        }
        finally
        {
            await stop.CancelAsync();
            await sweeper.ConfigureAwait(true);
        }

        for (var attempt = 0; attempt < 3; attempt++)
            await service.CollectGarbageAsync(CancellationToken.None).ConfigureAwait(true);
        var root = Path.Combine(application.DataPath, "chunks");
        Assert.Empty(Directory.EnumerateFiles(root, "*.chunk", SearchOption.AllDirectories));
        Assert.Empty(Directory.EnumerateDirectories(root, "*", SearchOption.AllDirectories));
    }

    [Fact]
    public async Task PackedSmallChunksDoNotAllocateUnusedHashDirectories()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });
        await using var applicationDisposal5 = application.ConfigureAwait(false);
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
            DeterministicTestBytes.Fill(0x6100 + index, bytes);
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
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:EnableSmallChunkPacking"] = "false",
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });
        await using var applicationDisposal6 = application.ConfigureAwait(false);
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
        DeterministicTestBytes.Fill(0x5A8A, original);
        var inserted = new byte[3 * 1024];
        DeterministicTestBytes.Fill(0x5A8B, inserted);
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
