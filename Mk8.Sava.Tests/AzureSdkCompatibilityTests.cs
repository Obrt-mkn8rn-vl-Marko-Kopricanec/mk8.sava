using Azure;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using System.IdentityModel.Tokens.Jwt;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Security.Claims;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.IdentityModel.Tokens;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Data.Sqlite;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class AzureSdkCompatibilityTests(SavaWebApplicationFactory factory)
    : IClassFixture<SavaWebApplicationFactory>
{
    [Fact]
    public async Task BlockBlobRoundTripPreservesBytesPropertiesMetadataTagsRangesAndListings()
    {
        var service = CreateClient(factory);
        var containerName = $"sdk-{Guid.NewGuid():N}";
        var container = service.GetBlobContainerClient(containerName);
        var created = await container.CreateAsync(PublicAccessType.None, new Dictionary<string, string> { ["scope"] = "tests" });
        Assert.Equal(201, created.GetRawResponse().Status);

        var content = Enumerable.Range(0, 256 * 1024)
            .Select(index => (byte)((index / 1024) % 7))
            .ToArray();
        var blob = container.GetBlobClient("folder/blob.bin");
        var upload = await blob.UploadAsync(BinaryData.FromBytes(content), new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders
            {
                ContentType = "application/x-mk8-test",
                CacheControl = "private,max-age=30"
            },
            Metadata = new Dictionary<string, string> { ["owner"] = "compatibility" },
            Tags = new Dictionary<string, string> { ["kind"] = "fixture" }
        });
        Assert.Equal(201, upload.GetRawResponse().Status);

        var downloaded = await blob.DownloadContentAsync();
        Assert.Equal(content, downloaded.Value.Content.ToArray());
        Assert.Equal("application/x-mk8-test", downloaded.Value.Details.ContentType);
        Assert.Equal("compatibility", downloaded.Value.Details.Metadata["owner"]);

        var range = new HttpRange(12_345, 7_777);
        var ranged = await blob.DownloadStreamingAsync(new BlobDownloadOptions { Range = range });
        using var rangeBuffer = new MemoryStream();
        await ranged.Value.Content.CopyToAsync(rangeBuffer);
        Assert.Equal(content.AsSpan(12_345, 7_777).ToArray(), rangeBuffer.ToArray());

        var properties = await blob.GetPropertiesAsync();
        Assert.Equal(content.LongLength, properties.Value.ContentLength);
        Assert.Equal("fixture", (await blob.GetTagsAsync()).Value.Tags["kind"]);

        var names = new List<string>();
        await foreach (var item in container.GetBlobsAsync(BlobTraits.Metadata | BlobTraits.Tags, prefix: "folder/"))
            names.Add(item.Name);
        Assert.Contains("folder/blob.bin", names);
    }

    [Fact]
    public async Task PagedFlatVersionSnapshotAndHierarchyListingsNeverSkipOrRepeatEntries()
    {
        var application = new SavaWebApplicationFactory();
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var containerName = $"pages-{Guid.NewGuid():N}";
            var container = service.GetBlobContainerClient(containerName);
            await container.CreateAsync();
            var metadata = application.Services.GetRequiredService<MetadataStore>();
            var properties = await metadata.GetServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                CancellationToken.None);
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                properties with { VersioningEnabled = true },
                CancellationToken.None);

            var versioned = container.GetBlobClient("paged/same-name.txt");
            await versioned.UploadAsync(BinaryData.FromString("one"), overwrite: true);
            await versioned.UploadAsync(BinaryData.FromString("two"), overwrite: true);
            await versioned.UploadAsync(BinaryData.FromString("three"), overwrite: true);
            await versioned.CreateSnapshotAsync();
            await versioned.CreateSnapshotAsync();

            var expectedRecords = (await metadata.ListBlobsAsync(
                    SavaWebApplicationFactory.AccountName,
                    containerName,
                    includeVersions: true,
                    includeSnapshots: true,
                    includeDeleted: false,
                    CancellationToken.None))
                .Where(item => item.Name == versioned.Name)
                .ToArray();
            var listed = new List<BlobItem>();
            var flatTokens = new HashSet<string>(StringComparer.Ordinal);
            await foreach (var page in container
                               .GetBlobsAsync(
                                   BlobTraits.None,
                                   BlobStates.Version | BlobStates.Snapshots,
                                   prefix: versioned.Name)
                               .AsPages(pageSizeHint: 1))
            {
                Assert.Single(page.Values);
                listed.Add(page.Values[0]);
                if (page.ContinuationToken is not null)
                    Assert.True(flatTokens.Add(page.ContinuationToken));
            }
            Assert.Equal(expectedRecords.Length, listed.Count);
            Assert.Equal(
                expectedRecords.Select(ListIdentity).Order(StringComparer.Ordinal),
                listed.Select(ListIdentity).Order(StringComparer.Ordinal));
            Assert.NotEmpty(flatTokens);
            var reboundMarkerUri = AppendQuery(
                container.GenerateSasUri(
                    BlobContainerSasPermissions.List,
                    DateTimeOffset.UtcNow.AddMinutes(5)),
                "restype=container&comp=list&include=versions%2Csnapshots&prefix=other" +
                $"&maxresults=1&marker={Uri.EscapeDataString(flatTokens.First())}");
            using (var transport = new HttpClient(application.Server.CreateHandler()))
            using (var response = await transport.GetAsync(reboundMarkerUri))
            {
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("InvalidQueryParameterValue", response.Headers.GetValues("x-ms-error-code").Single());
            }

            await container.GetBlobClient("folders/a/one").UploadAsync(BinaryData.FromString("a1"));
            await container.GetBlobClient("folders/a/two").UploadAsync(BinaryData.FromString("a2"));
            await container.GetBlobClient("folders/b/one").UploadAsync(BinaryData.FromString("b1"));
            await container.GetBlobClient("folders/root").UploadAsync(BinaryData.FromString("root"));
            var hierarchy = new List<string>();
            var hierarchyTokens = new HashSet<string>(StringComparer.Ordinal);
            await foreach (var page in container
                               .GetBlobsByHierarchyAsync(delimiter: "/", prefix: "folders/")
                               .AsPages(pageSizeHint: 1))
            {
                Assert.Single(page.Values);
                var item = page.Values[0];
                hierarchy.Add(item.IsPrefix ? $"P:{item.Prefix}" : $"B:{item.Blob.Name}");
                if (page.ContinuationToken is not null)
                    Assert.True(hierarchyTokens.Add(page.ContinuationToken));
            }
            Assert.Equal(["P:folders/a/", "P:folders/b/", "B:folders/root"], hierarchy);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task IdenticalContentIsCompressedAndPhysicallyDeduplicated()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"space-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var content = Enumerable.Repeat((byte)'A', 512 * 1024).ToArray();

        await container.GetBlobClient("one").UploadAsync(BinaryData.FromBytes(content));
        var afterFirst = EnumerateChunkFiles(factory.DataPath).ToArray();
        await container.GetBlobClient("two").UploadAsync(BinaryData.FromBytes(content));
        var afterSecond = EnumerateChunkFiles(factory.DataPath).ToArray();

        Assert.NotEmpty(afterFirst);
        Assert.Equal(afterFirst.Length, afterSecond.Length);
        Assert.True(afterFirst.Sum(file => file.Length) < content.LongLength / 4);
        Assert.Equal(content, (await container.GetBlobClient("two").DownloadContentAsync()).Value.Content.ToArray());
    }

    [Fact]
    public async Task ColdChunksAreRecompressedAtomicallyWithoutChangingLogicalState()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>
        {
            ["Sava:CompressionQuality"] = "0",
            ["Sava:CompressionMinimumSavingsBytes"] = "0",
            ["Sava:BackgroundCompressionQuality"] = "11",
            ["Sava:BackgroundCompressionMinimumSavingsBytes"] = "1",
            ["Sava:BackgroundCompressionMinimumAge"] = "00:00:00",
            ["Sava:BackgroundCompressionChunksPerMaintenancePass"] = "1000",
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var containerName = $"recompress-{Guid.NewGuid():N}";
            var container = service.GetBlobContainerClient(containerName);
            await container.CreateAsync();
            var content = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Range(0, 5000).Select(index =>
                $"{{\"tenant\":\"stable-tenant\",\"category\":\"storage-event\",\"sequence\":{index % 97},\"payload\":\"alpha-beta-gamma-delta\"}}\n")));
            var blob = container.GetBlobClient("cold.jsonl");
            await blob.UploadAsync(BinaryData.FromBytes(content));
            var snapshotId = (await blob.CreateSnapshotAsync()).Value.Snapshot;
            var propertiesBefore = await blob.GetPropertiesAsync();

            var customerKey = RandomNumberGenerator.GetBytes(32);
            var customerBlob = CreateEncryptedClient(
                    application,
                    new CustomerProvidedKey(customerKey),
                    encryptionScope: null)
                .GetBlobContainerClient(containerName)
                .GetBlobClient("customer-key.jsonl");
            await customerBlob.UploadAsync(BinaryData.FromBytes(content));

            var blobService = application.Services.GetRequiredService<BlobService>();
            var chunkStore = application.Services.GetRequiredService<ChunkStore>();
            var record = await blobService.GetBlobAsync(
                SavaWebApplicationFactory.AccountName,
                containerName,
                blob.Name,
                versionId: null,
                snapshot: null,
                includeDeleted: false,
                CancellationToken.None);
            var customerRecord = await blobService.GetBlobAsync(
                SavaWebApplicationFactory.AccountName,
                containerName,
                customerBlob.Name,
                versionId: null,
                snapshot: null,
                includeDeleted: false,
                CancellationToken.None);
            var paths = record.Content.Chunks
                .Where(chunk => !chunk.Id.EndsWith("/$zero", StringComparison.Ordinal))
                .Select(chunk => ChunkPath(application.DataPath, chunk.Id))
                .Distinct(StringComparer.Ordinal)
                .ToArray();
            var customerPaths = customerRecord.Content.Chunks
                .Where(chunk => !chunk.Id.EndsWith("/$zero", StringComparison.Ordinal))
                .Select(chunk => ChunkPath(application.DataPath, chunk.Id))
                .Distinct(StringComparer.Ordinal)
                .ToArray();
            var beforeBytes = paths.Sum(path => new FileInfo(path).Length);
            var customerBytes = customerPaths.Sum(path => new FileInfo(path).Length);

            using (chunkStore.Pin(record.Content))
            {
                var pinnedPass = await blobService.RunMaintenanceAsync(CancellationToken.None);
                Assert.Equal(0, pinnedPass.RecompressedChunks);
                Assert.Equal(beforeBytes, paths.Sum(path => new FileInfo(path).Length));
            }

            var optimized = await blobService.RunMaintenanceAsync(CancellationToken.None);
            var afterBytes = paths.Sum(path => new FileInfo(path).Length);
            Assert.True(optimized.RecompressedChunks > 0);
            Assert.Equal(beforeBytes - afterBytes, optimized.RecompressionBytesSaved);
            Assert.True(afterBytes < beforeBytes);
            Assert.Equal(customerBytes, customerPaths.Sum(path => new FileInfo(path).Length));

            var propertiesAfter = await blob.GetPropertiesAsync();
            Assert.Equal(propertiesBefore.Value.ETag, propertiesAfter.Value.ETag);
            Assert.Equal(propertiesBefore.Value.LastModified, propertiesAfter.Value.LastModified);
            Assert.Equal(content, (await blob.DownloadContentAsync()).Value.Content.ToArray());
            Assert.Equal(
                content,
                (await blob.WithSnapshot(snapshotId).DownloadContentAsync()).Value.Content.ToArray());
            Assert.Equal(content, (await customerBlob.DownloadContentAsync()).Value.Content.ToArray());

            using var operatorClient = application.CreateClient();
            var metrics = await operatorClient.GetStringAsync("/metrics");
            Assert.Contains("mk8_sava_maintenance_recompressed_chunks_total", metrics, StringComparison.Ordinal);
            Assert.Contains("mk8_sava_maintenance_recompression_bytes_saved_total", metrics, StringComparison.Ordinal);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task CommittedBlocksCanBeReusedAndReorderedWithoutRestaging()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"blocks-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlockBlobClient("composed.bin");
        var firstId = Convert.ToBase64String("000001"u8);
        var secondId = Convert.ToBase64String("000002"u8);
        var first = Enumerable.Repeat((byte)'A', 24_000).ToArray();
        var second = Enumerable.Repeat((byte)'B', 17_000).ToArray();

        await blob.StageBlockAsync(firstId, new MemoryStream(first));
        await blob.StageBlockAsync(secondId, new MemoryStream(second));
        await blob.CommitBlockListAsync([firstId, secondId]);
        await blob.CommitBlockListAsync([secondId, firstId]);

        var expected = second.Concat(first).ToArray();
        Assert.Equal(expected, (await blob.DownloadContentAsync()).Value.Content.ToArray());
        var blocks = await blob.GetBlockListAsync(BlockListTypes.Committed);
        Assert.Equal([secondId, firstId], blocks.Value.CommittedBlocks.Select(item => item.Name));
        Assert.Equal([second.LongLength, first.LongLength], blocks.Value.CommittedBlocks.Select(item => item.SizeLong));
    }

    [Fact]
    public async Task AppendCountAndLeaseStateMatchSdkExpectations()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"lease-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var append = container.GetAppendBlobClient("events.log");
        await append.CreateAsync();
        await append.AppendBlockAsync(new MemoryStream("first\n"u8.ToArray()));
        await append.AppendBlockAsync(new MemoryStream("second\n"u8.ToArray()));
        Assert.Equal(2, (await append.GetPropertiesAsync()).Value.BlobCommittedBlockCount);

        var beforeLease = await append.GetPropertiesAsync();
        var lease = append.GetBlobLeaseClient();
        var acquired = await lease.AcquireAsync(TimeSpan.FromSeconds(15));
        var afterLease = await append.GetPropertiesAsync();
        Assert.Equal(beforeLease.Value.ETag, afterLease.Value.ETag);
        Assert.Equal(beforeLease.Value.LastModified, afterLease.Value.LastModified);

        var denied = await Assert.ThrowsAsync<RequestFailedException>(() =>
            append.SetMetadataAsync(new Dictionary<string, string> { ["locked"] = "true" }));
        Assert.Equal(412, denied.Status);
        await append.SetMetadataAsync(
            new Dictionary<string, string> { ["locked"] = "true" },
            new BlobRequestConditions { LeaseId = acquired.Value.LeaseId });
        await lease.ReleaseAsync();
    }

    [Fact]
    public async Task DeduplicationIsIsolatedAcrossAccountsAndChunksAreEncryptedAtRest()
    {
        var content = Enumerable.Repeat((byte)'Q', 96 * 1024).ToArray();
        var first = CreateClient(factory, SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);
        var second = CreateClient(factory, SavaWebApplicationFactory.SecondAccountName, SavaWebApplicationFactory.SecondAccountKey);
        var firstContainer = first.GetBlobContainerClient($"isolation-{Guid.NewGuid():N}");
        var secondContainer = second.GetBlobContainerClient($"isolation-{Guid.NewGuid():N}");
        await firstContainer.CreateAsync();
        await secondContainer.CreateAsync();
        await firstContainer.GetBlobClient("same").UploadAsync(BinaryData.FromBytes(content));
        await secondContainer.GetBlobClient("same").UploadAsync(BinaryData.FromBytes(content));

        var accountRoots = Directory.EnumerateDirectories(Path.Combine(factory.DataPath, "chunks"))
            .Select(Path.GetFileName)
            .ToHashSet(StringComparer.Ordinal);
        Assert.Contains(SavaWebApplicationFactory.AccountName, accountRoots);
        Assert.Contains(SavaWebApplicationFactory.SecondAccountName, accountRoots);
        foreach (var path in Directory.EnumerateFiles(Path.Combine(factory.DataPath, "chunks"), "*.chunk", SearchOption.AllDirectories))
        {
            var stored = await File.ReadAllBytesAsync(path);
            Assert.DoesNotContain(Enumerable.Repeat((byte)'Q', 64).ToArray(), stored);
        }
    }

    [Fact]
    public async Task ServiceAndAccountSasSignaturesScopesAndPermissionsAreEnforced()
    {
        var service = CreateClient(factory);
        var containerName = $"sas-{Guid.NewGuid():N}";
        var blobName = "protected.txt";
        var container = service.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        await container.GetBlobClient(blobName).UploadAsync(BinaryData.FromString("sas payload"));
        var credential = new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);

        var serviceBuilder = new BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = blobName,
            Resource = "b",
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp,
            ContentType = "text/x-sas-override"
        };
        serviceBuilder.SetPermissions(BlobSasPermissions.Read);
        var serviceSas = serviceBuilder.ToSasQueryParameters(credential);
        var sasBlob = CreateBlobClient(factory, new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}?{serviceSas}"));
        var sasDownload = await sasBlob.DownloadContentAsync();
        Assert.Equal("sas payload", sasDownload.Value.Content.ToString());
        Assert.Equal("text/x-sas-override", sasDownload.Value.Details.ContentType);
        var deniedDelete = await Assert.ThrowsAsync<RequestFailedException>(() => sasBlob.DeleteAsync());
        Assert.Equal(403, deniedDelete.Status);

        var accountBuilder = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Object,
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp
        };
        accountBuilder.SetPermissions(AccountSasPermissions.Read);
        var accountSas = accountBuilder.ToSasQueryParameters(credential);
        var accountBlob = CreateBlobClient(factory, new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}?{accountSas}"));
        Assert.Equal("sas payload", (await accountBlob.DownloadContentAsync()).Value.Content.ToString());
    }

    [Fact]
    public async Task StoredAccessPolicyCanAuthorizeAndImmediatelyRevokeServiceSas()
    {
        var service = CreateClient(factory);
        var containerName = $"policy-{Guid.NewGuid():N}";
        var blobName = "policy.txt";
        var container = service.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        await container.GetBlobClient(blobName).UploadAsync(BinaryData.FromString("policy payload"));
        await container.SetAccessPolicyAsync(
            PublicAccessType.None,
            [new BlobSignedIdentifier
            {
                Id = "read-policy",
                AccessPolicy = new BlobAccessPolicy
                {
                    StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
                    ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
                    Permissions = "r"
                }
            }]);

        var builder = new BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = blobName,
            Resource = "b",
            Identifier = "read-policy",
            Protocol = SasProtocol.HttpsAndHttp
        };
        var credential = new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);
        var sas = builder.ToSasQueryParameters(credential);
        var policyBlob = CreateBlobClient(factory, new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}?{sas}"));
        Assert.Equal("policy payload", (await policyBlob.DownloadContentAsync()).Value.Content.ToString());

        await container.SetAccessPolicyAsync(PublicAccessType.None, []);
        var revoked = await Assert.ThrowsAsync<RequestFailedException>(() => policyBlob.DownloadContentAsync());
        Assert.Equal(403, revoked.Status);
    }

    [Fact]
    public async Task BearerTokensRequireTrustedSignatureIssuerAudiencePrincipalAndPermission()
    {
        var owner = CreateClient(factory);
        var containerName = $"bearer-{Guid.NewGuid():N}";
        var container = owner.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        await container.GetBlobClient("readable.txt").UploadAsync(BinaryData.FromString("bearer payload"));

        var token = CreateJwt(SavaWebApplicationFactory.AccountKey, "reader-1");
        var reader = CreateBearerClient(factory, token);
        Assert.Equal("bearer payload", (await reader.GetBlobContainerClient(containerName).GetBlobClient("readable.txt").DownloadContentAsync()).Value.Content.ToString());
        var names = new List<string>();
        await foreach (var item in reader.GetBlobContainerClient(containerName).GetBlobsAsync())
            names.Add(item.Name);
        Assert.Contains("readable.txt", names);
        var deniedWrite = await Assert.ThrowsAsync<RequestFailedException>(() =>
            reader.GetBlobContainerClient(containerName).GetBlobClient("denied.txt").UploadAsync(BinaryData.FromString("no")));
        Assert.Equal(403, deniedWrite.Status);

        var invalidToken = CreateJwt(SavaWebApplicationFactory.SecondAccountKey, "reader-1");
        var invalid = CreateBearerClient(factory, invalidToken);
        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() =>
            invalid.GetBlobContainerClient(containerName).GetBlobClient("readable.txt").DownloadContentAsync());
        Assert.Equal(401, rejected.Status);
    }

    [Fact]
    public async Task BearerDelegationKeysProduceScopedUserDelegationSasTokens()
    {
        var owner = CreateClient(factory);
        var containerName = $"delegation-{Guid.NewGuid():N}";
        var blobName = "delegated.txt";
        var container = owner.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        await container.GetBlobClient(blobName).UploadAsync(BinaryData.FromString("delegated payload"));

        var token = CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            SavaWebApplicationFactory.DelegatorObjectId,
            SavaWebApplicationFactory.TenantId);
        var delegator = CreateBearerClient(factory, token);
        var startsOn = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expiresOn = DateTimeOffset.UtcNow.AddHours(1);
        var key = await delegator.GetUserDelegationKeyAsync(startsOn, expiresOn);
        Assert.Equal(SavaWebApplicationFactory.DelegatorObjectId, key.Value.SignedObjectId);
        Assert.Equal(SavaWebApplicationFactory.TenantId, key.Value.SignedTenantId);

        var builder = new BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = blobName,
            Resource = "b",
            StartsOn = startsOn,
            ExpiresOn = expiresOn,
            Protocol = SasProtocol.HttpsAndHttp
        };
        builder.SetPermissions(BlobSasPermissions.Read | BlobSasPermissions.Delete);
        var sas = builder.ToSasQueryParameters(key.Value, SavaWebApplicationFactory.AccountName);
        var delegatedBlob = CreateBlobClient(
            factory,
            new Uri($"https://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}?{sas}"));

        Assert.Equal("delegated payload", (await delegatedBlob.DownloadContentAsync()).Value.Content.ToString());
        var deniedDelete = await Assert.ThrowsAsync<RequestFailedException>(() => delegatedBlob.DeleteAsync());
        Assert.Equal(403, deniedDelete.Status);
    }

    [Fact]
    public async Task UrlTransfersSupportBlockAppendPageAndWholeBlobOperations()
    {
        var sourceBytes = Enumerable.Range(0, 16 * 1024).Select(index => (byte)(index % 251)).ToArray();
        await using var source = await LoopbackSource.StartAsync(sourceBytes);
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"url-{Guid.NewGuid():N}");
        await container.CreateAsync();

        var whole = container.GetBlockBlobClient("whole.bin");
        await whole.SyncUploadFromUriAsync(source.Uri, overwrite: true);
        Assert.Equal(sourceBytes, (await whole.DownloadContentAsync()).Value.Content.ToArray());

        const int blockOffset = 900;
        const int blockLength = 2_300;
        var block = container.GetBlockBlobClient("block.bin");
        var blockId = Convert.ToBase64String("url-block-1"u8);
        var blockSlice = sourceBytes.AsSpan(blockOffset, blockLength).ToArray();
        await block.StageBlockFromUriAsync(source.Uri, blockId, new StageBlockFromUriOptions
        {
            SourceRange = new HttpRange(blockOffset, blockLength),
            SourceContentHash = MD5.HashData(blockSlice)
        });
        await block.CommitBlockListAsync([blockId]);
        Assert.Equal(blockSlice, (await block.DownloadContentAsync()).Value.Content.ToArray());

        const int appendOffset = 4_000;
        const int appendLength = 1_500;
        var append = container.GetAppendBlobClient("append.bin");
        await append.CreateAsync();
        var appendSlice = sourceBytes.AsSpan(appendOffset, appendLength).ToArray();
        await append.AppendBlockFromUriAsync(source.Uri, new AppendBlobAppendBlockFromUriOptions
        {
            SourceRange = new HttpRange(appendOffset, appendLength),
            SourceContentHash = MD5.HashData(appendSlice)
        });
        Assert.Equal(appendSlice, (await append.DownloadContentAsync()).Value.Content.ToArray());

        var page = container.GetPageBlobClient("page.bin");
        await page.CreateAsync(1024);
        var pageSlice = sourceBytes.AsSpan(0, 512).ToArray();
        await page.UploadPagesFromUriAsync(
            source.Uri,
            new HttpRange(0, 512),
            new HttpRange(512, 512),
            new PageBlobUploadPagesFromUriOptions { SourceContentHash = MD5.HashData(pageSlice) });
        var expectedPage = new byte[1024];
        pageSlice.CopyTo(expectedPage, 512);
        Assert.Equal(expectedPage, (await page.DownloadContentAsync()).Value.Content.ToArray());
    }

    [Fact]
    public async Task SourceCustomerKeysAreValidatedAndForwardedForEveryUrlWriteOperation()
    {
        var sourceBytes = Enumerable.Range(0, 2048).Select(index => (byte)(index % 239)).ToArray();
        var sourceKey = RandomNumberGenerator.GetBytes(32);
        var sourceHash = SHA256.HashData(sourceKey);
        var source = new EncryptedSourceHandler(sourceBytes, sourceKey, sourceHash);
        var application = new SavaWebApplicationFactory(() => source);
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var containerName = $"source-cpk-{Guid.NewGuid():N}";
            var container = service.GetBlobContainerClient(containerName);
            await container.CreateAsync();
            using var transport = new HttpClient(application.Server.CreateHandler());

            var destinationKey = RandomNumberGenerator.GetBytes(32);
            var destinationHash = SHA256.HashData(destinationKey);
            var whole = container.GetBlobClient("whole.bin");
            using (var request = CreateSourceKeyRequest(
                       HttpsSasUri(whole, BlobSasPermissions.Create | BlobSasPermissions.Write),
                       sourceKey,
                       sourceHash))
            {
                request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
                AddCustomerKeyHeaders(request, destinationKey, destinationHash);
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }
            var encryptedWhole = CreateEncryptedClient(
                    application,
                    new CustomerProvidedKey(destinationKey),
                    encryptionScope: null)
                .GetBlobContainerClient(containerName)
                .GetBlobClient(whole.Name);
            Assert.Equal(sourceBytes, (await encryptedWhole.DownloadContentAsync()).Value.Content.ToArray());

            const int blockStart = 128;
            const int blockEnd = 1023;
            var block = container.GetBlockBlobClient("block.bin");
            var blockId = Convert.ToBase64String("source-cpk-block"u8);
            var blockUri = AppendQuery(
                HttpsSasUri(block, BlobSasPermissions.Create | BlobSasPermissions.Write),
                $"comp=block&blockid={Uri.EscapeDataString(blockId)}");
            using (var request = CreateSourceKeyRequest(blockUri, sourceKey, sourceHash))
            {
                request.Headers.TryAddWithoutValidation("x-ms-source-range", $"bytes={blockStart}-{blockEnd}");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }
            await block.CommitBlockListAsync([blockId]);
            Assert.Equal(
                sourceBytes[blockStart..(blockEnd + 1)],
                (await block.DownloadContentAsync()).Value.Content.ToArray());

            const int appendStart = 256;
            const int appendEnd = 767;
            var append = container.GetAppendBlobClient("append.bin");
            await append.CreateAsync();
            var appendUri = AppendQuery(
                HttpsSasUri(append, BlobSasPermissions.Add | BlobSasPermissions.Write),
                "comp=appendblock");
            using (var request = CreateSourceKeyRequest(appendUri, sourceKey, sourceHash))
            {
                request.Headers.TryAddWithoutValidation("x-ms-source-range", $"bytes={appendStart}-{appendEnd}");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }
            Assert.Equal(
                sourceBytes[appendStart..(appendEnd + 1)],
                (await append.DownloadContentAsync()).Value.Content.ToArray());

            var page = container.GetPageBlobClient("page.bin");
            await page.CreateAsync(1024);
            var pageUri = AppendQuery(
                HttpsSasUri(page, BlobSasPermissions.Write),
                "comp=page");
            using (var request = CreateSourceKeyRequest(pageUri, sourceKey, sourceHash))
            {
                request.Headers.TryAddWithoutValidation("x-ms-page-write", "update");
                request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=0-511");
                request.Headers.TryAddWithoutValidation("x-ms-source-range", "bytes=512-1023");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }
            var expectedPage = new byte[1024];
            sourceBytes.AsSpan(512, 512).CopyTo(expectedPage);
            Assert.Equal(expectedPage, (await page.DownloadContentAsync()).Value.Content.ToArray());
            Assert.Equal(4, source.RequestCount);

            var invalid = container.GetBlobClient("invalid.bin");
            using (var request = CreateSourceKeyRequest(
                       HttpsSasUri(invalid, BlobSasPermissions.Create | BlobSasPermissions.Write),
                       sourceKey,
                       sourceHash,
                       version: "2025-11-05"))
            {
                request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
            }

            using (var request = CreateSourceKeyRequest(
                       HttpsSasUri(invalid, BlobSasPermissions.Create | BlobSasPermissions.Write),
                       sourceKey,
                       RandomNumberGenerator.GetBytes(32)))
            {
                request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
            }

            using (var request = CreateSourceKeyRequest(
                       HttpsSasUri(invalid, BlobSasPermissions.Create | BlobSasPermissions.Write),
                       sourceKey,
                       sourceHash,
                       sourceUri: "http://source.example/encrypted"))
            {
                request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("InvalidRequest", response.Headers.GetValues("x-ms-error-code").Single());
            }

            using (var request = CreateSourceKeyRequest(
                       HttpsSasUri(invalid, BlobSasPermissions.Create | BlobSasPermissions.Write),
                       sourceKey,
                       sourceHash))
            {
                request.Headers.TryAddWithoutValidation("x-ms-requires-sync", "true");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
            }
            Assert.Equal(4, source.RequestCount);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task PageBlobsUseSparseCapacityAndTrackAllocatedZeroPages()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"pages-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var page = container.GetPageBlobClient("disk.vhd");
        var chunksBeforeCreate = EnumerateChunkFiles(factory.DataPath).Count();
        const long initialLength = 1L * 1024 * 1024 * 1024 * 1024;
        const long allocationOffset = 4L * 1024 * 1024 * 1024;

        await page.CreateAsync(initialLength);
        Assert.Equal(chunksBeforeCreate, EnumerateChunkFiles(factory.DataPath).Count());

        await page.UploadPagesAsync(new MemoryStream(new byte[512]), allocationOffset);
        Assert.Equal(chunksBeforeCreate, EnumerateChunkFiles(factory.DataPath).Count());
        var allocated = (await page.GetPageRangesAsync()).Value.PageRanges.ToArray();
        var allocatedRange = Assert.Single(allocated);
        Assert.Equal(allocationOffset, allocatedRange.Offset);
        Assert.Equal(512, allocatedRange.Length);

        var payload = Enumerable.Repeat((byte)0x5a, 512).ToArray();
        await page.UploadPagesAsync(new MemoryStream(payload), allocationOffset + 512);
        var merged = Assert.Single((await page.GetPageRangesAsync()).Value.PageRanges);
        Assert.Equal(allocationOffset, merged.Offset);
        Assert.Equal(1024, merged.Length);
        var downloaded = await page.DownloadStreamingAsync(new BlobDownloadOptions
        {
            Range = new HttpRange(allocationOffset, 1024)
        });
        using var bytes = new MemoryStream();
        await downloaded.Value.Content.CopyToAsync(bytes);
        Assert.Equal(new byte[512].Concat(payload).ToArray(), bytes.ToArray());

        await page.ClearPagesAsync(new HttpRange(allocationOffset, 512));
        var remaining = Assert.Single((await page.GetPageRangesAsync()).Value.PageRanges);
        Assert.Equal(allocationOffset + 512, remaining.Offset);
        Assert.Equal(512, remaining.Length);

        await page.ResizeAsync(4096);
        Assert.Empty((await page.GetPageRangesAsync()).Value.PageRanges);
        Assert.Equal(4096, (await page.GetPropertiesAsync()).Value.ContentLength);
    }

    [Fact]
    public async Task PageBlobSequenceConditionsAndSnapshotDiffsMatchTheSdk()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"page-diff-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var page = container.GetPageBlobClient("disk.vhd");
        await page.CreateAsync(2048, new PageBlobCreateOptions { SequenceNumber = 7 });

        var first = Enumerable.Repeat((byte)0x11, 512).ToArray();
        var second = Enumerable.Repeat((byte)0x22, 512).ToArray();
        await page.UploadPagesAsync(
            new MemoryStream(first),
            offset: 0,
            new PageBlobUploadPagesOptions
            {
                Conditions = new PageBlobRequestConditions { IfSequenceNumberEqual = 7 }
            });
        await page.UploadPagesAsync(new MemoryStream(second), offset: 512);
        var snapshot = (await page.CreateSnapshotAsync()).Value.Snapshot;

        var changedFirst = Enumerable.Repeat((byte)0x33, 512).ToArray();
        var third = Enumerable.Repeat((byte)0x44, 512).ToArray();
        await page.UploadPagesAsync(
            new MemoryStream(changedFirst),
            offset: 0,
            new PageBlobUploadPagesOptions
            {
                Conditions = new PageBlobRequestConditions { IfSequenceNumberLessThanOrEqual = 7 }
            });
        await page.ClearPagesAsync(new HttpRange(512, 512));
        await page.UploadPagesAsync(new MemoryStream(third), offset: 1024);

        var failed = await Assert.ThrowsAsync<RequestFailedException>(async () =>
            await page.UploadPagesAsync(
                new MemoryStream(first),
                offset: 1536,
                new PageBlobUploadPagesOptions
                {
                    Conditions = new PageBlobRequestConditions { IfSequenceNumberLessThan = 7 }
                }));
        Assert.Equal(412, failed.Status);
        Assert.Equal("SequenceNumberConditionNotMet", failed.ErrorCode);

        var diff = (await page.GetPageRangesDiffAsync(previousSnapshot: snapshot)).Value;
        Assert.Collection(
            diff.PageRanges,
            range =>
            {
                Assert.Equal(0, range.Offset);
                Assert.Equal(512, range.Length);
            },
            range =>
            {
                Assert.Equal(1024, range.Offset);
                Assert.Equal(512, range.Length);
            });
        var cleared = Assert.Single(diff.ClearRanges);
        Assert.Equal(512, cleared.Offset);
        Assert.Equal(512, cleared.Length);
    }

    [Fact]
    public async Task IncrementalPageCopiesCreateReadableSnapshotsAndReuseExtents()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"incremental-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var source = container.GetPageBlobClient("source.vhd");
        await source.CreateAsync(2048);
        var firstPage = Enumerable.Repeat((byte)0x51, 512).ToArray();
        var secondPage = Enumerable.Repeat((byte)0x62, 512).ToArray();
        await source.UploadPagesAsync(new MemoryStream(firstPage), offset: 0);
        await source.UploadPagesAsync(new MemoryStream(secondPage), offset: 512);
        var firstSourceSnapshot = (await source.CreateSnapshotAsync()).Value.Snapshot;
        var chunksBeforeFirstCopy = EnumerateChunkFiles(factory.DataPath).Count();

        var destination = container.GetPageBlobClient("backup.vhd");
        var firstCopy = await destination.StartCopyIncrementalAsync(source.Uri, firstSourceSnapshot);
        Assert.Equal(202, firstCopy.GetRawResponse().Status);
        await firstCopy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None);
        var firstProperties = (await destination.GetPropertiesAsync()).Value;
        Assert.True(firstProperties.IsIncrementalCopy);
        Assert.Equal(CopyStatus.Success, firstProperties.CopyStatus);
        Assert.NotNull(firstProperties.DestinationSnapshot);
        Assert.Equal(chunksBeforeFirstCopy, EnumerateChunkFiles(factory.DataPath).Count());

        var baseRead = await Assert.ThrowsAsync<RequestFailedException>(() => destination.DownloadContentAsync());
        Assert.Equal(409, baseRead.Status);
        Assert.Equal("OperationNotAllowedOnIncrementalCopyBlob", baseRead.ErrorCode);
        var firstExpected = new byte[2048];
        firstPage.CopyTo(firstExpected, 0);
        secondPage.CopyTo(firstExpected, 512);
        var firstDestinationSnapshot = firstProperties.DestinationSnapshot!;
        Assert.Equal(
            firstExpected,
            (await destination.WithSnapshot(firstDestinationSnapshot).DownloadContentAsync()).Value.Content.ToArray());

        var changedPage = Enumerable.Repeat((byte)0x73, 512).ToArray();
        await source.UploadPagesAsync(new MemoryStream(changedPage), offset: 0);
        await source.ClearPagesAsync(new HttpRange(512, 512));
        var secondSourceSnapshot = (await source.CreateSnapshotAsync()).Value.Snapshot;
        var chunksBeforeSecondCopy = EnumerateChunkFiles(factory.DataPath).Count();
        var secondCopy = await destination.StartCopyIncrementalAsync(source.Uri, secondSourceSnapshot);
        await secondCopy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None);
        var secondProperties = (await destination.GetPropertiesAsync()).Value;
        Assert.NotEqual(firstDestinationSnapshot, secondProperties.DestinationSnapshot);
        Assert.Equal(chunksBeforeSecondCopy, EnumerateChunkFiles(factory.DataPath).Count());

        var secondExpected = new byte[2048];
        changedPage.CopyTo(secondExpected, 0);
        Assert.Equal(
            secondExpected,
            (await destination.WithSnapshot(secondProperties.DestinationSnapshot!).DownloadContentAsync()).Value.Content.ToArray());
        Assert.Equal(
            firstExpected,
            (await destination.WithSnapshot(firstDestinationSnapshot).DownloadContentAsync()).Value.Content.ToArray());

        var earlier = await Assert.ThrowsAsync<RequestFailedException>(() =>
            destination.StartCopyIncrementalAsync(source.Uri, firstSourceSnapshot));
        Assert.Equal(409, earlier.Status);
        Assert.Equal("IncrementalCopyOfEarlierVersionSnapshotNotAllowed", earlier.ErrorCode);

        await source.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots);
        await source.CreateAsync(2048);
        var replacementSnapshot = (await source.CreateSnapshotAsync()).Value.Snapshot;
        var replacedSource = await Assert.ThrowsAsync<RequestFailedException>(() =>
            destination.StartCopyIncrementalAsync(source.Uri, replacementSnapshot));
        Assert.Equal(409, replacedSource.Status);
        Assert.Equal("IncrementalCopyBlobMismatch", replacedSource.ErrorCode);

        var listed = new List<BlobItem>();
        await foreach (var item in container.GetBlobsAsync(BlobTraits.None, BlobStates.Snapshots, prefix: destination.Name))
            listed.Add(item);
        var listedBase = Assert.Single(listed, item => item.Snapshot is null);
        Assert.Equal(secondProperties.DestinationSnapshot, listedBase.Properties.DestinationSnapshot);
        Assert.Equal(2, listed.Count(item => item.Snapshot is not null));

        var listUri = new UriBuilder(container.GenerateSasUri(
            BlobContainerSasPermissions.List,
            DateTimeOffset.UtcNow.AddMinutes(5)));
        listUri.Query = $"{listUri.Query.TrimStart('?')}&restype=container&comp=list&include=snapshots&prefix={destination.Name}";
        using var httpClient = new HttpClient(factory.Server.CreateHandler());
        var listXml = await httpClient.GetStringAsync(listUri.Uri);
        Assert.Equal(3, listXml.Split("<IncrementalCopy>true</IncrementalCopy>", StringSplitOptions.None).Length - 1);
    }

    [Fact]
    public async Task ImmutabilityPoliciesAndLegalHoldsArePersistedAndEnforced()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"worm-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("retained.txt");
        var expiresOn = DateTimeOffset.UtcNow.AddHours(2);

        await blob.UploadAsync(BinaryData.FromString("retained payload"), new BlobUploadOptions
        {
            ImmutabilityPolicy = new BlobImmutabilityPolicy
            {
                ExpiresOn = expiresOn,
                PolicyMode = BlobImmutabilityPolicyMode.Unlocked
            },
            LegalHold = true
        });

        var properties = (await blob.GetPropertiesAsync()).Value;
        Assert.True(properties.HasLegalHold);
        Assert.Equal(BlobImmutabilityPolicyMode.Unlocked, properties.ImmutabilityPolicy.PolicyMode);
        Assert.Equal(expiresOn.ToUnixTimeSeconds(), properties.ImmutabilityPolicy.ExpiresOn!.Value.ToUnixTimeSeconds());

        var held = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.SetMetadataAsync(new Dictionary<string, string> { ["attempt"] = "held" }));
        Assert.Equal(409, held.Status);
        Assert.Equal("BlobImmutableDueToLegalHold", held.ErrorCode);

        await blob.SetLegalHoldAsync(false);
        var retained = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.SetMetadataAsync(new Dictionary<string, string> { ["attempt"] = "retained" }));
        Assert.Equal(409, retained.Status);
        Assert.Equal("BlobImmutableDueToPolicy", retained.ErrorCode);

        await blob.DeleteImmutabilityPolicyAsync();
        await blob.SetMetadataAsync(new Dictionary<string, string> { ["state"] = "mutable" });
        Assert.Equal("mutable", (await blob.GetPropertiesAsync()).Value.Metadata["state"]);

        var locked = container.GetBlobClient("locked.txt");
        await locked.UploadAsync(BinaryData.FromString("locked payload"));
        await locked.SetImmutabilityPolicyAsync(new BlobImmutabilityPolicy
        {
            ExpiresOn = expiresOn,
            PolicyMode = BlobImmutabilityPolicyMode.Unlocked
        });
        await locked.SetImmutabilityPolicyAsync(new BlobImmutabilityPolicy
        {
            ExpiresOn = expiresOn.AddHours(1),
            PolicyMode = BlobImmutabilityPolicyMode.Locked
        });
        var cannotUnlock = await Assert.ThrowsAsync<RequestFailedException>(() =>
            locked.SetImmutabilityPolicyAsync(new BlobImmutabilityPolicy
            {
                ExpiresOn = expiresOn.AddHours(2),
                PolicyMode = BlobImmutabilityPolicyMode.Unlocked
            }));
        Assert.Equal("BlobImmutableDueToPolicy", cannotUnlock.ErrorCode);
        var cannotDelete = await Assert.ThrowsAsync<RequestFailedException>(() => locked.DeleteImmutabilityPolicyAsync());
        Assert.Equal("BlobImmutableDueToPolicy", cannotDelete.ErrorCode);
    }

    [Fact]
    public async Task ArchiveTierBlocksReadsAndRehydratesThroughPendingState()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"archive-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("cold.bin");
        var content = Enumerable.Range(0, 32 * 1024).Select(index => (byte)(index % 251)).ToArray();
        await blob.UploadAsync(BinaryData.FromBytes(content));

        var archived = await blob.SetAccessTierAsync(AccessTier.Archive);
        Assert.Equal(200, archived.Status);
        var archivedProperties = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal(AccessTier.Archive, archivedProperties.AccessTier);
        Assert.Null(archivedProperties.ArchiveStatus);
        var offline = await Assert.ThrowsAsync<RequestFailedException>(() => blob.DownloadContentAsync());
        Assert.Equal(409, offline.Status);
        Assert.Equal("BlobArchived", offline.ErrorCode);

        var pending = await blob.SetAccessTierAsync(
            AccessTier.Hot,
            rehydratePriority: RehydratePriority.Standard);
        Assert.Equal(202, pending.Status);
        var pendingProperties = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal(AccessTier.Archive, pendingProperties.AccessTier);
        Assert.Equal("rehydrate-pending-to-hot", pendingProperties.ArchiveStatus);
        Assert.Equal("Standard", pendingProperties.RehydratePriority);

        var wrongTarget = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.SetAccessTierAsync(AccessTier.Cool, rehydratePriority: RehydratePriority.High));
        Assert.Equal("BlobBeingRehydrated", wrongTarget.ErrorCode);

        var prioritized = await blob.SetAccessTierAsync(
            AccessTier.Hot,
            rehydratePriority: RehydratePriority.High);
        Assert.Equal(202, prioritized.Status);
        Assert.Equal("High", (await blob.GetPropertiesAsync()).Value.RehydratePriority);

        await Task.Delay(300);
        var online = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal(AccessTier.Hot, online.AccessTier);
        Assert.Null(online.ArchiveStatus);
        Assert.Equal(content, (await blob.DownloadContentAsync()).Value.Content.ToArray());
    }

    [Fact]
    public async Task AsynchronousCopiesCompleteDurablyAndCanBeAborted()
    {
        var remoteBytes = Enumerable.Range(0, 48 * 1024).Select(index => (byte)(index % 241)).ToArray();
        await using var remote = await LoopbackSource.StartAsync(remoteBytes);
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"copy-{Guid.NewGuid():N}");
        await container.CreateAsync();

        var abortDestination = container.GetBlobClient("abort.bin");
        var credentialedSource = new UriBuilder(remote.Uri) { Query = "source=remote&sig=must-not-leak" }.Uri;
        var abortOperation = await abortDestination.StartCopyFromUriAsync(
            credentialedSource,
            new BlobCopyFromUriOptions
            {
                Metadata = new Dictionary<string, string> { ["copy"] = "abort" }
            });
        Assert.Equal(202, abortOperation.GetRawResponse().Status);
        Assert.False(abortOperation.HasCompleted);
        var pending = (await abortDestination.GetPropertiesAsync()).Value;
        Assert.Equal(CopyStatus.Pending, pending.CopyStatus);
        Assert.Equal(0, pending.ContentLength);
        Assert.DoesNotContain("sig=", pending.CopySource.AbsoluteUri, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("abort", pending.Metadata["copy"]);

        var aborted = await abortDestination.AbortCopyFromUriAsync(abortOperation.Id);
        Assert.Equal(204, aborted.Status);
        var abortedProperties = (await abortDestination.GetPropertiesAsync()).Value;
        Assert.Equal(CopyStatus.Aborted, abortedProperties.CopyStatus);
        Assert.Equal(0, abortedProperties.ContentLength);
        Assert.Equal("abort", abortedProperties.Metadata["copy"]);

        var source = container.GetBlobClient("source.bin");
        await source.UploadAsync(BinaryData.FromBytes(remoteBytes), new BlobUploadOptions
        {
            Metadata = new Dictionary<string, string> { ["origin"] = "internal" },
            Tags = new Dictionary<string, string> { ["class"] = "copy" }
        });
        var completedDestination = container.GetBlobClient("completed.bin");
        var completion = await completedDestination.StartCopyFromUriAsync(source.Uri);
        Assert.Equal(CopyStatus.Pending, (await completedDestination.GetPropertiesAsync()).Value.CopyStatus);
        await completion.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None);

        var completed = (await completedDestination.GetPropertiesAsync()).Value;
        Assert.Equal(CopyStatus.Success, completed.CopyStatus);
        Assert.Equal($"{remoteBytes.LongLength}/{remoteBytes.LongLength}", completed.CopyProgress);
        Assert.Equal("internal", completed.Metadata["origin"]);
        Assert.Empty((await completedDestination.GetTagsAsync()).Value.Tags);
        Assert.Equal(remoteBytes, (await completedDestination.DownloadContentAsync()).Value.Content.ToArray());
    }

    [Fact]
    public async Task BlobBatchesExecuteIndependentDeleteAndTierSubrequests()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"batch-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var deleteTarget = container.GetBlobClient("delete.bin");
        await deleteTarget.UploadAsync(BinaryData.FromString("delete me"));

        var serviceBatchClient = service.GetBlobBatchClient();
        using (var deleteBatch = serviceBatchClient.CreateBatch())
        {
            var deleted = deleteBatch.DeleteBlob(container.Name, deleteTarget.Name);
            var missing = deleteBatch.DeleteBlob(container.Name, "missing.bin");
            var submitted = await serviceBatchClient.SubmitBatchAsync(deleteBatch, throwOnAnyFailure: false);

            Assert.Equal(202, submitted.Status);
            Assert.Equal(202, deleted.Status);
            Assert.Equal(404, missing.Status);
        }
        Assert.False(await deleteTarget.ExistsAsync());

        var tierTarget = container.GetBlobClient("tier.bin");
        await tierTarget.UploadAsync(BinaryData.FromString("tier me"));
        var containerBatchClient = container.GetBlobBatchClient();
        using (var tierBatch = containerBatchClient.CreateBatch())
        {
            var changed = tierBatch.SetBlobAccessTier(container.Name, tierTarget.Name, AccessTier.Cool);
            var missing = tierBatch.SetBlobAccessTier(
                container.Name,
                "missing-tier.bin",
                AccessTier.Hot);
            var submitted = await containerBatchClient.SubmitBatchAsync(tierBatch, throwOnAnyFailure: false);

            Assert.Equal(202, submitted.Status);
            Assert.Equal(200, changed.Status);
            Assert.Equal(404, missing.Status);
        }
        Assert.Equal(AccessTier.Cool, (await tierTarget.GetPropertiesAsync()).Value.AccessTier);
    }

    [Fact]
    public async Task CustomerProvidedKeysAndEncryptionScopesAreEnforcedAndIsolated()
    {
        var normalService = CreateClient(factory);
        var containerName = $"encryption-{Guid.NewGuid():N}";
        await normalService.GetBlobContainerClient(containerName).CreateAsync();
        var content = RandomNumberGenerator.GetBytes(64 * 1024);
        var key = RandomNumberGenerator.GetBytes(32);
        var wrongKey = RandomNumberGenerator.GetBytes(32);
        var expectedHash = Convert.ToBase64String(SHA256.HashData(key));
        var chunksBefore = Directory.GetFiles(Path.Combine(factory.DataPath, "chunks"), "*.chunk", SearchOption.AllDirectories).Length;

        var keyedService = CreateEncryptedClient(factory, new CustomerProvidedKey(key), encryptionScope: null);
        var keyedBlob = keyedService.GetBlobContainerClient(containerName).GetBlobClient("keyed.bin");
        await keyedBlob.UploadAsync(BinaryData.FromBytes(content), new BlobUploadOptions
        {
            Metadata = new Dictionary<string, string> { ["private"] = "metadata" }
        });
        var chunksAfterKeyed = Directory.GetFiles(Path.Combine(factory.DataPath, "chunks"), "*.chunk", SearchOption.AllDirectories).Length;
        var keyedProperties = (await keyedBlob.GetPropertiesAsync()).Value;
        Assert.Equal(expectedHash, keyedProperties.EncryptionKeySha256);
        Assert.Equal(content, (await keyedBlob.DownloadContentAsync()).Value.Content.ToArray());

        var missingKey = await Assert.ThrowsAsync<RequestFailedException>(() =>
            normalService.GetBlobContainerClient(containerName).GetBlobClient("keyed.bin").GetPropertiesAsync());
        Assert.Equal(409, missingKey.Status);
        Assert.Equal("CustomerProvidedKeyInUse", missingKey.ErrorCode);
        var wrongKeyService = CreateEncryptedClient(factory, new CustomerProvidedKey(wrongKey), encryptionScope: null);
        var mismatchedKey = await Assert.ThrowsAsync<RequestFailedException>(() =>
            wrongKeyService.GetBlobContainerClient(containerName).GetBlobClient("keyed.bin").DownloadContentAsync());
        Assert.Equal(409, mismatchedKey.Status);
        Assert.Equal("CustomerProvidedKeyInUse", mismatchedKey.ErrorCode);

        const string scope = "records-scope";
        var scopedService = CreateEncryptedClient(factory, customerProvidedKey: null, scope);
        var scopedBlob = scopedService.GetBlobContainerClient(containerName).GetBlobClient("scoped.bin");
        await scopedBlob.UploadAsync(BinaryData.FromBytes(content));
        var chunksAfterScoped = Directory.GetFiles(Path.Combine(factory.DataPath, "chunks"), "*.chunk", SearchOption.AllDirectories).Length;
        var scopedProperties = (await scopedBlob.GetPropertiesAsync()).Value;
        Assert.Equal(scope, scopedProperties.EncryptionScope);
        Assert.Equal(content, (await normalService.GetBlobContainerClient(containerName).GetBlobClient("scoped.bin").DownloadContentAsync()).Value.Content.ToArray());

        var listed = new List<BlobItem>();
        await foreach (var item in normalService.GetBlobContainerClient(containerName).GetBlobsAsync(BlobTraits.Metadata))
            listed.Add(item);
        var listedKeyed = Assert.Single(listed, item => item.Name == "keyed.bin");
        Assert.Equal(expectedHash, listedKeyed.Properties.CustomerProvidedKeySha256);
        Assert.Empty(listedKeyed.Metadata);
        Assert.Equal(scope, Assert.Single(listed, item => item.Name == "scoped.bin").Properties.EncryptionScope);
        Assert.True(chunksAfterKeyed > chunksBefore);
        Assert.True(chunksAfterScoped > chunksAfterKeyed);
        var metadataBytes = await File.ReadAllBytesAsync(Path.Combine(factory.DataPath, "metadata.db"));
        Assert.DoesNotContain(Convert.ToBase64String(key), Encoding.Latin1.GetString(metadataBytes), StringComparison.Ordinal);
    }

    [Fact]
    public async Task CustomerProvidedKeysFlowThroughSpecializedBlobOperations()
    {
        var key = RandomNumberGenerator.GetBytes(32);
        var service = CreateEncryptedClient(factory, new CustomerProvidedKey(key), encryptionScope: null);
        var container = service.GetBlobContainerClient($"cpk-specialized-{Guid.NewGuid():N}");
        await container.CreateAsync();

        var block = container.GetBlockBlobClient("blocks.bin");
        var blockId = Convert.ToBase64String(Encoding.UTF8.GetBytes("block-0001"));
        var blockBytes = RandomNumberGenerator.GetBytes(12 * 1024);
        await block.StageBlockAsync(blockId, new MemoryStream(blockBytes));
        await block.CommitBlockListAsync([blockId]);
        await block.SetMetadataAsync(new Dictionary<string, string> { ["protected"] = "true" });
        var snapshot = await block.CreateSnapshotAsync();
        Assert.Equal(blockBytes, (await block.WithSnapshot(snapshot.Value.Snapshot).DownloadContentAsync()).Value.Content.ToArray());

        var append = container.GetAppendBlobClient("append.bin");
        await append.CreateAsync();
        var appendBytes = RandomNumberGenerator.GetBytes(4096);
        await append.AppendBlockAsync(new MemoryStream(appendBytes));
        Assert.Equal(appendBytes, (await append.DownloadContentAsync()).Value.Content.ToArray());

        var page = container.GetPageBlobClient("page.bin");
        await page.CreateAsync(1024);
        var pageBytes = RandomNumberGenerator.GetBytes(1024);
        await page.UploadPagesAsync(new MemoryStream(pageBytes), offset: 0);
        var downloaded = (await page.DownloadContentAsync()).Value.Content.ToArray();
        Assert.Equal(pageBytes, downloaded);
        await page.ResizeAsync(512);
        Assert.Equal(512, (await page.GetPropertiesAsync()).Value.ContentLength);
        Assert.Equal(pageBytes[..512], (await page.DownloadContentAsync()).Value.Content.ToArray());
    }

    [Fact]
    public async Task MaintenanceExpiresUncommittedBlocksAndNeverReclaimsPinnedContent()
    {
        var client = CreateClient(factory);
        var containerName = $"maintenance-{Guid.NewGuid():N}";
        var container = client.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        var blobService = factory.Services.GetRequiredService<BlobService>();
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var chunkStore = factory.Services.GetRequiredService<ChunkStore>();

        var protectedBlob = container.GetBlobClient("active-reader.bin");
        var protectedBytes = RandomNumberGenerator.GetBytes(48 * 1024);
        await protectedBlob.UploadAsync(BinaryData.FromBytes(protectedBytes));
        var protectedRecord = await blobService.GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            containerName,
            protectedBlob.Name,
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None);
        var protectedChunkPaths = protectedRecord.Content.Chunks
            .Where(chunk => !chunk.Id.EndsWith("/$zero", StringComparison.Ordinal))
            .Select(chunk => ChunkPath(factory.DataPath, chunk.Id))
            .ToArray();

        using (chunkStore.Pin(protectedRecord.Content))
        {
            await protectedBlob.DeleteAsync();
            await blobService.RunMaintenanceAsync(CancellationToken.None);
            Assert.All(protectedChunkPaths, path => Assert.True(File.Exists(path)));
        }

        await blobService.RunMaintenanceAsync(CancellationToken.None);
        Assert.All(protectedChunkPaths, path => Assert.False(File.Exists(path)));

        var expiring = container.GetBlobClient("expiring.bin");
        await expiring.UploadAsync(BinaryData.FromBytes(RandomNumberGenerator.GetBytes(20 * 1024)));
        var expiringRecord = await blobService.GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            containerName,
            expiring.Name,
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None);
        await blobService.SetExpiryAsync(
            expiringRecord,
            DateTimeOffset.UtcNow.AddMilliseconds(100),
            CancellationToken.None);
        await Task.Delay(150);
        await blobService.RunMaintenanceAsync(CancellationToken.None);
        Assert.False((await expiring.ExistsAsync()).Value);

        var uncommitted = container.GetBlockBlobClient("uncommitted.bin");
        var blockId = Convert.ToBase64String("stale-block-0001"u8);
        await uncommitted.StageBlockAsync(blockId, new MemoryStream(RandomNumberGenerator.GetBytes(32 * 1024)));
        var staged = Assert.Single(await blobService.ListStagedBlocksAsync(
            SavaWebApplicationFactory.AccountName,
            containerName,
            uncommitted.Name,
            CancellationToken.None));
        await metadata.PutStagedBlockAsync(
            staged with { CreatedAt = DateTimeOffset.UtcNow.AddDays(-8) },
            CancellationToken.None);

        await blobService.RunMaintenanceAsync(CancellationToken.None);
        Assert.Empty(await blobService.ListStagedBlocksAsync(
            SavaWebApplicationFactory.AccountName,
            containerName,
            uncommitted.Name,
            CancellationToken.None));
        Assert.All(
            staged.Content.Chunks.Where(chunk => !chunk.Id.EndsWith("/$zero", StringComparison.Ordinal)),
            chunk => Assert.False(File.Exists(ChunkPath(factory.DataPath, chunk.Id))));
    }

    [Fact]
    public async Task MaintenanceReclaimsAbandonedStagingAndSurfacesChunkCorruption()
    {
        var blobService = factory.Services.GetRequiredService<BlobService>();
        var staging = Path.Combine(factory.DataPath, "staging");
        var abandonedPath = Path.Combine(staging, $"abandoned-{Guid.NewGuid():N}.tmp");
        var activePath = Path.Combine(staging, $"active-{Guid.NewGuid():N}.tmp");
        var freshPath = Path.Combine(staging, $"fresh-{Guid.NewGuid():N}.tmp");
        await File.WriteAllBytesAsync(abandonedPath, "abandoned"u8.ToArray());
        await File.WriteAllBytesAsync(freshPath, "fresh"u8.ToArray());
        File.SetLastWriteTimeUtc(abandonedPath, DateTime.UtcNow.AddDays(-2));

        await using (var active = new FileStream(
                         activePath,
                         FileMode.CreateNew,
                         FileAccess.ReadWrite,
                         FileShare.None,
                         bufferSize: 4096,
                         FileOptions.Asynchronous))
        {
            await active.WriteAsync("active"u8.ToArray());
            await active.FlushAsync();
            File.SetLastWriteTimeUtc(activePath, DateTime.UtcNow.AddDays(-2));

            await blobService.RunMaintenanceAsync(CancellationToken.None);
            Assert.False(File.Exists(abandonedPath));
            Assert.True(File.Exists(activePath));
            Assert.True(File.Exists(freshPath));
        }

        await blobService.RunMaintenanceAsync(CancellationToken.None);
        Assert.False(File.Exists(activePath));
        Assert.True(File.Exists(freshPath));

        var service = CreateClient(factory);
        var containerName = $"integrity-{Guid.NewGuid():N}";
        var container = service.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        var blob = container.GetBlobClient("corrupt.bin");
        await blob.UploadAsync(BinaryData.FromBytes(RandomNumberGenerator.GetBytes(48 * 1024)));
        var record = await blobService.GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            containerName,
            blob.Name,
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None);
        var chunk = record.Content.Chunks.First(item => !item.Id.EndsWith("/$zero", StringComparison.Ordinal));
        var chunkPath = ChunkPath(factory.DataPath, chunk.Id);
        await using (var file = new FileStream(
                         chunkPath,
                         FileMode.Open,
                         FileAccess.ReadWrite,
                         FileShare.None,
                         bufferSize: 4096,
                         FileOptions.Asynchronous))
        {
            file.Position = file.Length - 1;
            var value = file.ReadByte();
            Assert.NotEqual(-1, value);
            file.Position--;
            file.WriteByte((byte)(value ^ 0xff));
            file.Flush(flushToDisk: true);
        }

        await blobService.RunMaintenanceAsync(CancellationToken.None);
        using var operatorClient = factory.CreateClient();
        var unavailable = await operatorClient.GetAsync("/health/ready");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, unavailable.StatusCode);
        var metrics = await operatorClient.GetStringAsync("/metrics");
        Assert.Contains("mk8_sava_integrity_corrupt_chunks 1", metrics, StringComparison.Ordinal);
        Assert.Contains("mk8_sava_storage_physical_chunk_bytes", metrics, StringComparison.Ordinal);
        Assert.Contains("mk8_sava_http_request_duration_seconds_sum", metrics, StringComparison.Ordinal);

        await blob.DeleteAsync();
        await blobService.RunMaintenanceAsync(CancellationToken.None);
        Assert.False(File.Exists(chunkPath));
        var recovered = await operatorClient.GetAsync("/health/ready");
        Assert.Equal(HttpStatusCode.OK, recovered.StatusCode);

        var missing = container.GetBlobClient("missing.bin");
        await missing.UploadAsync(BinaryData.FromBytes(RandomNumberGenerator.GetBytes(40 * 1024)));
        var missingRecord = await blobService.GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            containerName,
            missing.Name,
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None);
        var missingChunk = missingRecord.Content.Chunks.First(item => !item.Id.EndsWith("/$zero", StringComparison.Ordinal));
        File.Delete(ChunkPath(factory.DataPath, missingChunk.Id));
        await blobService.RunMaintenanceAsync(CancellationToken.None);
        var missingMetrics = await operatorClient.GetStringAsync("/metrics");
        Assert.Contains("mk8_sava_integrity_missing_chunks 1", missingMetrics, StringComparison.Ordinal);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await operatorClient.GetAsync("/health/ready")).StatusCode);

        await missing.DeleteAsync();
        await blobService.RunMaintenanceAsync(CancellationToken.None);
        Assert.Equal(HttpStatusCode.OK, (await operatorClient.GetAsync("/health/ready")).StatusCode);

        File.Delete(freshPath);
    }

    [Fact]
    public async Task ConsistentBackupRestoresExactSharedSnapshotAndUncommittedContent()
    {
        var source = new SavaWebApplicationFactory();
        SavaWebApplicationFactory? restored = null;
        var backupPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-backup-{Guid.NewGuid():N}");
        var restoredPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-restored-{Guid.NewGuid():N}");
        try
        {
            await source.InitializeAsync();
            var service = CreateClient(source);
            var containerName = $"backup-{Guid.NewGuid():N}";
            var container = service.GetBlobContainerClient(containerName);
            await container.CreateAsync();
            var sharedBytes = Enumerable.Range(0, 96 * 1024).Select(index => (byte)(index % 251)).ToArray();
            var first = container.GetBlobClient("first.bin");
            var second = container.GetBlobClient("second.bin");
            await first.UploadAsync(BinaryData.FromBytes(sharedBytes));
            await second.UploadAsync(BinaryData.FromBytes(sharedBytes));
            var snapshot = (await first.CreateSnapshotAsync()).Value.Snapshot;

            var uncommitted = container.GetBlockBlobClient("uncommitted.bin");
            var blockId = Convert.ToBase64String("backup-block-0001"u8);
            var uncommittedBytes = RandomNumberGenerator.GetBytes(24 * 1024);
            await uncommitted.StageBlockAsync(blockId, new MemoryStream(uncommittedBytes));

            var customerKey = RandomNumberGenerator.GetBytes(32);
            var encrypted = CreateEncryptedClient(source, new CustomerProvidedKey(customerKey), encryptionScope: null)
                .GetBlobContainerClient(containerName)
                .GetBlobClient("customer-key.bin");
            var encryptedBytes = RandomNumberGenerator.GetBytes(20 * 1024);
            await encrypted.UploadAsync(BinaryData.FromBytes(encryptedBytes));

            var backup = source.Services.GetRequiredService<StorageBackupService>();
            var created = await backup.CreateAsync(backupPath, CancellationToken.None);
            Assert.True(created.BlobRecordCount >= 4);
            Assert.True(created.ChunkCount > 0);
            Assert.Equal(created, await backup.ValidateAsync(backupPath, CancellationToken.None));

            var options = source.Services
                .GetRequiredService<Microsoft.Extensions.Options.IOptions<Mk8.Sava.Configuration.SavaOptions>>()
                .Value;
            var wrongKeyOptions = new Mk8.Sava.Configuration.SavaOptions
            {
                DefaultAccount = SavaWebApplicationFactory.AccountName,
                Accounts = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    [SavaWebApplicationFactory.AccountName] = Convert.ToBase64String(RandomNumberGenerator.GetBytes(64))
                }
            };
            await Assert.ThrowsAsync<InvalidDataException>(() => StorageBackupService.ValidateBackupAsync(
                backupPath,
                wrongKeyOptions,
                CancellationToken.None));
            var existingTarget = Path.Combine(Path.GetTempPath(), $"mk8-sava-existing-{Guid.NewGuid():N}");
            Directory.CreateDirectory(existingTarget);
            try
            {
                await Assert.ThrowsAsync<IOException>(() => StorageBackupService.RestoreAsync(
                    backupPath,
                    existingTarget,
                    options,
                    CancellationToken.None));
            }
            finally
            {
                Directory.Delete(existingTarget);
            }
            var restoredBackup = await StorageBackupService.RestoreAsync(
                backupPath,
                restoredPath,
                options,
                CancellationToken.None);
            Assert.Equal(created.ChunkCount, restoredBackup.ChunkCount);
            Assert.Equal(created.ChunkCount, EnumerateChunkFiles(restoredPath).Count());
            Assert.Empty(Directory.EnumerateFiles(Path.Combine(restoredPath, "staging")));
            Assert.Equal(
                ["metadata.db"],
                Directory.EnumerateFiles(restoredPath).Select(Path.GetFileName).Order(StringComparer.Ordinal));

            restored = new SavaWebApplicationFactory(restoredPath);
            await restored.InitializeAsync();
            var restoredContainer = CreateClient(restored).GetBlobContainerClient(containerName);
            Assert.Equal(sharedBytes, (await restoredContainer.GetBlobClient(first.Name).DownloadContentAsync()).Value.Content.ToArray());
            Assert.Equal(sharedBytes, (await restoredContainer.GetBlobClient(second.Name).DownloadContentAsync()).Value.Content.ToArray());
            Assert.Equal(
                sharedBytes,
                (await restoredContainer.GetBlobClient(first.Name).WithSnapshot(snapshot).DownloadContentAsync()).Value.Content.ToArray());

            var restoredBlocks = await restoredContainer.GetBlockBlobClient(uncommitted.Name)
                .GetBlockListAsync(BlockListTypes.Uncommitted);
            var restoredBlock = Assert.Single(restoredBlocks.Value.UncommittedBlocks);
            Assert.Equal(blockId, restoredBlock.Name);
            Assert.Equal(uncommittedBytes.Length, restoredBlock.SizeLong);
            await restoredContainer.GetBlockBlobClient(uncommitted.Name).CommitBlockListAsync([blockId]);
            Assert.Equal(
                uncommittedBytes,
                (await restoredContainer.GetBlobClient(uncommitted.Name).DownloadContentAsync()).Value.Content.ToArray());
            var restoredEncrypted = CreateEncryptedClient(
                    restored,
                    new CustomerProvidedKey(customerKey),
                    encryptionScope: null)
                .GetBlobContainerClient(containerName)
                .GetBlobClient(encrypted.Name);
            Assert.Equal(encryptedBytes, (await restoredEncrypted.DownloadContentAsync()).Value.Content.ToArray());

            var backedUpChunk = Directory.EnumerateFiles(
                Path.Combine(backupPath, "chunks"),
                "*.chunk",
                SearchOption.AllDirectories).First();
            await using (var corrupt = new FileStream(
                             backedUpChunk,
                             FileMode.Open,
                             FileAccess.ReadWrite,
                             FileShare.None,
                             4096,
                             FileOptions.Asynchronous))
            {
                corrupt.Position = corrupt.Length - 1;
                var value = corrupt.ReadByte();
                Assert.NotEqual(-1, value);
                corrupt.Position--;
                corrupt.WriteByte((byte)(value ^ 0xff));
                corrupt.Flush(flushToDisk: true);
            }
            await Assert.ThrowsAsync<InvalidDataException>(() =>
                backup.ValidateAsync(backupPath, CancellationToken.None));
        }
        finally
        {
            if (restored is not null)
                await restored.DisposeAsync();
            await source.DisposeAsync();
            if (Directory.Exists(backupPath))
                Directory.Delete(backupPath, recursive: true);
            if (Directory.Exists(restoredPath))
                Directory.Delete(restoredPath, recursive: true);
        }
    }

    [Fact]
    public async Task SchemaOneMetadataMigratesAndBackfillsAuthoritativeChunkReferences()
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-v1-{Guid.NewGuid():N}");
        var backupPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-v1-backup-{Guid.NewGuid():N}");
        var legacyBackupPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-legacy-backup-{Guid.NewGuid():N}");
        var rejectedBackupPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-index-mismatch-{Guid.NewGuid():N}");
        const string containerName = "migrated-container";
        await CreateVersionOneDatabaseAsync(dataPath, containerName);
        await CreateVersionOneBackupAsync(dataPath, legacyBackupPath);
        var application = new SavaWebApplicationFactory(dataPath);
        try
        {
            await application.InitializeAsync();
            var metadata = application.Services.GetRequiredService<MetadataStore>();
            var configuredOptions = application.Services
                .GetRequiredService<Microsoft.Extensions.Options.IOptions<Mk8.Sava.Configuration.SavaOptions>>()
                .Value;
            var legacy = await StorageBackupService.ValidateBackupAsync(
                legacyBackupPath,
                configuredOptions,
                CancellationToken.None);
            Assert.Equal(1, legacy.BlobRecordCount);
            Assert.Equal(1, legacy.StagedBlockCount);
            var inventory = await metadata.GetStorageInventoryAsync(CancellationToken.None);
            Assert.Equal(1, inventory.BlobRecordCount);
            Assert.Equal(1, inventory.StagedBlockCount);
            Assert.Equal(1024, inventory.LogicalBlobBytes);
            Assert.Equal(512, inventory.LogicalStagedBlockBytes);
            Assert.Equal(
                new HashSet<string>([SavaWebApplicationFactory.AccountName + "/$zero"], StringComparer.Ordinal),
                inventory.ReachableChunkIds);

            var container = CreateClient(application).GetBlobContainerClient(containerName);
            Assert.Equal(
                new byte[1024],
                (await container.GetPageBlobClient("sparse.bin").DownloadContentAsync()).Value.Content.ToArray());
            var blocks = await container.GetBlockBlobClient("staged.bin").GetBlockListAsync(BlockListTypes.Uncommitted);
            var staged = Assert.Single(blocks.Value.UncommittedBlocks);
            Assert.Equal(Convert.ToBase64String("migrated-block"u8), staged.Name);
            Assert.Equal(512, staged.SizeLong);

            await using (var connection = new SqliteConnection($"Data Source={Path.Combine(dataPath, "metadata.db")}"))
            {
                await connection.OpenAsync();
                await using var version = connection.CreateCommand();
                version.CommandText = "PRAGMA user_version;";
                Assert.Equal(2L, Convert.ToInt64(await version.ExecuteScalarAsync(), CultureInfo.InvariantCulture));
                await using var references = connection.CreateCommand();
                references.CommandText = """
                    SELECT
                        (SELECT COUNT(*) FROM blob_chunk_references) +
                        (SELECT COUNT(*) FROM staged_block_chunk_references);
                    """;
                Assert.Equal(2L, Convert.ToInt64(await references.ExecuteScalarAsync(), CultureInfo.InvariantCulture));
            }

            var backup = application.Services.GetRequiredService<StorageBackupService>();
            var created = await backup.CreateAsync(backupPath, CancellationToken.None);
            Assert.Equal(1, created.BlobRecordCount);
            Assert.Equal(1, created.StagedBlockCount);
            Assert.Equal(created, await backup.ValidateAsync(backupPath, CancellationToken.None));

            await using (var connection = new SqliteConnection($"Data Source={Path.Combine(dataPath, "metadata.db")}"))
            {
                await connection.OpenAsync();
                await using var corruptIndex = connection.CreateCommand();
                corruptIndex.CommandText = """
                    DELETE FROM blob_chunk_references;
                    DELETE FROM staged_block_chunk_references;
                    """;
                await corruptIndex.ExecuteNonQueryAsync();
            }
            var mismatch = await Assert.ThrowsAsync<InvalidDataException>(() =>
                backup.CreateAsync(rejectedBackupPath, CancellationToken.None));
            Assert.Contains("chunk-reference index", mismatch.Message, StringComparison.Ordinal);
        }
        finally
        {
            await application.DisposeAsync();
            if (Directory.Exists(backupPath))
                Directory.Delete(backupPath, recursive: true);
            if (Directory.Exists(legacyBackupPath))
                Directory.Delete(legacyBackupPath, recursive: true);
            if (Directory.Exists(rejectedBackupPath))
                Directory.Delete(rejectedBackupPath, recursive: true);
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, recursive: true);
        }
    }

    [Fact]
    public async Task SoftDeleteRetentionSurvivesPolicyChangesAndExpiredRecordsArePurged()
    {
        var client = CreateClient(factory);
        var original = (await client.GetPropertiesAsync()).Value;
        var containerName = $"retention-{Guid.NewGuid():N}";
        var container = client.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        var blob = container.GetBlobClient("retained.bin");
        await blob.UploadAsync(BinaryData.FromBytes(RandomNumberGenerator.GetBytes(24 * 1024)));
        var blobService = factory.Services.GetRequiredService<BlobService>();
        var metadata = factory.Services.GetRequiredService<MetadataStore>();

        try
        {
            var enabled = (await client.GetPropertiesAsync()).Value;
            enabled.DeleteRetentionPolicy.Enabled = true;
            enabled.DeleteRetentionPolicy.Days = 1;
            await client.SetPropertiesAsync(enabled);
            await blob.DeleteAsync();

            var deleted = await metadata.GetBlobAsync(
                SavaWebApplicationFactory.AccountName,
                containerName,
                blob.Name,
                versionId: null,
                snapshot: null,
                includeDeleted: true,
                CancellationToken.None);
            Assert.NotNull(deleted?.DeleteRetentionUntil);
            Assert.InRange(
                deleted!.DeleteRetentionUntil!.Value - deleted.DeletedAt!.Value,
                TimeSpan.FromHours(23.9),
                TimeSpan.FromHours(24.1));

            var disabled = (await client.GetPropertiesAsync()).Value;
            disabled.DeleteRetentionPolicy.Enabled = false;
            await client.SetPropertiesAsync(disabled);
            await blob.UndeleteAsync();
            Assert.True((await blob.ExistsAsync()).Value);

            enabled = (await client.GetPropertiesAsync()).Value;
            enabled.DeleteRetentionPolicy.Enabled = true;
            enabled.DeleteRetentionPolicy.Days = 1;
            await client.SetPropertiesAsync(enabled);
            await blob.DeleteAsync();
            deleted = await metadata.GetBlobAsync(
                SavaWebApplicationFactory.AccountName,
                containerName,
                blob.Name,
                versionId: null,
                snapshot: null,
                includeDeleted: true,
                CancellationToken.None);
            Assert.NotNull(deleted);
            await metadata.PutBlobRecordAsync(
                deleted! with
                {
                    Revision = MetadataStore.NewRevision(),
                    DeleteRetentionUntil = DateTimeOffset.UtcNow.AddMinutes(-1)
                },
                deleted.Revision,
                CancellationToken.None);

            await blobService.RunMaintenanceAsync(CancellationToken.None);
            Assert.Null(await metadata.GetBlobAsync(
                SavaWebApplicationFactory.AccountName,
                containerName,
                blob.Name,
                versionId: null,
                snapshot: null,
                includeDeleted: true,
                CancellationToken.None));
        }
        finally
        {
            await client.SetPropertiesAsync(original);
        }
    }

    [Fact]
    public async Task SoftDeleteProtectsOverwritesAndVersioningRemovesTheCurrentVersionOnDelete()
    {
        var service = CreateClient(factory);
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var originalProperties = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            CancellationToken.None);
        var container = service.GetBlobContainerClient($"overwrite-retention-{Guid.NewGuid():N}");
        await container.CreateAsync();

        try
        {
            var softDelete = (await service.GetPropertiesAsync()).Value;
            softDelete.DeleteRetentionPolicy.Enabled = true;
            softDelete.DeleteRetentionPolicy.Days = 7;
            await service.SetPropertiesAsync(softDelete);
            var configured = await metadata.GetServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                CancellationToken.None);
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                configured with { VersioningEnabled = false },
                CancellationToken.None);

            var overwritten = container.GetBlockBlobClient("overwritten.txt");
            await overwritten.UploadAsync(new MemoryStream("before overwrite"u8.ToArray()));
            await overwritten.UploadAsync(new MemoryStream("after overwrite"u8.ToArray()));
            Assert.Equal("after overwrite", (await overwritten.DownloadContentAsync()).Value.Content.ToString());

            var deletedSnapshots = new List<BlobItem>();
            await foreach (var item in container.GetBlobsAsync(
                               states: BlobStates.Deleted | BlobStates.Snapshots,
                               prefix: overwritten.Name))
            {
                if (item.Deleted && item.Snapshot is not null)
                    deletedSnapshots.Add(item);
            }
            var overwrittenSnapshot = Assert.Single(deletedSnapshots);

            await overwritten.UndeleteAsync();
            var restoredSnapshot = overwritten.WithSnapshot(overwrittenSnapshot.Snapshot);
            Assert.Equal("before overwrite", (await restoredSnapshot.DownloadContentAsync()).Value.Content.ToString());
            Assert.Equal("after overwrite", (await overwritten.DownloadContentAsync()).Value.Content.ToString());

            var recreated = container.GetBlockBlobClient("recreated.txt");
            await recreated.UploadAsync(new MemoryStream("soft-deleted original"u8.ToArray()));
            await recreated.DeleteAsync();
            await recreated.UploadAsync(new MemoryStream("replacement"u8.ToArray()));
            Assert.Equal("replacement", (await recreated.DownloadContentAsync()).Value.Content.ToString());
            await recreated.UndeleteAsync();

            BlobItem? recreatedSnapshot = null;
            await foreach (var item in container.GetBlobsAsync(
                               states: BlobStates.Snapshots,
                               prefix: recreated.Name))
            {
                if (item.Name == recreated.Name && item.Snapshot is not null)
                    recreatedSnapshot = item;
            }
            Assert.NotNull(recreatedSnapshot);
            Assert.Equal(
                "soft-deleted original",
                (await recreated.WithSnapshot(recreatedSnapshot!.Snapshot).DownloadContentAsync()).Value.Content.ToString());

            var typeChangedName = "type-changed";
            var typeChangedAppend = container.GetAppendBlobClient(typeChangedName);
            await typeChangedAppend.CreateAsync();
            await typeChangedAppend.AppendBlockAsync(new MemoryStream("append state"u8.ToArray()));
            await typeChangedAppend.DeleteAsync();
            var typeChangedBlock = container.GetBlockBlobClient(typeChangedName);
            await typeChangedBlock.UploadAsync(new MemoryStream("block replacement"u8.ToArray()));
            var retainedDifferentType = new List<BlobItem>();
            await foreach (var item in container.GetBlobsAsync(
                               states: BlobStates.Deleted | BlobStates.Snapshots,
                               prefix: typeChangedName))
            {
                if (item.Name == typeChangedName && item.Deleted)
                    retainedDifferentType.Add(item);
            }
            Assert.Empty(retainedDifferentType);

            configured = await metadata.GetServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                CancellationToken.None);
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                configured with { VersioningEnabled = true },
                CancellationToken.None);
            var versioned = container.GetBlockBlobClient("versioned.txt");
            await versioned.UploadAsync(new MemoryStream("version one"u8.ToArray()));
            await versioned.UploadAsync(new MemoryStream("version two"u8.ToArray()));
            await versioned.SetMetadataAsync(new Dictionary<string, string> { ["revision"] = "metadata-write" });
            var versionedSnapshot = await versioned.CreateSnapshotAsync(
                new Dictionary<string, string> { ["snapshot"] = "override" });
            Assert.False(string.IsNullOrEmpty(versionedSnapshot.Value.VersionId));
            var snapshotProperties = await versioned.WithSnapshot(versionedSnapshot.Value.Snapshot).GetPropertiesAsync();
            Assert.Equal("override", snapshotProperties.Value.Metadata["snapshot"]);
            await versioned.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots);
            Assert.False((await versioned.ExistsAsync()).Value);

            var versions = new List<BlobItem>();
            await foreach (var item in container.GetBlobsAsync(states: BlobStates.Version, prefix: versioned.Name))
            {
                if (item.Name == versioned.Name && item.VersionId is not null)
                    versions.Add(item);
            }
            Assert.Equal(4, versions.Count);
            Assert.All(versions, item => Assert.False(item.Deleted));
            Assert.All(versions, item => Assert.False(item.IsLatestVersion));
            var contents = new HashSet<string>(StringComparer.Ordinal);
            foreach (var version in versions)
            {
                contents.Add((await versioned.WithVersion(version.VersionId).DownloadContentAsync()).Value.Content.ToString());
            }
            Assert.Equal(new HashSet<string>(["version one", "version two"], StringComparer.Ordinal), contents);
        }
        finally
        {
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                originalProperties,
                CancellationToken.None);
        }
    }

    [Fact]
    public async Task ConcurrentPublicationAndReclamationPreserveEveryAcknowledgedBlob()
    {
        var client = CreateClient(factory);
        var container = client.GetBlobContainerClient($"gc-race-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var content = RandomNumberGenerator.GetBytes(96 * 1024);
        var blobService = factory.Services.GetRequiredService<BlobService>();
        using var stop = new CancellationTokenSource();
        var sweeper = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    stop.Token.ThrowIfCancellationRequested();
                    await blobService.CollectGarbageAsync(stop.Token);
                    await Task.Yield();
                }
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
            }
        });

        try
        {
            await Task.WhenAll(Enumerable.Range(0, 16).Select(index =>
                container.GetBlobClient($"published-{index:D2}.bin").UploadAsync(BinaryData.FromBytes(content))));
        }
        finally
        {
            await stop.CancelAsync();
            await sweeper;
        }

        foreach (var index in Enumerable.Range(0, 16))
        {
            var downloaded = await container.GetBlobClient($"published-{index:D2}.bin").DownloadContentAsync();
            Assert.Equal(content, downloaded.Value.Content.ToArray());
        }
    }

    [Fact]
    public async Task TransactionalCrc64IsValidatedBeforePublication()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"crc-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var content = Enumerable.Range(0, 64 * 1024).Select(index => (byte)(index % 239)).ToArray();
        var valid = container.GetBlobClient("valid.bin");
        await valid.UploadAsync(new MemoryStream(content), new BlobUploadOptions
        {
            TransferValidation = new UploadTransferValidationOptions
            {
                ChecksumAlgorithm = StorageChecksumAlgorithm.StorageCrc64
            }
        });
        Assert.Equal(content, (await valid.DownloadContentAsync()).Value.Content.ToArray());

        var invalid = container.GetBlobClient("invalid.bin");
        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() =>
            invalid.UploadAsync(new MemoryStream(content), new BlobUploadOptions
            {
                TransferValidation = new UploadTransferValidationOptions
                {
                    ChecksumAlgorithm = StorageChecksumAlgorithm.StorageCrc64,
                    PrecalculatedChecksum = new byte[8]
                }
            }));
        Assert.Equal(400, rejected.Status);
        Assert.Equal("Crc64Mismatch", rejected.ErrorCode);
        Assert.False((await invalid.ExistsAsync()).Value);
    }

    [Fact]
    public void StorageCrc64MatchesTheAzureSdkImplementation()
    {
        var content = Enumerable.Range(0, 4_097).Select(index => (byte)(index % 233)).ToArray();
        var implementationType = typeof(StorageSharedKeyCredential).Assembly.GetType("Azure.Storage.StorageCrc64HashAlgorithm")
                                 ?? throw new InvalidOperationException("The Azure SDK CRC64 implementation was not found.");
        var constructor = implementationType.GetConstructors(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            .Single();
        var arguments = constructor.GetParameters()
            .Select(parameter => parameter.ParameterType == typeof(ulong) ? (object)0UL : throw new InvalidOperationException(parameter.ParameterType.FullName))
            .ToArray();
        var sdk = constructor.Invoke(arguments);
        var methods = implementationType.GetMethods(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic);
        var append = methods.Single(method =>
            method.Name == "Append" &&
            method.GetParameters() is [{ ParameterType: var parameterType }] &&
            parameterType == typeof(byte[]));
        var getHash = methods.Single(method => method.Name == "GetHashAndReset" && method.GetParameters().Length == 0);
        append.Invoke(sdk, [content]);
        var expected = (byte[]?)getHash.Invoke(sdk, null)
                       ?? throw new InvalidOperationException("The Azure SDK CRC64 implementation returned no hash.");
        var actual = new Mk8.Sava.Protocol.StorageCrc64();
        actual.Append(content.AsSpan(0, 1_111));
        actual.Append(content.AsSpan(1_111));
        Assert.Equal(expected, actual.GetHash());
    }

    [Fact]
    public async Task AccountPolicyBlocksPublicContainers()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"private-{Guid.NewGuid():N}");
        await container.CreateAsync();

        var denied = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.SetAccessPolicyAsync(PublicAccessType.Blob));
        Assert.Equal("PublicAccessNotPermitted", denied.ErrorCode);
        Assert.Equal(409, denied.Status);
    }

    [Fact]
    public async Task CorsAndConditionalResponsesMatchHttpSemantics()
    {
        var service = CreateClient(factory);
        var properties = (await service.GetPropertiesAsync()).Value;
        properties.Cors.Clear();
        properties.Cors.Add(new BlobCorsRule
        {
            AllowedOrigins = "https://client.example",
            AllowedMethods = "GET,HEAD",
            AllowedHeaders = "*",
            ExposedHeaders = "ETag,x-ms-request-id",
            MaxAgeInSeconds = 120
        });
        await service.SetPropertiesAsync(properties);

        var containerName = $"cors-{Guid.NewGuid():N}";
        var blobName = "conditional.txt";
        var container = service.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        var blob = container.GetBlobClient(blobName);
        await blob.UploadAsync(BinaryData.FromString("conditional payload"));
        var etag = (await blob.GetPropertiesAsync()).Value.ETag;

        var sasBuilder = new BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = blobName,
            Resource = "b",
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp
        };
        sasBuilder.SetPermissions(BlobSasPermissions.Read);
        var credential = new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);
        var sas = sasBuilder.ToSasQueryParameters(credential);
        var uri = new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}?{sas}");
        using var client = new HttpClient(factory.Server.CreateHandler());

        using var corsRequest = new HttpRequestMessage(HttpMethod.Get, uri);
        corsRequest.Headers.Add("Origin", "https://client.example");
        using var corsResponse = await client.SendAsync(corsRequest);
        Assert.Equal(HttpStatusCode.OK, corsResponse.StatusCode);
        Assert.Equal("https://client.example", corsResponse.Headers.GetValues("Access-Control-Allow-Origin").Single());
        Assert.Contains("ETag", corsResponse.Headers.GetValues("Access-Control-Expose-Headers").Single());

        using var conditionalRequest = new HttpRequestMessage(HttpMethod.Get, uri);
        conditionalRequest.Headers.IfNoneMatch.Add(new System.Net.Http.Headers.EntityTagHeaderValue(etag.ToString()));
        using var conditionalResponse = await client.SendAsync(conditionalRequest);
        Assert.Equal(HttpStatusCode.NotModified, conditionalResponse.StatusCode);
        Assert.False(conditionalResponse.Headers.Contains("x-ms-error-code"));
        Assert.Empty(await conditionalResponse.Content.ReadAsByteArrayAsync());
    }

    [Fact]
    public async Task QueryBlobContentsStreamsSdkCompatibleAvroForCsvAndJsonResults()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"query-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlockBlobClient("rows.csv");
        await blob.UploadAsync(
            new MemoryStream("100,200,300,400\n300,400,500,600\n"u8.ToArray()),
            new BlobUploadOptions { Tags = new Dictionary<string, string> { ["kind"] = "query" } });
        var properties = await blob.GetPropertiesAsync();

        var progress = new CaptureProgress();
        var csvResponse = await blob.QueryAsync(
            "SELECT _2 FROM BlobStorage WHERE _1 > 250;",
            new BlobQueryOptions
            {
                Conditions = new BlobRequestConditions
                {
                    IfMatch = properties.Value.ETag,
                    TagConditions = "\"kind\" = 'query'"
                },
                ProgressHandler = progress
            });
        using (var reader = new StreamReader(csvResponse.Value.Content))
            Assert.Equal("400\n", await reader.ReadToEndAsync());
        Assert.Equal(200, csvResponse.GetRawResponse().Status);
        Assert.Equal(properties.Value.ETag, csvResponse.Value.Details.ETag);
        Assert.Equal([32L, 32L], progress.Values);

        var jsonResponse = await blob.QueryAsync(
            "SELECT _2 FROM BlobStorage WHERE _1 >= 300;",
            new BlobQueryOptions
            {
                InputTextConfiguration = new BlobQueryCsvTextOptions
                {
                    ColumnSeparator = ",",
                    QuotationCharacter = '"',
                    EscapeCharacter = '\\',
                    RecordSeparator = "\n"
                },
                OutputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "\n" }
            });
        using var jsonReader = new StreamReader(jsonResponse.Value.Content);
        Assert.Equal("{\"_1\":\"400\"}\n", await jsonReader.ReadToEndAsync());

        var append = container.GetAppendBlobClient("not-queryable");
        await append.CreateAsync();
        var invalidType = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlockBlobClient("not-queryable").QueryAsync("SELECT * FROM BlobStorage"));
        Assert.Equal(409, invalidType.Status);
        Assert.Equal("InvalidBlobType", invalidType.ErrorCode);
    }

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory app) =>
        CreateClient(app, SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);

    private static async Task CreateVersionOneDatabaseAsync(string dataPath, string containerName)
    {
        Directory.CreateDirectory(dataPath);
        var now = DateTimeOffset.UtcNow;
        var domain = SavaWebApplicationFactory.AccountName;
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        options.Converters.Add(new JsonStringEnumConverter());
        var container = new ContainerRecord
        {
            Account = SavaWebApplicationFactory.AccountName,
            Name = containerName,
            Revision = MetadataStore.NewRevision(),
            ETag = MetadataStore.NewETag(),
            CreatedAt = now,
            LastModified = now
        };
        var blob = new BlobRecord
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = containerName,
            Name = "sparse.bin",
            GenerationId = Guid.NewGuid().ToString("N"),
            Revision = MetadataStore.NewRevision(),
            IsCurrent = true,
            Kind = BlobKind.PageBlob,
            Content = new ContentManifest(
                domain,
                1024,
                ContentManifest.SparseHash,
                [new ChunkReference(domain + "/$zero", 0, 1024)]),
            ETag = MetadataStore.NewETag(),
            CreatedAt = now,
            LastModified = now
        };
        var block = new StagedBlockRecord
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = containerName,
            BlobName = "staged.bin",
            BlockId = Convert.ToBase64String("migrated-block"u8),
            Content = new ContentManifest(
                domain,
                512,
                ContentManifest.SparseHash,
                [new ChunkReference(domain + "/$zero", 0, 512)]),
            CreatedAt = now
        };

        await using var connection = new SqliteConnection($"Data Source={Path.Combine(dataPath, "metadata.db")}");
        await connection.OpenAsync();
        await using (var schema = connection.CreateCommand())
        {
            schema.CommandText = """
                CREATE TABLE containers (
                    account TEXT NOT NULL,
                    name TEXT NOT NULL,
                    deleted INTEGER NOT NULL,
                    modified_ticks INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    PRIMARY KEY (account, name)
                );
                CREATE TABLE blobs (
                    generation_id TEXT PRIMARY KEY,
                    account TEXT NOT NULL,
                    container TEXT NOT NULL,
                    name TEXT NOT NULL,
                    version_id TEXT NULL,
                    snapshot TEXT NULL,
                    is_current INTEGER NOT NULL,
                    is_deleted INTEGER NOT NULL,
                    modified_ticks INTEGER NOT NULL,
                    data TEXT NOT NULL
                );
                CREATE TABLE staged_blocks (
                    account TEXT NOT NULL,
                    container TEXT NOT NULL,
                    blob_name TEXT NOT NULL,
                    block_id TEXT NOT NULL,
                    created_ticks INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    PRIMARY KEY (account, container, blob_name, block_id)
                );
                CREATE TABLE service_properties (
                    account TEXT PRIMARY KEY,
                    data TEXT NOT NULL
                );
                PRAGMA user_version=1;
                """;
            await schema.ExecuteNonQueryAsync();
        }
        await using (var insert = connection.CreateCommand())
        {
            insert.CommandText = """
                INSERT INTO containers(account, name, deleted, modified_ticks, data)
                VALUES ($account, $name, 0, $modified, $data);
                INSERT INTO blobs(
                    generation_id, account, container, name, version_id, snapshot,
                    is_current, is_deleted, modified_ticks, data)
                VALUES ($generation, $account, $name, $blob, NULL, NULL, 1, 0, $modified, $blob_data);
                INSERT INTO staged_blocks(account, container, blob_name, block_id, created_ticks, data)
                VALUES ($account, $name, $staged_blob, $block, $modified, $block_data);
                """;
            insert.Parameters.AddWithValue("$account", SavaWebApplicationFactory.AccountName);
            insert.Parameters.AddWithValue("$name", containerName);
            insert.Parameters.AddWithValue("$modified", now.UtcTicks);
            insert.Parameters.AddWithValue("$data", JsonSerializer.Serialize(container, options));
            insert.Parameters.AddWithValue("$generation", blob.GenerationId);
            insert.Parameters.AddWithValue("$blob", blob.Name);
            insert.Parameters.AddWithValue("$blob_data", JsonSerializer.Serialize(blob, options));
            insert.Parameters.AddWithValue("$staged_blob", block.BlobName);
            insert.Parameters.AddWithValue("$block", block.BlockId);
            insert.Parameters.AddWithValue("$block_data", JsonSerializer.Serialize(block, options));
            await insert.ExecuteNonQueryAsync();
        }
    }

    private static async Task CreateVersionOneBackupAsync(string dataPath, string backupPath)
    {
        Directory.CreateDirectory(backupPath);
        Directory.CreateDirectory(Path.Combine(backupPath, "chunks"));
        var source = Path.Combine(dataPath, "metadata.db");
        var destination = Path.Combine(backupPath, "metadata.db");
        File.Copy(source, destination);
        var metadata = await File.ReadAllBytesAsync(destination);
        var manifest = new
        {
            format = "mk8.sava.backup",
            formatVersion = 1,
            metadataSchemaVersion = 1,
            createdAt = DateTimeOffset.UtcNow,
            metadata = new
            {
                length = metadata.LongLength,
                sha256 = Convert.ToHexStringLower(SHA256.HashData(metadata))
            },
            blobRecordCount = 1,
            stagedBlockCount = 1,
            logicalBlobBytes = 1024,
            logicalStagedBlockBytes = 512,
            keyRequirements = new Dictionary<string, string>(),
            chunks = Array.Empty<object>()
        };
        await File.WriteAllBytesAsync(
            Path.Combine(backupPath, "backup-manifest.json"),
            JsonSerializer.SerializeToUtf8Bytes(manifest, new JsonSerializerOptions(JsonSerializerDefaults.Web)
            {
                WriteIndented = true
            }));
    }

    private static string ListIdentity(BlobRecord item) =>
        $"{item.Name}|{item.VersionId}|{item.Snapshot}|{(item.VersionId is null ? null : item.IsCurrent)}";

    private static string ListIdentity(BlobItem item) =>
        $"{item.Name}|{item.VersionId}|{item.Snapshot}|{item.IsLatestVersion}";

    private static HttpRequestMessage CreateSourceKeyRequest(
        Uri destination,
        byte[] sourceKey,
        byte[] sourceHash,
        string version = "2026-02-06",
        string sourceUri = "https://source.example/encrypted")
    {
        var request = new HttpRequestMessage(HttpMethod.Put, destination)
        {
            Content = new ByteArrayContent([])
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        request.Headers.TryAddWithoutValidation("x-ms-copy-source", sourceUri);
        request.Headers.TryAddWithoutValidation("x-ms-source-encryption-key", Convert.ToBase64String(sourceKey));
        request.Headers.TryAddWithoutValidation("x-ms-source-encryption-key-sha256", Convert.ToBase64String(sourceHash));
        request.Headers.TryAddWithoutValidation("x-ms-source-encryption-algorithm", "AES256");
        return request;
    }

    private static void AddCustomerKeyHeaders(HttpRequestMessage request, byte[] key, byte[] hash)
    {
        request.Headers.TryAddWithoutValidation("x-ms-encryption-key", Convert.ToBase64String(key));
        request.Headers.TryAddWithoutValidation("x-ms-encryption-key-sha256", Convert.ToBase64String(hash));
        request.Headers.TryAddWithoutValidation("x-ms-encryption-algorithm", "AES256");
    }

    private static Uri HttpsSasUri(BlobBaseClient blob, BlobSasPermissions permissions)
    {
        var builder = new UriBuilder(blob.GenerateSasUri(permissions, DateTimeOffset.UtcNow.AddMinutes(10)))
        {
            Scheme = Uri.UriSchemeHttps,
            Port = -1
        };
        return builder.Uri;
    }

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

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory app, string accountName, string accountKey)
    {
        var transportClient = new HttpClient(app.Server.CreateHandler())
        {
            BaseAddress = new Uri($"http://{accountName}.localhost")
        };
        var options = new BlobClientOptions
        {
            Transport = new HttpClientTransport(transportClient),
            Retry = { MaxRetries = 0 }
        };
        return new BlobServiceClient(
            new Uri($"http://{accountName}.localhost"),
            new StorageSharedKeyCredential(accountName, accountKey),
            options);
    }

    private static BlobClient CreateBlobClient(SavaWebApplicationFactory app, Uri uri) =>
        new(uri, new BlobClientOptions
        {
            Transport = new HttpClientTransport(new HttpClient(app.Server.CreateHandler()) { BaseAddress = uri }),
            Retry = { MaxRetries = 0 }
        });

    private static BlobServiceClient CreateEncryptedClient(
        SavaWebApplicationFactory app,
        CustomerProvidedKey? customerProvidedKey,
        string? encryptionScope)
    {
        var endpoint = new Uri($"https://{SavaWebApplicationFactory.AccountName}.localhost");
        var options = new BlobClientOptions
        {
            Transport = new HttpClientTransport(new HttpClient(app.Server.CreateHandler()) { BaseAddress = endpoint }),
            CustomerProvidedKey = customerProvidedKey,
            EncryptionScope = encryptionScope,
            Retry = { MaxRetries = 0 }
        };
        return new BlobServiceClient(
            endpoint,
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            options);
    }

    private static BlobServiceClient CreateBearerClient(SavaWebApplicationFactory app, string token)
    {
        var endpoint = new Uri($"https://{SavaWebApplicationFactory.AccountName}.localhost");
        return new BlobServiceClient(endpoint, new StaticTokenCredential(token), new BlobClientOptions
        {
            Transport = new HttpClientTransport(new HttpClient(app.Server.CreateHandler()) { BaseAddress = endpoint }),
            Retry = { MaxRetries = 0 }
        });
    }

    private static string CreateJwt(string base64Key, string objectId, string? tenantId = null)
    {
        var key = new SymmetricSecurityKey(Convert.FromBase64String(base64Key)) { KeyId = "test-key" };
        var claims = new List<Claim> { new("oid", objectId) };
        if (tenantId is not null)
            claims.Add(new Claim("tid", tenantId));
        var token = new JwtSecurityToken(
            issuer: "https://issuer.mk8.test",
            audience: "https://storage.azure.com/",
            claims: claims,
            notBefore: DateTime.UtcNow.AddMinutes(-1),
            expires: DateTime.UtcNow.AddMinutes(10),
            signingCredentials: new SigningCredentials(key, SecurityAlgorithms.HmacSha256));
        return new JwtSecurityTokenHandler().WriteToken(token);
    }

    private sealed class StaticTokenCredential(string token) : TokenCredential
    {
        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            new(token, DateTimeOffset.UtcNow.AddMinutes(10));

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            ValueTask.FromResult(GetToken(requestContext, cancellationToken));
    }

    private sealed class CaptureProgress : IProgress<long>
    {
        public List<long> Values { get; } = [];

        public void Report(long value) => Values.Add(value);
    }

    private static IEnumerable<FileInfo> EnumerateChunkFiles(string dataPath) =>
        Directory.EnumerateFiles(Path.Combine(dataPath, "chunks"), "*.chunk", SearchOption.AllDirectories)
            .Select(path => new FileInfo(path));

    private static string ChunkPath(string dataPath, string chunkId) =>
        Path.Combine(dataPath, "chunks", chunkId.Replace('/', Path.DirectorySeparatorChar) + ".chunk");

    private sealed class EncryptedSourceHandler(byte[] content, byte[] key, byte[] hash) : HttpMessageHandler
    {
        private readonly string _encodedKey = Convert.ToBase64String(key);
        private readonly string _encodedHash = Convert.ToBase64String(hash);
        private int _requestCount;

        public int RequestCount => Volatile.Read(ref _requestCount);

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Assert.Equal(Uri.UriSchemeHttps, request.RequestUri?.Scheme);
            Assert.Equal(_encodedKey, request.Headers.GetValues("x-ms-encryption-key").Single());
            Assert.Equal(_encodedHash, request.Headers.GetValues("x-ms-encryption-key-sha256").Single());
            Assert.Equal("AES256", request.Headers.GetValues("x-ms-encryption-algorithm").Single());
            Assert.Equal("2026-02-06", request.Headers.GetValues("x-ms-version").Single());
            Assert.False(request.Headers.Contains("x-ms-source-encryption-key"));
            Interlocked.Increment(ref _requestCount);

            var start = 0;
            var end = content.Length - 1;
            var status = HttpStatusCode.OK;
            var range = request.Headers.Range?.Ranges.SingleOrDefault();
            if (range is not null)
            {
                start = checked((int)(range.From ?? 0));
                end = checked((int)(range.To ?? end));
                Assert.InRange(start, 0, content.Length - 1);
                Assert.InRange(end, start, content.Length - 1);
                status = HttpStatusCode.PartialContent;
            }

            var response = new HttpResponseMessage(status)
            {
                Content = new ByteArrayContent(content[start..(end + 1)]),
                RequestMessage = request
            };
            response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
            if (status == HttpStatusCode.PartialContent)
                response.Content.Headers.ContentRange = new ContentRangeHeaderValue(start, end, content.Length);
            response.Headers.ETag = new EntityTagHeaderValue("\"encrypted-source\"");
            return Task.FromResult(response);
        }
    }

    private sealed class LoopbackSource(WebApplication application, Uri uri) : IAsyncDisposable
    {
        public Uri Uri { get; } = uri;

        public static async Task<LoopbackSource> StartAsync(byte[] content)
        {
            var builder = WebApplication.CreateSlimBuilder();
            builder.WebHost.ConfigureKestrel(server => server.Listen(IPAddress.Loopback, 0));
            var application = builder.Build();
            application.MapGet("/source", async context =>
            {
                var start = 0;
                var end = content.Length - 1;
                var range = context.Request.Headers.Range.ToString();
                if (!string.IsNullOrEmpty(range))
                {
                    var bounds = range[6..].Split('-', 2);
                    start = int.Parse(bounds[0], CultureInfo.InvariantCulture);
                    end = string.IsNullOrEmpty(bounds[1])
                        ? end
                        : int.Parse(bounds[1], CultureInfo.InvariantCulture);
                    if (start < 0 || end < start || end >= content.Length)
                    {
                        context.Response.StatusCode = StatusCodes.Status416RangeNotSatisfiable;
                        return;
                    }
                    context.Response.StatusCode = StatusCodes.Status206PartialContent;
                    context.Response.Headers.ContentRange = $"bytes {start}-{end}/{content.Length}";
                }
                context.Response.ContentType = "application/x-url-source";
                context.Response.ContentLength = end - start + 1;
                context.Response.Headers.ETag = "\"source-etag\"";
                await context.Response.Body.WriteAsync(content.AsMemory(start, end - start + 1));
            });
            await application.StartAsync();
            var addresses = application.Services
                .GetRequiredService<IServer>()
                .Features
                .Get<IServerAddressesFeature>()
                ?.Addresses;
            var address = addresses?.Single() ?? throw new InvalidOperationException("The source server did not publish an address.");
            return new LoopbackSource(application, new Uri(new Uri(address), "/source"));
        }

        public async ValueTask DisposeAsync()
        {
            await application.StopAsync();
            await application.DisposeAsync();
        }
    }
}
