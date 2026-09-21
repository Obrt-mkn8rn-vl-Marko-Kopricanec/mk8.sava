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
using System.Security.Cryptography;
using System.Security.Claims;
using System.Text;
using Microsoft.IdentityModel.Tokens;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
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
        var pageBytes = RandomNumberGenerator.GetBytes(512);
        await page.UploadPagesAsync(new MemoryStream(pageBytes), offset: 512);
        var downloaded = (await page.DownloadContentAsync()).Value.Content.ToArray();
        Assert.Equal(new byte[512], downloaded[..512]);
        Assert.Equal(pageBytes, downloaded[512..]);
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

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory app) =>
        CreateClient(app, SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);

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

    private static IEnumerable<FileInfo> EnumerateChunkFiles(string dataPath) =>
        Directory.EnumerateFiles(Path.Combine(dataPath, "chunks"), "*.chunk", SearchOption.AllDirectories)
            .Select(path => new FileInfo(path));

    private static string ChunkPath(string dataPath, string chunkId) =>
        Path.Combine(dataPath, "chunks", chunkId.Replace('/', Path.DirectorySeparatorChar) + ".chunk");

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
