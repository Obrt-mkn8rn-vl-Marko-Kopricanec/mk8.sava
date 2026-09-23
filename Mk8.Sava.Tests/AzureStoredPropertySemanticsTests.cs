using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;

namespace Mk8.Sava.Tests;

public sealed class AzureStoredPropertySemanticsTests(SavaWebApplicationFactory factory)
    : IClassFixture<SavaWebApplicationFactory>
{
    [Fact]
    public async Task PutBlobReplacesPropertiesAndPersistsGeneratedOrExplicitMd5()
    {
        var service = CreateClient();
        var container = service.GetBlobContainerClient($"stored-properties-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlockBlobClient("put.bin");
        await blob.UploadAsync(
            BinaryData.FromString("old payload").ToStream(),
            new BlobUploadOptions
            {
                HttpHeaders = FullHeaders("application/x-old"),
                Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["old"] = "metadata" },
                Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["old"] = "tag" }
            });

        var firstPayload = "replacement with standard properties"u8.ToArray();
        using var transport = new HttpClient(factory.Server.CreateHandler());
        using (var request = PutBlobRequest(blob, firstPayload))
        {
            request.Content!.Headers.ContentType = MediaTypeHeaderValue.Parse("application/x-standard");
            request.Content.Headers.ContentEncoding.Add("gzip");
            request.Content.Headers.ContentLanguage.Add("hr-HR");
            request.Headers.CacheControl = CacheControlHeaderValue.Parse("private, max-age=120");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            Assert.Equal(AzureProtocolChecksum.Md5(firstPayload), response.Content.Headers.ContentMD5);
        }

        var standard = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal("application/x-standard", standard.ContentType);
        Assert.Equal("gzip", standard.ContentEncoding);
        Assert.Equal("hr-HR", standard.ContentLanguage);
        Assert.Equal("max-age=120, private", standard.CacheControl);
        Assert.Null(standard.ContentDisposition);
        Assert.Equal(AzureProtocolChecksum.Md5(firstPayload), standard.ContentHash);
        Assert.Empty(standard.Metadata);
        Assert.Empty((await blob.GetTagsAsync()).Value.Tags);

        var secondPayload = "custom md5 takes precedence"u8.ToArray();
        using (var request = PutBlobRequest(blob, secondPayload))
        {
            request.Content!.Headers.ContentType = MediaTypeHeaderValue.Parse("application/x-custom-md5");
            request.Content.Headers.ContentMD5 = AzureProtocolChecksum.Md5("different transport hash"u8);
            request.Headers.TryAddWithoutValidation(
                "x-ms-blob-content-md5",
                Convert.ToBase64String(AzureProtocolChecksum.Md5(secondPayload)));
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }

        var custom = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal("application/x-custom-md5", custom.ContentType);
        Assert.Null(custom.ContentEncoding);
        Assert.Null(custom.ContentLanguage);
        Assert.Null(custom.CacheControl);
        Assert.Null(custom.ContentDisposition);
        Assert.Equal(AzureProtocolChecksum.Md5(secondPayload), custom.ContentHash);
    }

    [Fact]
    public async Task BlockListAndSetPropertiesUseAzureReplacementSemantics()
    {
        var service = CreateClient();
        var container = service.GetBlobContainerClient($"replace-properties-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlockBlobClient("blocks.bin");
        await blob.UploadAsync(
            BinaryData.FromString("old block payload").ToStream(),
            new BlobUploadOptions
            {
                HttpHeaders = FullHeaders("application/x-old-block"),
                Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["old"] = "metadata" }
            });

        var blockId = Convert.ToBase64String("replacement-block-0001"u8);
        await blob.StageBlockAsync(blockId, BinaryData.FromString("new block payload").ToStream());
        var blockList = $"<BlockList><Latest>{blockId}</Latest></BlockList>";
        using var transport = new HttpClient(factory.Server.CreateHandler());
        using (var request = new HttpRequestMessage(
                   HttpMethod.Put,
                   AppendQuery(WriteUri(blob), "comp=blocklist"))
        {
            Content = new StringContent(blockList, Encoding.UTF8, "application/xml")
        })
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }

        var committed = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal("application/octet-stream", committed.ContentType);
        Assert.Null(committed.ContentEncoding);
        Assert.Null(committed.ContentLanguage);
        Assert.Null(committed.CacheControl);
        Assert.Null(committed.ContentDisposition);
        Assert.Null(committed.ContentHash);
        Assert.Empty(committed.Metadata);

        await blob.SetHttpHeadersAsync(
            FullHeaders("application/x-before-set", AzureProtocolChecksum.Md5("new block payload"u8)));
        using (var request = new HttpRequestMessage(
                   HttpMethod.Put,
                   AppendQuery(WriteUri(blob), "comp=properties"))
        {
            Content = new ByteArrayContent([])
        })
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            request.Headers.TryAddWithoutValidation("x-ms-blob-content-type", "application/x-only-property");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }

        var replaced = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal("application/x-only-property", replaced.ContentType);
        Assert.Null(replaced.ContentEncoding);
        Assert.Null(replaced.ContentLanguage);
        Assert.Null(replaced.CacheControl);
        Assert.Null(replaced.ContentDisposition);
        Assert.Null(replaced.ContentHash);
    }

    [Fact]
    public async Task PageSpecificPropertyUpdatePreservesHttpPropertyGroup()
    {
        var service = CreateClient();
        var container = service.GetBlobContainerClient($"page-properties-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var page = container.GetPageBlobClient("page.bin");
        var expectedHash = AzureProtocolChecksum.Md5(Array.Empty<byte>());
        await page.CreateAsync(
            512,
            new PageBlobCreateOptions
            {
                SequenceNumber = 7,
                HttpHeaders = FullHeaders("application/x-page", expectedHash)
            });

        using var transport = new HttpClient(factory.Server.CreateHandler());
        using (var request = new HttpRequestMessage(
                   HttpMethod.Put,
                   AppendQuery(WriteUri(page), "comp=properties"))
        {
            Content = new ByteArrayContent([])
        })
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            request.Headers.TryAddWithoutValidation("x-ms-sequence-number-action", "increment");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }

        var properties = (await page.GetPropertiesAsync()).Value;
        Assert.Equal(8, properties.BlobSequenceNumber);
        Assert.Equal("application/x-page", properties.ContentType);
        Assert.Equal("br", properties.ContentEncoding);
        Assert.Equal("en-US", properties.ContentLanguage);
        Assert.Equal("no-store", properties.CacheControl);
        Assert.Equal("attachment; filename=fixture.bin", properties.ContentDisposition);
        Assert.Equal(expectedHash, properties.ContentHash);
    }

    [Fact]
    public async Task PutBlobFromUrlCopiesOrReplacesSourcePropertiesAndMetadata()
    {
        var payload = "source properties and metadata"u8.ToArray();
        var application = new SavaWebApplicationFactory(() => new PropertySourceHandler(payload));
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var container = service.GetBlobContainerClient($"url-properties-{Guid.NewGuid():N}");
            await container.CreateAsync();
            var source = new Uri("https://source.example/blob");

            var copied = container.GetBlockBlobClient("copied.bin");
            await copied.SyncUploadFromUriAsync(source);
            var copiedProperties = (await copied.GetPropertiesAsync()).Value;
            Assert.Equal("application/x-source", copiedProperties.ContentType);
            Assert.Equal("gzip", copiedProperties.ContentEncoding);
            Assert.Equal("en-GB", copiedProperties.ContentLanguage);
            Assert.Equal("public, max-age=45", copiedProperties.CacheControl);
            Assert.Equal("inline; filename=source.bin", copiedProperties.ContentDisposition);
            Assert.Equal(AzureProtocolChecksum.Md5(payload), copiedProperties.ContentHash);
            Assert.Equal("metadata", copiedProperties.Metadata["source"]);

            var replaced = container.GetBlockBlobClient("replaced.bin");
            await replaced.SyncUploadFromUriAsync(
                source,
                new BlobSyncUploadFromUriOptions
                {
                    CopySourceBlobProperties = false
                });
            var replacedProperties = (await replaced.GetPropertiesAsync()).Value;
            Assert.Equal("application/octet-stream", replacedProperties.ContentType);
            Assert.Null(replacedProperties.ContentEncoding);
            Assert.Null(replacedProperties.ContentLanguage);
            Assert.Null(replacedProperties.CacheControl);
            Assert.Null(replacedProperties.ContentDisposition);
            Assert.Equal(AzureProtocolChecksum.Md5(payload), replacedProperties.ContentHash);
            Assert.Equal("metadata", replacedProperties.Metadata["source"]);

            var overridden = container.GetBlockBlobClient("overridden.bin");
            await overridden.SyncUploadFromUriAsync(
                source,
                new BlobSyncUploadFromUriOptions
                {
                    HttpHeaders = new BlobHttpHeaders { ContentType = "application/x-destination" },
                    Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["destination"] = "metadata" }
                });
            var overriddenProperties = (await overridden.GetPropertiesAsync()).Value;
            Assert.Equal("application/x-destination", overriddenProperties.ContentType);
            Assert.Equal("gzip", overriddenProperties.ContentEncoding);
            Assert.Equal("metadata", overriddenProperties.Metadata["destination"]);
            Assert.False(overriddenProperties.Metadata.ContainsKey("source"));

            await AssertSourceTagsCopiedAsync(container, source).ConfigureAwait(true);
        }
        finally
        {
            await application.DisposeAsync().ConfigureAwait(true);
        }
    }

    private static async Task AssertSourceTagsCopiedAsync(BlobContainerClient container, Uri source)
    {
        var tagged = container.GetBlockBlobClient("tagged.bin");
        await tagged.SyncUploadFromUriAsync(
            source,
            new BlobSyncUploadFromUriOptions { CopySourceTagsMode = BlobCopySourceTagsMode.Copy })
            .ConfigureAwait(false);
        Assert.Equal("copied", (await tagged.GetTagsAsync().ConfigureAwait(false)).Value.Tags["source-tag"]);
    }

    [Fact]
    public async Task SynchronousCopyUsesItsOwnResponseLimitsAndSourceStateContract()
    {
        var service = CreateClient();
        var container = service.GetBlobContainerClient($"sync-copy-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var source = container.GetBlockBlobClient("source.bin");
        var blockId = Convert.ToBase64String("sync-copy-block-0001"u8);
        await source.StageBlockAsync(blockId, BinaryData.FromString("synchronous copy payload").ToStream());
        await source.CommitBlockListAsync(
            [blockId],
            new CommitBlockListOptions
            {
                HttpHeaders = FullHeaders("application/x-sync-source"),
                Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["source"] = "metadata" },
                Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["source"] = "tag" }
            });

        var destination = container.GetBlockBlobClient("destination.bin");
        var copy = await destination.SyncCopyFromUriAsync(
            source.Uri,
            new BlobCopyFromUriOptions
            {
                CopySourceTagsMode = BlobCopySourceTagsMode.Copy
            });
        Assert.Equal(202, copy.GetRawResponse().Status);
        Assert.Equal(CopyStatus.Success, copy.Value.CopyStatus);
        Assert.False(string.IsNullOrWhiteSpace(copy.Value.CopyId));
        Assert.False(copy.GetRawResponse().Headers.TryGetValue("Content-MD5", out _));
        Assert.False(copy.GetRawResponse().Headers.TryGetValue("x-ms-content-crc64", out _));

        var properties = (await destination.GetPropertiesAsync()).Value;
        Assert.Equal("application/x-sync-source", properties.ContentType);
        Assert.Equal("br", properties.ContentEncoding);
        Assert.Equal("metadata", properties.Metadata["source"]);
        Assert.Equal("tag", (await destination.GetTagsAsync()).Value.Tags["source"]);
        var blocks = await destination.GetBlockListAsync(BlockListTypes.Committed);
        Assert.Equal(blockId, Assert.Single(blocks.Value.CommittedBlocks).Name);

        var replaced = container.GetBlobClient("replaced-tags.bin");
        await replaced.SyncCopyFromUriAsync(
            source.Uri,
            new BlobCopyFromUriOptions
            {
                CopySourceTagsMode = BlobCopySourceTagsMode.Replace,
                Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["destination"] = "tag" }
            });
        var replacedTags = (await replaced.GetTagsAsync()).Value.Tags;
        Assert.Equal("tag", replacedTags["destination"]);
        Assert.False(replacedTags.ContainsKey("source"));

        var append = container.GetAppendBlobClient("append-source.bin");
        await append.CreateAsync();
        var invalidType = await Assert.ThrowsAsync<Azure.RequestFailedException>(() =>
            container.GetBlobClient("append-copy.bin").SyncCopyFromUriAsync(append.Uri));
        Assert.Equal(409, invalidType.Status);
        Assert.Equal("InvalidSourceBlobType", invalidType.ErrorCode);
    }

    [Fact]
    public async Task CopyPreservesDestinationTierAndRequiresAnOnlineTierForArchivedSources()
    {
        var service = CreateClient();
        var container = service.GetBlobContainerClient($"copy-tier-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var source = container.GetBlockBlobClient("source.bin");
        await source.UploadAsync(
            BinaryData.FromString("tiered copy payload").ToStream(),
            new BlobUploadOptions { AccessTier = AccessTier.Cool });

        var existing = container.GetBlockBlobClient("existing.bin");
        await existing.UploadAsync(
            BinaryData.FromString("old destination").ToStream(),
            new BlobUploadOptions { AccessTier = AccessTier.Cold });
        var overwrite = await existing.StartCopyFromUriAsync(source.Uri);
        await overwrite.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None);
        var overwritten = (await existing.GetPropertiesAsync()).Value;
        Assert.Equal(AccessTier.Cold, overwritten.AccessTier);
        Assert.False(overwritten.AccessTierInferred);

        var created = container.GetBlockBlobClient("created.bin");
        var create = await created.StartCopyFromUriAsync(source.Uri);
        await create.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None);
        var createdProperties = (await created.GetPropertiesAsync()).Value;
        Assert.Equal(AccessTier.Hot, createdProperties.AccessTier);
        Assert.True(createdProperties.AccessTierInferred);

        var synchronousExisting = container.GetBlockBlobClient("sync-existing.bin");
        await synchronousExisting.UploadAsync(
            BinaryData.FromString("old synchronous destination").ToStream(),
            new BlobUploadOptions { AccessTier = AccessTier.Cold });
        await synchronousExisting.SyncCopyFromUriAsync(source.Uri);
        var synchronousProperties = (await synchronousExisting.GetPropertiesAsync()).Value;
        Assert.Equal(AccessTier.Cold, synchronousProperties.AccessTier);
        Assert.False(synchronousProperties.AccessTierInferred);

        await source.SetAccessTierAsync(AccessTier.Archive);
        var missingTier = await Assert.ThrowsAsync<Azure.RequestFailedException>(() =>
            container.GetBlockBlobClient("archive-without-tier.bin").StartCopyFromUriAsync(source.Uri));
        Assert.Equal(409, missingTier.Status);
        Assert.Equal("BlobArchived", missingTier.ErrorCode);

        var rehydratedCopy = container.GetBlockBlobClient("archive-to-hot.bin");
        var rehydrate = await rehydratedCopy.StartCopyFromUriAsync(
            source.Uri,
            new BlobCopyFromUriOptions
            {
                AccessTier = AccessTier.Hot,
                RehydratePriority = RehydratePriority.High
            });
        await rehydrate.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None);
        Assert.Equal(AccessTier.Hot, (await rehydratedCopy.GetPropertiesAsync()).Value.AccessTier);

        var synchronousArchive = await Assert.ThrowsAsync<Azure.RequestFailedException>(() =>
            container.GetBlockBlobClient("sync-archive.bin").SyncCopyFromUriAsync(
                source.Uri,
                new BlobCopyFromUriOptions { AccessTier = AccessTier.Hot }));
        Assert.Equal(409, synchronousArchive.Status);
        Assert.Equal("BlobArchived", synchronousArchive.ErrorCode);
    }

    [Fact]
    public async Task SynchronousCopyUsesTheOperationSpecificEncryptionScopeContract()
    {
        var service = CreateClient();
        var container = service.GetBlobContainerClient($"copy-encryption-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var source = container.GetBlockBlobClient("source.bin");
        await source.UploadAsync(BinaryData.FromString("encryption-domain copy payload").ToStream());
        var sourceUri = source.GenerateSasUri(
            BlobSasPermissions.Read,
            DateTimeOffset.UtcNow.AddMinutes(5));
        using var transport = new HttpClient(factory.Server.CreateHandler());

        var oldVersionDestination = container.GetBlockBlobClient("old-version.bin");
        using (var request = SyncCopyRequest(oldVersionDestination, sourceUri, "2020-04-08"))
        {
            request.Headers.TryAddWithoutValidation("x-ms-encryption-scope", "scope-old");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal(
                "FeatureVersionMismatch",
                response.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await oldVersionDestination.ExistsAsync()).Value);

        var encrypted = container.GetBlockBlobClient("encrypted.bin");
        using (var request = SyncCopyRequest(encrypted, sourceUri, "2020-12-06"))
        {
            request.Headers.TryAddWithoutValidation("x-ms-encryption-scope", "copy-scope");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
            Assert.Equal("copy-scope", response.Headers.GetValues("x-ms-encryption-scope").Single());
        }
        var encryptedProperties = (await encrypted.GetPropertiesAsync()).Value;
        Assert.Equal("copy-scope", encryptedProperties.EncryptionScope);
        Assert.Equal(
            "encryption-domain copy payload",
            (await encrypted.DownloadContentAsync()).Value.Content.ToString());

        var customerKeyDestination = container.GetBlockBlobClient("customer-key.bin");
        using (var request = SyncCopyRequest(customerKeyDestination, sourceUri, "2023-11-03"))
        {
            var key = RandomNumberGenerator.GetBytes(32);
            request.Headers.TryAddWithoutValidation("x-ms-encryption-key", Convert.ToBase64String(key));
            request.Headers.TryAddWithoutValidation(
                "x-ms-encryption-key-sha256",
                Convert.ToBase64String(SHA256.HashData(key)));
            request.Headers.TryAddWithoutValidation("x-ms-encryption-algorithm", "AES256");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await customerKeyDestination.ExistsAsync()).Value);
    }

    [Fact]
    public async Task ExternalAsynchronousCopyPreservesTheCompleteAzureBlobShape()
    {
        using var sourceHandler = new CopyShapeSourceHandler();
        var application = new SavaWebApplicationFactory(() => sourceHandler);
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var container = service.GetBlobContainerClient($"remote-shape-{Guid.NewGuid():N}");
            await container.CreateAsync();

            var block = await AssertBlockCopyShapeAsync(container).ConfigureAwait(true);

            await AssertAppendCopyShapeAsync(container, block).ConfigureAwait(true);
            await AssertPageCopyShapeAsync(container).ConfigureAwait(true);

            Assert.True(sourceHandler.BlockListReadWithPinnedEtag);
            Assert.Equal(4, sourceHandler.PageListRequests);
            Assert.True(sourceHandler.PageListReadWithPinnedEtag);
        }
        finally
        {
            await application.DisposeAsync().ConfigureAwait(true);
        }
    }

    private static async Task<BlockBlobClient> AssertBlockCopyShapeAsync(BlobContainerClient container)
    {
        var block = container.GetBlockBlobClient("block-copy.bin");
        var blockCopy = await block.StartCopyFromUriAsync(new Uri("https://source.example/block")).ConfigureAwait(false);
        var pendingBlock = (await block.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(CopyStatus.Pending, pendingBlock.CopyStatus);
        Assert.Equal(0, pendingBlock.ContentLength);
        Assert.Empty((await block.GetBlockListAsync(BlockListTypes.Committed).ConfigureAwait(false)).Value.CommittedBlocks);
        await blockCopy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None).ConfigureAwait(false);
        var blockProperties = (await block.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(BlobType.Block, blockProperties.BlobType);
        Assert.Equal("block", blockProperties.Metadata["shape"]);
        Assert.Equal(CopyShapeSourceHandler.BlockPayload,
            (await block.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        var committed = (await block.GetBlockListAsync(BlockListTypes.Committed).ConfigureAwait(false)).Value.CommittedBlocks;
        Assert.Equal(CopyShapeSourceHandler.BlockIds, committed.Select(item => item.Name), StringComparer.Ordinal);
        Assert.Equal(CopyShapeSourceHandler.BlockLengths, committed.Select(item => item.SizeLong));

        var synchronousBlock = container.GetBlockBlobClient("synchronous-block-copy.bin");
        await synchronousBlock.SyncCopyFromUriAsync(new Uri("https://source.example/block")).ConfigureAwait(false);
        var synchronousBlocks = (await synchronousBlock.GetBlockListAsync(BlockListTypes.Committed).ConfigureAwait(false))
            .Value.CommittedBlocks;
        Assert.Equal(CopyShapeSourceHandler.BlockIds, synchronousBlocks.Select(item => item.Name), StringComparer.Ordinal);
        Assert.Equal(CopyShapeSourceHandler.BlockPayload,
            (await synchronousBlock.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        return block;
    }

    private static async Task AssertAppendCopyShapeAsync(BlobContainerClient container, BlockBlobClient block)
    {
        var append = container.GetAppendBlobClient("append-copy.bin");
        var appendCopy = await append.StartCopyFromUriAsync(new Uri("https://source.example/append")).ConfigureAwait(false);
        var pendingAppend = (await append.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(CopyStatus.Pending, pendingAppend.CopyStatus);
        Assert.Equal(0, pendingAppend.ContentLength);
        Assert.Equal(0, pendingAppend.BlobCommittedBlockCount);
        Assert.False(pendingAppend.IsSealed);
        await appendCopy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None).ConfigureAwait(false);
        var appendProperties = (await append.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(BlobType.Append, appendProperties.BlobType);
        Assert.Equal(2, appendProperties.BlobCommittedBlockCount);
        Assert.True(appendProperties.IsSealed);
        Assert.Equal(CopyShapeSourceHandler.AppendPayload,
            (await append.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        var sealedAppend = await Assert.ThrowsAsync<Azure.RequestFailedException>(() =>
            append.AppendBlockAsync(BinaryData.FromString("rejected").ToStream())).ConfigureAwait(false);
        Assert.Equal("BlobIsSealed", sealedAppend.ErrorCode);

        var unsealedAppend = container.GetAppendBlobClient("unsealed-append-copy.bin");
        var unsealedCopy = await unsealedAppend.StartCopyFromUriAsync(
            new Uri("https://source.example/append"),
            new BlobCopyFromUriOptions { ShouldSealDestination = false }).ConfigureAwait(false);
        await unsealedCopy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None).ConfigureAwait(false);
        Assert.False((await unsealedAppend.GetPropertiesAsync().ConfigureAwait(false)).Value.IsSealed);
        await unsealedAppend.AppendBlockAsync(BinaryData.FromString("|allowed").ToStream()).ConfigureAwait(false);
        Assert.Equal(CopyShapeSourceHandler.AppendPayload.Concat("|allowed"u8.ToArray()).ToArray(),
            (await unsealedAppend.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());

        var invalidSeal = await Assert.ThrowsAsync<Azure.RequestFailedException>(() =>
            block.StartCopyFromUriAsync(
                new Uri("https://source.example/block"),
                new BlobCopyFromUriOptions { ShouldSealDestination = true })).ConfigureAwait(false);
        Assert.Equal(400, invalidSeal.Status);
        Assert.Equal("InvalidHeaderValue", invalidSeal.ErrorCode);

        var unsealedSource = container.GetAppendBlobClient("unsealed-source.bin");
        await unsealedSource.CreateAsync().ConfigureAwait(false);
        await unsealedSource.AppendBlockAsync(BinaryData.FromString("seal on completion").ToStream()).ConfigureAwait(false);
        var explicitlySealed = container.GetAppendBlobClient("explicitly-sealed-copy.bin");
        var explicitlySealedCopy = await explicitlySealed.StartCopyFromUriAsync(
            unsealedSource.Uri,
            new BlobCopyFromUriOptions { ShouldSealDestination = true }).ConfigureAwait(false);
        Assert.False((await explicitlySealed.GetPropertiesAsync().ConfigureAwait(false)).Value.IsSealed);
        await explicitlySealedCopy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None)
            .ConfigureAwait(false);
        Assert.True((await explicitlySealed.GetPropertiesAsync().ConfigureAwait(false)).Value.IsSealed);
    }

    private static async Task AssertPageCopyShapeAsync(BlobContainerClient container)
    {
        var page = container.GetPageBlobClient("page-copy.bin");
        var pageCopy = await page.StartCopyFromUriAsync(new Uri("https://source.example/page")).ConfigureAwait(false);
        var pendingPage = (await page.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(BlobType.Page, pendingPage.BlobType);
        Assert.Equal(CopyShapeSourceHandler.PagePayload.LongLength, pendingPage.ContentLength);
        Assert.Empty((await page.GetPageRangesAsync().ConfigureAwait(false)).Value.PageRanges);
        await pageCopy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None).ConfigureAwait(false);
        var pageProperties = (await page.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(42, pageProperties.BlobSequenceNumber);
        Assert.Equal(CopyShapeSourceHandler.PagePayload,
            (await page.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        var ranges = (await page.GetPageRangesAsync().ConfigureAwait(false)).Value.PageRanges;
        Assert.Collection(ranges,
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

        var wrongType = container.GetBlockBlobClient("wrong-type.bin");
        await wrongType.UploadAsync(BinaryData.FromString("existing block").ToStream()).ConfigureAwait(false);
        var mismatch = await Assert.ThrowsAsync<Azure.RequestFailedException>(() =>
            wrongType.StartCopyFromUriAsync(new Uri("https://source.example/page"))).ConfigureAwait(false);
        Assert.Equal(409, mismatch.Status);
        Assert.Equal("InvalidBlobType", mismatch.ErrorCode);
    }

    [Fact]
    public async Task AsynchronousCopyTimesOutToDurableFailedEmptyBlob()
    {
        var clock = new AdjustableTimeProvider(new DateTimeOffset(2026, 9, 22, 12, 0, 0, TimeSpan.Zero));
        var application = new SavaWebApplicationFactory(
            clock,
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sava:AsyncCopyCompletionDelay"] = "15.00:00:00"
            });
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var container = service.GetBlobContainerClient($"copy-timeout-{Guid.NewGuid():N}");
            await container.CreateAsync();
            var source = container.GetBlobClient("source.bin");
            await source.UploadAsync(BinaryData.FromString("copy timeout payload"));
            var destination = container.GetBlobClient("destination.bin");

            var operation = await destination.StartCopyFromUriAsync(source.Uri);
            Assert.False(operation.HasCompleted);
            Assert.Equal(CopyStatus.Pending, (await destination.GetPropertiesAsync()).Value.CopyStatus);

            clock.Advance(TimeSpan.FromDays(14) + TimeSpan.FromTicks(1));
            var failed = (await destination.GetPropertiesAsync()).Value;
            Assert.Equal(CopyStatus.Failed, failed.CopyStatus);
            Assert.Equal("500 (OperationCancelled)", failed.CopyStatusDescription);
            Assert.Equal(0, failed.ContentLength);
            Assert.Empty((await destination.DownloadContentAsync()).Value.Content.ToArray());

            clock.Advance(TimeSpan.FromDays(2));
            var durable = (await destination.GetPropertiesAsync()).Value;
            Assert.Equal(CopyStatus.Failed, durable.CopyStatus);
            Assert.Equal("500 (OperationCancelled)", durable.CopyStatusDescription);
            Assert.Equal(0, durable.ContentLength);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task AsynchronousCopyRequiresAndPreservesAnInfiniteDestinationLease()
    {
        var service = CreateClient();
        var container = service.GetBlobContainerClient($"copy-lease-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var source = container.GetBlobClient("source.bin");
        await source.UploadAsync(BinaryData.FromString("leased copy source"));

        await AssertFiniteLeaseRejectedAsync(container, source.Uri).ConfigureAwait(true);

        var infiniteDestination = container.GetBlobClient("infinite.bin");
        await infiniteDestination.UploadAsync(BinaryData.FromString("original infinite destination")).ConfigureAwait(true);
        var infiniteLeaseId = Guid.NewGuid().ToString();
        var infiniteLease = infiniteDestination.GetBlobLeaseClient(infiniteLeaseId);
        await infiniteLease.AcquireAsync(BlobLeaseClient.InfiniteLeaseDuration).ConfigureAwait(true);

        var missingLease = await Assert.ThrowsAsync<Azure.RequestFailedException>(() =>
            infiniteDestination.StartCopyFromUriAsync(source.Uri)).ConfigureAwait(true);
        Assert.Equal(412, missingLease.Status);
        Assert.Equal("LeaseIdMissing", missingLease.ErrorCode);

        var mismatchedLease = await Assert.ThrowsAsync<Azure.RequestFailedException>(() =>
            infiniteDestination.StartCopyFromUriAsync(
                source.Uri,
                new BlobCopyFromUriOptions
                {
                    DestinationConditions = new BlobRequestConditions { LeaseId = Guid.NewGuid().ToString() }
                })).ConfigureAwait(true);
        Assert.Equal(412, mismatchedLease.Status);
        Assert.Equal("LeaseIdMismatchWithBlobOperation", mismatchedLease.ErrorCode);

        var operation = await infiniteDestination.StartCopyFromUriAsync(
            source.Uri,
            new BlobCopyFromUriOptions
            {
                DestinationConditions = new BlobRequestConditions { LeaseId = infiniteLeaseId }
            }).ConfigureAwait(true);
        var pending = (await infiniteDestination.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(CopyStatus.Pending, pending.CopyStatus);
        Assert.Equal(Azure.Storage.Blobs.Models.LeaseState.Leased, pending.LeaseState);
        Assert.Equal(LeaseDurationType.Infinite, pending.LeaseDuration);

        var pendingLeaseOperation = await Assert.ThrowsAsync<Azure.RequestFailedException>(() => infiniteLease.RenewAsync())
            .ConfigureAwait(true);
        Assert.Equal(409, pendingLeaseOperation.Status);
        Assert.Equal("PendingCopyOperation", pendingLeaseOperation.ErrorCode);

        await operation.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None).ConfigureAwait(true);
        var completed = (await infiniteDestination.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(CopyStatus.Success, completed.CopyStatus);
        Assert.Equal(Azure.Storage.Blobs.Models.LeaseState.Leased, completed.LeaseState);
        Assert.Equal(LeaseDurationType.Infinite, completed.LeaseDuration);
        Assert.Equal("leased copy source",
            (await infiniteDestination.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        await infiniteLease.ReleaseAsync().ConfigureAwait(true);

        await AssertAbsentLeaseRejectedAsync(container, source.Uri).ConfigureAwait(true);
    }

    private static async Task AssertFiniteLeaseRejectedAsync(BlobContainerClient container, Uri source)
    {
        var finiteDestination = container.GetBlobClient("finite.bin");
        await finiteDestination.UploadAsync(BinaryData.FromString("original finite destination")).ConfigureAwait(false);
        var finiteLeaseId = Guid.NewGuid().ToString();
        var finiteLease = finiteDestination.GetBlobLeaseClient(finiteLeaseId);
        await finiteLease.AcquireAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);
        var finiteFailure = await Assert.ThrowsAsync<Azure.RequestFailedException>(() =>
            finiteDestination.StartCopyFromUriAsync(
                source,
                new BlobCopyFromUriOptions
                {
                    DestinationConditions = new BlobRequestConditions { LeaseId = finiteLeaseId }
                })).ConfigureAwait(false);
        Assert.Equal(412, finiteFailure.Status);
        Assert.Equal("InfiniteLeaseDurationRequired", finiteFailure.ErrorCode);
        Assert.Equal("original finite destination",
            (await finiteDestination.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        await finiteLease.ReleaseAsync().ConfigureAwait(false);
    }

    private static async Task AssertAbsentLeaseRejectedAsync(BlobContainerClient container, Uri source)
    {
        var absentDestination = container.GetBlobClient("absent.bin");
        var absentLease = await Assert.ThrowsAsync<Azure.RequestFailedException>(() =>
            absentDestination.StartCopyFromUriAsync(
                source,
                new BlobCopyFromUriOptions
                {
                    DestinationConditions = new BlobRequestConditions { LeaseId = Guid.NewGuid().ToString() }
                })).ConfigureAwait(false);
        Assert.Equal(412, absentLease.Status);
        Assert.Equal("LeaseNotPresentWithBlobOperation", absentLease.ErrorCode);
        Assert.False((await absentDestination.ExistsAsync().ConfigureAwait(false)).Value);
    }

    [Fact]
    public async Task PendingCopyRejectsMutationsBeforeSourceOrBodyIngestion()
    {
        var service = CreateClient();
        var container = service.GetBlobContainerClient($"copy-conflict-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var source = container.GetBlobClient("source.bin");
        await source.UploadAsync(BinaryData.FromString("pending copy source"));
        var destination = container.GetBlockBlobClient("destination.bin");
        var operation = await destination.StartCopyFromUriAsync(source.Uri);

        var secondCopy = await Assert.ThrowsAsync<Azure.RequestFailedException>(() =>
            destination.StartCopyFromUriAsync(container.GetBlobClient("missing-source.bin").Uri));
        Assert.Equal(409, secondCopy.Status);
        Assert.Equal("PendingCopyOperation", secondCopy.ErrorCode);

        var stage = await Assert.ThrowsAsync<Azure.RequestFailedException>(() =>
            destination.StageBlockAsync(
                Convert.ToBase64String("pending-block-0001"u8),
                BinaryData.FromString("must not stage").ToStream()));
        Assert.Equal(409, stage.Status);
        Assert.Equal("PendingCopyOperation", stage.ErrorCode);

        var overwrite = await Assert.ThrowsAsync<Azure.RequestFailedException>(() =>
            container.GetBlobClient(destination.Name)
                .UploadAsync(BinaryData.FromString("must not overwrite"), overwrite: true));
        Assert.Equal(409, overwrite.Status);
        Assert.Equal("PendingCopyOperation", overwrite.ErrorCode);

        await destination.AbortCopyFromUriAsync(operation.Id);

        var bodyDestination = container.GetBlobClient("body-copy.bin");
        using var transport = new HttpClient(factory.Server.CreateHandler());
        using var bodyCopy = new HttpRequestMessage(HttpMethod.Put, WriteUri(bodyDestination))
        {
            Content = new ByteArrayContent([0x42])
        };
        bodyCopy.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        bodyCopy.Headers.TryAddWithoutValidation("x-ms-copy-source", source.Uri.AbsoluteUri);
        using var bodyCopyResponse = await transport.SendAsync(bodyCopy);
        Assert.Equal(HttpStatusCode.BadRequest, bodyCopyResponse.StatusCode);
        Assert.Equal("InvalidHeaderValue", bodyCopyResponse.Headers.GetValues("x-ms-error-code").Single());
        Assert.False((await bodyDestination.ExistsAsync()).Value);
    }

    [Fact]
    public async Task CopyPropertiesSurviveLeasePageAndBlockStagingButClearOnOtherWrites()
    {
        var service = CreateClient();
        var container = service.GetBlobContainerClient($"copy-properties-{Guid.NewGuid():N}");
        await container.CreateAsync();

        var pageSource = container.GetPageBlobClient("page-source.bin");
        await pageSource.CreateAsync(512);
        await pageSource.UploadPagesAsync(
            new MemoryStream(Enumerable.Repeat((byte)0x41, 512).ToArray()),
            offset: 0);
        var pageDestination = container.GetPageBlobClient("page-destination.bin");
        var pageCopy = await pageDestination.StartCopyFromUriAsync(pageSource.Uri);
        await pageCopy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None);
        Assert.Equal(CopyStatus.Success, (await pageDestination.GetPropertiesAsync()).Value.CopyStatus);

        var lease = pageDestination.GetBlobLeaseClient(Guid.NewGuid().ToString());
        await lease.AcquireAsync(BlobLeaseClient.InfiniteLeaseDuration);
        Assert.Equal(CopyStatus.Success, (await pageDestination.GetPropertiesAsync()).Value.CopyStatus);
        await lease.ReleaseAsync();
        Assert.Equal(CopyStatus.Success, (await pageDestination.GetPropertiesAsync()).Value.CopyStatus);

        await pageDestination.UploadPagesAsync(
            new MemoryStream(Enumerable.Repeat((byte)0x42, 512).ToArray()),
            offset: 0);
        Assert.Equal(CopyStatus.Success, (await pageDestination.GetPropertiesAsync()).Value.CopyStatus);
        await pageDestination.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["mutation"] = "metadata" });
        Assert.Equal(default, (await pageDestination.GetPropertiesAsync()).Value.CopyStatus);

        var blockSource = container.GetBlockBlobClient("block-source.bin");
        await blockSource.UploadAsync(BinaryData.FromString("block copy source").ToStream());
        var blockDestination = container.GetBlockBlobClient("block-destination.bin");
        var blockCopy = await blockDestination.StartCopyFromUriAsync(blockSource.Uri);
        await blockCopy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None);
        Assert.Equal(CopyStatus.Success, (await blockDestination.GetPropertiesAsync()).Value.CopyStatus);

        var blockId = Convert.ToBase64String("copy-property-block-0001"u8);
        await blockDestination.StageBlockAsync(blockId, BinaryData.FromString("replacement").ToStream());
        Assert.Equal(CopyStatus.Success, (await blockDestination.GetPropertiesAsync()).Value.CopyStatus);
        await blockDestination.CommitBlockListAsync([blockId]);
        Assert.Equal(default, (await blockDestination.GetPropertiesAsync()).Value.CopyStatus);
    }

    private static HttpRequestMessage PutBlobRequest(BlockBlobClient blob, byte[] payload)
    {
        var request = new HttpRequestMessage(HttpMethod.Put, WriteUri(blob))
        {
            Content = new ByteArrayContent(payload)
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
        return request;
    }

    private static HttpRequestMessage SyncCopyRequest(
        BlockBlobClient destination,
        Uri source,
        string serviceVersion)
    {
        var request = new HttpRequestMessage(HttpMethod.Put, WriteUri(destination))
        {
            Content = new ByteArrayContent([])
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", serviceVersion);
        request.Headers.TryAddWithoutValidation("x-ms-copy-source", source.AbsoluteUri);
        request.Headers.TryAddWithoutValidation("x-ms-requires-sync", "true");
        return request;
    }

    private static BlobHttpHeaders FullHeaders(string contentType, byte[]? contentHash = null) => new()
    {
        ContentType = contentType,
        ContentEncoding = "br",
        ContentLanguage = "en-US",
        CacheControl = "no-store",
        ContentDisposition = "attachment; filename=fixture.bin",
        ContentHash = contentHash
    };

    private static Uri WriteUri(BlobBaseClient blob) => blob.GenerateSasUri(
        BlobSasPermissions.Create | BlobSasPermissions.Write | BlobSasPermissions.Tag,
        DateTimeOffset.UtcNow.AddMinutes(5));

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

    private BlobServiceClient CreateClient()
        => CreateClient(factory);

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
                Transport = new HttpClientTransport(application.Server.CreateHandler()),
                Retry = { MaxRetries = 0 }
            });
    }

    private sealed class PropertySourceHandler(byte[] payload) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            if (request.RequestUri?.Query.Contains("comp=tags", StringComparison.OrdinalIgnoreCase) == true)
            {
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(
                        "<Tags><TagSet><Tag><Key>source-tag</Key><Value>copied</Value></Tag></TagSet></Tags>",
                        Encoding.UTF8,
                        "application/xml"),
                    RequestMessage = request
                });
            }
            var response = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new ByteArrayContent(payload),
                RequestMessage = request
            };
            response.Content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/x-source");
            response.Content.Headers.ContentEncoding.Add("gzip");
            response.Content.Headers.ContentLanguage.Add("en-GB");
            response.Content.Headers.ContentDisposition = ContentDispositionHeaderValue.Parse("inline; filename=source.bin");
            response.Content.Headers.ContentMD5 = AzureProtocolChecksum.Md5(payload);
            response.Headers.CacheControl = CacheControlHeaderValue.Parse("public, max-age=45");
            response.Headers.ETag = new EntityTagHeaderValue("\"source-etag\"");
            response.Headers.TryAddWithoutValidation("x-ms-meta-source", "metadata");
            return Task.FromResult(response);
        }
    }

    private sealed class CopyShapeSourceHandler : HttpMessageHandler
    {
        private const string SourceEtag = "\"shape-source-etag\"";
        private static readonly byte[][] Blocks = ["first-"u8.ToArray(), "second-block"u8.ToArray()];

        public static byte[] BlockPayload { get; } = Blocks.SelectMany(block => block).ToArray();
        public static string[] BlockIds { get; } =
        [
            Convert.ToBase64String("remote-block-0001"u8),
            Convert.ToBase64String("remote-block-0002"u8)
        ];
        public static long[] BlockLengths { get; } = Blocks.Select(block => (long)block.Length).ToArray();
        public static byte[] AppendPayload { get; } = "first append|second append"u8.ToArray();
        public static byte[] PagePayload { get; } = CreatePagePayload();

        public bool BlockListReadWithPinnedEtag { get; private set; }
        public bool PageListReadWithPinnedEtag { get; private set; }
        public int PageListRequests { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var path = request.RequestUri?.AbsolutePath ?? string.Empty;
            var query = Microsoft.AspNetCore.WebUtilities.QueryHelpers.ParseQuery(
                request.RequestUri?.Query ?? string.Empty);
            if (query.TryGetValue("comp", out var component) && component == "blocklist")
            {
                BlockListReadWithPinnedEtag = HasPinnedEtag(request);
                return Task.FromResult(XmlResponse(
                    $"<BlockList><CommittedBlocks>" +
                    $"<Block><Name>{BlockIds[0]}</Name><Size>{BlockLengths[0]}</Size></Block>" +
                    $"<Block><Name>{BlockIds[1]}</Name><Size>{BlockLengths[1]}</Size></Block>" +
                    "</CommittedBlocks></BlockList>",
                    request));
            }
            if (query.TryGetValue("comp", out component) && component == "pagelist")
            {
                PageListRequests++;
                PageListReadWithPinnedEtag |= HasPinnedEtag(request);
                var next = query.ContainsKey("marker")
                    ? "<PageRange><Start>1024</Start><End>1535</End></PageRange>"
                    : "<PageRange><Start>0</Start><End>511</End></PageRange><NextMarker>next</NextMarker>";
                return Task.FromResult(XmlResponse($"<PageList>{next}</PageList>", request));
            }

            var response = path switch
            {
                "/block" => BlobResponse(BlockPayload, "BlockBlob", request),
                "/append" => BlobResponse(AppendPayload, "AppendBlob", request),
                "/page" => BlobResponse(PagePayload, "PageBlob", request),
                _ => new HttpResponseMessage(HttpStatusCode.NotFound) { RequestMessage = request }
            };
            if (string.Equals(path, "/append", StringComparison.Ordinal))
            {
                response.Headers.TryAddWithoutValidation("x-ms-blob-committed-block-count", "2");
                response.Headers.TryAddWithoutValidation("x-ms-blob-sealed", "true");
            }
            if (string.Equals(path, "/page", StringComparison.Ordinal))
                response.Headers.TryAddWithoutValidation("x-ms-blob-sequence-number", "42");
            response.Headers.TryAddWithoutValidation("x-ms-meta-shape", path.TrimStart('/'));
            return Task.FromResult(response);
        }

        private static HttpResponseMessage BlobResponse(
            byte[] payload,
            string kind,
            HttpRequestMessage request)
        {
            var response = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new ByteArrayContent(payload),
                RequestMessage = request
            };
            response.Headers.ETag = EntityTagHeaderValue.Parse(SourceEtag);
            response.Headers.TryAddWithoutValidation("x-ms-blob-type", kind);
            return response;
        }

        private static HttpResponseMessage XmlResponse(string xml, HttpRequestMessage request) => new(HttpStatusCode.OK)
        {
            Content = new StringContent(xml, Encoding.UTF8, "application/xml"),
            RequestMessage = request
        };

        private static bool HasPinnedEtag(HttpRequestMessage request) =>
            request.Headers.IfMatch.Any(value => string.Equals(value.Tag, SourceEtag, StringComparison.Ordinal));

        private static byte[] CreatePagePayload()
        {
            var payload = new byte[1536];
            Array.Fill(payload, (byte)0x31, 0, 512);
            Array.Fill(payload, (byte)0x33, 1024, 512);
            return payload;
        }
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
