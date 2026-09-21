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
                Metadata = new Dictionary<string, string> { ["old"] = "metadata" },
                Tags = new Dictionary<string, string> { ["old"] = "tag" }
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
            Assert.Equal(MD5.HashData(firstPayload), response.Content.Headers.ContentMD5);
        }

        var standard = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal("application/x-standard", standard.ContentType);
        Assert.Equal("gzip", standard.ContentEncoding);
        Assert.Equal("hr-HR", standard.ContentLanguage);
        Assert.Equal("max-age=120, private", standard.CacheControl);
        Assert.Null(standard.ContentDisposition);
        Assert.Equal(MD5.HashData(firstPayload), standard.ContentHash);
        Assert.Empty(standard.Metadata);
        Assert.Empty((await blob.GetTagsAsync()).Value.Tags);

        var secondPayload = "custom md5 takes precedence"u8.ToArray();
        using (var request = PutBlobRequest(blob, secondPayload))
        {
            request.Content!.Headers.ContentType = MediaTypeHeaderValue.Parse("application/x-custom-md5");
            request.Content.Headers.ContentMD5 = MD5.HashData("different transport hash"u8.ToArray());
            request.Headers.TryAddWithoutValidation(
                "x-ms-blob-content-md5",
                Convert.ToBase64String(MD5.HashData(secondPayload)));
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }

        var custom = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal("application/x-custom-md5", custom.ContentType);
        Assert.Null(custom.ContentEncoding);
        Assert.Null(custom.ContentLanguage);
        Assert.Null(custom.CacheControl);
        Assert.Null(custom.ContentDisposition);
        Assert.Equal(MD5.HashData(secondPayload), custom.ContentHash);
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
                Metadata = new Dictionary<string, string> { ["old"] = "metadata" }
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
            FullHeaders("application/x-before-set", MD5.HashData("new block payload"u8.ToArray())));
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
        var expectedHash = MD5.HashData(Array.Empty<byte>());
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
            Assert.Equal(MD5.HashData(payload), copiedProperties.ContentHash);
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
            Assert.Equal(MD5.HashData(payload), replacedProperties.ContentHash);
            Assert.Equal("metadata", replacedProperties.Metadata["source"]);

            var overridden = container.GetBlockBlobClient("overridden.bin");
            await overridden.SyncUploadFromUriAsync(
                source,
                new BlobSyncUploadFromUriOptions
                {
                    HttpHeaders = new BlobHttpHeaders { ContentType = "application/x-destination" },
                    Metadata = new Dictionary<string, string> { ["destination"] = "metadata" }
                });
            var overriddenProperties = (await overridden.GetPropertiesAsync()).Value;
            Assert.Equal("application/x-destination", overriddenProperties.ContentType);
            Assert.Equal("gzip", overriddenProperties.ContentEncoding);
            Assert.Equal("metadata", overriddenProperties.Metadata["destination"]);
            Assert.False(overriddenProperties.Metadata.ContainsKey("source"));

            var tagged = container.GetBlockBlobClient("tagged.bin");
            await tagged.SyncUploadFromUriAsync(
                source,
                new BlobSyncUploadFromUriOptions
                {
                    CopySourceTagsMode = BlobCopySourceTagsMode.Copy
                });
            Assert.Equal("copied", (await tagged.GetTagsAsync()).Value.Tags["source-tag"]);
        }
        finally
        {
            await application.DisposeAsync();
        }
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
                Metadata = new Dictionary<string, string> { ["source"] = "metadata" },
                Tags = new Dictionary<string, string> { ["source"] = "tag" }
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
                Tags = new Dictionary<string, string> { ["destination"] = "tag" }
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
            new BlobCopyFromUriOptions { AccessTier = AccessTier.Hot });
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
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
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
            response.Content.Headers.ContentMD5 = MD5.HashData(payload);
            response.Headers.CacheControl = CacheControlHeaderValue.Parse("public, max-age=45");
            response.Headers.ETag = new EntityTagHeaderValue("\"source-etag\"");
            response.Headers.TryAddWithoutValidation("x-ms-meta-source", "metadata");
            return Task.FromResult(response);
        }
    }
}
