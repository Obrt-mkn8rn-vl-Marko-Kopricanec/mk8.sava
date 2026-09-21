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
using Microsoft.IdentityModel.Tokens;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;

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
