using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class LiveAzureFactAttribute : FactAttribute
{
    public const string ConnectionStringVariable = "MK8_SAVA_LIVE_AZURE_BLOB_CONNECTION_STRING";
    public const string HnsConnectionStringVariable = "MK8_SAVA_LIVE_AZURE_HNS_CONNECTION_STRING";

    public LiveAzureFactAttribute(string connectionStringVariable = ConnectionStringVariable)
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(connectionStringVariable)))
            Skip = $"Set {connectionStringVariable} to a disposable Azure test account.";
    }
}

public sealed class LiveAzureDifferentialTests(ITestOutputHelper output)
{
    [Fact]
    public async Task FlatNamespaceDifferentialScenarioRunsAgainstLocalService()
    {
        await using var application = new SavaWebApplicationFactory(new Dictionary<string, string?>
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });
        await application.InitializeAsync();
        var client = CreateLocalClient(application);
        var container = client.GetBlobContainerClient($"mk8diff-local-{Guid.NewGuid():N}");
        try
        {
            var observation = await ExerciseAsync(container);
            Assert.Equal(404, observation.MissingStatus);
            Assert.Equal("BlobNotFound", observation.MissingCode);
        }
        finally
        {
            await DeleteIfExistsAsync(container);
        }
    }

    [Fact]
    public async Task HierarchicalNamespaceDifferentialScenarioRunsAgainstLocalService()
    {
        await using var application = new SavaWebApplicationFactory(new Dictionary<string, string?>
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true",
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });
        await application.InitializeAsync();
        var client = CreateLocalClient(application);
        var container = client.GetBlobContainerClient($"mk8diff-hns-local-{Guid.NewGuid():N}");
        try
        {
            var observation = await ExerciseHierarchicalAsync(container);
            Assert.Equal("$superuser", observation.FileOwner);
            Assert.Equal("rwxr-x---", observation.DirectoryPermissions);
            Assert.Equal("rw-r-----", observation.FilePermissions);
            Assert.Equal(409, observation.NonemptyDirectoryDeleteStatus);
        }
        finally
        {
            await DeleteIfExistsAsync(container);
        }
    }

    [Fact]
    public async Task FlatAuthorizationDifferentialScenarioRunsAgainstLocalService()
    {
        await using var application = new SavaWebApplicationFactory(new Dictionary<string, string?>
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });
        await application.InitializeAsync();
        var client = CreateLocalClient(application);
        var container = client.GetBlobContainerClient($"mk8diff-auth-local-{Guid.NewGuid():N}");
        try
        {
            var observation = await ExerciseAuthorizationAsync(
                container,
                uri => CreateLocalSasClient(application, uri));
            Assert.Equal("original-content", observation.SasReadBytes);
            Assert.Equal(403, observation.DeniedSasWriteStatus);
            Assert.Equal("AuthorizationPermissionMismatch", observation.DeniedSasWriteCode);
            Assert.Equal(412, observation.StaleConditionStatus);
            Assert.Equal("ConditionNotMet", observation.StaleConditionCode);
            Assert.Equal(412, observation.MissingLeaseStatus);
            Assert.Equal("LeaseIdMissing", observation.MissingLeaseCode);
            Assert.Equal("authorized", observation.FinalMetadata);
            Assert.Equal("original-content", observation.FinalBytes);
        }
        finally
        {
            await DeleteIfExistsAsync(container);
        }
    }

    [LiveAzureFact]
    [Trait("Category", "LiveAzure")]
    public async Task FlatNamespaceSdkStateAndErrorsMatchLiveAzure()
    {
        var connectionString = Environment.GetEnvironmentVariable(LiveAzureFactAttribute.ConnectionStringVariable)
                               ?? throw new InvalidOperationException("The live Azure test account was removed after discovery.");

        var remote = new BlobServiceClient(connectionString, new BlobClientOptions(
            BlobClientOptions.ServiceVersion.V2023_11_03)
        {
            Retry = { MaxRetries = 0 }
        });
        Assert.False((await remote.GetAccountInfoAsync()).Value.IsHierarchicalNamespaceEnabled);
        await using var localApplication = new SavaWebApplicationFactory(new Dictionary<string, string?>
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });
        await localApplication.InitializeAsync();
        var local = CreateLocalClient(localApplication);

        var containerName = $"mk8diff-{Guid.NewGuid():N}";
        var remoteContainer = remote.GetBlobContainerClient(containerName);
        var localContainer = local.GetBlobContainerClient(containerName);
        try
        {
            var expected = await ExerciseAsync(remoteContainer);
            var actual = await ExerciseAsync(localContainer);
            Assert.Equal(expected, actual);
            output.WriteLine("Live Azure and mk8.sava matched the pinned flat-account SDK scenario.");
        }
        finally
        {
            await DeleteIfExistsAsync(localContainer);
            await DeleteIfExistsAsync(remoteContainer);
        }
    }

    [LiveAzureFact(LiveAzureFactAttribute.HnsConnectionStringVariable)]
    [Trait("Category", "LiveAzure")]
    public async Task HierarchicalNamespaceSdkStateAndErrorsMatchLiveAzure()
    {
        var connectionString = Environment.GetEnvironmentVariable(
            LiveAzureFactAttribute.HnsConnectionStringVariable)
                               ?? throw new InvalidOperationException("The live HNS account was removed after discovery.");
        var remote = new BlobServiceClient(connectionString, new BlobClientOptions(
            BlobClientOptions.ServiceVersion.V2023_11_03)
        {
            Retry = { MaxRetries = 0 }
        });
        Assert.True((await remote.GetAccountInfoAsync()).Value.IsHierarchicalNamespaceEnabled);
        await using var localApplication = new SavaWebApplicationFactory(new Dictionary<string, string?>
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true",
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });
        await localApplication.InitializeAsync();
        var local = CreateLocalClient(localApplication);
        var containerName = $"mk8diff-hns-{Guid.NewGuid():N}";
        var remoteContainer = remote.GetBlobContainerClient(containerName);
        var localContainer = local.GetBlobContainerClient(containerName);
        try
        {
            var expected = await ExerciseHierarchicalAsync(remoteContainer);
            var actual = await ExerciseHierarchicalAsync(localContainer);
            Assert.Equal(expected, actual);
            output.WriteLine("Live Azure and mk8.sava matched the pinned HNS Shared Key SDK scenario.");
        }
        finally
        {
            await DeleteIfExistsAsync(localContainer);
            await DeleteIfExistsAsync(remoteContainer);
        }
    }

    [LiveAzureFact]
    [Trait("Category", "LiveAzure")]
    public async Task FlatAuthorizationAndConcurrencyMatchLiveAzure()
    {
        var connectionString = Environment.GetEnvironmentVariable(LiveAzureFactAttribute.ConnectionStringVariable)
                               ?? throw new InvalidOperationException("The live Azure test account was removed after discovery.");
        var remote = new BlobServiceClient(connectionString, new BlobClientOptions(
            BlobClientOptions.ServiceVersion.V2023_11_03)
        {
            Retry = { MaxRetries = 0 }
        });
        Assert.False((await remote.GetAccountInfoAsync()).Value.IsHierarchicalNamespaceEnabled);
        await using var localApplication = new SavaWebApplicationFactory(new Dictionary<string, string?>
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });
        await localApplication.InitializeAsync();
        var local = CreateLocalClient(localApplication);
        var containerName = $"mk8diff-auth-{Guid.NewGuid():N}";
        var remoteContainer = remote.GetBlobContainerClient(containerName);
        var localContainer = local.GetBlobContainerClient(containerName);
        try
        {
            var expected = await ExerciseAuthorizationAsync(
                remoteContainer,
                uri => new BlobClient(uri, new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03)
                {
                    Retry = { MaxRetries = 0 }
                }));
            var actual = await ExerciseAuthorizationAsync(
                localContainer,
                uri => CreateLocalSasClient(localApplication, uri));
            Assert.Equal(expected, actual);
            output.WriteLine("Live Azure and mk8.sava matched the pinned flat-account SAS/condition/lease scenario.");
        }
        finally
        {
            await DeleteIfExistsAsync(localContainer);
            await DeleteIfExistsAsync(remoteContainer);
        }
    }

    private static BlobServiceClient CreateLocalClient(SavaWebApplicationFactory application)
    {
        var endpoint = new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost");
        return new BlobServiceClient(
            endpoint,
            new StorageSharedKeyCredential(
                SavaWebApplicationFactory.AccountName,
                SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03)
            {
                Transport = new HttpClientTransport(new HttpClient(application.Server.CreateHandler())
                {
                    BaseAddress = endpoint
                }),
                Retry = { MaxRetries = 0 }
            });
    }

    private static BlobClient CreateLocalSasClient(SavaWebApplicationFactory application, Uri uri) =>
        new(uri, new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03)
        {
            Transport = new HttpClientTransport(new HttpClient(application.Server.CreateHandler())
            {
                BaseAddress = uri
            }),
            Retry = { MaxRetries = 0 }
        });

    private static async Task<AuthorizationObservation> ExerciseAuthorizationAsync(
        BlobContainerClient container,
        Func<Uri, BlobClient> createSasClient)
    {
        await container.CreateAsync();
        var blob = container.GetBlobClient("protected.txt");
        await blob.UploadAsync(BinaryData.FromString("original-content"));
        Assert.True(blob.CanGenerateSasUri);
        var sas = createSasClient(blob.GenerateSasUri(
            BlobSasPermissions.Read,
            DateTimeOffset.UtcNow.AddMinutes(10)));
        var sasBytes = (await sas.DownloadContentAsync()).Value.Content.ToString();
        var deniedSasWrite = await Assert.ThrowsAsync<RequestFailedException>(() =>
            sas.UploadAsync(BinaryData.FromString("blocked"), overwrite: true));
        var staleCondition = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.GetPropertiesAsync(new BlobRequestConditions
            {
                IfMatch = new ETag("\"not-the-current-etag\"")
            }));

        var lease = blob.GetBlobLeaseClient();
        var acquired = await lease.AcquireAsync(TimeSpan.FromSeconds(15));
        RequestFailedException missingLease;
        try
        {
            missingLease = await Assert.ThrowsAsync<RequestFailedException>(() =>
                blob.SetMetadataAsync(new Dictionary<string, string> { ["state"] = "blocked" }));
            await blob.SetMetadataAsync(
                new Dictionary<string, string> { ["state"] = "authorized" },
                new BlobRequestConditions { LeaseId = acquired.Value.LeaseId });
        }
        finally
        {
            await lease.ReleaseAsync();
        }

        var properties = await blob.GetPropertiesAsync();
        var finalBytes = (await blob.DownloadContentAsync()).Value.Content.ToString();
        return new AuthorizationObservation(
            sasBytes,
            deniedSasWrite.Status,
            deniedSasWrite.ErrorCode ?? string.Empty,
            staleCondition.Status,
            staleCondition.ErrorCode ?? string.Empty,
            missingLease.Status,
            missingLease.ErrorCode ?? string.Empty,
            properties.Value.Metadata["state"],
            finalBytes);
    }

    private static async Task<HierarchicalObservation> ExerciseHierarchicalAsync(BlobContainerClient container)
    {
        await container.CreateAsync();
        var file = container.GetBlobClient("alpha/beta/file.txt");
        await file.UploadAsync(BinaryData.FromString("nested HNS content"));
        await container.GetBlobClient("zeta.txt").UploadAsync(BinaryData.FromString("root content"));
        var alpha = await container.GetBlobClient("alpha").GetPropertiesAsync();
        var beta = await container.GetBlobClient("alpha/beta").GetPropertiesAsync();
        var fileProperties = await file.GetPropertiesAsync();
        var downloaded = await file.DownloadContentAsync();
        var names = new List<string>();
        await foreach (var item in container.GetBlobsAsync())
            names.Add(item.Name);
        var notEmpty = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient("alpha").DeleteAsync());

        Assert.Equal("directory", RequiredHeader(alpha, "x-ms-resource-type"));
        Assert.Equal("directory", RequiredHeader(beta, "x-ms-resource-type"));
        Assert.Equal("file", RequiredHeader(fileProperties, "x-ms-resource-type"));
        Assert.Equal("nested HNS content", downloaded.Value.Content.ToString());

        return new HierarchicalObservation(
            RequiredHeader(alpha, "x-ms-owner"),
            RequiredHeader(alpha, "x-ms-group"),
            RequiredHeader(alpha, "x-ms-permissions"),
            RequiredHeader(beta, "x-ms-owner"),
            RequiredHeader(beta, "x-ms-group"),
            RequiredHeader(fileProperties, "x-ms-owner"),
            RequiredHeader(fileProperties, "x-ms-group"),
            RequiredHeader(fileProperties, "x-ms-permissions"),
            RequiredHeader(fileProperties, "x-ms-acl"),
            downloaded.Value.Content.ToString(),
            string.Join(',', names),
            notEmpty.Status,
            notEmpty.ErrorCode ?? string.Empty);
    }

    private static string RequiredHeader(Response<BlobProperties> response, string name)
    {
        Assert.True(response.GetRawResponse().Headers.TryGetValue(name, out var value));
        return value;
    }

    private static async Task<FlatObservation> ExerciseAsync(BlobContainerClient container)
    {
        var created = await container.CreateAsync();
        var bytes = new byte[128 * 1024];
        new Random(0x4D4B38).NextBytes(bytes);
        var blob = container.GetBlobClient("nested/original.bin");
        var uploaded = await blob.UploadAsync(BinaryData.FromBytes(bytes), new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders
            {
                ContentType = "application/x-mk8-differential"
            },
            Metadata = new Dictionary<string, string> { ["case"] = "flat" },
            Tags = new Dictionary<string, string> { ["phase"] = "initial" }
        });
        var properties = await blob.GetPropertiesAsync();
        var tags = await blob.GetTagsAsync();
        var downloaded = await blob.DownloadContentAsync();
        var ranged = await blob.DownloadStreamingAsync(new BlobDownloadOptions
        {
            Range = new HttpRange(4093, 8195)
        });
        using var rangeBuffer = new MemoryStream();
        await ranged.Value.Content.CopyToAsync(rangeBuffer);

        var snapshot = await blob.CreateSnapshotAsync();
        await blob.UploadAsync(BinaryData.FromString("replacement"), overwrite: true);
        var snapshotContent = await blob.WithSnapshot(snapshot.Value.Snapshot).DownloadContentAsync();

        var block = container.GetBlockBlobClient("blocks.bin");
        var firstBlockId = Convert.ToBase64String("000001"u8);
        var secondBlockId = Convert.ToBase64String("000002"u8);
        await block.StageBlockAsync(firstBlockId, BinaryData.FromString("alpha").ToStream());
        await block.StageBlockAsync(secondBlockId, BinaryData.FromString("beta").ToStream());
        await block.CommitBlockListAsync([secondBlockId, firstBlockId]);
        var blockContent = await block.DownloadContentAsync();
        var blockList = await block.GetBlockListAsync(BlockListTypes.Committed);

        var append = container.GetAppendBlobClient("append.log");
        await append.CreateAsync();
        await append.AppendBlockAsync(BinaryData.FromString("first|").ToStream());
        await append.AppendBlockAsync(BinaryData.FromString("second").ToStream());
        var appendContent = await append.DownloadContentAsync();

        var page = container.GetPageBlobClient("page.bin");
        await page.CreateAsync(1024);
        var pageBytes = new byte[512];
        new Random(0x50414745).NextBytes(pageBytes);
        await page.UploadPagesAsync(new MemoryStream(pageBytes, writable: false), 512);
        var pageContent = await page.DownloadContentAsync();

        var missing = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient("missing.bin").GetPropertiesAsync());
        Assert.NotNull(missing.ErrorCode);
        var names = new List<string>();
        await foreach (var item in container.GetBlobsAsync())
            names.Add(item.Name);

        Assert.Equal(bytes, downloaded.Value.Content.ToArray());
        Assert.Equal(bytes.AsSpan(4093, 8195).ToArray(), rangeBuffer.ToArray());
        Assert.Equal(bytes, snapshotContent.Value.Content.ToArray());
        Assert.Equal("betaalpha", blockContent.Value.Content.ToString());
        Assert.Equal("first|second", appendContent.Value.Content.ToString());
        Assert.Equal(pageBytes, pageContent.Value.Content.ToArray().AsSpan(512, 512).ToArray());
        Assert.Equal(new[] { firstBlockId, secondBlockId },
            blockList.Value.CommittedBlocks.Select(item => item.Name).Order(StringComparer.Ordinal));

        return new FlatObservation(
            created.GetRawResponse().Status,
            uploaded.GetRawResponse().Status,
            properties.Value.ContentLength,
            properties.Value.ContentType,
            properties.Value.Metadata["case"],
            tags.Value.Tags["phase"],
            Convert.ToBase64String(downloaded.Value.Content.ToArray()),
            Convert.ToBase64String(rangeBuffer.ToArray()),
            Convert.ToBase64String(snapshotContent.Value.Content.ToArray()),
            blockContent.Value.Content.ToString(),
            string.Join(',', blockList.Value.CommittedBlocks.Select(item => item.Name)),
            appendContent.Value.Content.ToString(),
            Convert.ToBase64String(pageContent.Value.Content.ToArray()),
            missing.Status,
            missing.ErrorCode,
            string.Join(',', names));
    }

    private static async Task DeleteIfExistsAsync(BlobContainerClient container)
    {
        try
        {
            await container.DeleteIfExistsAsync();
        }
        catch (RequestFailedException exception) when (exception.Status == 404)
        {
        }
    }

    private sealed record FlatObservation(
        int CreateStatus,
        int UploadStatus,
        long Length,
        string ContentType,
        string Metadata,
        string Tag,
        string FullBytes,
        string RangeBytes,
        string SnapshotBytes,
        string BlockBytes,
        string CommittedBlockIds,
        string AppendBytes,
        string PageBytes,
        int MissingStatus,
        string MissingCode,
        string ListedNames);

    private sealed record AuthorizationObservation(
        string SasReadBytes,
        int DeniedSasWriteStatus,
        string DeniedSasWriteCode,
        int StaleConditionStatus,
        string StaleConditionCode,
        int MissingLeaseStatus,
        string MissingLeaseCode,
        string FinalMetadata,
        string FinalBytes);

    private sealed record HierarchicalObservation(
        string DirectoryOwner,
        string DirectoryGroup,
        string DirectoryPermissions,
        string NestedDirectoryOwner,
        string NestedDirectoryGroup,
        string FileOwner,
        string FileGroup,
        string FilePermissions,
        string FileAcl,
        string FileBytes,
        string ListedNames,
        int NonemptyDirectoryDeleteStatus,
        string NonemptyDirectoryDeleteCode);
}
