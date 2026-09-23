using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace Mk8.Sava.Tests;

public sealed class AzuriteDifferentialTests
{
    [AzuriteFact]
    [Trait("Category", "Azurite")]
    public async Task SupportedFlatBlobSdkOperationsMatchAzurite()
    {
        var connectionString = Environment.GetEnvironmentVariable(AzuriteFactAttribute.ConnectionStringVariable)
            ?? throw new InvalidOperationException("The Azurite connection string was removed after discovery.");
        var azurite = new BlobServiceClient(connectionString, CreateOptions());
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var local = CreateLocalClient(application);
        var name = $"mk8-azurite-{Guid.NewGuid():N}";
        var azuriteContainer = azurite.GetBlobContainerClient(name);
        var localContainer = local.GetBlobContainerClient(name);
        try
        {
            var expected = await ExerciseAsync(azuriteContainer).ConfigureAwait(false);
            var actual = await ExerciseAsync(localContainer).ConfigureAwait(false);
            Assert.Equal(expected, actual);
        }
        finally
        {
            await DeleteIfExistsAsync(localContainer).ConfigureAwait(false);
            await DeleteIfExistsAsync(azuriteContainer).ConfigureAwait(false);
        }
    }

    [AzuriteFact]
    [Trait("Category", "Azurite")]
    public async Task StagedSnapshotLeaseAndTagOperationsMatchAzurite()
    {
        var connectionString = Environment.GetEnvironmentVariable(AzuriteFactAttribute.ConnectionStringVariable)
            ?? throw new InvalidOperationException("The Azurite connection string was removed after discovery.");
        var azurite = new BlobServiceClient(connectionString, CreateOptions());
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var local = CreateLocalClient(application);
        var name = $"mk8-azurite-{Guid.NewGuid():N}";
        var azuriteContainer = azurite.GetBlobContainerClient(name);
        var localContainer = local.GetBlobContainerClient(name);
        try
        {
            var expected = await ExerciseStagedBlobAsync(azuriteContainer).ConfigureAwait(false);
            var actual = await ExerciseStagedBlobAsync(localContainer).ConfigureAwait(false);
            Assert.Equal(expected, actual);
        }
        finally
        {
            await DeleteIfExistsAsync(localContainer).ConfigureAwait(false);
            await DeleteIfExistsAsync(azuriteContainer).ConfigureAwait(false);
        }
    }

    [AzuriteFact]
    [Trait("Category", "Azurite")]
    public async Task AppendAndPageBlobOperationsMatchAzurite()
    {
        var connectionString = Environment.GetEnvironmentVariable(AzuriteFactAttribute.ConnectionStringVariable)
            ?? throw new InvalidOperationException("The Azurite connection string was removed after discovery.");
        var azurite = new BlobServiceClient(connectionString, CreateOptions());
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var local = CreateLocalClient(application);
        var name = $"mk8-azurite-{Guid.NewGuid():N}";
        var azuriteContainer = azurite.GetBlobContainerClient(name);
        var localContainer = local.GetBlobContainerClient(name);
        try
        {
            var expected = await ExerciseAppendAndPageAsync(azuriteContainer).ConfigureAwait(false);
            var actual = await ExerciseAppendAndPageAsync(localContainer).ConfigureAwait(false);
            Assert.Equal(expected, actual);
        }
        finally
        {
            await DeleteIfExistsAsync(localContainer).ConfigureAwait(false);
            await DeleteIfExistsAsync(azuriteContainer).ConfigureAwait(false);
        }
    }

    [AzuriteFact]
    [Trait("Category", "Azurite")]
    public async Task ContainerMetadataPolicyAndLeaseOperationsMatchAzurite()
    {
        var connectionString = Environment.GetEnvironmentVariable(AzuriteFactAttribute.ConnectionStringVariable)
            ?? throw new InvalidOperationException("The Azurite connection string was removed after discovery.");
        var azurite = new BlobServiceClient(connectionString, CreateOptions());
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:AllowAnonymousPublicAccess"] = "true"
        });
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var local = CreateLocalClient(application);
        var name = $"mk8-azurite-{Guid.NewGuid():N}";
        var azuriteContainer = azurite.GetBlobContainerClient(name);
        var localContainer = local.GetBlobContainerClient(name);
        try
        {
            var expected = await ExerciseContainerAsync(azuriteContainer).ConfigureAwait(false);
            var actual = await ExerciseContainerAsync(localContainer).ConfigureAwait(false);
            Assert.Equal(expected, actual);
        }
        finally
        {
            await DeleteIfExistsAsync(localContainer).ConfigureAwait(false);
            await DeleteIfExistsAsync(azuriteContainer).ConfigureAwait(false);
        }
    }

    [AzuriteFact]
    [Trait("Category", "Azurite")]
    public async Task ContainerPrefixMetadataAndPagingMatchAzurite()
    {
        var connectionString = Environment.GetEnvironmentVariable(AzuriteFactAttribute.ConnectionStringVariable)
            ?? throw new InvalidOperationException("The Azurite connection string was removed after discovery.");
        var azurite = new BlobServiceClient(connectionString, CreateOptions());
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var local = CreateLocalClient(application);
        var prefix = $"mk8-azurite-{Guid.NewGuid():N}-";
        try
        {
            var expected = await ExerciseContainerListingAsync(azurite, prefix).ConfigureAwait(false);
            var actual = await ExerciseContainerListingAsync(local, prefix).ConfigureAwait(false);
            Assert.Equal(expected, actual);
        }
        finally
        {
            foreach (var suffix in new[] { "a", "b", "c" })
            {
                await DeleteIfExistsAsync(local.GetBlobContainerClient(prefix + suffix)).ConfigureAwait(false);
                await DeleteIfExistsAsync(azurite.GetBlobContainerClient(prefix + suffix)).ConfigureAwait(false);
            }
            await DeleteIfExistsAsync(local.GetBlobContainerClient(prefix[..^1] + "x")).ConfigureAwait(false);
            await DeleteIfExistsAsync(azurite.GetBlobContainerClient(prefix[..^1] + "x")).ConfigureAwait(false);
        }
    }

    [AzuriteFact]
    [Trait("Category", "Azurite")]
    public async Task ServiceCorsPropertiesMatchAzurite()
    {
        var connectionString = Environment.GetEnvironmentVariable(AzuriteFactAttribute.ConnectionStringVariable)
            ?? throw new InvalidOperationException("The Azurite connection string was removed after discovery.");
        var azurite = new BlobServiceClient(connectionString, CreateOptions());
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var local = CreateLocalClient(application);
        using var azuriteTransport = new HttpClient();
        using var localTransport = new HttpClient(application.Server.CreateHandler());
        var expected = await ExerciseServiceCorsAsync(azurite, azuriteTransport).ConfigureAwait(false);
        var actual = await ExerciseServiceCorsAsync(local, localTransport).ConfigureAwait(false);
        Assert.Equal(expected, actual);
    }

    private static BlobServiceClient CreateLocalClient(SavaWebApplicationFactory application)
    {
        var account = SavaWebApplicationFactory.AccountName;
        var endpoint = new Uri($"http://{account}.localhost");
        var options = CreateOptions();
        options.Transport = new HttpClientTransport(application.Server.CreateHandler());
        return new BlobServiceClient(
            endpoint,
            new StorageSharedKeyCredential(account, SavaWebApplicationFactory.AccountKey),
            options);
    }

    private static BlobClientOptions CreateOptions() =>
        new(BlobClientOptions.ServiceVersion.V2023_11_03) { Retry = { MaxRetries = 0 } };

    private static async Task<FlatBlobObservation> ExerciseAsync(BlobContainerClient container)
    {
        var created = await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("nested/payload.bin");
        var bytes = new byte[8192];
        DeterministicTestBytes.Fill(0xA20B, bytes);
        var uploaded = await blob.UploadAsync(BinaryData.FromBytes(bytes), new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders { ContentType = "application/x-mk8-azurite" },
            Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["phase"] = "initial" }
        }).ConfigureAwait(false);
        var properties = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        var full = (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray();
        var range = await ReadRangeAsync(blob).ConfigureAwait(false);
        var condition = await ExerciseConditionAsync(blob).ConfigureAwait(false);
        var names = new List<string>();
        await foreach (var item in container.GetBlobsAsync().ConfigureAwait(false))
            names.Add(item.Name);
        await blob.DeleteAsync().ConfigureAwait(false);
        var missing = await Assert.ThrowsAsync<RequestFailedException>(
            () => blob.GetPropertiesAsync()).ConfigureAwait(false);
        return new FlatBlobObservation(
            created.GetRawResponse().Status,
            uploaded.GetRawResponse().Status,
            properties.ContentLength,
            properties.ContentType,
            properties.Metadata["phase"],
            Convert.ToBase64String(full),
            Convert.ToBase64String(range),
            condition.Status,
            condition.ErrorCode,
            missing.Status,
            missing.ErrorCode,
            string.Join(',', names));
    }

    private static async Task<byte[]> ReadRangeAsync(BlobClient blob)
    {
        var result = await blob.DownloadStreamingAsync(new BlobDownloadOptions
        {
            Range = new HttpRange(4093, 1025)
        }).ConfigureAwait(false);
        var stream = result.Value.Content;
        await using var streamDisposal = stream.ConfigureAwait(false);
        using var buffer = new MemoryStream();
        await stream.CopyToAsync(buffer).ConfigureAwait(false);
        return buffer.ToArray();
    }

    private static async Task<RequestFailedException> ExerciseConditionAsync(BlobClient blob)
    {
        var stale = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag;
        await blob.SetMetadataAsync(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["phase"] = "updated" })
            .ConfigureAwait(false);
        return await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.SetMetadataAsync(
                new Dictionary<string, string>(StringComparer.Ordinal) { ["phase"] = "rejected" },
                new BlobRequestConditions { IfMatch = stale })).ConfigureAwait(false);
    }

    private static async Task<StagedBlobObservation> ExerciseStagedBlobAsync(BlobContainerClient container)
    {
        await container.CreateAsync().ConfigureAwait(false);
        var block = container.GetBlockBlobClient("staged.bin");
        var firstId = Convert.ToBase64String("0001"u8);
        var secondId = Convert.ToBase64String("0002"u8);
        using var first = new MemoryStream("north"u8.ToArray(), writable: false);
        using var second = new MemoryStream("south"u8.ToArray(), writable: false);
        await block.StageBlockAsync(firstId, first).ConfigureAwait(false);
        await block.StageBlockAsync(secondId, second).ConfigureAwait(false);
        var uncommitted = (await block.GetBlockListAsync(BlockListTypes.Uncommitted).ConfigureAwait(false))
            .Value.UncommittedBlocks.Count();
        var commit = await block.CommitBlockListAsync([secondId, firstId]).ConfigureAwait(false);
        var committed = (await block.GetBlockListAsync(BlockListTypes.Committed).ConfigureAwait(false))
            .Value.CommittedBlocks.Count();
        var content = (await block.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString();
        var snapshot = (await block.CreateSnapshotAsync().ConfigureAwait(false)).Value.Snapshot;
        var snapshotContent = (await block.WithSnapshot(snapshot).DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToString();
        var lease = block.GetBlobLeaseClient();
        await lease.AcquireAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() =>
            block.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["phase"] = "leased"
            })).ConfigureAwait(false);
        await lease.ReleaseAsync().ConfigureAwait(false);
        await block.SetTagsAsync(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["phase"] = "committed"
        }).ConfigureAwait(false);
        var tags = (await block.GetTagsAsync().ConfigureAwait(false)).Value.Tags;
        return new StagedBlobObservation(
            uncommitted, commit.GetRawResponse().Status, committed,
            content, snapshotContent, rejected.Status, rejected.ErrorCode, tags["phase"]);
    }

    private static async Task<AppendPageObservation> ExerciseAppendAndPageAsync(BlobContainerClient container)
    {
        await container.CreateAsync().ConfigureAwait(false);
        var append = container.GetAppendBlobClient("events.log");
        var appendCreate = await append.CreateAsync().ConfigureAwait(false);
        using var appendContent = new MemoryStream("event-one"u8.ToArray(), writable: false);
        var appendWrite = await append.AppendBlockAsync(appendContent).ConfigureAwait(false);
        var appended = (await append.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString();

        var page = container.GetPageBlobClient("disk.vhd");
        var pageCreate = await page.CreateAsync(1024).ConfigureAwait(false);
        var payload = Enumerable.Repeat((byte)0x5a, 512).ToArray();
        using var pageContent = new MemoryStream(payload, writable: false);
        var pageWrite = await page.UploadPagesAsync(pageContent, offset: 512).ConfigureAwait(false);
        var range = Assert.Single((await page.GetPageRangesAsync().ConfigureAwait(false)).Value.PageRanges);
        var download = (await page.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray();
        await page.ClearPagesAsync(new HttpRange(512, 512)).ConfigureAwait(false);
        var remaining = (await page.GetPageRangesAsync().ConfigureAwait(false)).Value.PageRanges.Count();
        return new AppendPageObservation(
            appendCreate.GetRawResponse().Status,
            appendWrite.GetRawResponse().Status,
            appended,
            pageCreate.GetRawResponse().Status,
            pageWrite.GetRawResponse().Status,
            range.Offset,
            range.Length,
            Convert.ToBase64String(download),
            remaining);
    }

    private static async Task<ContainerObservation> ExerciseContainerAsync(BlobContainerClient container)
    {
        var created = await container.CreateAsync(
            PublicAccessType.None,
            new Dictionary<string, string>(StringComparer.Ordinal) { ["phase"] = "initial" })
            .ConfigureAwait(false);
        var initial = (await container.GetPropertiesAsync().ConfigureAwait(false)).Value.Metadata["phase"];
        var metadataWrite = await container.SetMetadataAsync(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["phase"] = "updated" })
            .ConfigureAwait(false);
        var updated = (await container.GetPropertiesAsync().ConfigureAwait(false)).Value.Metadata["phase"];
        var policyWrite = await container.SetAccessPolicyAsync(PublicAccessType.Blob).ConfigureAwait(false);
        var policy = (await container.GetAccessPolicyAsync().ConfigureAwait(false)).Value.BlobPublicAccess;
        var lease = container.GetBlobLeaseClient();
        await lease.AcquireAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() => container.DeleteAsync())
            .ConfigureAwait(false);
        await lease.ReleaseAsync().ConfigureAwait(false);
        var deleted = await container.DeleteAsync().ConfigureAwait(false);
        return new ContainerObservation(
            created.GetRawResponse().Status, initial, metadataWrite.GetRawResponse().Status,
            updated, policyWrite.GetRawResponse().Status, policy.ToString(),
            rejected.Status, rejected.ErrorCode, deleted.Status);
    }

    private static async Task<ContainerListingObservation> ExerciseContainerListingAsync(
        BlobServiceClient service,
        string prefix)
    {
        foreach (var suffix in new[] { "a", "b", "c" })
        {
            await service.GetBlobContainerClient(prefix + suffix).CreateAsync(
                metadata: new Dictionary<string, string>(StringComparer.Ordinal) { ["phase"] = suffix })
                .ConfigureAwait(false);
        }
        await service.GetBlobContainerClient(prefix[..^1] + "x").CreateAsync().ConfigureAwait(false);

        var pages = new List<string>();
        var continuationCount = 0;
        await foreach (var page in service.GetBlobContainersAsync(BlobContainerTraits.Metadata, prefix: prefix)
                           .AsPages(pageSizeHint: 1).ConfigureAwait(false))
        {
            pages.Add(string.Join(',', page.Values.Select(item => $"{item.Name}:{item.Properties.Metadata["phase"]}")));
            if (!string.IsNullOrEmpty(page.ContinuationToken))
                continuationCount++;
        }

        Assert.Equal([$"{prefix}a:a", $"{prefix}b:b", $"{prefix}c:c"], pages);
        Assert.Equal(2, continuationCount);

        return new ContainerListingObservation(string.Join('|', pages), continuationCount);
    }

    private static async Task<ServiceCorsObservation> ExerciseServiceCorsAsync(
        BlobServiceClient service,
        HttpClient transport)
    {
        var initial = await service.GetPropertiesAsync().ConfigureAwait(false);
        var properties = initial.Value;
        properties.Cors.Clear();
        properties.Cors.Add(new BlobCorsRule
        {
            AllowedOrigins = "https://client.example.test",
            AllowedMethods = "GET,HEAD",
            AllowedHeaders = "x-ms-meta-*",
            ExposedHeaders = "x-ms-request-id",
            MaxAgeInSeconds = 321
        });
        var written = await service.SetPropertiesAsync(properties).ConfigureAwait(false);
        var read = await service.GetPropertiesAsync().ConfigureAwait(false);
        var rule = Assert.Single(read.Value.Cors);
        var preflight = await ExerciseCorsPreflightAsync(transport, service.Uri).ConfigureAwait(false);
        return new ServiceCorsObservation(initial.GetRawResponse().Status, written.Status,
            read.GetRawResponse().Status, rule.AllowedOrigins, rule.AllowedMethods,
            rule.AllowedHeaders, rule.ExposedHeaders, rule.MaxAgeInSeconds, preflight);
    }

    private static async Task<CorsPreflightObservation> ExerciseCorsPreflightAsync(
        HttpClient transport,
        Uri serviceUri)
    {
        using var request = new HttpRequestMessage(HttpMethod.Options, new Uri(serviceUri, "?comp=list"));
        request.Headers.TryAddWithoutValidation("Origin", "https://client.example.test");
        request.Headers.TryAddWithoutValidation("Access-Control-Request-Method", "GET");
        request.Headers.TryAddWithoutValidation("Access-Control-Request-Headers", "x-ms-meta-test");
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        return new CorsPreflightObservation(
            (int)response.StatusCode,
            string.Join(',', response.Headers.GetValues("Access-Control-Allow-Origin")),
            string.Join(',', response.Headers.GetValues("Access-Control-Allow-Methods")),
            string.Join(',', response.Headers.GetValues("Access-Control-Max-Age")));
    }

    private static async Task DeleteIfExistsAsync(BlobContainerClient container)
    {
        try
        {
            await container.DeleteIfExistsAsync().ConfigureAwait(false);
        }
        catch (RequestFailedException exception) when (exception.Status == 404)
        {
        }
    }

    private sealed record FlatBlobObservation(
        int CreateStatus,
        int UploadStatus,
        long Length,
        string ContentType,
        string Metadata,
        string FullBytes,
        string RangeBytes,
        int StaleConditionStatus,
        string? StaleConditionCode,
        int MissingStatus,
        string? MissingCode,
        string ListedNames);

    private sealed record StagedBlobObservation(
        int UncommittedBlocks,
        int CommitStatus,
        int CommittedBlocks,
        string Content,
        string SnapshotContent,
        int MissingLeaseStatus,
        string? MissingLeaseCode,
        string Tag);

    private sealed record AppendPageObservation(
        int AppendCreateStatus,
        int AppendWriteStatus,
        string AppendedContent,
        int PageCreateStatus,
        int PageWriteStatus,
        long PageRangeOffset,
        long? PageRangeLength,
        string PageContent,
        int RemainingRanges);

    private sealed record ContainerObservation(
        int CreateStatus,
        string InitialMetadata,
        int SetMetadataStatus,
        string UpdatedMetadata,
        int SetPolicyStatus,
        string PublicAccess,
        int MissingLeaseStatus,
        string? MissingLeaseCode,
        int DeleteStatus);

    private sealed record ContainerListingObservation(string Pages, int ContinuationCount);

    private sealed record ServiceCorsObservation(
        int InitialStatus, int SetStatus, int GetStatus,
        string AllowedOrigins, string AllowedMethods, string AllowedHeaders,
        string ExposedHeaders, int MaxAgeInSeconds, CorsPreflightObservation Preflight);

    private sealed record CorsPreflightObservation(int Status, string AllowedOrigin, string AllowedMethods, string MaxAge);
}
