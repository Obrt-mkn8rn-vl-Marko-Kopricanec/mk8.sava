using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class ObjectReplicationTests
{
    private const string PolicyId = "70dc1326-15a2-45f4-9408-487f6581f204";
    private const string RuleId = "a2455a77-8f44-4d22-b07e-a03203ae64f0";

    [Fact]
    public async Task MaintenanceReplicatesBlockBlobAndProjectsAzureSdkProperties()
    {
        const string sourceContainerName = "orsource";
        const string destinationContainerName = "ordestination";
        await using var factory = CreateFactory(sourceContainerName, destinationContainerName, replicateTags: true);
        await factory.InitializeAsync();
        var source = CreateClient(
            factory,
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        var destination = CreateClient(
            factory,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var sourceContainer = source.GetBlobContainerClient(sourceContainerName);
        var destinationContainer = destination.GetBlobContainerClient(destinationContainerName);
        await sourceContainer.CreateAsync();
        await destinationContainer.CreateAsync();

        var bytes = Enumerable.Range(0, 96 * 1024).Select(index => (byte)(index % 17)).ToArray();
        var sourceBlob = sourceContainer.GetBlobClient("eligible/blob.bin");
        await sourceBlob.UploadAsync(BinaryData.FromBytes(bytes), new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders { ContentType = "application/x-object-replication" },
            Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["owner"] = "source" },
            Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["replicated"] = "yes" }
        });

        var maintenance = await factory.Services.GetRequiredService<BlobService>()
            .RunMaintenanceAsync(CancellationToken.None);

        Assert.Equal(1, maintenance.CompletedObjectReplications);
        Assert.Equal(0, maintenance.FailedObjectReplications);
        var destinationBlob = destinationContainer.GetBlobClient(sourceBlob.Name);
        var downloaded = await destinationBlob.DownloadContentAsync();
        Assert.Equal(bytes, downloaded.Value.Content.ToArray());
        Assert.Equal("application/x-object-replication", downloaded.Value.Details.ContentType);
        Assert.Equal("source", downloaded.Value.Details.Metadata["owner"]);
        Assert.Equal(PolicyId, downloaded.Value.Details.ObjectReplicationDestinationPolicyId);
        Assert.Equal("yes", (await destinationBlob.GetTagsAsync()).Value.Tags["replicated"]);

        var destinationProperties = (await destinationBlob.GetPropertiesAsync()).Value;
        Assert.Equal(PolicyId, destinationProperties.ObjectReplicationDestinationPolicyId);
        var sourceProperties = (await sourceBlob.GetPropertiesAsync()).Value;
        var policy = Assert.Single(sourceProperties.ObjectReplicationSourceProperties);
        Assert.Equal(PolicyId, policy.PolicyId);
        var rule = Assert.Single(policy.Rules);
        Assert.Equal(RuleId, rule.RuleId);
        Assert.Equal(ObjectReplicationStatus.Complete, rule.ReplicationStatus);

        BlobItem? listed = null;
        await foreach (var item in sourceContainer.GetBlobsAsync(
                           traits: BlobTraits.None,
                           states: BlobStates.None,
                           prefix: "eligible/",
                           cancellationToken: CancellationToken.None))
            listed = item;
        var listedPolicy = Assert.Single(Assert.IsType<BlobItem>(listed).ObjectReplicationSourceProperties);
        Assert.Equal(PolicyId, listedPolicy.PolicyId);
        Assert.Equal(ObjectReplicationStatus.Complete, Assert.Single(listedPolicy.Rules).ReplicationStatus);
    }

    [Fact]
    public async Task DestinationRejectsWritesButAllowsTierDeleteAndLaterSourceChange()
    {
        const string sourceContainerName = "write-source";
        const string destinationContainerName = "write-destination";
        await using var factory = CreateFactory(sourceContainerName, destinationContainerName, replicateTags: false);
        await factory.InitializeAsync();
        var source = CreateClient(
            factory,
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        var destination = CreateClient(
            factory,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var sourceContainer = source.GetBlobContainerClient(sourceContainerName);
        var destinationContainer = destination.GetBlobContainerClient(destinationContainerName);
        await sourceContainer.CreateAsync();
        await destinationContainer.CreateAsync();
        var sourceBlob = sourceContainer.GetBlobClient("blob.bin");
        await sourceBlob.UploadAsync(BinaryData.FromString("replicated"), new BlobUploadOptions
        {
            Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["source-only"] = "true" }
        });
        var service = factory.Services.GetRequiredService<BlobService>();
        _ = await service.RunMaintenanceAsync(CancellationToken.None);
        var destinationBlob = destinationContainer.GetBlobClient(sourceBlob.Name);
        Assert.Empty((await destinationBlob.GetTagsAsync()).Value.Tags);

        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() =>
            destinationBlob.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["forbidden"] = "true" }));
        Assert.Equal(409, rejected.Status);
        Assert.Equal("BlobOperationNotSupported", rejected.ErrorCode);
        await destinationBlob.SetAccessTierAsync(AccessTier.Archive);
        await sourceBlob.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["archive_trigger"] = "true" });
        var archivedDestination = await service.RunMaintenanceAsync(CancellationToken.None);
        Assert.Equal(2, archivedDestination.FailedObjectReplications);
        var failedRule = Assert.Single(Assert.Single(
            (await sourceBlob.GetPropertiesAsync()).Value.ObjectReplicationSourceProperties).Rules);
        Assert.Equal(ObjectReplicationStatus.Failed, failedRule.ReplicationStatus);

        await destinationBlob.DeleteAsync();
        _ = await service.RunMaintenanceAsync(CancellationToken.None);
        Assert.False(await destinationBlob.ExistsAsync());

        await sourceBlob.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["changed"] = "true" });
        var changed = await service.RunMaintenanceAsync(CancellationToken.None);
        Assert.Equal(2, changed.CompletedObjectReplications);
        Assert.True(await destinationBlob.ExistsAsync());
        Assert.Equal("true", (await destinationBlob.GetPropertiesAsync()).Value.Metadata["changed"]);

        await sourceBlob.DeleteAsync();
        _ = await service.RunMaintenanceAsync(CancellationToken.None);
        Assert.False(await destinationBlob.ExistsAsync());
    }

    [Fact]
    public async Task OverwritesReplicateAsVersionsAndArchivedMutationReportsFailed()
    {
        const string sourceContainerName = "version-source";
        const string destinationContainerName = "version-destination";
        await using var factory = CreateFactory(sourceContainerName, destinationContainerName, replicateTags: false);
        await factory.InitializeAsync();
        var source = CreateClient(
            factory,
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        var destination = CreateClient(
            factory,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var sourceContainer = source.GetBlobContainerClient(sourceContainerName);
        var destinationContainer = destination.GetBlobContainerClient(destinationContainerName);
        await sourceContainer.CreateAsync();
        await destinationContainer.CreateAsync();
        var sourceBlob = sourceContainer.GetBlobClient("versioned.bin");
        await sourceBlob.UploadAsync(BinaryData.FromString("first"));
        var service = factory.Services.GetRequiredService<BlobService>();
        _ = await service.RunMaintenanceAsync(CancellationToken.None);
        await sourceBlob.UploadAsync(BinaryData.FromString("second"), overwrite: true);

        var overwrite = await service.RunMaintenanceAsync(CancellationToken.None);

        Assert.Equal(2, overwrite.CompletedObjectReplications);
        var destinationBlob = destinationContainer.GetBlobClient(sourceBlob.Name);
        Assert.Equal("second", (await destinationBlob.DownloadContentAsync()).Value.Content.ToString());
        var versions = new List<BlobItem>();
        await foreach (var item in destinationContainer.GetBlobsAsync(
                           traits: BlobTraits.None,
                           states: BlobStates.Version,
                           prefix: sourceBlob.Name,
                           cancellationToken: CancellationToken.None))
        {
            versions.Add(item);
        }
        Assert.True(versions.Count >= 2);
        Assert.Contains(versions, item => item.IsLatestVersion == true);
        Assert.Contains(versions, item => item.IsLatestVersion == false);

        await sourceBlob.SetAccessTierAsync(AccessTier.Archive);
        var archived = await service.RunMaintenanceAsync(CancellationToken.None);
        Assert.Equal(1, archived.FailedObjectReplications);
        var sourceProperties = (await sourceBlob.GetPropertiesAsync()).Value;
        var sourceRule = Assert.Single(Assert.Single(sourceProperties.ObjectReplicationSourceProperties).Rules);
        Assert.Equal(ObjectReplicationStatus.Failed, sourceRule.ReplicationStatus);
        Assert.Equal("second", (await destinationBlob.DownloadContentAsync()).Value.Content.ToString());
    }

    [Fact]
    public async Task DurableLedgerPreventsDuplicateReplicaAfterRestart()
    {
        const string sourceContainerName = "restart-source";
        const string destinationContainerName = "restart-destination";
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-object-replication-{Guid.NewGuid():N}");
        try
        {
            {
                var first = new SavaWebApplicationFactory(
                             dataPath,
                             CreateConfiguration(sourceContainerName, destinationContainerName, replicateTags: false),
                             deleteDataPath: false);
                await using (first.ConfigureAwait(false))
                {
                    await first.InitializeAsync();
                    var source = CreateClient(
                        first,
                        SavaWebApplicationFactory.AccountName,
                        SavaWebApplicationFactory.AccountKey);
                    var destination = CreateClient(
                        first,
                        SavaWebApplicationFactory.SecondAccountName,
                        SavaWebApplicationFactory.SecondAccountKey);
                    await source.GetBlobContainerClient(sourceContainerName).CreateAsync();
                    await destination.GetBlobContainerClient(destinationContainerName).CreateAsync();
                    await source.GetBlobContainerClient(sourceContainerName)
                        .GetBlobClient("persistent.bin")
                        .UploadAsync(BinaryData.FromString("persistent"));
                    var initial = await first.Services.GetRequiredService<BlobService>()
                        .RunMaintenanceAsync(CancellationToken.None);
                    Assert.Equal(1, initial.CompletedObjectReplications);
                }
            }

            {
                var restarted = new SavaWebApplicationFactory(
                             dataPath,
                             CreateConfiguration(sourceContainerName, destinationContainerName, replicateTags: false),
                             deleteDataPath: false);
                await using (restarted.ConfigureAwait(false))
                {
                    await restarted.InitializeAsync();
                    var maintenance = await restarted.Services.GetRequiredService<BlobService>()
                        .RunMaintenanceAsync(CancellationToken.None);
                    Assert.Equal(0, maintenance.CompletedObjectReplications);
                    var destination = CreateClient(
                        restarted,
                        SavaWebApplicationFactory.SecondAccountName,
                        SavaWebApplicationFactory.SecondAccountKey);
                    var versions = new List<BlobItem>();
                    await foreach (var item in destination.GetBlobContainerClient(destinationContainerName).GetBlobsAsync(
                                       traits: BlobTraits.None,
                                       states: BlobStates.Version,
                                       prefix: "persistent.bin",
                                       cancellationToken: CancellationToken.None))
                    {
                        versions.Add(item);
                    }
                    Assert.Single(versions);
                }
            }
        }
        finally
        {
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, recursive: true);
        }
    }

    [Fact]
    public async Task RuleFiltersOutPrefixesSnapshotsAndNonBlockBlobs()
    {
        const string sourceContainerName = "filter-source";
        const string destinationContainerName = "filter-destination";
        var configuration = CreateConfiguration(
            sourceContainerName,
            destinationContainerName,
            replicateTags: false);
        configuration["Sava:ObjectReplicationPolicies:0:Rules:0:PrefixMatch:0"] = "eligible/";
        var factory = new SavaWebApplicationFactory(configuration);
        await using var factoryDisposal1 = factory.ConfigureAwait(false);
        await factory.InitializeAsync();
        var source = CreateClient(
            factory,
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        var destination = CreateClient(
            factory,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var sourceContainer = source.GetBlobContainerClient(sourceContainerName);
        var destinationContainer = destination.GetBlobContainerClient(destinationContainerName);
        await sourceContainer.CreateAsync();
        await destinationContainer.CreateAsync();
        var eligible = sourceContainer.GetBlobClient("eligible/block.bin");
        await eligible.UploadAsync(BinaryData.FromString("block"));
        _ = await eligible.CreateSnapshotAsync();
        await sourceContainer.GetBlobClient("excluded/block.bin").UploadAsync(BinaryData.FromString("excluded"));
        var append = sourceContainer.GetAppendBlobClient("eligible/append.bin");
        await append.CreateAsync();
        await append.AppendBlockAsync(BinaryData.FromString("append").ToStream());

        var maintenance = await factory.Services.GetRequiredService<BlobService>()
            .RunMaintenanceAsync(CancellationToken.None);

        // Snapshot Blob creates a previous version plus a new current version when
        // source versioning is enabled. Both versions replicate; the snapshot does not.
        Assert.Equal(2, maintenance.CompletedObjectReplications);
        Assert.True(await destinationContainer.GetBlobClient(eligible.Name).ExistsAsync());
        Assert.False(await destinationContainer.GetBlobClient("excluded/block.bin").ExistsAsync());
        Assert.False(await destinationContainer.GetBlobClient(append.Name).ExistsAsync());
        var destinationVersions = new List<BlobItem>();
        await foreach (var item in destinationContainer.GetBlobsAsync(
                           traits: BlobTraits.None,
                           states: BlobStates.Version | BlobStates.Snapshots,
                           prefix: eligible.Name,
                           cancellationToken: CancellationToken.None))
        {
            destinationVersions.Add(item);
        }
        Assert.Equal(2, destinationVersions.Count);
        Assert.All(destinationVersions, item => Assert.Null(item.Snapshot));
    }

    private static SavaWebApplicationFactory CreateFactory(
        string sourceContainer,
        string destinationContainer,
        bool replicateTags) =>
        new(CreateConfiguration(sourceContainer, destinationContainer, replicateTags));

    private static Dictionary<string, string?> CreateConfiguration(
        string sourceContainer,
        string destinationContainer,
        bool replicateTags) =>
        new(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "1.00:00:00",
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:VersioningEnabled"] = "true",
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:ChangeFeedEnabled"] = "true",
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:VersioningEnabled"] = "true",
            ["Sava:ObjectReplicationPolicies:0:PolicyId"] = PolicyId,
            ["Sava:ObjectReplicationPolicies:0:SourceAccount"] = SavaWebApplicationFactory.AccountName,
            ["Sava:ObjectReplicationPolicies:0:DestinationAccount"] = SavaWebApplicationFactory.SecondAccountName,
            ["Sava:ObjectReplicationPolicies:0:EnabledAt"] = "2000-01-01T00:00:00Z",
            ["Sava:ObjectReplicationPolicies:0:Rules:0:RuleId"] = RuleId,
            ["Sava:ObjectReplicationPolicies:0:Rules:0:SourceContainer"] = sourceContainer,
            ["Sava:ObjectReplicationPolicies:0:Rules:0:DestinationContainer"] = destinationContainer,
            ["Sava:ObjectReplicationPolicies:0:Rules:0:ReplicateBlobTags"] = replicateTags.ToString()
        };

    private static BlobServiceClient CreateClient(
        SavaWebApplicationFactory factory,
        string account,
        string key)
    {
        var endpoint = new Uri($"http://{account}.localhost");
        var options = new BlobClientOptions
        {
            Transport = new HttpClientTransport(new HttpClient(factory.Server.CreateHandler())
            {
                BaseAddress = endpoint
            }),
            Retry = { MaxRetries = 0 }
        };
        return new BlobServiceClient(endpoint, new StorageSharedKeyCredential(account, key), options);
    }
}
