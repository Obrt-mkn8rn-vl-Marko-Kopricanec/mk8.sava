using System.Security.Cryptography;
using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;

namespace Mk8.Sava.Tests;

public sealed class AzuriteDifferentialTests
{
    [AzuriteFact]
    [Trait("Category", "Azurite")]
    public async Task AccountInformationShapeMatchesAzuriteWithSkuDivergence()
    {
        var connectionString = Environment.GetEnvironmentVariable(AzuriteFactAttribute.ConnectionStringVariable)
            ?? throw new InvalidOperationException("The Azurite connection string was removed after discovery.");
        var azurite = new BlobServiceClient(connectionString, CreateOptions());
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var local = CreateLocalClient(application);
        var missingName = $"mk8-azurite-account-info-{Guid.NewGuid():N}";

        var expected = await ObserveAccountInformationAsync(azurite, missingName).ConfigureAwait(false);
        var actual = await ObserveAccountInformationAsync(local, missingName).ConfigureAwait(false);
        Assert.Equal(SkuName.StandardRagrs, expected.SkuName);
        Assert.Equal(SkuName.StandardLrs, actual.SkuName);
        Assert.Equal(expected with { SkuName = SkuName.StandardLrs }, actual);
        Assert.Equal(200, expected.ServiceStatus);
        Assert.Equal(AccountKind.StorageV2, expected.AccountKind);
        Assert.False(expected.IsHierarchicalNamespaceEnabled);
    }

    [AzuriteFact]
    [Trait("Category", "Azurite")]
    public async Task EscapedExclamationAndLiteralPercentBlobNamesMatchAzurite()
    {
        var connectionString = Environment.GetEnvironmentVariable(AzuriteFactAttribute.ConnectionStringVariable)
            ?? throw new InvalidOperationException("The Azurite connection string was removed after discovery.");
        var azurite = new BlobServiceClient(connectionString, CreateOptions());
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var local = CreateLocalClient(application);
        var name = $"mk8-azurite-escaped-{Guid.NewGuid():N}";
        var azuriteContainer = azurite.GetBlobContainerClient(name);
        var localContainer = local.GetBlobContainerClient(name);
        try
        {
            var expected = await ExerciseEscapedNamesAsync(azuriteContainer).ConfigureAwait(false);
            var actual = await ExerciseEscapedNamesAsync(localContainer).ConfigureAwait(false);
            Assert.Equal(expected, actual);
            Assert.Equal("a!file,a%21file", expected.ListedNames);
        }
        finally
        {
            await DeleteIfExistsAsync(localContainer).ConfigureAwait(false);
            await DeleteIfExistsAsync(azuriteContainer).ConfigureAwait(false);
        }
    }

    [AzuriteFact]
    [Trait("Category", "Azurite")]
    public async Task BlobHttpPropertyReplacementAndMetadataIsolationMatchAzurite()
    {
        var connectionString = Environment.GetEnvironmentVariable(AzuriteFactAttribute.ConnectionStringVariable)
            ?? throw new InvalidOperationException("The Azurite connection string was removed after discovery.");
        var azurite = new BlobServiceClient(connectionString, CreateOptions());
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var local = CreateLocalClient(application);
        var name = $"mk8-azurite-properties-{Guid.NewGuid():N}";
        var azuriteContainer = azurite.GetBlobContainerClient(name);
        var localContainer = local.GetBlobContainerClient(name);
        try
        {
            var expected = await ExerciseBlobHttpPropertyReplacementAsync(azuriteContainer).ConfigureAwait(false);
            var actual = await ExerciseBlobHttpPropertyReplacementAsync(localContainer).ConfigureAwait(false);
            Assert.Equal(expected, actual);
            Assert.Equal(200, expected.SetPropertiesStatus);
            Assert.Equal(200, expected.SetMetadataStatus);
            Assert.Equal(412, expected.StaleStatus);
            Assert.Equal("ConditionNotMet", expected.StaleErrorCode);
            Assert.True(expected.PropertiesChangedETag);
            Assert.True(expected.MetadataChangedETag);
            Assert.Equal("application/octet-stream", expected.ContentType);
            Assert.Empty(expected.ClearedHttpProperties);
            Assert.Equal("second", expected.Metadata);
            Assert.Equal("payload", expected.Content);
        }
        finally
        {
            await DeleteIfExistsAsync(localContainer).ConfigureAwait(false);
            await DeleteIfExistsAsync(azuriteContainer).ConfigureAwait(false);
        }
    }

    [AzuriteFact]
    [Trait("Category", "Azurite")]
    public async Task BlobMetadataGetHeadAndSnapshotMatchAzuriteExceptDocumentedBody()
    {
        var connectionString = Environment.GetEnvironmentVariable(AzuriteFactAttribute.ConnectionStringVariable)
            ?? throw new InvalidOperationException("The Azurite connection string was removed after discovery.");
        var azurite = new BlobServiceClient(connectionString, CreateOptions());
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var local = CreateLocalClient(application);
        var name = $"mk8-azurite-metadata-{Guid.NewGuid():N}";
        var azuriteContainer = azurite.GetBlobContainerClient(name);
        var localContainer = local.GetBlobContainerClient(name);
        using var azuriteTransport = new HttpClient();
        using var localTransport = new HttpClient(application.Server.CreateHandler());
        try
        {
            var expected = await ExerciseBlobMetadataAsync(azuriteContainer, azuriteTransport)
                .ConfigureAwait(false);
            var actual = await ExerciseBlobMetadataAsync(localContainer, localTransport).ConfigureAwait(false);
            // Get Blob Metadata has no response body; Azurite incorrectly returns blob bytes for GET.
            Assert.Equal(7, expected.Current.GetBodyLength);
            Assert.Equal(7, expected.Snapshot.GetBodyLength);
            Assert.Equal(0, actual.Current.GetBodyLength);
            Assert.Equal(0, actual.Snapshot.GetBodyLength);
            Assert.Equal(expected with
            {
                Current = expected.Current with { GetBodyLength = 0 },
                Snapshot = expected.Snapshot with { GetBodyLength = 0 }
            }, actual);
        }
        finally
        {
            await DeleteIfExistsAsync(localContainer).ConfigureAwait(false);
            await DeleteIfExistsAsync(azuriteContainer).ConfigureAwait(false);
        }
    }

    [AzuriteFact]
    [Trait("Category", "Azurite")]
    public async Task SnapshotDeletionModesMatchAzuriteAndPublishedDeleteContract()
    {
        var connectionString = Environment.GetEnvironmentVariable(AzuriteFactAttribute.ConnectionStringVariable)
            ?? throw new InvalidOperationException("The Azurite connection string was removed after discovery.");
        var azurite = new BlobServiceClient(connectionString, CreateOptions());
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var local = CreateLocalClient(application);
        var name = $"mk8-azurite-snapshot-delete-{Guid.NewGuid():N}";
        var azuriteContainer = azurite.GetBlobContainerClient(name);
        var localContainer = local.GetBlobContainerClient(name);
        try
        {
            var expected = await ExerciseSnapshotDeletionModesAsync(azuriteContainer).ConfigureAwait(false);
            var actual = await ExerciseSnapshotDeletionModesAsync(localContainer).ConfigureAwait(false);
            Assert.Equal(expected, actual);
            Assert.Equal(409, expected.MissingModeStatus);
            Assert.Equal("SnapshotsPresent", expected.MissingModeCode);
            Assert.Equal(400, expected.SnapshotWithModeStatus);
            Assert.Equal(202, expected.OnlyStatus);
            Assert.Equal(202, expected.IncludeStatus);
            Assert.Equal("payload", expected.BaseAfterOnly);
            Assert.False(expected.FirstSnapshotAfterOnly);
            Assert.False(expected.BaseAfterInclude);
            Assert.False(expected.SecondSnapshotAfterInclude);
        }
        finally
        {
            await DeleteIfExistsAsync(localContainer).ConfigureAwait(false);
            await DeleteIfExistsAsync(azuriteContainer).ConfigureAwait(false);
        }
    }

    [AzuriteFact]
    [Trait("Category", "Azurite")]
    public async Task LeasedSnapshotDeletionRequiresLeaseIdAndMatchesAzurite()
    {
        var connectionString = Environment.GetEnvironmentVariable(AzuriteFactAttribute.ConnectionStringVariable)
            ?? throw new InvalidOperationException("The Azurite connection string was removed after discovery.");
        var azurite = new BlobServiceClient(connectionString, CreateOptions());
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var local = CreateLocalClient(application);
        var name = $"mk8-azurite-snapshot-lease-{Guid.NewGuid():N}";
        var azuriteContainer = azurite.GetBlobContainerClient(name);
        var localContainer = local.GetBlobContainerClient(name);
        try
        {
            var expected = await ExerciseLeasedSnapshotDeletionAsync(azuriteContainer).ConfigureAwait(false);
            var actual = await ExerciseLeasedSnapshotDeletionAsync(localContainer).ConfigureAwait(false);
            Assert.Equal(expected, actual);
            Assert.Equal(412, expected.OnlyWithoutLeaseStatus);
            Assert.Equal("LeaseIdMissing", expected.OnlyWithoutLeaseCode);
            Assert.Equal(412, expected.OnlyWithWrongLeaseStatus);
            Assert.Equal("LeaseIdMismatchWithBlobOperation", expected.OnlyWithWrongLeaseCode);
            Assert.Equal("leased payload", expected.BaseBeforeOnly);
            Assert.Equal("leased payload", expected.SnapshotBeforeOnly);
            Assert.Equal(202, expected.OnlyWithLeaseStatus);
            Assert.Equal("leased payload", expected.BaseAfterOnly);
            Assert.False(expected.SnapshotAfterOnly);
            Assert.Equal(412, expected.BaseWithoutLeaseStatus);
            Assert.Equal("LeaseIdMissing", expected.BaseWithoutLeaseCode);
            Assert.Equal(202, expected.BaseWithLeaseStatus);
            Assert.False(expected.BaseAfterDelete);
        }
        finally
        {
            await DeleteIfExistsAsync(localContainer).ConfigureAwait(false);
            await DeleteIfExistsAsync(azuriteContainer).ConfigureAwait(false);
        }
    }

    [AzuriteFact]
    [Trait("Category", "Azurite")]
    public async Task LeasedBaseAndSnapshotsDeleteRequiresMatchingLeaseId()
    {
        var connectionString = Environment.GetEnvironmentVariable(AzuriteFactAttribute.ConnectionStringVariable)
            ?? throw new InvalidOperationException("The Azurite connection string was removed after discovery.");
        var azurite = new BlobServiceClient(connectionString, CreateOptions());
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var local = CreateLocalClient(application);
        var name = $"mk8-snapshot-lease-{Guid.NewGuid():N}";
        var azuriteContainer = azurite.GetBlobContainerClient(name);
        var localContainer = local.GetBlobContainerClient(name);
        try
        {
            var expected = await ExerciseLeasedSnapshotIncludeAsync(azuriteContainer).ConfigureAwait(false);
            var actual = await ExerciseLeasedSnapshotIncludeAsync(localContainer).ConfigureAwait(false);
            Assert.Equal(expected, actual);
            Assert.Equal(412, expected.MissingLeaseStatus);
            Assert.Equal("LeaseIdMissing", expected.MissingLeaseCode);
            Assert.Equal(412, expected.WrongLeaseStatus);
            Assert.Equal("LeaseIdMismatchWithBlobOperation", expected.WrongLeaseCode);
            Assert.Equal("leased include payload", expected.BaseBeforeSuccess);
            Assert.Equal("leased include payload", expected.SnapshotBeforeSuccess);
            Assert.Equal(202, expected.IncludeStatus);
            Assert.False(expected.BaseAfterSuccess);
            Assert.False(expected.SnapshotAfterSuccess);
        }
        finally
        {
            await DeleteIfExistsAsync(localContainer).ConfigureAwait(false);
            await DeleteIfExistsAsync(azuriteContainer).ConfigureAwait(false);
        }
    }

    [AzuriteFact]
    [Trait("Category", "Azurite")]
    public async Task ContainerMetadataAccountSasGetAndHeadMatchAzuriteExceptDeniedCode()
    {
        var connectionString = Environment.GetEnvironmentVariable(AzuriteFactAttribute.ConnectionStringVariable)
            ?? throw new InvalidOperationException("The Azurite connection string was removed after discovery.");
        var azurite = new BlobServiceClient(connectionString, CreateOptions());
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var local = CreateLocalClient(application);
        var name = $"mk8-azurite-container-metadata-{Guid.NewGuid():N}";
        var azuriteContainer = azurite.GetBlobContainerClient(name);
        var localContainer = local.GetBlobContainerClient(name);
        using var azuriteTransport = new HttpClient();
        using var localTransport = new HttpClient(application.Server.CreateHandler());
        try
        {
            var expected = await ExerciseContainerMetadataAsync(azuriteContainer, azuriteTransport)
                .ConfigureAwait(false);
            var actual = await ExerciseContainerMetadataAsync(localContainer, localTransport)
                .ConfigureAwait(false);
            Assert.Equal(403, expected.ServiceSasDeniedStatus);
            Assert.Equal(403, actual.ServiceSasDeniedStatus);
            // Both reject service SAS, but Azurite classifies this unsupported operation as a missing permission.
            Assert.Equal("AuthorizationPermissionMismatch", expected.ServiceSasDeniedCode);
            Assert.Equal("AuthorizationFailure", actual.ServiceSasDeniedCode);
            Assert.Equal(expected with { ServiceSasDeniedCode = actual.ServiceSasDeniedCode }, actual);
        }
        finally
        {
            await DeleteIfExistsAsync(localContainer).ConfigureAwait(false);
            await DeleteIfExistsAsync(azuriteContainer).ConfigureAwait(false);
        }
    }

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
    public async Task SnapshotBlockListsAndOmittedListTypeMatchAzurite()
    {
        var connectionString = Environment.GetEnvironmentVariable(AzuriteFactAttribute.ConnectionStringVariable)
            ?? throw new InvalidOperationException("The Azurite connection string was removed after discovery.");
        var azurite = new BlobServiceClient(connectionString, CreateOptions());
        var application = new SavaWebApplicationFactory();
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync().ConfigureAwait(false);
        var local = CreateLocalClient(application);
        var name = $"mk8-azurite-block-history-{Guid.NewGuid():N}";
        var azuriteContainer = azurite.GetBlobContainerClient(name);
        var localContainer = local.GetBlobContainerClient(name);
        try
        {
            using var azuriteTransport = new HttpClient();
            using var localTransport = new HttpClient(application.Server.CreateHandler());
            var expected = await ObserveSnapshotBlockListsAsync(azuriteContainer, azuriteTransport)
                .ConfigureAwait(false);
            var actual = await ObserveSnapshotBlockListsAsync(localContainer, localTransport)
                .ConfigureAwait(false);
            // Azurite 3.35.0 attaches live uncommitted blocks to snapshots and ignores
            // If-Match. The published Get Block List contract requires neither behavior.
            Assert.Equal("MDAwNA==", expected.SnapshotUncommitted);
            Assert.Equal(200, expected.StaleIfMatchStatus);
            Assert.Null(expected.StaleIfMatchCode);
            Assert.Equal(expected with
            {
                SnapshotUncommitted = string.Empty,
                StaleIfMatchStatus = 412,
                StaleIfMatchCode = "ConditionNotMet"
            }, actual);
            Assert.Equal("MDAwMg==,MDAwMQ==", expected.SnapshotCommitted);
            Assert.Equal("MDAwMw==", expected.CurrentCommitted);
            Assert.Equal("MDAwNA==", expected.CurrentUncommitted);
            Assert.Equal(string.Empty, actual.SnapshotUncommitted);
            Assert.False(expected.OmittedTypeIncludesUncommitted);
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

    [AzuriteFact]
    [Trait("Category", "Azurite")]
    public async Task SameAccountCopyMatchAzurite()
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
            var expected = await ExerciseCopyAsync(azuriteContainer).ConfigureAwait(false);
            var actual = await ExerciseCopyAsync(localContainer).ConfigureAwait(false);
            // Azurite uses the generic error; the published Blob error catalog names the source-specific one.
            Assert.Equal("ConditionNotMet", expected.StaleSourceCode);
            Assert.Equal("SourceConditionNotMet", actual.StaleSourceCode);
            Assert.Equal(expected with { StaleSourceCode = actual.StaleSourceCode }, actual);
        }
        finally
        {
            await DeleteIfExistsAsync(localContainer).ConfigureAwait(false);
            await DeleteIfExistsAsync(azuriteContainer).ConfigureAwait(false);
        }
    }

    [AzuriteFact]
    [Trait("Category", "Azurite")]
    public async Task BlobPrefixMetadataAndHierarchyPagingMatchAzurite()
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
            var expected = await ExerciseBlobListingAsync(azuriteContainer).ConfigureAwait(false);
            var actual = await ExerciseBlobListingAsync(localContainer).ConfigureAwait(false);
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
    public async Task StoredAccessPolicySasReadAndWriteDenialMatchAzurite()
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
            var expected = await ExerciseStoredPolicySasAsync(
                azuriteContainer, uri => new BlobClient(uri, CreateOptions())).ConfigureAwait(false);
            var actual = await ExerciseStoredPolicySasAsync(localContainer, uri =>
            {
                var options = CreateOptions();
                options.Transport = new HttpClientTransport(application.Server.CreateHandler());
                return new BlobClient(uri, options);
            }).ConfigureAwait(false);
            Assert.Equal("reader", expected.Identifier);
            Assert.Equal("r", expected.Permissions);
            Assert.Equal("policy payload", expected.ReadBytes);
            Assert.Equal("policy payload", expected.RetainedBytes);
            Assert.Equal(403, expected.DeniedWriteStatus);
            Assert.Equal(403, expected.RevokedReadStatus);
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
    public async Task RangedReadChecksumsAndConditionsMatchAzurite()
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
        using var azuriteTransport = new HttpClient();
        using var localTransport = new HttpClient(application.Server.CreateHandler());
        try
        {
            var expected = await ExerciseRangedReadAsync(azuriteContainer, azuriteTransport).ConfigureAwait(false);
            var actual = await ExerciseRangedReadAsync(localContainer, localTransport).ConfigureAwait(false);
            // Azurite accepts the header without a range, contrary to the published Get Blob contract.
            Assert.Equal(200, expected.ChecksumWithoutRange.Status);
            Assert.Equal(400, actual.ChecksumWithoutRange.Status);
            Assert.Equal("InvalidHeaderValue", actual.ChecksumWithoutRange.ErrorCode);
            Assert.Equal(expected with { ChecksumWithoutRange = actual.ChecksumWithoutRange }, actual);
        }
        finally
        {
            await DeleteIfExistsAsync(localContainer).ConfigureAwait(false);
            await DeleteIfExistsAsync(azuriteContainer).ConfigureAwait(false);
        }
    }

    [AzuriteFact]
    [Trait("Category", "Azurite")]
    public async Task BlobLeaseLifecycleMatchesAzurite()
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
            var expected = await ExerciseBlobLeaseLifecycleAsync(azuriteContainer).ConfigureAwait(false);
            var actual = await ExerciseBlobLeaseLifecycleAsync(localContainer).ConfigureAwait(false);
            AssertPublishedBlobLeaseStatusCodes(expected);
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
    public async Task ContainerLeaseLifecycleMatchesAzurite()
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
            var expected = await ExerciseContainerLeaseLifecycleAsync(azuriteContainer).ConfigureAwait(false);
            var actual = await ExerciseContainerLeaseLifecycleAsync(localContainer).ConfigureAwait(false);
            AssertPublishedContainerLeaseStatusCodes(expected);
            Assert.Equal(expected, actual);
        }
        finally
        {
            await DeleteIfExistsAsync(localContainer).ConfigureAwait(false);
            await DeleteIfExistsAsync(azuriteContainer).ConfigureAwait(false);
        }
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

    private static async Task<EscapedNameObservation> ExerciseEscapedNamesAsync(BlobContainerClient container)
    {
        await container.CreateAsync().ConfigureAwait(false);
        var bang = container.GetBlobClient("a!file");
        var percent = container.GetBlobClient("a%21file");
        var bangUpload = await bang.UploadAsync(BinaryData.FromString("exclamation")).ConfigureAwait(false);
        var percentUpload = await percent.UploadAsync(BinaryData.FromString("literal percent")).ConfigureAwait(false);
        var bangContent = (await bang.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString();
        var percentContent = (await percent.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString();
        var names = new List<string>();
        await foreach (var blob in container.GetBlobsAsync().ConfigureAwait(false))
            names.Add(blob.Name);
        return new EscapedNameObservation(
            bangUpload.GetRawResponse().Status,
            percentUpload.GetRawResponse().Status,
            bangContent,
            percentContent,
            string.Join(',', names));
    }

    private static async Task<BlobHttpPropertyObservation> ExerciseBlobHttpPropertyReplacementAsync(
        BlobContainerClient container)
    {
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("properties.bin");
        await blob.UploadAsync(BinaryData.FromString("payload"), new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders
            {
                ContentType = "text/plain",
                CacheControl = "max-age=10",
                ContentEncoding = "gzip",
                ContentLanguage = "en-US",
                ContentDisposition = "inline"
            },
            Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["phase"] = "initial" }
        }).ConfigureAwait(false);
        var original = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal("max-age=10", original.CacheControl);
        Assert.Equal("gzip", original.ContentEncoding);
        Assert.Equal("en-US", original.ContentLanguage);
        Assert.Equal("inline", original.ContentDisposition);
        var replaced = await blob.SetHttpHeadersAsync(
            new BlobHttpHeaders { ContentType = "application/octet-stream" },
            new BlobRequestConditions { IfMatch = original.ETag }).ConfigureAwait(false);
        var afterProperties = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal("initial", afterProperties.Metadata["phase"]);
        var metadata = await blob.SetMetadataAsync(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["phase"] = "second" },
            new BlobRequestConditions { IfMatch = afterProperties.ETag }).ConfigureAwait(false);
        var final = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        var stale = await Assert.ThrowsAsync<RequestFailedException>(() => blob.SetMetadataAsync(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["phase"] = "rejected" },
            new BlobRequestConditions { IfMatch = original.ETag })).ConfigureAwait(false);
        var content = (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString();
        return new BlobHttpPropertyObservation(
            replaced.GetRawResponse().Status,
            metadata.GetRawResponse().Status,
            stale.Status,
            stale.ErrorCode,
            original.ETag != afterProperties.ETag,
            afterProperties.ETag != final.ETag,
            final.ContentType,
            string.Concat(final.CacheControl, final.ContentEncoding, final.ContentLanguage, final.ContentDisposition),
            final.Metadata["phase"],
            content);
    }

    private static async Task<BlobMetadataObservation> ExerciseBlobMetadataAsync(
        BlobContainerClient container, HttpClient transport)
    {
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("metadata.bin");
        await blob.UploadAsync(BinaryData.FromString("payload"), new BlobUploadOptions
        {
            Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["phase"] = "initial" }
        }).ConfigureAwait(false);
        var snapshot = (await blob.CreateSnapshotAsync(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["phase"] = "snapshot"
        }).ConfigureAwait(false)).Value.Snapshot;
        await blob.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["phase"] = "current"
        }).ConfigureAwait(false);

        var current = await ObserveBlobMetadataAsync(blob, transport).ConfigureAwait(false);
        var historical = await ObserveBlobMetadataAsync(blob.WithSnapshot(snapshot), transport)
            .ConfigureAwait(false);
        Assert.Equal("current", current.Phase);
        Assert.Equal("snapshot", historical.Phase);
        return new BlobMetadataObservation(current, historical);
    }

    private static async Task<BlobMetadataReadObservation> ObserveBlobMetadataAsync(
        BlobClient blob, HttpClient transport)
    {
        var sasUri = blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        var metadataUri = new UriBuilder(sasUri)
        {
            Query = sasUri.Query.TrimStart('?') + "&comp=metadata"
        }.Uri;
        using var get = new HttpRequestMessage(HttpMethod.Get, metadataUri);
        using var head = new HttpRequestMessage(HttpMethod.Head, metadataUri);
        get.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        head.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        using var getResponse = await transport.SendAsync(get).ConfigureAwait(false);
        using var headResponse = await transport.SendAsync(head).ConfigureAwait(false);
        using var conditional = new HttpRequestMessage(HttpMethod.Get, metadataUri);
        conditional.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        conditional.Headers.TryAddWithoutValidation("If-None-Match",
            getResponse.Headers.ETag?.ToString()
            ?? throw new InvalidOperationException("Get Blob Metadata omitted its ETag."));
        using var conditionalResponse = await transport.SendAsync(conditional).ConfigureAwait(false);
        var getBody = await getResponse.Content.ReadAsByteArrayAsync().ConfigureAwait(false);
        var headBody = await headResponse.Content.ReadAsByteArrayAsync().ConfigureAwait(false);
        var phase = getResponse.Headers.GetValues("x-ms-meta-phase").Single();
        Assert.Equal(200, (int)getResponse.StatusCode);
        Assert.Equal(200, (int)headResponse.StatusCode);
        Assert.Equal(304, (int)conditionalResponse.StatusCode);
        Assert.Equal(phase, headResponse.Headers.GetValues("x-ms-meta-phase").Single());
        Assert.Equal(getResponse.Headers.ETag, headResponse.Headers.ETag);
        return new BlobMetadataReadObservation(
            (int)getResponse.StatusCode, (int)headResponse.StatusCode,
            (int)conditionalResponse.StatusCode, phase, getBody.Length, headBody.Length);
    }

    private static async Task<ContainerMetadataObservation> ExerciseContainerMetadataAsync(
        BlobContainerClient container, HttpClient transport)
    {
        await container.CreateAsync(metadata: new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["phase"] = "initial"
        }).ConfigureAwait(false);
        await container.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["phase"] = "updated"
        }).ConfigureAwait(false);
        var uri = CreateContainerMetadataAccountSasUri(container);
        var before = await ObserveContainerMetadataAsync(uri, transport).ConfigureAwait(false);
        await container.GetBlobClient("child.bin").UploadAsync(BinaryData.FromString("payload"))
            .ConfigureAwait(false);
        var after = await ObserveContainerMetadataAsync(uri, transport).ConfigureAwait(false);
        var serviceSas = container.GenerateSasUri(
            BlobContainerSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        var serviceSasUri = new UriBuilder(serviceSas)
        {
            Query = serviceSas.Query.TrimStart('?') + "&restype=container&comp=metadata"
        }.Uri;
        using var deniedRequest = new HttpRequestMessage(HttpMethod.Get, serviceSasUri);
        deniedRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        using var deniedResponse = await transport.SendAsync(deniedRequest).ConfigureAwait(false);
        Assert.Equal("updated", before.Phase);
        Assert.Equal("updated", after.Phase);
        return new ContainerMetadataObservation(
            before.GetStatus, before.HeadStatus, before.Phase,
            before.GetBodyLength, before.HeadBodyLength,
            string.Equals(before.ETag, after.ETag, StringComparison.Ordinal),
            (int)deniedResponse.StatusCode,
            deniedResponse.Headers.TryGetValues("x-ms-error-code", out var codes) ? codes.Single() : null);
    }

    private static Uri CreateContainerMetadataAccountSasUri(BlobContainerClient container)
    {
        var builder = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Container,
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(5),
            Protocol = SasProtocol.HttpsAndHttp
        };
        builder.SetPermissions(AccountSasPermissions.Read);
        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);
        return new UriBuilder(container.Uri)
        {
            Query = "restype=container&comp=metadata&" + builder.ToSasQueryParameters(credential)
        }.Uri;
    }

    private static async Task<ContainerMetadataReadObservation> ObserveContainerMetadataAsync(
        Uri uri, HttpClient transport)
    {
        using var get = new HttpRequestMessage(HttpMethod.Get, uri);
        using var head = new HttpRequestMessage(HttpMethod.Head, uri);
        get.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        head.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        using var getResponse = await transport.SendAsync(get).ConfigureAwait(false);
        using var headResponse = await transport.SendAsync(head).ConfigureAwait(false);
        var phase = getResponse.Headers.GetValues("x-ms-meta-phase").Single();
        Assert.Equal(200, (int)getResponse.StatusCode);
        Assert.Equal(200, (int)headResponse.StatusCode);
        Assert.Equal(phase, headResponse.Headers.GetValues("x-ms-meta-phase").Single());
        Assert.Equal(getResponse.Headers.ETag, headResponse.Headers.ETag);
        return new ContainerMetadataReadObservation(
            (int)getResponse.StatusCode, (int)headResponse.StatusCode,
            phase, getResponse.Headers.ETag?.ToString(),
            (await getResponse.Content.ReadAsByteArrayAsync().ConfigureAwait(false)).Length,
            (await headResponse.Content.ReadAsByteArrayAsync().ConfigureAwait(false)).Length);
    }

    private static async Task<AccountInformationObservation> ObserveAccountInformationAsync(
        BlobServiceClient service, string missingName)
    {
        var serviceResponse = await service.GetAccountInfoAsync().ConfigureAwait(false);
        var containerResponse = await service.GetBlobContainerClient(missingName)
            .GetAccountInfoAsync().ConfigureAwait(false);
        var blobResponse = await service.GetBlobContainerClient(missingName)
            .GetBlobClient("missing.bin").GetAccountInfoAsync().ConfigureAwait(false);
        Assert.Equal(serviceResponse.Value.SkuName, containerResponse.Value.SkuName);
        Assert.Equal(serviceResponse.Value.AccountKind, containerResponse.Value.AccountKind);
        Assert.Equal(serviceResponse.Value.IsHierarchicalNamespaceEnabled,
            containerResponse.Value.IsHierarchicalNamespaceEnabled);
        Assert.Equal(serviceResponse.Value.SkuName, blobResponse.Value.SkuName);
        Assert.Equal(serviceResponse.Value.AccountKind, blobResponse.Value.AccountKind);
        Assert.Equal(serviceResponse.Value.IsHierarchicalNamespaceEnabled,
            blobResponse.Value.IsHierarchicalNamespaceEnabled);
        return new AccountInformationObservation(
            serviceResponse.GetRawResponse().Status,
            containerResponse.GetRawResponse().Status,
            blobResponse.GetRawResponse().Status,
            serviceResponse.Value.SkuName,
            serviceResponse.Value.AccountKind,
            serviceResponse.Value.IsHierarchicalNamespaceEnabled);
    }

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

    private static async Task<BlockListSnapshotObservation> ObserveSnapshotBlockListsAsync(
        BlobContainerClient container,
        HttpClient transport)
    {
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlockBlobClient("history.bin");
        var firstId = Convert.ToBase64String("0001"u8);
        var secondId = Convert.ToBase64String("0002"u8);
        var thirdId = Convert.ToBase64String("0003"u8);
        var fourthId = Convert.ToBase64String("0004"u8);
        await blob.StageBlockAsync(firstId, new MemoryStream("first"u8.ToArray(), writable: false))
            .ConfigureAwait(false);
        await blob.StageBlockAsync(secondId, new MemoryStream("second"u8.ToArray(), writable: false))
            .ConfigureAwait(false);
        await blob.CommitBlockListAsync([secondId, firstId]).ConfigureAwait(false);
        var snapshot = (await blob.CreateSnapshotAsync().ConfigureAwait(false)).Value.Snapshot;
        await blob.StageBlockAsync(thirdId, new MemoryStream("third"u8.ToArray(), writable: false))
            .ConfigureAwait(false);
        await blob.CommitBlockListAsync([thirdId]).ConfigureAwait(false);
        await blob.StageBlockAsync(fourthId, new MemoryStream("fourth"u8.ToArray(), writable: false))
            .ConfigureAwait(false);

        var current = (await blob.GetBlockListAsync(BlockListTypes.All).ConfigureAwait(false)).Value;
        var historical = (await blob.WithSnapshot(snapshot).GetBlockListAsync(BlockListTypes.All)
            .ConfigureAwait(false)).Value;
        var sasUri = blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        var defaultRequestUri = new UriBuilder(sasUri)
        {
            Query = sasUri.Query.TrimStart('?') + "&comp=blocklist"
        }.Uri;
        using var response = await transport.GetAsync(defaultRequestUri).ConfigureAwait(false);
        var defaultBody = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        var conditionalUri = new UriBuilder(sasUri)
        {
            Query = sasUri.Query.TrimStart('?') + "&comp=blocklist&blocklisttype=committed"
        }.Uri;
        using var conditionalRequest = new HttpRequestMessage(HttpMethod.Get, conditionalUri);
        conditionalRequest.Headers.TryAddWithoutValidation("If-Match", "\"missing\"");
        using var conditionalResponse = await transport.SendAsync(conditionalRequest).ConfigureAwait(false);
        var conditionCode = conditionalResponse.Headers.TryGetValues("x-ms-error-code", out var errorCodes)
            ? errorCodes.Single()
            : null;

        return new BlockListSnapshotObservation(
            string.Join(',', current.CommittedBlocks.Select(block => block.Name)),
            string.Join(',', current.UncommittedBlocks.Select(block => block.Name)),
            string.Join(',', historical.CommittedBlocks.Select(block => block.Name)),
            string.Join(',', historical.UncommittedBlocks.Select(block => block.Name)),
            (int)response.StatusCode,
            defaultBody.Contains("<UncommittedBlocks>", StringComparison.Ordinal),
            (int)conditionalResponse.StatusCode,
            conditionCode);
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

    private static async Task<BlobLeaseLifecycleObservation> ExerciseBlobLeaseLifecycleAsync(
        BlobContainerClient container)
    {
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("lease.bin");
        await blob.UploadAsync(BinaryData.FromString("leased content")).ConfigureAwait(false);
        var original = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;

        var acquired = await blob.GetBlobLeaseClient().AcquireAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
        var originalLeaseId = acquired.Value.LeaseId;
        var oldLease = blob.GetBlobLeaseClient(originalLeaseId);
        var renewed = await oldLease.RenewAsync().ConfigureAwait(false);
        var changedId = Guid.NewGuid().ToString();
        var changed = await oldLease.ChangeAsync(changedId).ConfigureAwait(false);
        var stale = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.GetBlobLeaseClient(originalLeaseId).RenewAsync()).ConfigureAwait(false);
        var currentLease = blob.GetBlobLeaseClient(changedId);
        var released = await currentLease.ReleaseAsync().ConfigureAwait(false);
        var afterRelease = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;

        var second = await blob.GetBlobLeaseClient().AcquireAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
        var broken = await blob.GetBlobLeaseClient(second.Value.LeaseId)
            .BreakAsync(TimeSpan.Zero).ConfigureAwait(false);
        var afterBreak = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        var reacquired = await blob.GetBlobLeaseClient().AcquireAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
        await blob.GetBlobLeaseClient(reacquired.Value.LeaseId).ReleaseAsync().ConfigureAwait(false);

        Assert.Equal(original.ETag, afterRelease.ETag);
        Assert.Equal(original.LastModified, afterRelease.LastModified);
        Assert.Equal(original.ETag, afterBreak.ETag);
        Assert.Equal(original.LastModified, afterBreak.LastModified);
        Assert.Equal("leased content", (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        return new BlobLeaseLifecycleObservation(
            acquired.GetRawResponse().Status, renewed.GetRawResponse().Status,
            changed.GetRawResponse().Status,
            string.Equals(changed.Value.LeaseId, changedId, StringComparison.Ordinal),
            stale.Status, stale.ErrorCode, released.GetRawResponse().Status,
            second.GetRawResponse().Status, broken.GetRawResponse().Status,
            broken.Value.LeaseTime, afterBreak.LeaseState.ToString(),
            reacquired.GetRawResponse().Status);
    }

    private static void AssertPublishedBlobLeaseStatusCodes(BlobLeaseLifecycleObservation observation)
    {
        Assert.Equal(201, observation.AcquireStatus);
        Assert.Equal(200, observation.RenewStatus);
        Assert.Equal(200, observation.ChangeStatus);
        Assert.True(observation.ChangedIdMatches);
        Assert.Equal(409, observation.StaleLeaseStatus);
        Assert.Equal(200, observation.ReleaseStatus);
        Assert.Equal(201, observation.SecondAcquireStatus);
        Assert.Equal(202, observation.BreakStatus);
        Assert.Equal(0, observation.BreakTime);
        Assert.Equal("Broken", observation.BrokenState);
        Assert.Equal(201, observation.ReacquireStatus);
    }

    private static async Task<ContainerLeaseLifecycleObservation> ExerciseContainerLeaseLifecycleAsync(
        BlobContainerClient container)
    {
        await container.CreateAsync().ConfigureAwait(false);
        var original = (await container.GetPropertiesAsync().ConfigureAwait(false)).Value;
        var acquired = await container.GetBlobLeaseClient().AcquireAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
        var originalId = acquired.Value.LeaseId;
        var currentLease = container.GetBlobLeaseClient(originalId);
        var renewed = await currentLease.RenewAsync().ConfigureAwait(false);
        var changedId = Guid.NewGuid().ToString();
        var changed = await currentLease.ChangeAsync(changedId).ConfigureAwait(false);
        var stale = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobLeaseClient(originalId).RenewAsync()).ConfigureAwait(false);
        var released = await container.GetBlobLeaseClient(changedId).ReleaseAsync().ConfigureAwait(false);
        var afterRelease = (await container.GetPropertiesAsync().ConfigureAwait(false)).Value;

        var second = await container.GetBlobLeaseClient().AcquireAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
        var broken = await container.GetBlobLeaseClient(second.Value.LeaseId)
            .BreakAsync(TimeSpan.Zero).ConfigureAwait(false);
        var afterBreak = (await container.GetPropertiesAsync().ConfigureAwait(false)).Value;
        var reacquired = await container.GetBlobLeaseClient().AcquireAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
        await container.GetBlobLeaseClient(reacquired.Value.LeaseId).ReleaseAsync().ConfigureAwait(false);

        Assert.Equal(original.ETag, afterRelease.ETag);
        Assert.Equal(original.LastModified, afterRelease.LastModified);
        Assert.Equal(original.ETag, afterBreak.ETag);
        Assert.Equal(original.LastModified, afterBreak.LastModified);
        return new ContainerLeaseLifecycleObservation(
            acquired.GetRawResponse().Status, renewed.GetRawResponse().Status,
            changed.GetRawResponse().Status,
            string.Equals(changed.Value.LeaseId, changedId, StringComparison.Ordinal),
            stale.Status, stale.ErrorCode, released.GetRawResponse().Status,
            second.GetRawResponse().Status, broken.GetRawResponse().Status,
            broken.Value.LeaseTime, afterBreak.LeaseState?.ToString() ?? string.Empty,
            reacquired.GetRawResponse().Status);
    }

    private static void AssertPublishedContainerLeaseStatusCodes(ContainerLeaseLifecycleObservation observation)
    {
        Assert.Equal(201, observation.AcquireStatus);
        Assert.Equal(200, observation.RenewStatus);
        Assert.Equal(200, observation.ChangeStatus);
        Assert.True(observation.ChangedIdMatches);
        Assert.Equal(409, observation.StaleLeaseStatus);
        Assert.Equal(200, observation.ReleaseStatus);
        Assert.Equal(201, observation.SecondAcquireStatus);
        Assert.Equal(202, observation.BreakStatus);
        Assert.Equal(0, observation.BreakTime);
        Assert.Equal("Broken", observation.BrokenState);
        Assert.Equal(201, observation.ReacquireStatus);
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
        var deniedOrigin = await GetPreflightStatusAsync(
            transport, service.Uri, "https://denied.example.test", "GET").ConfigureAwait(false);
        var missingMethod = await GetPreflightStatusAsync(
            transport, service.Uri, "https://client.example.test", method: null).ConfigureAwait(false);
        Assert.Equal(403, deniedOrigin);
        Assert.Equal(400, missingMethod);
        return new ServiceCorsObservation(initial.GetRawResponse().Status, written.Status,
            read.GetRawResponse().Status, rule.AllowedOrigins, rule.AllowedMethods,
            rule.AllowedHeaders, rule.ExposedHeaders, rule.MaxAgeInSeconds,
            preflight, deniedOrigin, missingMethod);
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

    private static async Task<int> GetPreflightStatusAsync(
        HttpClient transport,
        Uri serviceUri,
        string origin,
        string? method)
    {
        using var request = new HttpRequestMessage(HttpMethod.Options, new Uri(serviceUri, "?comp=list"));
        request.Headers.TryAddWithoutValidation("Origin", origin);
        if (method is not null)
            request.Headers.TryAddWithoutValidation("Access-Control-Request-Method", method);
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        return (int)response.StatusCode;
    }

    private static async Task<CopyObservation> ExerciseCopyAsync(BlobContainerClient container)
    {
        await container.CreateAsync().ConfigureAwait(false);
        var source = container.GetBlobClient("source.bin");
        var bytes = new byte[8192];
        DeterministicTestBytes.Fill(0xA20C, bytes);
        await source.UploadAsync(BinaryData.FromBytes(bytes)).ConfigureAwait(false);
        var stale = (await source.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag;
        await source.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["phase"] = "changed"
        }).ConfigureAwait(false);
        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient("rejected.bin").StartCopyFromUriAsync(source.Uri,
                new BlobCopyFromUriOptions
                {
                    SourceConditions = new BlobRequestConditions { IfMatch = stale }
                })).ConfigureAwait(false);
        var destination = container.GetBlobClient("copy.bin");
        var copy = await destination.StartCopyFromUriAsync(source.Uri).ConfigureAwait(false);
        await copy.WaitForCompletionAsync().ConfigureAwait(false);
        var properties = (await destination.GetPropertiesAsync().ConfigureAwait(false)).Value;
        var actual = (await destination.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray();
        Assert.Equal(bytes, actual);
        return new CopyObservation(
            rejected.Status, rejected.ErrorCode, copy.GetRawResponse().Status, properties.CopyStatus.ToString(),
            properties.ContentLength, Convert.ToHexString(SHA256.HashData(actual)));
    }

    private static async Task<BlobListingObservation> ExerciseBlobListingAsync(BlobContainerClient container)
    {
        await container.CreateAsync().ConfigureAwait(false);
        foreach (var name in new[] { "folder/a.bin", "folder/b.bin", "folder/deeper/c.bin", "root.bin" })
        {
            await container.GetBlobClient(name).UploadAsync(BinaryData.FromString(name), new BlobUploadOptions
            {
                Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["name"] = name }
            }).ConfigureAwait(false);
        }

        var flatPages = new List<string>();
        var flatContinuations = 0;
        await foreach (var page in container.GetBlobsAsync(new GetBlobsOptions
        {
            Traits = BlobTraits.Metadata,
            Prefix = "folder/"
        }).AsPages(pageSizeHint: 1).ConfigureAwait(false))
        {
            flatPages.Add(string.Join(',', page.Values.Select(item => $"{item.Name}:{item.Metadata["name"]}")));
            if (!string.IsNullOrEmpty(page.ContinuationToken))
                flatContinuations++;
        }

        var hierarchyPages = new List<string>();
        var hierarchyContinuations = 0;
        await foreach (var page in container.GetBlobsByHierarchyAsync(new GetBlobsByHierarchyOptions
        {
            Delimiter = "/",
            Prefix = "folder/"
        }).AsPages(pageSizeHint: 1).ConfigureAwait(false))
        {
            hierarchyPages.Add(string.Join(',', page.Values.Select(item =>
                item.IsPrefix ? $"P:{item.Prefix}" : $"B:{item.Blob.Name}")));
            if (!string.IsNullOrEmpty(page.ContinuationToken))
                hierarchyContinuations++;
        }

        Assert.Equal(3, flatPages.Count);
        Assert.Equal(2, flatContinuations);
        Assert.Equal(3, hierarchyPages.Count);
        Assert.Equal(2, hierarchyContinuations);
        return new BlobListingObservation(
            string.Join('|', flatPages), flatContinuations,
            string.Join('|', hierarchyPages), hierarchyContinuations);
    }

    private static async Task<StoredPolicySasObservation> ExerciseStoredPolicySasAsync(
        BlobContainerClient container, Func<Uri, BlobClient> createSasClient)
    {
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("policy.txt");
        await blob.UploadAsync(BinaryData.FromString("policy payload")).ConfigureAwait(false);
        await container.SetAccessPolicyAsync(PublicAccessType.None,
            [new BlobSignedIdentifier
            {
                Id = "reader",
                AccessPolicy = new BlobAccessPolicy
                {
                    StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
                    ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
                    Permissions = "r"
                }
            }]).ConfigureAwait(false);
        var policy = Assert.Single((await container.GetAccessPolicyAsync().ConfigureAwait(false)).Value.SignedIdentifiers);
        var sas = new BlobSasBuilder
        {
            BlobContainerName = container.Name,
            BlobName = blob.Name,
            Resource = "b",
            Identifier = "reader",
            Protocol = SasProtocol.HttpsAndHttp
        };
        var client = createSasClient(blob.GenerateSasUri(sas));
        var read = await client.DownloadContentAsync().ConfigureAwait(false);
        var denied = await Assert.ThrowsAsync<RequestFailedException>(() =>
            client.UploadAsync(BinaryData.FromString("blocked"), overwrite: true)).ConfigureAwait(false);
        var retained = await blob.DownloadContentAsync().ConfigureAwait(false);
        await container.SetAccessPolicyAsync(PublicAccessType.None, []).ConfigureAwait(false);
        var revoked = await Assert.ThrowsAsync<RequestFailedException>(() =>
            client.DownloadContentAsync()).ConfigureAwait(false);
        return new StoredPolicySasObservation(
            policy.Id, policy.AccessPolicy.Permissions,
            read.Value.Content.ToString(), denied.Status, denied.ErrorCode,
            retained.Value.Content.ToString(), revoked.Status, revoked.ErrorCode);
    }

    private static async Task<RangedReadObservation> ExerciseRangedReadAsync(
        BlobContainerClient container, HttpClient transport)
    {
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("range.bin");
        var bytes = new byte[8192];
        DeterministicTestBytes.Fill(0xA20D, bytes);
        await blob.UploadAsync(BinaryData.FromBytes(bytes)).ConfigureAwait(false);
        var etag = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag.ToString();
        var uri = blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));

        using var rangeRequest = new HttpRequestMessage(HttpMethod.Get, uri);
        rangeRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        rangeRequest.Headers.TryAddWithoutValidation("Range", "bytes=1024-2047");
        rangeRequest.Headers.TryAddWithoutValidation("x-ms-range-get-content-md5", "true");
        using var rangeResponse = await transport.SendAsync(rangeRequest).ConfigureAwait(false);
        var rangedBytes = await rangeResponse.Content.ReadAsByteArrayAsync().ConfigureAwait(false);
        Assert.Equal(bytes.AsSpan(1024, 1024).ToArray(), rangedBytes);
        Assert.Equal(206, (int)rangeResponse.StatusCode);
        Assert.Equal("bytes 1024-2047/8192", rangeResponse.Content.Headers.ContentRange?.ToString());
        var contentMd5 = Assert.IsType<byte[]>(rangeResponse.Content.Headers.ContentMD5);
        Assert.Equal(AzureProtocolChecksum.Md5(rangedBytes), contentMd5);

        var withoutRange = await GetReadStatusAsync(transport, uri, request =>
            request.Headers.TryAddWithoutValidation("x-ms-range-get-content-md5", "true")).ConfigureAwait(false);
        var notModified = await GetReadStatusAsync(transport, uri, request =>
            request.Headers.TryAddWithoutValidation("If-None-Match", etag)).ConfigureAwait(false);
        var staleMatch = await GetReadStatusAsync(transport, uri, request =>
            request.Headers.TryAddWithoutValidation("If-Match", "\"stale\"")).ConfigureAwait(false);
        var invalidRange = await GetReadStatusAsync(transport, uri, request =>
            request.Headers.TryAddWithoutValidation("Range", "bytes=9000-9999")).ConfigureAwait(false);
        Assert.Equal(304, notModified.Status);
        Assert.Equal(412, staleMatch.Status);
        Assert.Equal(416, invalidRange.Status);
        return new RangedReadObservation(
            (int)rangeResponse.StatusCode, rangeResponse.Content.Headers.ContentRange!.ToString(),
            Convert.ToBase64String(contentMd5), Convert.ToHexString(SHA256.HashData(rangedBytes)),
            withoutRange, notModified, staleMatch, invalidRange);
    }

    private static async Task<(int Status, string? ErrorCode)> GetReadStatusAsync(
        HttpClient transport, Uri uri, Action<HttpRequestMessage> configure)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, uri);
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        configure(request);
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        var errorCode = response.Headers.TryGetValues("x-ms-error-code", out var values)
            ? values.Single() : null;
        return ((int)response.StatusCode, errorCode);
    }

    private static async Task<SnapshotDeleteObservation> ExerciseSnapshotDeletionModesAsync(
        BlobContainerClient container)
    {
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("snapshot-delete.txt");
        await blob.UploadAsync(BinaryData.FromString("payload")).ConfigureAwait(false);
        var firstSnapshot = (await blob.CreateSnapshotAsync().ConfigureAwait(false)).Value.Snapshot;
        var first = blob.WithSnapshot(firstSnapshot);
        var missingMode = await Assert.ThrowsAsync<RequestFailedException>(() => blob.DeleteAsync())
            .ConfigureAwait(false);
        var snapshotWithMode = await Assert.ThrowsAsync<RequestFailedException>(() =>
            first.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots)).ConfigureAwait(false);
        var baseBefore = (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString();
        var snapshotBefore = (await first.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString();
        var only = await blob.DeleteAsync(DeleteSnapshotsOption.OnlySnapshots).ConfigureAwait(false);
        var firstAfterOnly = (await first.ExistsAsync().ConfigureAwait(false)).Value;
        var baseAfterOnly = (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString();
        var secondSnapshot = (await blob.CreateSnapshotAsync().ConfigureAwait(false)).Value.Snapshot;
        var second = blob.WithSnapshot(secondSnapshot);
        var include = await blob.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots).ConfigureAwait(false);
        var baseAfterInclude = (await blob.ExistsAsync().ConfigureAwait(false)).Value;
        var secondAfterInclude = (await second.ExistsAsync().ConfigureAwait(false)).Value;
        return new SnapshotDeleteObservation(
            missingMode.Status, missingMode.ErrorCode,
            snapshotWithMode.Status, snapshotWithMode.ErrorCode,
            baseBefore, snapshotBefore, only.Status, firstAfterOnly,
            baseAfterOnly, include.Status, baseAfterInclude, secondAfterInclude);
    }

    private static async Task<LeasedSnapshotDeleteObservation> ExerciseLeasedSnapshotDeletionAsync(
        BlobContainerClient container)
    {
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("leased-snapshot.txt");
        await blob.UploadAsync(BinaryData.FromString("leased payload")).ConfigureAwait(false);
        var snapshotId = (await blob.CreateSnapshotAsync().ConfigureAwait(false)).Value.Snapshot;
        var snapshot = blob.WithSnapshot(snapshotId);
        var leaseId = (await blob.GetBlobLeaseClient().AcquireAsync(TimeSpan.FromSeconds(15))
            .ConfigureAwait(false)).Value.LeaseId;
        var condition = new BlobRequestConditions { LeaseId = leaseId };
        var onlyWithoutLease = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.DeleteAsync(DeleteSnapshotsOption.OnlySnapshots)).ConfigureAwait(false);
        var onlyWithWrongLease = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.DeleteAsync(DeleteSnapshotsOption.OnlySnapshots,
                new BlobRequestConditions { LeaseId = Guid.NewGuid().ToString("D") })).ConfigureAwait(false);
        var baseBeforeOnly = (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString();
        var snapshotBeforeOnly = (await snapshot.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString();
        var onlyWithLease = await blob.DeleteAsync(DeleteSnapshotsOption.OnlySnapshots, condition)
            .ConfigureAwait(false);
        var baseAfterOnly = (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString();
        var snapshotAfterOnly = (await snapshot.ExistsAsync().ConfigureAwait(false)).Value;
        var baseWithoutLease = await Assert.ThrowsAsync<RequestFailedException>(() => blob.DeleteAsync())
            .ConfigureAwait(false);
        var baseWithLease = await blob.DeleteAsync(conditions: condition).ConfigureAwait(false);
        var baseAfterDelete = (await blob.ExistsAsync().ConfigureAwait(false)).Value;
        return new LeasedSnapshotDeleteObservation(
            onlyWithoutLease.Status, onlyWithoutLease.ErrorCode,
            onlyWithWrongLease.Status, onlyWithWrongLease.ErrorCode,
            baseBeforeOnly, snapshotBeforeOnly,
            onlyWithLease.Status, baseAfterOnly, snapshotAfterOnly,
            baseWithoutLease.Status, baseWithoutLease.ErrorCode,
            baseWithLease.Status, baseAfterDelete);
    }

    private static async Task<LeasedSnapshotIncludeObservation> ExerciseLeasedSnapshotIncludeAsync(
        BlobContainerClient container)
    {
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("leased-include.txt");
        await blob.UploadAsync(BinaryData.FromString("leased include payload")).ConfigureAwait(false);
        var snapshotId = (await blob.CreateSnapshotAsync().ConfigureAwait(false)).Value.Snapshot;
        var snapshot = blob.WithSnapshot(snapshotId);
        var leaseId = (await blob.GetBlobLeaseClient().AcquireAsync(TimeSpan.FromSeconds(15))
            .ConfigureAwait(false)).Value.LeaseId;
        var missing = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots)).ConfigureAwait(false);
        var wrong = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.DeleteAsync(
                DeleteSnapshotsOption.IncludeSnapshots,
                new BlobRequestConditions { LeaseId = Guid.NewGuid().ToString("D") })).ConfigureAwait(false);
        var baseBeforeSuccess = (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString();
        var snapshotBeforeSuccess = (await snapshot.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString();
        var include = await blob.DeleteAsync(
            DeleteSnapshotsOption.IncludeSnapshots, new BlobRequestConditions { LeaseId = leaseId })
            .ConfigureAwait(false);
        var baseAfterSuccess = (await blob.ExistsAsync().ConfigureAwait(false)).Value;
        var snapshotAfterSuccess = (await snapshot.ExistsAsync().ConfigureAwait(false)).Value;
        return new LeasedSnapshotIncludeObservation(
            missing.Status, missing.ErrorCode, wrong.Status, wrong.ErrorCode,
            baseBeforeSuccess, snapshotBeforeSuccess, include.Status,
            baseAfterSuccess, snapshotAfterSuccess);
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

    private sealed record AccountInformationObservation(
        int ServiceStatus, int ContainerStatus, int BlobStatus,
        SkuName SkuName, AccountKind AccountKind, bool IsHierarchicalNamespaceEnabled);

    private sealed record EscapedNameObservation(
        int ExclamationUploadStatus, int LiteralPercentUploadStatus,
        string ExclamationContent, string LiteralPercentContent, string ListedNames);

    private sealed record BlobHttpPropertyObservation(
        int SetPropertiesStatus, int SetMetadataStatus, int StaleStatus, string? StaleErrorCode,
        bool PropertiesChangedETag, bool MetadataChangedETag, string ContentType,
        string ClearedHttpProperties, string Metadata, string Content);

    private sealed record BlobMetadataObservation(
        BlobMetadataReadObservation Current, BlobMetadataReadObservation Snapshot);

    private sealed record BlobMetadataReadObservation(
        int GetStatus, int HeadStatus, int NotModifiedStatus,
        string Phase, int GetBodyLength, int HeadBodyLength);

    private sealed record SnapshotDeleteObservation(
        int MissingModeStatus, string? MissingModeCode,
        int SnapshotWithModeStatus, string? SnapshotWithModeCode,
        string BaseBefore, string SnapshotBefore, int OnlyStatus,
        bool FirstSnapshotAfterOnly, string BaseAfterOnly,
        int IncludeStatus, bool BaseAfterInclude, bool SecondSnapshotAfterInclude);

    private sealed record LeasedSnapshotDeleteObservation(
        int OnlyWithoutLeaseStatus, string? OnlyWithoutLeaseCode,
        int OnlyWithWrongLeaseStatus, string? OnlyWithWrongLeaseCode,
        string BaseBeforeOnly, string SnapshotBeforeOnly,
        int OnlyWithLeaseStatus, string BaseAfterOnly, bool SnapshotAfterOnly,
        int BaseWithoutLeaseStatus, string? BaseWithoutLeaseCode,
        int BaseWithLeaseStatus, bool BaseAfterDelete);

    private sealed record LeasedSnapshotIncludeObservation(
        int MissingLeaseStatus, string? MissingLeaseCode,
        int WrongLeaseStatus, string? WrongLeaseCode,
        string BaseBeforeSuccess, string SnapshotBeforeSuccess,
        int IncludeStatus, bool BaseAfterSuccess, bool SnapshotAfterSuccess);

    private sealed record ContainerMetadataObservation(
        int GetStatus, int HeadStatus, string Phase,
        int GetBodyLength, int HeadBodyLength, bool BlobWritePreservedETag,
        int ServiceSasDeniedStatus, string? ServiceSasDeniedCode);

    private sealed record ContainerMetadataReadObservation(
        int GetStatus, int HeadStatus, string Phase, string? ETag,
        int GetBodyLength, int HeadBodyLength);

    private sealed record StagedBlobObservation(
        int UncommittedBlocks,
        int CommitStatus,
        int CommittedBlocks,
        string Content,
        string SnapshotContent,
        int MissingLeaseStatus,
        string? MissingLeaseCode,
        string Tag);

    private sealed record BlockListSnapshotObservation(
        string CurrentCommitted,
        string CurrentUncommitted,
        string SnapshotCommitted,
        string SnapshotUncommitted,
        int OmittedTypeStatus,
        bool OmittedTypeIncludesUncommitted,
        int StaleIfMatchStatus,
        string? StaleIfMatchCode);

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

    private sealed record BlobLeaseLifecycleObservation(
        int AcquireStatus, int RenewStatus, int ChangeStatus, bool ChangedIdMatches,
        int StaleLeaseStatus, string? StaleLeaseCode, int ReleaseStatus,
        int SecondAcquireStatus, int BreakStatus, int? BreakTime,
        string BrokenState, int ReacquireStatus);

    private sealed record ContainerLeaseLifecycleObservation(
        int AcquireStatus, int RenewStatus, int ChangeStatus, bool ChangedIdMatches,
        int StaleLeaseStatus, string? StaleLeaseCode, int ReleaseStatus,
        int SecondAcquireStatus, int BreakStatus, int? BreakTime,
        string BrokenState, int ReacquireStatus);

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
        string ExposedHeaders, int MaxAgeInSeconds, CorsPreflightObservation Preflight,
        int DeniedOriginStatus, int MissingMethodStatus);

    private sealed record CorsPreflightObservation(int Status, string AllowedOrigin, string AllowedMethods, string MaxAge);

    private sealed record CopyObservation(
        int StaleSourceStatus, string? StaleSourceCode, int StartStatus,
        string CopyStatus, long Length, string ContentSha256);

    private sealed record BlobListingObservation(
        string FlatPages, int FlatContinuations,
        string HierarchyPages, int HierarchyContinuations);

    private sealed record StoredPolicySasObservation(
        string Identifier, string Permissions, string ReadBytes,
        int DeniedWriteStatus, string? DeniedWriteCode, string RetainedBytes,
        int RevokedReadStatus, string? RevokedReadCode);

    private sealed record RangedReadObservation(
        int RangeStatus, string ContentRange, string ContentMd5, string ContentSha256,
        (int Status, string? ErrorCode) ChecksumWithoutRange,
        (int Status, string? ErrorCode) NotModified,
        (int Status, string? ErrorCode) StaleMatch,
        (int Status, string? ErrorCode) InvalidRange);
}
