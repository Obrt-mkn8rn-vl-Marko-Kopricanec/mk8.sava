using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class MetadataStoreLifetimeTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData("ready", false)]
    [InlineData("inventory", false)]
    [InlineData("summary", false)]
    [InlineData("container", false)]
    [InlineData("properties", false)]
    [InlineData("async-count", false)]
    [InlineData("packed-chunk", false)]
    [InlineData("pack", false)]
    [InlineData("sync-count", false)]
    [InlineData("ready", true)]
    [InlineData("inventory", true)]
    [InlineData("summary", true)]
    [InlineData("container", true)]
    [InlineData("properties", true)]
    [InlineData("async-count", true)]
    [InlineData("packed-chunk", true)]
    [InlineData("pack", true)]
    [InlineData("sync-count", true)]
    public async Task RetiredStoreCannotReopenOrRecreateItsDatabase(string operation, bool removeDatabase)
    {
        using var fixture = new StoreFixture();
        var metadata = fixture.Metadata;
        await metadata.InitializeAsync().ConfigureAwait(true);
        Assert.True(await metadata.IsReadyAsync(CancellationToken.None).ConfigureAwait(true));
        metadata.Dispose();
        metadata.Dispose();
        if (removeDatabase)
            File.Delete(fixture.Paths.Database);
        var before = Directory.GetFiles(fixture.Root).Order(StringComparer.Ordinal).ToArray();

        var failure = await Record.ExceptionAsync(() => ReadAsync(metadata, operation)).ConfigureAwait(true);
        output.WriteLine($"Operation={operation}; RemoveDatabase={removeDatabase}; " +
            $"Failure={failure?.GetType().FullName ?? "none"}; DatabaseExists={File.Exists(fixture.Paths.Database)}");

        var disposed = Assert.IsType<ObjectDisposedException>(failure);
        Assert.Equal(typeof(MetadataStore).FullName, disposed.ObjectName);
        Assert.Equal(!removeDatabase, File.Exists(fixture.Paths.Database));
        Assert.Equal(before, Directory.GetFiles(fixture.Root).Order(StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public async Task RetiringMetadataDoesNotReleaseBorrowedPathsOrPreventAFreshOwnerReadingStoredRecords()
    {
        using var fixture = new StoreFixture();
        var metadata = fixture.Metadata;
        await metadata.InitializeAsync().ConfigureAwait(true);
        var record = new ContainerRecord
        {
            Account = SavaWebApplicationFactory.AccountName,
            Name = "retained-container",
            Revision = "owned-revision",
            ETag = "\"owned-etag\"",
            CreatedAt = DateTimeOffset.UnixEpoch,
            LastModified = DateTimeOffset.UnixEpoch,
            Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["retained"] = "value" },
        };
        Assert.True(await metadata.TryCreateContainerAsync(record, CancellationToken.None).ConfigureAwait(true));
        metadata.Dispose();

        Assert.Throws<StorageRootLeaseException>(() =>
        {
            using var contender = new StoragePaths(
                new TestEnvironment(fixture.Root), Options.Create(new SavaOptions { DataPath = fixture.Root }));
        });
        using var fresh = new MetadataStore(fixture.Paths, new NullStorageFaultInjector());
        await fresh.InitializeAsync().ConfigureAwait(true);
        var retained = await fresh.GetContainerAsync(record.Account, record.Name, includeDeleted: false,
            CancellationToken.None).ConfigureAwait(true);

        Assert.NotNull(retained);
        Assert.Equal(record.ETag, retained.ETag);
        Assert.Equal(record.Revision, retained.Revision);
        Assert.Equal(record.Metadata["retained"], retained.Metadata["retained"]);
        Assert.True(await fresh.IsReadyAsync(CancellationToken.None).ConfigureAwait(true));
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            metadata.GetContainerAsync(record.Account, record.Name, includeDeleted: false, CancellationToken.None))
            .ConfigureAwait(true);
    }

    private static async Task ReadAsync(MetadataStore metadata, string operation)
    {
        switch (operation)
        {
            case "ready":
                _ = await metadata.IsReadyAsync(CancellationToken.None).ConfigureAwait(false);
                break;
            case "inventory":
                _ = await metadata.GetStorageInventoryAsync(CancellationToken.None).ConfigureAwait(false);
                break;
            case "summary":
                _ = await metadata.GetStorageInventorySummaryAsync(CancellationToken.None).ConfigureAwait(false);
                break;
            case "container":
                _ = await metadata.GetContainerAsync(SavaWebApplicationFactory.AccountName, "absent",
                    includeDeleted: false, CancellationToken.None).ConfigureAwait(false);
                break;
            case "properties":
                _ = await metadata.GetServicePropertiesAsync(SavaWebApplicationFactory.AccountName,
                    CancellationToken.None).ConfigureAwait(false);
                break;
            case "async-count":
                _ = await metadata.CountPackedChunksAsync(CancellationToken.None).ConfigureAwait(false);
                break;
            case "packed-chunk":
            case "pack":
            case "sync-count":
                ReadSynchronously(metadata, operation);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(operation));
        }
    }

    private static void ReadSynchronously(MetadataStore metadata, string operation)
    {
        switch (operation)
        {
            case "packed-chunk":
                _ = metadata.PackedChunkExists("absent");
                break;
            case "pack":
                _ = metadata.ChunkPackExists("absent");
                break;
            case "sync-count":
                _ = metadata.CountPackedChunks();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(operation));
        }
    }

    private sealed class StoreFixture : IDisposable
    {
        internal StoreFixture()
        {
            Root = Directory.CreateTempSubdirectory("mk8-sava-metadata-lifetime-").FullName;
            StoragePaths? paths = null;
            try
            {
                paths = new StoragePaths(new TestEnvironment(Root), Options.Create(new SavaOptions { DataPath = Root }));
                Metadata = new MetadataStore(paths, new NullStorageFaultInjector());
                Paths = paths;
                paths = null;
            }
            finally
            {
                paths?.Dispose();
                if (Metadata is null)
                    Directory.Delete(Root, recursive: true);
            }
        }

        internal string Root { get; }
        internal StoragePaths Paths { get; }
        internal MetadataStore Metadata { get; }

        public void Dispose()
        {
            try
            {
                Metadata.Dispose();
            }
            finally
            {
                Paths.Dispose();
                Directory.Delete(Root, recursive: true);
            }
        }
    }

    private sealed class TestEnvironment(string root) : IHostEnvironment
    {
        public string EnvironmentName { get; set; } = Environments.Production;
        public string ApplicationName { get; set; } = nameof(MetadataStoreLifetimeTests);
        public string ContentRootPath { get; set; } = root;
        public IFileProvider ContentRootFileProvider { get; set; } = new NullFileProvider();
    }
}
