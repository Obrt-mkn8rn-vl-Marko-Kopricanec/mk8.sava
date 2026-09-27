using System.Security.Cryptography;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageInitializationTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StorageInitializationCompletesBeforeAnyHostedServiceStarts(bool concurrentStart) =>
        await AssertInitializationOrderAsync(concurrentStart).ConfigureAwait(true);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RejectedStartupPreservesItsCauseAndReleasesTheRootLease(bool concurrentStart) =>
        await AssertRejectedRestartsAsync(concurrentStart).ConfigureAwait(true);

    private static async Task AssertInitializationOrderAsync(bool concurrentStart)
    {
        var factory = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:VersioningEnabled"] = "true"
        });
        await using var factoryDisposal = factory.ConfigureAwait(false);
        StartupObserver? observer = null;
        var application = factory.WithWebHostBuilder(builder => builder.ConfigureServices(services =>
        {
            services.Configure<HostOptions>(options => options.ServicesStartConcurrently = concurrentStart);
            services.AddSingleton<IHostedService>(provider => observer = new StartupObserver(provider));
        }));
        await using var applicationDisposal = application.ConfigureAwait(false);
        _ = application.Server;
        Assert.NotNull(observer);
        Assert.True(observer.Started);
        Assert.True(observer.StorageReady);
        Assert.True(observer.VersioningEnabled);
    }

    private static async Task AssertRejectedRestartsAsync(bool concurrentStart)
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-startup-failure-{Guid.NewGuid():N}");
        var content = RandomNumberGenerator.GetBytes(8192);
        var containerName = $"startup-{Guid.NewGuid():N}";
        try
        {
            var original = new SavaWebApplicationFactory(dataPath, deleteDataPath: false);
            await using (original.ConfigureAwait(false))
            {
                await original.InitializeAsync().ConfigureAwait(false);
                var container = CreateClient(original.CreateClient()).GetBlobContainerClient(containerName);
                await container.CreateAsync().ConfigureAwait(false);
                await container.GetBlobClient("retained.bin").UploadAsync(BinaryData.FromBytes(content))
                    .ConfigureAwait(false);
            }

            var changedKey = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
            for (var attempt = 0; attempt < 16; attempt++)
            {
                var rejected = new SavaWebApplicationFactory(dataPath, new Dictionary<string, string?>(StringComparer.Ordinal)
                {
                    [$"Sava:DataEncryptionKeys:{SavaWebApplicationFactory.AccountName}"] = changedKey
                }, deleteDataPath: false);
                await using var rejectedDisposal = rejected.ConfigureAwait(false);
                StartupObserver? observer = null;
                var application = rejected.WithWebHostBuilder(builder => builder.ConfigureServices(services =>
                {
                    services.Configure<HostOptions>(options => options.ServicesStartConcurrently = concurrentStart);
                    services.AddSingleton<IHostedService>(provider => observer = new StartupObserver(provider));
                }));
                await using var applicationDisposal = application.ConfigureAwait(false);
                var failure = Assert.Throws<InvalidDataException>(() => _ = application.Server);
                Assert.Contains("data encryption key", failure.Message, StringComparison.OrdinalIgnoreCase);
                Assert.NotNull(observer);
                Assert.False(observer.Started);
            }

            var recovered = new SavaWebApplicationFactory(dataPath, deleteDataPath: false);
            await using var recoveredDisposal = recovered.ConfigureAwait(false);
            await recovered.InitializeAsync().ConfigureAwait(false);
            var download = await CreateClient(recovered.CreateClient()).GetBlobContainerClient(containerName)
                .GetBlobClient("retained.bin").DownloadContentAsync().ConfigureAwait(false);
            Assert.Equal(content, download.Value.Content.ToArray());
        }
        finally
        {
            await SavaWebApplicationFactory.DeleteDataPathAsync(dataPath).ConfigureAwait(false);
        }
    }

    private static BlobServiceClient CreateClient(HttpClient client) => new(
        new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost"),
        new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
        new BlobClientOptions { Transport = new HttpClientTransport(client) });

    private sealed class StartupObserver(IServiceProvider services) : IHostedService
    {
        internal bool Started { get; private set; }
        internal bool StorageReady { get; private set; }
        internal bool VersioningEnabled { get; private set; }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            Started = true;
            var metadata = services.GetRequiredService<MetadataStore>();
            StorageReady = await metadata.IsReadyAsync(cancellationToken).ConfigureAwait(false);
            VersioningEnabled = (await metadata.GetServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName, cancellationToken).ConfigureAwait(false)).VersioningEnabled;
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
