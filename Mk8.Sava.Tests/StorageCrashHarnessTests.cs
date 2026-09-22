using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using System.Security.Cryptography;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageCrashHarnessTests
{
    private const string ScenarioVariable = "MK8_SAVA_CRASH_SCENARIO";
    private const string DataPathVariable = "MK8_SAVA_CRASH_DATA_PATH";
    private const string ContainerName = "crash-harness";
    private const string BlobName = "durable.bin";

    private static readonly IReadOnlyDictionary<string, string?> CrashConfiguration =
        new Dictionary<string, string?>
        {
            ["Sava:EnableSmallChunkPacking"] = "false",
            ["Sava:MaintenanceScanInterval"] = "01:00:00",
            ["Sava:AbandonedStagingRetention"] = "1.00:00:00"
        };

    [Fact]
    public async Task CrashWorker()
    {
        if (!TryReadHarnessEnvironment(out var scenario, out var dataPath))
            return;

        var faultInjector = new ProcessStorageFaultInjector();
        var application = CreateApplication(dataPath, faultInjector, deleteDataPath: false);
        try
        {
            await application.InitializeAsync();
            var container = CreateClient(application).GetBlobContainerClient(ContainerName);
            await container.CreateIfNotExistsAsync();
            var blob = container.GetBlobClient(BlobName);
            var content = CreateContent();

            switch (scenario)
            {
                case "chunk-publication":
                    faultInjector.ArmTermination(StorageFaultPoint.BeforeChunkPublication);
                    await blob.UploadAsync(BinaryData.FromBytes(content), overwrite: true);
                    break;
                case "metadata-precommit":
                    faultInjector.ArmTermination(StorageFaultPoint.BeforeBlobMetadataCommit);
                    await blob.UploadAsync(BinaryData.FromBytes(content), overwrite: true);
                    break;
                case "metadata-postcommit":
                    faultInjector.ArmTermination(StorageFaultPoint.AfterBlobMetadataCommit);
                    await blob.UploadAsync(BinaryData.FromBytes(content), overwrite: true);
                    break;
                case "reclamation-delete":
                    faultInjector.ArmException(StorageFaultPoint.BeforeBlobMetadataCommit);
                    var failure = await Assert.ThrowsAsync<RequestFailedException>(() =>
                        blob.UploadAsync(BinaryData.FromBytes(content), overwrite: true));
                    Assert.Equal(500, failure.Status);
                    faultInjector.ArmTermination(StorageFaultPoint.BeforeGarbageCollectionDelete);
                    await application.Services
                        .GetRequiredService<BlobService>()
                        .CollectGarbageAsync(CancellationToken.None);
                    break;
                default:
                    throw new InvalidOperationException($"Unknown crash scenario '{scenario}'.");
            }

            Assert.Fail($"Crash scenario '{scenario}' returned without terminating the worker process.");
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task ValidateCrashRecovery()
    {
        if (!TryReadHarnessEnvironment(out var scenario, out var dataPath))
            return;

        var application = CreateApplication(
            dataPath,
            new NullStorageFaultInjector(),
            deleteDataPath: true);
        try
        {
            await application.InitializeAsync();
            var blob = CreateClient(application)
                .GetBlobContainerClient(ContainerName)
                .GetBlobClient(BlobName);

            if (scenario == "metadata-postcommit")
            {
                var recovered = await blob.DownloadContentAsync();
                Assert.Equal(CreateContent(), recovered.Value.Content.ToArray());
                return;
            }

            Assert.False((await blob.ExistsAsync()).Value);
            var service = application.Services.GetRequiredService<BlobService>();
            if (scenario == "chunk-publication")
            {
                var stagingFiles = EnumerateStagingFiles(dataPath);
                Assert.NotEmpty(stagingFiles);
                foreach (var stagingFile in stagingFiles)
                    File.SetLastWriteTimeUtc(stagingFile, DateTime.UtcNow.AddDays(-2));
                var maintenance = await service.RunMaintenanceAsync(CancellationToken.None);
                Assert.True(maintenance.ReclaimedStagingFiles > 0);
                Assert.Empty(EnumerateStagingFiles(dataPath));
                Assert.Empty(EnumerateContentFiles(dataPath));
                return;
            }

            Assert.NotEmpty(EnumerateContentFiles(dataPath));
            Assert.True(await service.CollectGarbageAsync(CancellationToken.None) > 0);
            Assert.Empty(EnumerateContentFiles(dataPath));
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    private static bool TryReadHarnessEnvironment(out string scenario, out string dataPath)
    {
        scenario = Environment.GetEnvironmentVariable(ScenarioVariable) ?? string.Empty;
        dataPath = Environment.GetEnvironmentVariable(DataPathVariable) ?? string.Empty;
        if (string.IsNullOrWhiteSpace(scenario) || string.IsNullOrWhiteSpace(dataPath))
            return false;

        dataPath = Path.GetFullPath(dataPath);
        var temporaryRoot = Path.TrimEndingDirectorySeparator(Path.GetFullPath(Path.GetTempPath()));
        if (!string.Equals(Path.GetDirectoryName(dataPath), temporaryRoot, StringComparison.Ordinal) ||
            !Path.GetFileName(dataPath).StartsWith("mk8-sava-", StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"The crash harness data path must be an mk8-sava-* child of '{temporaryRoot}'.");
        }
        return true;
    }

    private static SavaWebApplicationFactory CreateApplication(
        string dataPath,
        IStorageFaultInjector faultInjector,
        bool deleteDataPath) =>
        new(
            dataPath,
            faultInjector,
            analyticsSink: null,
            configurationOverrides: CrashConfiguration,
            deleteDataPath,
            disableMaintenance: true);

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
                Transport = new HttpClientTransport(new HttpClient(application.Server.CreateHandler())
                {
                    BaseAddress = endpoint
                }),
                Retry = { MaxRetries = 0 }
            });
    }

    private static byte[] CreateContent()
    {
        var content = new byte[96 * 1024];
        for (var index = 0; index < content.Length; index++)
            content[index] = (byte)((index * 31 + 17) % 251);
        return content;
    }

    private static string[] EnumerateContentFiles(string dataPath) =>
        new[] { Path.Combine(dataPath, "chunks"), Path.Combine(dataPath, "packs") }
            .Where(Directory.Exists)
            .SelectMany(path => Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories))
            .Order(StringComparer.Ordinal)
            .ToArray();

    private static string[] EnumerateStagingFiles(string dataPath)
    {
        var path = Path.Combine(dataPath, "staging");
        return Directory.Exists(path)
            ? Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories).Order(StringComparer.Ordinal).ToArray()
            : [];
    }

    private sealed class ProcessStorageFaultInjector : IStorageFaultInjector
    {
        private readonly object _gate = new();
        private StorageFaultPoint _point;
        private bool _armed;
        private bool _terminate;

        public void ArmException(StorageFaultPoint point) => Arm(point, terminate: false);

        public void ArmTermination(StorageFaultPoint point) => Arm(point, terminate: true);

        public void Inject(StorageFaultPoint point)
        {
            bool terminate;
            lock (_gate)
            {
                if (!_armed || point != _point)
                    return;
                _armed = false;
                terminate = _terminate;
            }

            if (terminate)
            {
                Environment.FailFast($"Injected process termination at storage fault point {point}.");
            }
            throw new IOException($"Injected storage failure at {point}.");
        }

        private void Arm(StorageFaultPoint point, bool terminate)
        {
            lock (_gate)
            {
                _point = point;
                _terminate = terminate;
                _armed = true;
            }
        }
    }
}
