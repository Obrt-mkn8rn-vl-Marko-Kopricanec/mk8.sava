using System.Net;
using System.Security.Cryptography;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Mk8.Sava.Application;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

[Trait("Category", "SplitProcess")]
public sealed class SplitProcessTests
{
    [Fact]
    public async Task GatewayStartsAloneAndConnectsWithoutRestartOrOpeningTheDataRoot()
    {
        var host = await SplitProcessHost.StartAsync(application: false).ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        var gatewayProcess = host.GatewayProcessId;
        await SplitProcessHost.AssertStatusAsync(host.GatewayAddress, "/health/live", HttpStatusCode.OK).ConfigureAwait(true);
        await SplitProcessHost.AssertStatusAsync(host.GatewayAddress, "/health/ready", HttpStatusCode.ServiceUnavailable).ConfigureAwait(true);
        await SplitProcessHost.AssertStatusAsync(host.GatewayAddress, "/metrics", HttpStatusCode.OK).ConfigureAwait(true);
        Assert.False(Directory.Exists(host.StoragePath));
        Assert.False(Directory.Exists(host.GatewayUnusedDataPath));

        await host.StartApplicationAsync().ConfigureAwait(true);
        await SplitProcessHost.AssertStatusAsync(host.GatewayAddress, "/health/ready", HttpStatusCode.OK).ConfigureAwait(true);
        Assert.Equal(gatewayProcess, host.GatewayProcessId);
        var container = host.Client.GetBlobContainerClient("late-application");
        Assert.Equal(201, (await container.CreateAsync().ConfigureAwait(true)).GetRawResponse().Status);
        var blob = container.GetBlobClient("content");
        var bytes = RandomNumberGenerator.GetBytes(65537);
        await blob.UploadAsync(BinaryData.FromBytes(bytes)).ConfigureAwait(true);
        Assert.Equal(bytes, (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());
        Assert.Equal(202, (await blob.DeleteAsync().ConfigureAwait(true)).Status);
        Assert.Equal(202, (await container.DeleteAsync().ConfigureAwait(true)).Status);
        Assert.True(File.Exists(Path.Combine(host.StoragePath, "metadata.db")));
        Assert.False(Directory.Exists(host.GatewayUnusedDataPath));
    }

    [Fact]
    public async Task ApplicationRemainsReadyAndReadableAfterGatewayStops()
    {
        var host = await SplitProcessHost.StartAsync().ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        var container = host.Client.GetBlobContainerClient("application-alone");
        await container.CreateAsync().ConfigureAwait(true);
        var bytes = RandomNumberGenerator.GetBytes(16391);
        await container.GetBlobClient("content").UploadAsync(BinaryData.FromBytes(bytes)).ConfigureAwait(true);
        await host.StopGatewayAsync().ConfigureAwait(true);

        await SplitProcessHost.AssertStatusAsync(host.ApplicationAddress, "/health/live", HttpStatusCode.OK).ConfigureAwait(true);
        await SplitProcessHost.AssertStatusAsync(host.ApplicationAddress, "/health/ready", HttpStatusCode.OK).ConfigureAwait(true);
        using var application = host.CreateApplicationClient();
        var readiness = application.CreateProxy<IApplicationReadiness>();
        Assert.True((await readiness.GetAsync(CancellationToken.None).ConfigureAwait(true)).Ready);
        var metadata = application.CreateProxy<IMetadataApplication>();
        var record = await metadata.GetBlobAsync(SavaWebApplicationFactory.AccountName, container.Name, "content",
            null, null, false, CancellationToken.None).ConfigureAwait(true);
        Assert.NotNull(record);
        using var content = new MemoryStream();
        await application.CreateProxy<IBlobApplication>().WriteContentAsync(record, new BlobEncryption(null, null),
            0, bytes.Length, content, CancellationToken.None).ConfigureAwait(true);
        Assert.Equal(bytes, content.ToArray());
        Assert.False(Directory.Exists(host.GatewayUnusedDataPath));
    }

    [Fact]
    public async Task ApplicationRestartPreservesBytesWithoutRestartingGatewayOrReplayingFailedMutations()
    {
        var host = await SplitProcessHost.StartAsync().ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        var container = host.Client.GetBlobContainerClient("restart-boundary");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("acknowledged");
        var bytes = RandomNumberGenerator.GetBytes(1048897);
        var uploaded = await blob.UploadAsync(BinaryData.FromBytes(bytes)).ConfigureAwait(true);
        var gatewayProcess = host.GatewayProcessId;
        await host.StopApplicationAsync().ConfigureAwait(true);
        await SplitProcessHost.AssertStatusAsync(host.GatewayAddress, "/health/live", HttpStatusCode.OK).ConfigureAwait(true);
        await SplitProcessHost.AssertStatusAsync(host.GatewayAddress, "/health/ready", HttpStatusCode.ServiceUnavailable).ConfigureAwait(true);

        AssertUnavailable(await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.UploadAsync(BinaryData.FromString("must-not-overwrite"), overwrite: true)).ConfigureAwait(true));
        var failedCreate = container.GetBlobClient("must-not-appear-after-recovery");
        AssertUnavailable(await Assert.ThrowsAsync<RequestFailedException>(() =>
            failedCreate.UploadAsync(BinaryData.FromString("must-not-queue"))).ConfigureAwait(true));
        await host.StartApplicationAsync().ConfigureAwait(true);
        await SplitProcessHost.AssertStatusAsync(host.GatewayAddress, "/health/ready", HttpStatusCode.OK).ConfigureAwait(true);

        Assert.Equal(gatewayProcess, host.GatewayProcessId);
        Assert.Equal(uploaded.Value.ETag, (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value.ETag);
        await AssertFullAndRangeAsync(blob, bytes).ConfigureAwait(true);
        Assert.False((await failedCreate.ExistsAsync().ConfigureAwait(true)).Value);
        Assert.False(Directory.Exists(host.GatewayUnusedDataPath));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task CompletePayloadWithoutProducerSuccessNeverPublishesOrReplacesMetadata(bool staged, bool existing)
    {
        var host = await SplitProcessHost.StartAsync().ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        var container = host.Client.GetBlobContainerClient("missing-terminal");
        await container.CreateAsync().ConfigureAwait(true);
        using var application = host.CreateApplicationClient();
        var blobs = application.CreateProxy<IBlobApplication>();
        var before = await PrepareTerminalDestinationAsync(container, blobs, staged, existing).ConfigureAwait(true);
        var payload = RandomNumberGenerator.GetBytes(65537);
        using var producer = new FaultAtEofStream(payload);

        var failure = await Assert.ThrowsAsync<IOException>(() => staged
            ? StageFromProducerAsync(blobs, container.Name, producer, CancellationToken.None)
            : PutFromProducerAsync(blobs, container.Name, producer, before, CancellationToken.None)).ConfigureAwait(true);

        Assert.Contains("Injected producer EOF fault", failure.Message, StringComparison.Ordinal);
        Assert.Equal(payload.Length, producer.BytesProduced);
        Assert.Equal(1, producer.EofFaults);
        await AssertTerminalDestinationAsync(application.CreateProxy<IMetadataApplication>(), blobs, container,
            before, staged, existing).ConfigureAwait(true);
        Assert.True((await application.CreateProxy<IApplicationReadiness>().GetAsync(CancellationToken.None)
            .ConfigureAwait(true)).Ready);
    }

    [Fact]
    public async Task CancellationAfterAllProducerBytesDoesNotPublishAnUpload()
    {
        var host = await SplitProcessHost.StartAsync().ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        var container = host.Client.GetBlobContainerClient("canceled-terminal");
        await container.CreateAsync().ConfigureAwait(true);
        using var application = host.CreateApplicationClient();
        using var cancellation = new CancellationTokenSource();
        var bytes = RandomNumberGenerator.GetBytes(16391);
        using var producer = new FaultAtEofStream(bytes, cancellation);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => PutFromProducerAsync(
            application.CreateProxy<IBlobApplication>(), container.Name, producer, null, cancellation.Token)).ConfigureAwait(true);

        Assert.True(cancellation.IsCancellationRequested);
        Assert.Equal(bytes.Length, producer.BytesProduced);
        Assert.Equal(1, producer.EofFaults);
        Assert.Null(await application.CreateProxy<IMetadataApplication>().GetBlobAsync(
            SavaWebApplicationFactory.AccountName, container.Name, "target", null, null, false,
            CancellationToken.None).ConfigureAwait(true));
        await SplitProcessHost.AssertStatusAsync(host.GatewayAddress, "/health/ready", HttpStatusCode.OK).ConfigureAwait(true);
    }

    [Fact]
    public async Task MismatchedTransportKeyFailsSafelyWithoutChangingApplicationData()
    {
        var host = await SplitProcessHost.StartAsync().ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        var container = host.Client.GetBlobContainerClient("wrong-key");
        await container.CreateAsync().ConfigureAwait(true);
        var seed = BinaryData.FromString("acknowledged-before-key-mismatch");
        await container.GetBlobClient("acknowledged").UploadAsync(seed).ConfigureAwait(true);
        using var trustedApplication = host.CreateApplicationClient();
        await host.StopGatewayAsync().ConfigureAwait(true);
        var rejectedKey = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
        await File.WriteAllTextAsync(host.AccessKeyFile, rejectedKey).ConfigureAwait(true);
        await host.StartGatewayAsync().ConfigureAwait(true);

        await AssertRejectedGatewayAsync(host, container.Name, rejectedKey).ConfigureAwait(true);
        await AssertUnchangedApplicationAsync(trustedApplication.CreateProxy<IMetadataApplication>(), container.Name, seed)
            .ConfigureAwait(true);
    }

    [Fact]
    public async Task MismatchedGatewayPolicyFailsSafelyWithoutChangingApplicationData()
    {
        var host = await SplitProcessHost.StartAsync().ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        var container = host.Client.GetBlobContainerClient("wrong-policy");
        await container.CreateAsync().ConfigureAwait(true);
        var seed = BinaryData.FromString("acknowledged-before-policy-mismatch");
        await container.GetBlobClient("acknowledged").UploadAsync(seed).ConfigureAwait(true);
        using var trustedApplication = host.CreateApplicationClient();
        await host.StopGatewayAsync().ConfigureAwait(true);
        await host.StartGatewayAsync(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaximumRequestBodyBytes"] = "1048576"
        }).ConfigureAwait(true);

        await AssertRejectedGatewayAsync(host, container.Name, "1048576").ConfigureAwait(true);
        await AssertUnchangedApplicationAsync(trustedApplication.CreateProxy<IMetadataApplication>(), container.Name, seed)
            .ConfigureAwait(true);
    }

    [Fact]
    public async Task OfficialSdkStorageFamiliesCrossTheIndependentProcessBoundary()
    {
        var host = await SplitProcessHost.StartAsync().ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        var container = host.Client.GetBlobContainerClient("sdk-process-families");
        await container.CreateAsync().ConfigureAwait(true);
        await AssertBlockSnapshotLeaseAndSasCopyAsync(container).ConfigureAwait(true);
        await AssertAppendAndPageAsync(container).ConfigureAwait(true);
        await AssertQueryAsync(container).ConfigureAwait(true);
        Assert.False(Directory.Exists(host.GatewayUnusedDataPath));
    }

    private static void AssertUnavailable(RequestFailedException exception)
    {
        Assert.Equal(503, exception.Status);
        Assert.Equal("ServerBusy", exception.ErrorCode);
    }

    private static void AssertUnavailable(AzureStorageException exception)
    {
        Assert.Equal(503, exception.StatusCode);
        Assert.Equal("ServerBusy", exception.ErrorCode);
    }

    private static async Task AssertFullAndRangeAsync(BlobClient blob, byte[] bytes)
    {
        Assert.Equal(bytes, (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());
        var range = await blob.DownloadStreamingAsync(new BlobDownloadOptions { Range = new HttpRange(63, 8195) })
            .ConfigureAwait(true);
        using var content = range.Value.Content;
        using var destination = new MemoryStream();
        await content.CopyToAsync(destination).ConfigureAwait(true);
        Assert.Equal(bytes.AsSpan(63, 8195).ToArray(), destination.ToArray());
    }

    private static async Task<BlobRecord?> PrepareTerminalDestinationAsync(
        BlobContainerClient container, IBlobApplication blobs, bool staged, bool existing)
    {
        if (!existing)
            return null;
        if (staged)
        {
            using var content = BinaryData.FromString("previous-stage").ToStream();
            await container.GetBlockBlobClient("target").StageBlockAsync(Convert.ToBase64String("block-01"u8), content)
                .ConfigureAwait(true);
            return null;
        }
        await container.GetBlobClient("target").UploadAsync(BinaryData.FromString("previous-blob")).ConfigureAwait(true);
        return await blobs.GetBlobAsync(SavaWebApplicationFactory.AccountName, container.Name, "target",
            null, null, false, CancellationToken.None).ConfigureAwait(true);
    }

    private static Task<BlobRecord> PutFromProducerAsync(
        IBlobApplication blobs, string container, Stream producer, BlobRecord? before, CancellationToken cancellationToken) =>
        blobs.PutBlockBlobAsync(SavaWebApplicationFactory.AccountName, container, "target", producer,
            new BlobWriteOptions(new BlobHttpProperties(), new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)),
            before?.Lease ?? LeaseRecord.Available, before?.GenerationId, before?.Revision, cancellationToken);

    private static Task<BlobEncryption> StageFromProducerAsync(
        IBlobApplication blobs, string container, Stream producer, CancellationToken cancellationToken) =>
        blobs.StageBlockAsync(SavaWebApplicationFactory.AccountName, container, "target", Convert.ToBase64String("block-01"u8),
            producer, new BlobEncryption(null, null), cancellationToken);

    private static async Task AssertTerminalDestinationAsync(
        IMetadataApplication metadata, IBlobApplication blobs, BlobContainerClient container,
        BlobRecord? before, bool staged, bool existing)
    {
        var current = await metadata.GetBlobAsync(SavaWebApplicationFactory.AccountName, container.Name, "target",
            null, null, false, CancellationToken.None).ConfigureAwait(true);
        if (staged)
        {
            Assert.Null(current);
            var blocks = await blobs.ListStagedBlocksAsync(SavaWebApplicationFactory.AccountName, container.Name,
                "target", CancellationToken.None).ConfigureAwait(true);
            if (existing)
            {
                Assert.Equal(Convert.ToBase64String("block-01"u8), Assert.Single(blocks).BlockId);
                await container.GetBlockBlobClient("target").CommitBlockListAsync([Convert.ToBase64String("block-01"u8)])
                    .ConfigureAwait(true);
                Assert.Equal("previous-stage", (await container.GetBlobClient("target").DownloadContentAsync()
                    .ConfigureAwait(true)).Value.Content.ToString());
            }
            else
                Assert.Empty(blocks);
        }
        else if (existing)
        {
            Assert.NotNull(current);
            Assert.NotNull(before);
            Assert.Equal(before.Revision, current.Revision);
            Assert.Equal(before.ETag, current.ETag);
            Assert.Equal("previous-blob", (await container.GetBlobClient("target").DownloadContentAsync()
                .ConfigureAwait(true)).Value.Content.ToString());
        }
        else
            Assert.Null(current);
    }

    private static async Task AssertRejectedGatewayAsync(SplitProcessHost host, string container, string privateValue)
    {
        await SplitProcessHost.AssertStatusAsync(host.GatewayAddress, "/health/live", HttpStatusCode.OK).ConfigureAwait(true);
        await SplitProcessHost.AssertStatusAsync(host.GatewayAddress, "/health/ready", HttpStatusCode.ServiceUnavailable).ConfigureAwait(true);
        var destination = host.Client.GetBlobContainerClient(container).GetBlobClient("rejected");
        AssertUnavailable(await Assert.ThrowsAsync<RequestFailedException>(() =>
            destination.UploadAsync(BinaryData.FromString("must-not-publish"))).ConfigureAwait(true));
        using var transport = new HttpClient();
        using var response = await transport.GetAsync(new Uri(host.GatewayAddress, "/health/ready")).ConfigureAwait(true);
        var body = await response.Content.ReadAsStringAsync().ConfigureAwait(true);
        Assert.False(body.Contains(privateValue, StringComparison.Ordinal));
        Assert.False(host.CapturedLogs.Contains(privateValue, StringComparison.Ordinal));
        Assert.False(Directory.Exists(host.GatewayUnusedDataPath));
    }

    private static async Task AssertUnchangedApplicationAsync(IMetadataApplication metadata, string container, BinaryData seed)
    {
        var preserved = await metadata.GetBlobAsync(SavaWebApplicationFactory.AccountName, container, "acknowledged",
            null, null, false, CancellationToken.None).ConfigureAwait(true);
        Assert.NotNull(preserved);
        Assert.Equal(seed.ToMemory().Length, preserved.Content.Length);
        Assert.Equal(Convert.ToHexStringLower(SHA256.HashData(seed.ToMemory().Span)), preserved.Content.Sha256);
        Assert.Null(await metadata.GetBlobAsync(SavaWebApplicationFactory.AccountName, container, "rejected",
            null, null, false, CancellationToken.None).ConfigureAwait(true));
    }

    private static async Task AssertBlockSnapshotLeaseAndSasCopyAsync(BlobContainerClient container)
    {
        var block = container.GetBlockBlobClient("blocks");
        var firstId = Convert.ToBase64String("block-01"u8);
        var secondId = Convert.ToBase64String("block-02"u8);
        using (var first = BinaryData.FromString("first-").ToStream())
            await block.StageBlockAsync(firstId, first).ConfigureAwait(true);
        using (var second = BinaryData.FromString("second").ToStream())
            await block.StageBlockAsync(secondId, second).ConfigureAwait(true);
        await block.CommitBlockListAsync([firstId, secondId]).ConfigureAwait(true);
        Assert.Equal("first-second", (await block.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        var snapshot = (await block.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;
        var lease = block.GetBlobLeaseClient();
        var acquired = await lease.AcquireAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(true);
        Assert.Equal(201, acquired.GetRawResponse().Status);
        var denied = await Assert.ThrowsAsync<RequestFailedException>(() => block.SetMetadataAsync(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["test"] = "lease-denied" })).ConfigureAwait(true);
        Assert.Equal(412, denied.Status);
        Assert.Equal(200, (await lease.ReleaseAsync().ConfigureAwait(true)).GetRawResponse().Status);
        Assert.Equal("first-second", (await block.WithSnapshot(snapshot).DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());

        var sasSource = block.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
        var copied = container.GetBlobClient("copied");
        var copy = await copied.StartCopyFromUriAsync(sasSource).ConfigureAwait(true);
        await copy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None).ConfigureAwait(true);
        var properties = (await copied.GetPropertiesAsync().ConfigureAwait(true)).Value;
        Assert.Equal(CopyStatus.Success, properties.CopyStatus);
        Assert.NotNull(properties.CopySource);
        Assert.False(properties.CopySource.Query.Contains("sig=", StringComparison.OrdinalIgnoreCase));
        Assert.Equal("first-second", (await copied.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    private static async Task AssertAppendAndPageAsync(BlobContainerClient container)
    {
        var append = container.GetAppendBlobClient("append");
        await append.CreateAsync().ConfigureAwait(true);
        using (var first = BinaryData.FromString("first-").ToStream())
            await append.AppendBlockAsync(first).ConfigureAwait(true);
        using (var second = BinaryData.FromString("second").ToStream())
            await append.AppendBlockAsync(second).ConfigureAwait(true);
        Assert.Equal("first-second", (await append.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        var page = container.GetPageBlobClient("page");
        await page.CreateAsync(2048).ConfigureAwait(true);
        var bytes = RandomNumberGenerator.GetBytes(512);
        using (var content = new MemoryStream(bytes, writable: false))
            await page.UploadPagesAsync(content, 512).ConfigureAwait(true);
        var expected = new byte[2048];
        bytes.CopyTo(expected, 512);
        Assert.Equal(expected, (await page.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());
        Assert.Equal(new HttpRange(512, 512), Assert.Single((await page.GetPageRangesAsync().ConfigureAwait(true)).Value.PageRanges));
    }

    private static async Task AssertQueryAsync(BlobContainerClient container)
    {
        var blob = container.GetBlockBlobClient("query.csv");
        using (var content = BinaryData.FromString("name,value\nfirst,1\nsecond,2\n").ToStream())
            await blob.UploadAsync(content).ConfigureAwait(true);
        var result = await blob.QueryAsync("SELECT name AS name FROM BlobStorage WHERE CAST(value AS INT) = 2;",
            new BlobQueryOptions
            {
                InputTextConfiguration = new BlobQueryCsvTextOptions { HasHeaders = true, RecordSeparator = "\n" },
                OutputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "\n" }
            }).ConfigureAwait(true);
        using var reader = new StreamReader(result.Value.Content);
        Assert.Equal("{\"name\":\"second\"}\n", await reader.ReadToEndAsync().ConfigureAwait(true));
    }

    private sealed class FaultAtEofStream(byte[] bytes, CancellationTokenSource? cancelAtEof = null) : Stream
    {
        private int _position;
        public int BytesProduced => _position;
        public int EofFaults { get; private set; }
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => bytes.Length;
        public override long Position { get => _position; set => throw new NotSupportedException(); }
        public override void Flush() => throw new NotSupportedException();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_position == bytes.Length)
            {
                EofFaults++;
                if (cancelAtEof is not null)
                {
                    await cancelAtEof.CancelAsync().ConfigureAwait(false);
                    throw new OperationCanceledException(cancelAtEof.Token);
                }
                throw new IOException("Injected producer EOF fault after the complete payload.");
            }
            var count = Math.Min(buffer.Length, bytes.Length - _position);
            bytes.AsMemory(_position, count).CopyTo(buffer);
            _position += count;
            return count;
        }
    }
}
