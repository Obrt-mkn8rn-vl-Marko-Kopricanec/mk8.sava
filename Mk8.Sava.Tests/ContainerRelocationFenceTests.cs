using System.Runtime.ExceptionServices;
using System.Text.Json;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class ContainerRelocationFenceTests
{
    private const string SourceName = "relocation-source";
    private const string DestinationName = "relocation-destination";
    private const string BlobName = "retained.bin";

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task HeldAppendOrPageCannotWriteBackTheOldContainerAfterRelocation(bool restore, bool page)
    {
        var factory = CreateFactory();
        await using var lifetime = factory.ConfigureAwait(false);
        using var transport = new HttpClientTransport(factory.Server.CreateHandler());
        var owner = CreateOwner(transport);
        var source = owner.GetBlobContainerClient(SourceName);
        await source.CreateAsync().ConfigureAwait(true);
        if (page)
            await source.GetPageBlobClient(BlobName).CreateAsync(512).ConfigureAwait(true);
        else
            await source.GetAppendBlobClient(BlobName).CreateAsync().ConfigureAwait(true);
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var service = factory.Services.GetRequiredService<BlobService>();
        var captured = await GetRecordAsync(metadata, SourceName).ConfigureAwait(true);
        var body = new HeldBody();
        await using var bodyLifetime = body.ConfigureAwait(false);
        using var cancellation = new CancellationTokenSource();
#pragma warning disable CA2025 // This test owns the actual operation: finally releases the gate, cancels, and joins it before body/token/factory disposal; interprocedural Record.ExceptionAsync hides the join.
        var operation = MutateAsync(service, captured, body, page, cancellation.Token);
#pragma warning restore CA2025
        Exception? primary = null;
        Exception? observed = null;
        Exception? canceled = null;
        Exception? joined = null;
        try
        {
            primary = await Record.ExceptionAsync(async () =>
            {
                await body.Entered.Task.WaitAsync(TimeSpan.FromSeconds(15), CancellationToken.None).ConfigureAwait(true);
                await RelocateAsync(service, metadata, restore).ConfigureAwait(true);
                var moved = await GetRecordAsync(metadata, DestinationName).ConfigureAwait(true);
                await source.CreateAsync(cancellationToken: CancellationToken.None).ConfigureAwait(true);
                body.Release();
                observed = await Record.ExceptionAsync(() => operation.WaitAsync(TimeSpan.FromSeconds(15), CancellationToken.None)).ConfigureAwait(true);
                Assert.IsType<StorageConcurrencyException>(observed);
                Assert.Equal(JsonSerializer.Serialize(moved), JsonSerializer.Serialize(
                    await GetRecordAsync(metadata, DestinationName).ConfigureAwait(true)));
                Assert.False((await source.GetBlobClient(BlobName).ExistsAsync(CancellationToken.None).ConfigureAwait(true)).Value);
                var bytes = (await owner.GetBlobContainerClient(DestinationName).GetBlobClient(BlobName)
                    .DownloadContentAsync(cancellationToken: CancellationToken.None).ConfigureAwait(true)).Value.Content.ToArray();
                Assert.Equal(page ? new byte[512] : [], bytes);
            }).ConfigureAwait(true);
        }
        finally
        {
            body.Release();
            canceled = await Record.ExceptionAsync(() => cancellation.CancelAsync()).ConfigureAwait(true);
#pragma warning disable VSTHRD003 // Join this test's actual context-free mutation after release, including assertion failure, before disposing any dependency.
            joined = await Record.ExceptionAsync(() => operation).ConfigureAwait(true);
#pragma warning restore VSTHRD003
        }
        ThrowFailures(primary, canceled, ReferenceEquals(joined, observed) ? null : joined);
    }

    [Theory]
    [InlineData(false, "put")]
    [InlineData(false, "replace")]
    [InlineData(false, "delete")]
    [InlineData(true, "put")]
    [InlineData(true, "replace")]
    [InlineData(true, "delete")]
    public async Task CapturedMutationOrDeletionCannotUndoRelocation(bool restore, string operation)
    {
        var factory = CreateFactory();
        await using var lifetime = factory.ConfigureAwait(false);
        using var transport = new HttpClientTransport(factory.Server.CreateHandler());
        var owner = CreateOwner(transport);
        var source = owner.GetBlobContainerClient(SourceName);
        await source.CreateAsync().ConfigureAwait(true);
        await source.GetBlobClient(BlobName).UploadAsync(BinaryData.FromString("preserved relocation bytes")).ConfigureAwait(true);
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var service = factory.Services.GetRequiredService<BlobService>();
        var captured = await GetRecordAsync(metadata, SourceName).ConfigureAwait(true);
        await RelocateAsync(service, metadata, restore).ConfigureAwait(true);
        var moved = await GetRecordAsync(metadata, DestinationName).ConfigureAwait(true);
        await source.CreateAsync().ConfigureAwait(true);
        var replacement = captured with { Revision = MetadataStore.NewRevision(), ETag = MetadataStore.NewETag() };
        if (string.Equals(operation, "put", StringComparison.Ordinal))
        {
            await Assert.ThrowsAsync<StorageConcurrencyException>(() => metadata.PutBlobRecordAsync(
                replacement, captured.Revision, CancellationToken.None)).ConfigureAwait(true);
        }
        else
        {
            await Assert.ThrowsAsync<StorageConcurrencyException>(() => metadata.ApplyBlobRecordMutationsAsync(
                [new BlobRecordMutation(captured.GenerationId, captured.Revision,
                    string.Equals(operation, "delete", StringComparison.Ordinal) ? null : replacement)],
                CancellationToken.None)).ConfigureAwait(true);
        }
        Assert.Equal(JsonSerializer.Serialize(moved), JsonSerializer.Serialize(
            await GetRecordAsync(metadata, DestinationName).ConfigureAwait(true)));
        Assert.False((await source.GetBlobClient(BlobName).ExistsAsync().ConfigureAwait(true)).Value);
        var destination = owner.GetBlobContainerClient(DestinationName).GetBlobClient(BlobName);
        Assert.Equal("preserved relocation bytes", (await destination.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        // Freshly acquired revisions still authorize ordinary destination mutation.
        await destination.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["fresh"] = "accepted" }).ConfigureAwait(true);
        Assert.Equal("accepted", (await destination.GetPropertiesAsync().ConfigureAwait(true)).Value.Metadata["fresh"]);
    }

    private static SavaWebApplicationFactory CreateFactory() =>
        new(TimeProvider.System, new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);

    private static BlobServiceClient CreateOwner(HttpClientTransport transport) =>
        new(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });

    private static async Task<BlobRecord> GetRecordAsync(MetadataStore metadata, string container) =>
        Assert.IsType<BlobRecord>(await metadata.GetBlobAsync(SavaWebApplicationFactory.AccountName,
            container, BlobName, versionId: null, snapshot: null, includeDeleted: false, CancellationToken.None).ConfigureAwait(false));

    private static async Task RelocateAsync(BlobService service, MetadataStore metadata, bool restore)
    {
        if (restore)
        {
            var properties = await metadata.GetServicePropertiesAsync(SavaWebApplicationFactory.AccountName,
                CancellationToken.None).ConfigureAwait(false);
            await metadata.PutServicePropertiesAsync(SavaWebApplicationFactory.AccountName, properties with
            {
                ContainerSoftDeleteEnabled = true,
                ContainerSoftDeleteRetentionDays = 7,
            }, CancellationToken.None).ConfigureAwait(false);
            var current = await service.GetContainerAsync(SavaWebApplicationFactory.AccountName, SourceName,
                includeDeleted: false, CancellationToken.None).ConfigureAwait(false);
            await service.DeleteContainerAsync(current, CancellationToken.None).ConfigureAwait(false);
            var deleted = Assert.IsType<ContainerRecord>(await metadata.GetContainerAsync(SavaWebApplicationFactory.AccountName,
                SourceName, includeDeleted: true, CancellationToken.None).ConfigureAwait(false));
            Assert.NotNull(deleted.DeletedVersion);
            _ = await service.RestoreContainerAsync(SavaWebApplicationFactory.AccountName, SourceName,
                DestinationName, deleted.DeletedVersion, CancellationToken.None).ConfigureAwait(false);
        }
        else
        {
            _ = await service.RenameContainerAsync(SavaWebApplicationFactory.AccountName, SourceName,
                DestinationName, sourceLeaseId: null, CancellationToken.None).ConfigureAwait(false);
        }
    }

    private static async Task MutateAsync(BlobService service, BlobRecord captured, Stream body, bool page, CancellationToken cancellationToken)
    {
        var encryption = new BlobEncryption(Scope: null, CustomerProvidedKeySha256: null);
        if (page)
        {
            _ = await service.PutPageAsync(captured, 0, 511, body, clear: false, encryption, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            _ = await service.AppendBlockAsync(captured, body, expectedPosition: null, expectedMaximumSize: null,
                encryption, cancellationToken).ConfigureAwait(false);
        }
    }

    private static void ThrowFailures(params Exception?[] failures)
    {
        var retained = failures.OfType<Exception>().ToArray();
        foreach (var failure in retained.Where(CatastrophicExceptionPolicy.Contains))
            ExceptionDispatchInfo.Throw(failure);
        if (retained.Length == 1)
            ExceptionDispatchInfo.Throw(retained[0]);
        if (retained.Length > 1)
            throw new AggregateException("Verification and independent owned-operation failures.", retained);
    }

    private sealed class HeldBody() : MemoryStream(new byte[512], writable: false)
    {
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal void Release() => _released.TrySetResult();
        public override bool CanSeek => false;

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Entered.TrySetResult();
            await _released.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return await base.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }
}
