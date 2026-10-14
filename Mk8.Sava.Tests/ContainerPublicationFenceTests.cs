using System.Runtime.ExceptionServices;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class ContainerPublicationFenceTests
{
    [Theory]
    [InlineData("delete", false)]
    [InlineData("delete", true)]
    [InlineData("recreate", false)]
    [InlineData("recreate", true)]
    [InlineData("rename", false)]
    [InlineData("rename", true)]
    [InlineData("none", false)]
    [InlineData("none", true)]
    public async Task HeldContentCannotPublishIntoAReplacedOrMissingParent(string boundary, bool stageBlock)
    {
        var application = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);
        await using var applicationLifetime = application.ConfigureAwait(false);
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        var options = new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } };
        var owner = new BlobServiceClient(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey), options);
        var container = owner.GetBlobContainerClient("publication-parent");
        await container.CreateAsync().ConfigureAwait(true);
        var service = application.Services.GetRequiredService<BlobService>();
        var metadata = application.Services.GetRequiredService<MetadataStore>();
        var body = new HeldBody();
        var cancellation = new CancellationTokenSource();
        // Own the actual operation immediately. Cleanup always releases the model
        // and joins this task before the body, factory or dependencies retire.
        var operation = WriteAsync(service, body, stageBlock, cancellation.Token);
        Exception? observed = null;
        Exception? primary = null;
        Exception? cancelFailure = null;
        Exception? joinedFailure = null;
        try
        {
            var fixture = new VerificationFixture(container, service, metadata, body, operation);
            primary = await Record.ExceptionAsync(async () =>
                observed = await VerifyHeldWriteAsync(fixture, boundary, stageBlock).ConfigureAwait(false)).ConfigureAwait(true);
        }
        finally
        {
            body.Release();
            cancelFailure = await Record.ExceptionAsync(() => cancellation.CancelAsync()).ConfigureAwait(true);
            try
            {
                await operation.ConfigureAwait(true);
            }
            catch (Exception failure) when (!CatastrophicExceptionPolicy.Contains(failure))
            {
                joinedFailure = failure;
            }
            finally
            {
                try
                {
                    await body.DisposeAsync().ConfigureAwait(true);
                }
                finally
                {
                    cancellation.Dispose();
                }
            }
        }
        ThrowVerificationFailures(primary, cancelFailure, joinedFailure, observed);
    }

    private static void ThrowVerificationFailures(Exception? primary, Exception? cancelFailure,
        Exception? joinedFailure, Exception? observed)
    {
        var failures = new List<Exception>();
        if (primary is not null)
            failures.Add(primary);
        if (cancelFailure is not null)
            failures.Add(cancelFailure);
        if (joinedFailure is not null && !ReferenceEquals(joinedFailure, observed))
            failures.Add(joinedFailure);
        foreach (var failure in failures.Where(CatastrophicExceptionPolicy.Contains))
            ExceptionDispatchInfo.Throw(failure);
        if (failures.Count == 1)
            ExceptionDispatchInfo.Throw(failures[0]);
        if (failures.Count > 1)
            throw new AggregateException("Verification and independent owned-operation failures.", failures);
    }

    private sealed record VerificationFixture(BlobContainerClient Container, BlobService Service,
        MetadataStore Metadata, HeldBody Body, Task Operation);

    private static async Task<Exception?> VerifyHeldWriteAsync(VerificationFixture fixture, string boundary, bool stageBlock)
    {
        await fixture.Body.Entered.Task.WaitAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
        if (string.Equals(boundary, "rename", StringComparison.Ordinal))
        {
            await fixture.Service.RenameContainerAsync(SavaWebApplicationFactory.AccountName, fixture.Container.Name,
                "renamed-parent", sourceLeaseId: null, CancellationToken.None).ConfigureAwait(false);
            await fixture.Container.CreateAsync().ConfigureAwait(false);
        }
        else if (!string.Equals(boundary, "none", StringComparison.Ordinal))
        {
            await fixture.Container.DeleteAsync().ConfigureAwait(false);
            if (string.Equals(boundary, "recreate", StringComparison.Ordinal))
                await fixture.Container.CreateAsync().ConfigureAwait(false);
        }
        fixture.Body.Release();
        var observed = await Record.ExceptionAsync(() => fixture.Operation.WaitAsync(TimeSpan.FromSeconds(15))).ConfigureAwait(false);
        if (string.Equals(boundary, "none", StringComparison.Ordinal))
            Assert.Null(observed);
        else
            Assert.IsType<StorageConcurrencyException>(observed);
        await VerifyStoredStateAsync(fixture, boundary, stageBlock).ConfigureAwait(false);
        return observed;
    }

    private static async Task VerifyStoredStateAsync(VerificationFixture fixture, string boundary, bool stageBlock)
    {
        var blob = await fixture.Metadata.GetBlobAsync(SavaWebApplicationFactory.AccountName, fixture.Container.Name,
            "held.bin", versionId: null, snapshot: null, includeDeleted: true, CancellationToken.None).ConfigureAwait(false);
        var blocks = await fixture.Metadata.ListStagedBlocksAsync(SavaWebApplicationFactory.AccountName, fixture.Container.Name,
            "held.bin", CancellationToken.None).ConfigureAwait(false);
        if (string.Equals(boundary, "none", StringComparison.Ordinal))
        {
            if (stageBlock)
                Assert.Single(blocks);
            else
                Assert.NotNull(blob);
        }
        else
        {
            Assert.Null(blob);
            Assert.Empty(blocks);
        }
    }

    private static async Task WriteAsync(BlobService service, Stream body, bool stageBlock, CancellationToken cancellationToken)
    {
        if (stageBlock)
        {
            _ = await service.StageBlockAsync(SavaWebApplicationFactory.AccountName, "publication-parent", "held.bin",
                Convert.ToBase64String("held-block"u8), body,
                new BlobEncryption(Scope: null, CustomerProvidedKeySha256: null), cancellationToken).ConfigureAwait(false);
        }
        else
        {
            _ = await service.PutBlockBlobAsync(SavaWebApplicationFactory.AccountName, "publication-parent", "held.bin",
                body, new BlobWriteOptions(new BlobHttpProperties(), new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)),
                LeaseRecord.Available, expectedGeneration: null, expectedRevision: null,
                cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class HeldBody : Stream
    {
        private readonly MemoryStream _content = new("held parent publication"u8.ToArray(), writable: false);
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal void Release() => _released.TrySetResult();
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _content.Length;
        public override long Position { get => _content.Position; set => throw new NotSupportedException(); }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Entered.TrySetResult();
            await _released.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return await _content.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override void Flush() => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _content.Dispose();
            base.Dispose(disposing);
        }
    }
}
