using System.Runtime.ExceptionServices;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class BackupPinOrderingTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BackupWaitReleasesMetadataGateAndCanCancelWithoutCancelingTheMutation(bool cancel)
    {
        var application = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);
        await using var applicationLifetime = application.ConfigureAwait(false);
        var (metadata, chunks, id) = await CreateChunkFixtureAsync(application).ConfigureAwait(true);
        using var mutation = Assert.IsType<ChunkStore.ChunkMutationReservation>(chunks.TryReserveForMutation(id));
        using var cancellation = new CancellationTokenSource();
        var waiting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var path = Path.Combine(Path.GetTempPath(), $"mk8-sava-backup-order-{Guid.NewGuid():N}.db");
        var backup = Task.Run(() => metadata.CreateBackupSnapshotAsync(path, ids =>
        {
            var attempt = chunks.TryPinChunkIdsForBackup(ids);
            if (attempt.PendingMutation is not null)
                waiting.TrySetResult();
            return attempt;
        }, cancellation.Token), CancellationToken.None);
        var fixture = new WaitFixture(application, metadata, mutation, cancellation, waiting, backup, path, id);
        Exception? primary;
        Exception? cancelFailure;
        Exception? backupFailure;
        Exception? writeFailure;
        Exception? fileCleanupFailure;
        try
        {
            primary = await Record.ExceptionAsync(() => VerifyAsync(fixture, cancel)).ConfigureAwait(true);
        }
        finally
        {
            mutation.Dispose();
            cancelFailure = await Record.ExceptionAsync(() => cancellation.CancelAsync()).ConfigureAwait(true);
            backupFailure = null;
            writeFailure = null;
            try
            {
                (await backup.ConfigureAwait(true)).Dispose();
            }
            catch (Exception failure) when (!CatastrophicExceptionPolicy.Contains(failure))
            {
                backupFailure = failure;
            }
            finally
            {
                try
                {
#pragma warning disable VSTHRD003 // This test owns the metadata write started by its context-free VerifyAsync helper; join the actual task before fixture retirement.
                    await fixture.Write.ConfigureAwait(true);
#pragma warning restore VSTHRD003
                }
                catch (Exception failure) when (!CatastrophicExceptionPolicy.Contains(failure))
                {
                    writeFailure = failure;
                }
                finally
                {
                    fileCleanupFailure = Record.Exception(() => File.Delete(path));
                }
            }
        }
        ThrowFailures(primary, cancelFailure, backupFailure, writeFailure, fileCleanupFailure, cancellation.Token);
    }

    private static async Task<(MetadataStore Metadata, ChunkStore Chunks, string ChunkId)> CreateChunkFixtureAsync(
        SavaWebApplicationFactory application)
    {
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        var owner = new BlobServiceClient(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });
        var container = owner.GetBlobContainerClient("backup-pin-order");
        await container.CreateAsync().ConfigureAwait(false);
        await container.GetBlobClient("source.bin").UploadAsync(BinaryData.FromString("backup pin reservation")).ConfigureAwait(false);
        var metadata = application.Services.GetRequiredService<MetadataStore>();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var inventory = await metadata.GetStorageInventoryAsync(CancellationToken.None).ConfigureAwait(false);
        return (metadata, chunks, Assert.Single(inventory.ReachableChunkIds));
    }

    private sealed record WaitFixture(SavaWebApplicationFactory Application, MetadataStore Metadata, ChunkStore.ChunkMutationReservation Mutation,
        CancellationTokenSource Cancellation, TaskCompletionSource Waiting, Task<MetadataBackupSnapshot> Backup,
        string Path, string ChunkId)
    {
        internal Task Write { get; set; } = Task.CompletedTask;
    }

    private static async Task VerifyAsync(WaitFixture fixture, bool cancel)
    {
        await fixture.Waiting.Task.WaitAsync(TimeSpan.FromSeconds(15), CancellationToken.None).ConfigureAwait(false);
        fixture.Write = fixture.Metadata.PutServicePropertiesAsync(SavaWebApplicationFactory.AccountName,
            new ServiceProperties { VersioningEnabled = true }, fixture.Cancellation.Token);
        await fixture.Write.WaitAsync(TimeSpan.FromSeconds(15), CancellationToken.None).ConfigureAwait(false);
        Assert.False(fixture.Backup.IsCompleted);
        if (cancel)
        {
            await fixture.Cancellation.CancelAsync().ConfigureAwait(false);
            var failure = await Record.ExceptionAsync(() => fixture.Backup.WaitAsync(TimeSpan.FromSeconds(15), CancellationToken.None)).ConfigureAwait(false);
            Assert.IsType<OperationCanceledException>(failure, exactMatch: false);
            // Cancellation belongs to this backup wait, not the borrowed mutation.
            Assert.False(fixture.Mutation.Completion.IsCompleted);
            Assert.False(File.Exists(fixture.Path));
        }
        else
        {
            fixture.Write = PublishIndependentBlobAsync(fixture.Application, fixture.Cancellation.Token);
            await fixture.Write.WaitAsync(TimeSpan.FromSeconds(15), CancellationToken.None).ConfigureAwait(false);
            fixture.Mutation.Dispose();
            using var snapshot = await fixture.Backup.WaitAsync(TimeSpan.FromSeconds(15), CancellationToken.None).ConfigureAwait(false);
            Assert.Contains(fixture.ChunkId, snapshot.Inventory.ReachableChunkIds);
            Assert.Equal(2, snapshot.Inventory.ReachableChunkIds.Count);
            var inspection = await MetadataStore.InspectDatabaseAsync(fixture.Path, CancellationToken.None).ConfigureAwait(false);
            Assert.True(inspection.Inventory.ReachableChunkIds.SetEquals(snapshot.Inventory.ReachableChunkIds));
        }
    }

    private static async Task PublishIndependentBlobAsync(SavaWebApplicationFactory application, CancellationToken cancellationToken)
    {
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        var owner = new BlobServiceClient(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });
        await owner.GetBlobContainerClient("backup-pin-order").GetBlobClient("later.bin")
            .UploadAsync(BinaryData.FromString("independently published while backup waits"), cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    private static void ThrowFailures(Exception? primary, Exception? cancelFailure, Exception? backupFailure,
        Exception? writeFailure, Exception? fileCleanupFailure, CancellationToken cleanupToken)
    {
        var failures = new List<Exception>();
        foreach (var failure in new[] { primary, cancelFailure, backupFailure, writeFailure, fileCleanupFailure })
        {
            if (failure is not null &&
                !(ReferenceEquals(failure, backupFailure) && failure is OperationCanceledException canceled &&
                  canceled.CancellationToken == cleanupToken && cleanupToken.IsCancellationRequested))
            {
                failures.Add(failure);
            }
        }
        foreach (var failure in failures.Where(CatastrophicExceptionPolicy.Contains))
            ExceptionDispatchInfo.Throw(failure);
        if (failures.Count == 1)
            ExceptionDispatchInfo.Throw(failures[0]);
        if (failures.Count > 1)
            throw new AggregateException("Backup verification and independent owned-work failures.", failures);
    }
}
