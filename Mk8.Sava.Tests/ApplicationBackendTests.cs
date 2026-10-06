using System.Reflection;
using System.Security.Cryptography;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Mk8.Sava.Application;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Tests;

public sealed class ApplicationBackendTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task DispatcherRejectsStaleBlobIdentity(bool generation) =>
        await AssertStaleIdentityAsync(generation).ConfigureAwait(true);

    [Fact]
    public async Task DispatcherUsesAuthoritativeBlobContentAndProperties() =>
        await AssertAuthoritativeBlobAsync().ConfigureAwait(true);

    [Fact]
    public async Task DispatcherUsesAuthoritativeContainerProperties() =>
        await AssertAuthoritativeContainerAsync().ConfigureAwait(true);

    [Fact]
    public async Task LeaseCommandsUseTheApplicationClockAndRealLeaseState() =>
        await AssertApplicationLeasesAsync().ConfigureAwait(true);

    [Fact]
    public async Task AReadSessionPinsDeletedContentUntilItsLastActiveRangeCompletes() =>
        await AssertActiveReadPinAsync(stop: false).ConfigureAwait(true);

    [Fact]
    public async Task ShutdownDrainsActiveReadSessionsBeforeReleasingTheirPins() =>
        await AssertActiveReadPinAsync(stop: true).ConfigureAwait(true);

    [Fact]
    public async Task ReadSessionsRejectOtherPeersAndUnauthenticatedCallers() =>
        await AssertPeerBindingAsync().ConfigureAwait(true);

    [Fact]
    public async Task ReadSessionsBoundReservationsBeforeWaitingForAdmission() =>
        await AssertSessionBoundsAsync().ConfigureAwait(true);

    [Fact]
    public async Task CanceledQueuedOpenReleasesItsReservationAndStoragePermit() =>
        await AssertCanceledOpenAsync().ConfigureAwait(true);

    [Fact]
    public async Task AnIdleExpiredSessionReleasesItsPinAndCannotRead() =>
        await AssertIdleExpirationAsync().ConfigureAwait(true);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TimelyRenewalsRetainTheQueryPermitAndPinUntilCloseOrExpiration(bool expire) =>
        await AssertQueryRenewalAsync(expire).ConfigureAwait(true);

    [Fact]
    public async Task ADataAccessCasRetryAcceptsIdenticalReloadedChunkReferences() =>
        await AssertDataAccessRetryContentAsync().ConfigureAwait(true);

    [Fact]
    public async Task RpcAdmissionIsGlobalAcrossPeersAndKeepsReadinessAndControlIndependent() =>
        await AssertGlobalRpcAdmissionAsync().ConfigureAwait(true);

    [Fact]
    public async Task RpcAdmissionBoundsQueuedRequestsAndCancellationReleasesReservations() =>
        await AssertRpcQueueCancellationAsync().ConfigureAwait(true);

    [Fact]
    public async Task RpcAdmissionQueueTimeoutIsFiniteAndRecorded() =>
        await AssertRpcQueueTimeoutAsync().ConfigureAwait(true);

    [Fact]
    public async Task TheReservedControlLaneHasFourActiveAndThirtyTwoQueuedSlots() =>
        await AssertControlAdmissionBoundsAsync().ConfigureAwait(true);

    [Theory]
    [InlineData("MaximumReadSessions", "0")]
    [InlineData("ReadSessionIdleTimeout", "00:00:00")]
    [InlineData("ReadSessionIdleTimeout", "00:00:00.5000000")]
    [InlineData("ReadSessionIdleTimeout", "1.00:00:00")]
    [InlineData("MaximumConcurrentRpcRequests", "0")]
    [InlineData("MaximumConcurrentRpcRequests", "257")]
    [InlineData("MaximumQueuedRpcRequests", "-1")]
    [InlineData("MaximumQueuedRpcRequests", "4097")]
    [InlineData("RpcQueueTimeout", "00:00:00")]
    [InlineData("RpcQueueTimeout", "00:00:31")]
    public async Task InvalidReadSessionConfigurationRejectsStartup(string name, string value) =>
        await AssertInvalidSessionOptionsAsync(name, value).ConfigureAwait(true);

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task ReadinessReportsOrdinaryFailuresButPropagatesFatalGraphs(bool fatal, bool wrapped) =>
        await AssertReadinessFailureAsync(fatal, wrapped).ConfigureAwait(true);

    private static async Task AssertStaleIdentityAsync(bool generation)
    {
        var factory = CreateFactory();
        await using var disposal = factory.ConfigureAwait(false);
        var (record, _) = await CreateBlobAsync(factory).ConfigureAwait(false);
        var supplied = generation ? record with { GenerationId = "stale-generation" } : record with { Revision = "stale-revision" };
        await Assert.ThrowsAsync<StorageConcurrencyException>(() => InvokeBlobAsync<BlobRecord>(factory,
            nameof(IBlobApplication.SetBlobMetadataAsync), [supplied, Metadata("changed", "yes"), CancellationToken.None]))
            .ConfigureAwait(false);
        var actual = await factory.Services.GetRequiredService<BlobService>().GetBlobAsync(
            record.Account, record.Container, record.Name, null, null, false, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(record.Revision, actual.Revision);
        Assert.Empty(actual.Metadata);
    }

    private static async Task AssertAuthoritativeBlobAsync()
    {
        var factory = CreateFactory();
        await using var disposal = factory.ConfigureAwait(false);
        var (record, bytes) = await CreateBlobAsync(factory).ConfigureAwait(false);
        var supplied = record with
        {
            Content = ContentManifest.Empty("forged-domain"),
            Kind = BlobKind.PageBlob,
            HasLegalHold = true,
            Owner = "forged-owner",
            Metadata = Metadata("forged", "yes"),
            Lease = new LeaseRecord { State = LeaseState.Leased, Id = Guid.NewGuid().ToString("D") }
        };
        var updated = await InvokeBlobAsync<BlobRecord>(factory, nameof(IBlobApplication.SetBlobMetadataAsync),
            [supplied, Metadata("actual", "yes"), CancellationToken.None]).ConfigureAwait(false);
        Assert.Equal(record.Content.Domain, updated.Content.Domain);
        Assert.Equal(record.Content.Length, updated.Content.Length);
        Assert.Equal(record.Content.Sha256, updated.Content.Sha256);
        Assert.Equal(BlobKind.BlockBlob, updated.Kind);
        Assert.False(updated.HasLegalHold);
        Assert.Equal(record.Owner, updated.Owner);
        Assert.Equal(LeaseState.Available, updated.Lease.State);
        Assert.Equal("yes", updated.Metadata["actual"]);
        Assert.False(updated.Metadata.ContainsKey("forged"));
        using var output = new MemoryStream();
        await InvokeBlobAsync<object?>(factory, nameof(IBlobApplication.WriteContentAsync),
            [supplied with { Revision = updated.Revision }, new BlobEncryption(null, null), 0L,
                (long)bytes.Length, output, CancellationToken.None]).ConfigureAwait(false);
        Assert.Equal(bytes, output.ToArray());
    }

    private static async Task AssertAuthoritativeContainerAsync()
    {
        var factory = CreateFactory();
        await using var disposal = factory.ConfigureAwait(false);
        var (record, _) = await CreateBlobAsync(factory).ConfigureAwait(false);
        var service = factory.Services.GetRequiredService<BlobService>();
        var container = await service.GetContainerAsync(record.Account, record.Container, false, CancellationToken.None)
            .ConfigureAwait(false);
        var supplied = container with { Owner = "forged-owner", PublicAccess = "container", ImmutableStorageWithVersioningEnabled = true };
        var updated = await InvokeBlobAsync<ContainerRecord>(factory, nameof(IBlobApplication.SetContainerMetadataAsync),
            [supplied, Metadata("actual", "yes"), CancellationToken.None]).ConfigureAwait(false);
        Assert.Equal(container.Owner, updated.Owner);
        Assert.Null(updated.PublicAccess);
        Assert.False(updated.ImmutableStorageWithVersioningEnabled);
        var stale = container with { Revision = "stale-revision" };
        await Assert.ThrowsAsync<StorageConcurrencyException>(() => InvokeBlobAsync<ContainerRecord>(factory,
            nameof(IBlobApplication.SetContainerMetadataAsync), [stale, Metadata("unsafe", "yes"), CancellationToken.None]))
            .ConfigureAwait(false);
        Assert.Equal("yes", updated.Metadata["actual"]);
    }

    private static async Task AssertApplicationLeasesAsync()
    {
        var time = new AdjustableClock(new DateTimeOffset(2026, 10, 6, 10, 0, 0, TimeSpan.Zero));
        var factory = CreateFactory(time);
        await using var disposal = factory.ConfigureAwait(false);
        var (record, _) = await CreateBlobAsync(factory).ConfigureAwait(false);
        var acquired = await InvokeBlobAsync<BlobLeaseUpdate>(factory, nameof(IBlobApplication.ApplyBlobLeaseAsync),
            [record, LeaseAction.Acquire, 60, null, null, null, false, CancellationToken.None]).ConfigureAwait(false);
        Assert.Equal(time.GetUtcNow(), acquired.Record.Lease.AcquiredAt);
        Assert.Equal(time.GetUtcNow().AddSeconds(60), acquired.Record.Lease.ExpiresAt);
        var id = acquired.Record.Lease.Id;
        var forged = acquired.Record with { Lease = LeaseRecord.Available };
        await Assert.ThrowsAsync<AzureStorageException>(() => InvokeBlobAsync<BlobLeaseUpdate>(factory,
            nameof(IBlobApplication.ApplyBlobLeaseAsync),
            [forged, LeaseAction.Acquire, 60, null, null, null, false, CancellationToken.None])).ConfigureAwait(false);
        time.Advance(TimeSpan.FromSeconds(61));
        var renewed = await InvokeBlobAsync<BlobLeaseUpdate>(factory, nameof(IBlobApplication.ApplyBlobLeaseAsync),
            [forged, LeaseAction.Renew, null, null, id, null, false, CancellationToken.None]).ConfigureAwait(false);
        Assert.Equal(time.GetUtcNow(), renewed.Record.Lease.AcquiredAt);
        Assert.Equal(time.GetUtcNow().AddSeconds(60), renewed.Record.Lease.ExpiresAt);
        var container = await factory.Services.GetRequiredService<BlobService>().GetContainerAsync(
            record.Account, record.Container, false, CancellationToken.None).ConfigureAwait(false);
        var leased = await InvokeBlobAsync<ContainerLeaseUpdate>(factory, nameof(IBlobApplication.ApplyContainerLeaseAsync),
            [container, LeaseAction.Acquire, 60, null, null, null, false, false, CancellationToken.None]).ConfigureAwait(false);
        Assert.Equal(time.GetUtcNow(), leased.Record.Lease.AcquiredAt);
        Assert.Equal(container.ETag, leased.Record.ETag);
        time.Advance(TimeSpan.FromSeconds(5));
        var changed = await InvokeBlobAsync<ContainerLeaseUpdate>(factory, nameof(IBlobApplication.ApplyContainerLeaseAsync),
            [leased.Record, LeaseAction.Renew, null, null, leased.Record.Lease.Id, null, false, true, CancellationToken.None])
            .ConfigureAwait(false);
        Assert.Equal(time.GetUtcNow(), changed.Record.LastModified);
        Assert.NotEqual(leased.Record.ETag, changed.Record.ETag, StringComparer.Ordinal);
    }

    private static async Task AssertActiveReadPinAsync(bool stop)
    {
        var factory = CreateFactory();
        await using var disposal = factory.ConfigureAwait(false);
        var (record, bytes) = await CreateBlobAsync(factory).ConfigureAwait(false);
        var sessions = factory.Services.GetRequiredService<ApplicationReadSessions>();
        var service = factory.Services.GetRequiredService<BlobService>();
        using var peer = ApplicationRpcIdentity.Enter("backend-test-peer");
        var session = await sessions.OpenAsync(record, query: false, CancellationToken.None).ConfigureAwait(false);
        var output = new ControlledOutput();
        await using var outputDisposal = output.ConfigureAwait(false);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
#pragma warning disable CA2025 // Finally releases and joins the active range before disposing its destination or cancellation source.
        var reading = sessions.WriteRangeAsync(session.Token, new BlobEncryption(null, null), 0, bytes.Length, output, timeout.Token);
#pragma warning restore CA2025
        Task? stopping = null;
        try
        {
            await output.Entered.WaitAsync(timeout.Token).ConfigureAwait(false);
            await service.DeleteBlobAsync(record, false, BlobDeleteSnapshotsOption.Unspecified, timeout.Token).ConfigureAwait(false);
            if (stop)
            {
                stopping = sessions.StopAsync(timeout.Token);
                Assert.False(stopping.IsCompleted);
            }
            else
                await sessions.CloseAsync(session.Token, timeout.Token).ConfigureAwait(false);
            Assert.Equal(0, await service.CollectGarbageAsync(timeout.Token).ConfigureAwait(false));
            await Assert.ThrowsAsync<AzureStorageException>(() => sessions.WriteRangeAsync(
                session.Token, new BlobEncryption(null, null), 0, 1, Stream.Null, timeout.Token)).ConfigureAwait(false);
            output.Release();
            await reading.ConfigureAwait(false);
            if (stopping is not null)
                await stopping.ConfigureAwait(false);
            Assert.Equal(bytes, output.ToArray());
            Assert.Equal(record.Content.Chunks.Count, await service.CollectGarbageAsync(timeout.Token).ConfigureAwait(false));
        }
        finally
        {
            output.Release();
            await timeout.CancelAsync().ConfigureAwait(false);
            try
            {
                await reading.ConfigureAwait(false);
                if (stopping is not null)
                    await stopping.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    private static async Task AssertPeerBindingAsync()
    {
        var factory = CreateFactory();
        await using var disposal = factory.ConfigureAwait(false);
        var (record, bytes) = await CreateBlobAsync(factory).ConfigureAwait(false);
        var sessions = factory.Services.GetRequiredService<ApplicationReadSessions>();
        await Assert.ThrowsAsync<AzureStorageException>(() => sessions.OpenAsync(record, false, CancellationToken.None)).ConfigureAwait(false);
        var unauthenticatedTouch = await Assert.ThrowsAsync<AzureStorageException>(() =>
            sessions.TouchAsync("unknown-session", CancellationToken.None)).ConfigureAwait(false);
        Assert.Equal(403, unauthenticatedTouch.StatusCode);
        using var owner = ApplicationRpcIdentity.Enter("owner-peer");
        var session = await sessions.OpenAsync(record, false, CancellationToken.None).ConfigureAwait(false);
        using (ApplicationRpcIdentity.Enter("different-peer"))
        {
            var read = await Assert.ThrowsAsync<AzureStorageException>(() => sessions.WriteRangeAsync(
                session.Token, new BlobEncryption(null, null), 0, bytes.Length, Stream.Null, CancellationToken.None))
                .ConfigureAwait(false);
            Assert.Equal(403, read.StatusCode);
            var close = await Assert.ThrowsAsync<AzureStorageException>(() => sessions.CloseAsync(session.Token, CancellationToken.None))
                .ConfigureAwait(false);
            Assert.Equal(403, close.StatusCode);
            var touch = await Assert.ThrowsAsync<AzureStorageException>(() => sessions.TouchAsync(session.Token, CancellationToken.None))
                .ConfigureAwait(false);
            Assert.Equal(403, touch.StatusCode);
        }
        await sessions.TouchAsync(session.Token, CancellationToken.None).ConfigureAwait(false);
        using var output = new MemoryStream();
        await sessions.WriteRangeAsync(session.Token, new BlobEncryption(null, null), 0, bytes.Length, output, CancellationToken.None)
            .ConfigureAwait(false);
        Assert.Equal(bytes, output.ToArray());
        await sessions.CloseAsync(session.Token, CancellationToken.None).ConfigureAwait(false);
    }

    private static async Task AssertSessionBoundsAsync()
    {
        var factory = CreateFactory(overrides: new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["ApplicationHosting:MaximumReadSessions"] = "1"
        });
        await using var disposal = factory.ConfigureAwait(false);
        var (record, _) = await CreateBlobAsync(factory).ConfigureAwait(false);
        var sessions = factory.Services.GetRequiredService<ApplicationReadSessions>();
        using var peer = ApplicationRpcIdentity.Enter("backend-test-peer");
        var first = await sessions.OpenAsync(record, false, CancellationToken.None).ConfigureAwait(false);
        var rejected = await Assert.ThrowsAsync<AzureStorageException>(() => sessions.OpenAsync(record, true, CancellationToken.None))
            .ConfigureAwait(false);
        Assert.Equal("ServerBusy", rejected.ErrorCode);
        await sessions.CloseAsync(first.Token, CancellationToken.None).ConfigureAwait(false);
        var retry = await sessions.OpenAsync(record, true, CancellationToken.None).ConfigureAwait(false);
        await sessions.CloseAsync(retry.Token, CancellationToken.None).ConfigureAwait(false);
    }

    private static async Task AssertCanceledOpenAsync()
    {
        var factory = CreateFactory(overrides: new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["ApplicationHosting:MaximumReadSessions"] = "2",
            ["Sava:MaximumConcurrentStorageReads"] = "1"
        });
        await using var disposal = factory.ConfigureAwait(false);
        var (record, _) = await CreateBlobAsync(factory).ConfigureAwait(false);
        var sessions = factory.Services.GetRequiredService<ApplicationReadSessions>();
        using var peer = ApplicationRpcIdentity.Enter("backend-test-peer");
        var first = await sessions.OpenAsync(record, false, CancellationToken.None).ConfigureAwait(false);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
#pragma warning disable CA2025 // The queued open is canceled and awaited before its cancellation source is disposed.
        var queued = sessions.OpenAsync(record, false, cancellation.Token);
#pragma warning restore CA2025
        try
        {
            Assert.False(queued.IsCompleted);
            var bounded = await Assert.ThrowsAsync<AzureStorageException>(() => sessions.OpenAsync(
                record, true, CancellationToken.None)).ConfigureAwait(false);
            Assert.Equal("ServerBusy", bounded.ErrorCode);
            await cancellation.CancelAsync().ConfigureAwait(false);
            try
            {
                _ = await queued.ConfigureAwait(false);
                Assert.Fail("A canceled queued read-session open completed successfully.");
            }
            catch (OperationCanceledException)
            {
            }
        }
        finally
        {
            await cancellation.CancelAsync().ConfigureAwait(false);
            try
            {
                _ = await queued.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            await sessions.CloseAsync(first.Token, CancellationToken.None).ConfigureAwait(false);
        }
        var retry = await sessions.OpenAsync(record, false, CancellationToken.None).ConfigureAwait(false);
        await sessions.CloseAsync(retry.Token, CancellationToken.None).ConfigureAwait(false);
    }

    private static async Task AssertIdleExpirationAsync()
    {
        var time = new AdjustableClock(DateTimeOffset.UtcNow);
        var factory = CreateFactory(time);
        await using var disposal = factory.ConfigureAwait(false);
        var (record, _) = await CreateBlobAsync(factory).ConfigureAwait(false);
        var sessions = factory.Services.GetRequiredService<ApplicationReadSessions>();
        using var peer = ApplicationRpcIdentity.Enter("backend-test-peer");
        var session = await sessions.OpenAsync(record, false, CancellationToken.None).ConfigureAwait(false);
        time.Advance(TimeSpan.FromMinutes(3));
        await Assert.ThrowsAsync<AzureStorageException>(() => sessions.WriteRangeAsync(
            session.Token, new BlobEncryption(null, null), 0, 1, Stream.Null, CancellationToken.None)).ConfigureAwait(false);
        await factory.Services.GetRequiredService<BlobService>().DeleteBlobAsync(
            record, false, BlobDeleteSnapshotsOption.Unspecified, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(record.Content.Chunks.Count, await factory.Services.GetRequiredService<BlobService>()
            .CollectGarbageAsync(CancellationToken.None).ConfigureAwait(false));
        await sessions.CloseAsync(session.Token, CancellationToken.None).ConfigureAwait(false);
    }

    private static async Task AssertQueryRenewalAsync(bool expire)
    {
        var time = new AdjustableClock(DateTimeOffset.UtcNow);
        var factory = CreateFactory(time, new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["ApplicationHosting:ReadSessionIdleTimeout"] = "00:00:02",
            ["Sava:MaximumConcurrentBlobQueries"] = "1",
            ["Sava:MaximumQueuedStorageOperations"] = "0"
        });
        await using var disposal = factory.ConfigureAwait(false);
        var (record, bytes) = await CreateBlobAsync(factory).ConfigureAwait(false);
        var sessions = factory.Services.GetRequiredService<ApplicationReadSessions>();
        var service = factory.Services.GetRequiredService<BlobService>();
        var chunks = factory.Services.GetRequiredService<ChunkStore>();
        using var peer = ApplicationRpcIdentity.Enter("backend-test-peer");
        var session = await sessions.OpenAsync(record, true, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(TimeSpan.FromSeconds(2), session.IdleTimeout);
        await service.DeleteBlobAsync(record, false, BlobDeleteSnapshotsOption.Unspecified, CancellationToken.None)
            .ConfigureAwait(false);
        for (var renewal = 0; renewal < 3; renewal++)
        {
            time.Advance(TimeSpan.FromSeconds(1));
            await sessions.TouchAsync(session.Token, CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(0, await service.CollectGarbageAsync(CancellationToken.None).ConfigureAwait(false));
        }
        var busy = await Assert.ThrowsAsync<AzureStorageException>(() =>
            chunks.Admission.AcquireQueryAsync(CancellationToken.None).AsTask()).ConfigureAwait(false);
        Assert.Equal("ServerBusy", busy.ErrorCode);
        using var output = new MemoryStream();
        await sessions.WriteRangeAsync(session.Token, new BlobEncryption(null, null), 0, bytes.Length, output,
            CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(bytes, output.ToArray());
        if (expire)
            time.Advance(session.IdleTimeout);
        else
            await sessions.CloseAsync(session.Token, CancellationToken.None).ConfigureAwait(false);
        var denied = await Assert.ThrowsAsync<AzureStorageException>(() =>
            sessions.TouchAsync(session.Token, CancellationToken.None)).ConfigureAwait(false);
        Assert.Equal(404, denied.StatusCode);
        await Assert.ThrowsAsync<AzureStorageException>(() =>
            sessions.TouchAsync(session.Token, CancellationToken.None)).ConfigureAwait(false);
        using var releasedPermit = await chunks.Admission.AcquireQueryAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(record.Content.Chunks.Count, await service.CollectGarbageAsync(CancellationToken.None).ConfigureAwait(false));
    }

    private static async Task AssertDataAccessRetryContentAsync()
    {
        var time = new AdjustableClock(DateTimeOffset.UtcNow);
        var factory = CreateFactory(time, new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:LastAccessTimeTrackingEnabled"] = "true"
        });
        await using var disposal = factory.ConfigureAwait(false);
        var (record, _) = await CreateBlobAsync(factory).ConfigureAwait(false);
        var service = factory.Services.GetRequiredService<BlobService>();
        _ = await service.SetBlobMetadataAsync(record, Metadata("benign", "update"), CancellationToken.None)
            .ConfigureAwait(false);
        time.Advance(TimeSpan.FromDays(1));
        var accessed = await service.RecordDataAccessAsync(record, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal("update", accessed.Metadata["benign"]);
        Assert.Equal(time.GetUtcNow(), accessed.LastAccessedAt);
        Assert.NotSame(record.Content, accessed.Content);
        Assert.True(ApplicationReadSessions.ContentMatchesPinnedManifest(record.Content, accessed.Content));
        var original = record.Content;
        Assert.False(ApplicationReadSessions.ContentMatchesPinnedManifest(original, accessed.Content with { Domain = "different" }));
        Assert.False(ApplicationReadSessions.ContentMatchesPinnedManifest(original, accessed.Content with { Length = original.Length + 1 }));
        Assert.False(ApplicationReadSessions.ContentMatchesPinnedManifest(original, accessed.Content with { Sha256 = "different" }));
        var chunk = original.Chunks[0];
        foreach (var replacement in new[]
        {
            chunk with { Id = "different" }, chunk with { Offset = chunk.Offset + 1 }, chunk with { Length = chunk.Length + 1 }
        })
        {
            var mutated = original.Chunks.ToArray();
            mutated[0] = replacement;
            Assert.False(ApplicationReadSessions.ContentMatchesPinnedManifest(original,
                accessed.Content with { Chunks = mutated }));
        }
    }

    private static async Task AssertGlobalRpcAdmissionAsync()
    {
        var factory = CreateAdmissionFactory(queueLimit: 0);
        await using var disposal = factory.ConfigureAwait(false);
        await factory.InitializeAsync().ConfigureAwait(false);
        var admission = factory.Services.GetRequiredService<ApplicationRpcAdmission>();
        var bulk = await admission.AcquireAsync(ApplicationRpcLane.Bulk, CancellationToken.None).ConfigureAwait(false);
        await using var bulkDisposal = bulk.ConfigureAwait(false);
        using (ApplicationRpcIdentity.Enter("another-gateway-peer"))
        {
            var busy = await Assert.ThrowsAsync<AzureStorageException>(() =>
                admission.AcquireAsync(ApplicationRpcLane.Bulk, CancellationToken.None).AsTask()).ConfigureAwait(false);
            Assert.Equal("ServerBusy", busy.ErrorCode);
            Assert.Equal("1", busy.ResponseHeaders["Retry-After"]);
        }
        var control = await admission.AcquireAsync(ApplicationRpcLane.Control, CancellationToken.None).ConfigureAwait(false);
        await using var controlDisposal = control.ConfigureAwait(false);
        var readiness = factory.Services.GetRequiredService<IApplicationReadiness>();
        Assert.True((await readiness.GetAsync(CancellationToken.None).ConfigureAwait(false)).Ready);
        var metrics = await readiness.RenderStorageMetricsAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.Contains("mk8_sava_application_rpc_active_requests{lane=\"bulk\"} 1\n", metrics, StringComparison.Ordinal);
        Assert.Contains("mk8_sava_application_rpc_active_requests{lane=\"control\"} 1\n", metrics, StringComparison.Ordinal);
        Assert.Contains("mk8_sava_application_rpc_rejected_requests_total{lane=\"bulk\"} 1\n", metrics, StringComparison.Ordinal);
    }

    private static async Task AssertRpcQueueCancellationAsync()
    {
        var factory = CreateAdmissionFactory(queueLimit: 1);
        await using var disposal = factory.ConfigureAwait(false);
        await factory.InitializeAsync().ConfigureAwait(false);
        var admission = factory.Services.GetRequiredService<ApplicationRpcAdmission>();
        var first = await admission.AcquireAsync(ApplicationRpcLane.Bulk, CancellationToken.None).ConfigureAwait(false);
        await using var firstDisposal = first.ConfigureAwait(false);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
#pragma warning disable CA2025 // Finally cancels and joins the queued acquisition before disposing its cancellation source.
        var queued = admission.AcquireAsync(ApplicationRpcLane.Bulk, cancellation.Token).AsTask();
#pragma warning restore CA2025
        try
        {
            Assert.False(queued.IsCompleted);
            Assert.Contains("mk8_sava_application_rpc_queued_requests{lane=\"bulk\"} 1\n",
                admission.RenderMetrics(), StringComparison.Ordinal);
            var busy = await Assert.ThrowsAsync<AzureStorageException>(() =>
                admission.AcquireAsync(ApplicationRpcLane.Bulk, CancellationToken.None).AsTask()).ConfigureAwait(false);
            Assert.Equal("ServerBusy", busy.ErrorCode);
            await cancellation.CancelAsync().ConfigureAwait(false);
            try
            {
                var unexpected = await queued.ConfigureAwait(false);
                await unexpected.DisposeAsync().ConfigureAwait(false);
                Assert.Fail("The canceled queued RPC admission unexpectedly succeeded.");
            }
            catch (OperationCanceledException)
            {
            }
            Assert.Contains("mk8_sava_application_rpc_queued_requests{lane=\"bulk\"} 0\n",
                admission.RenderMetrics(), StringComparison.Ordinal);
        }
        finally
        {
            await cancellation.CancelAsync().ConfigureAwait(false);
            try
            {
                var unexpected = await queued.ConfigureAwait(false);
                await unexpected.DisposeAsync().ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    private static async Task AssertRpcQueueTimeoutAsync()
    {
        var factory = CreateAdmissionFactory(queueLimit: 1, timeout: "00:00:00.1000000");
        await using var disposal = factory.ConfigureAwait(false);
        await factory.InitializeAsync().ConfigureAwait(false);
        var admission = factory.Services.GetRequiredService<ApplicationRpcAdmission>();
        var first = await admission.AcquireAsync(ApplicationRpcLane.Bulk, CancellationToken.None).ConfigureAwait(false);
        await using var firstDisposal = first.ConfigureAwait(false);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var rejected = await Assert.ThrowsAsync<AzureStorageException>(() =>
            admission.AcquireAsync(ApplicationRpcLane.Bulk, deadline.Token).AsTask()).ConfigureAwait(false);
        Assert.Equal("ServerBusy", rejected.ErrorCode);
        var metrics = admission.RenderMetrics();
        Assert.Contains("mk8_sava_application_rpc_queued_requests{lane=\"bulk\"} 0\n", metrics, StringComparison.Ordinal);
        Assert.Contains("mk8_sava_application_rpc_rejected_requests_total{lane=\"bulk\"} 1\n", metrics, StringComparison.Ordinal);
    }

    private static async Task AssertControlAdmissionBoundsAsync()
    {
        var factory = CreateAdmissionFactory(queueLimit: 0);
        await using var disposal = factory.ConfigureAwait(false);
        await factory.InitializeAsync().ConfigureAwait(false);
        var admission = factory.Services.GetRequiredService<ApplicationRpcAdmission>();
        var held = new List<IAsyncDisposable>();
        var queued = new List<Task<IAsyncDisposable>>();
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        try
        {
            for (var active = 0; active < 4; active++)
                held.Add(await admission.AcquireAsync(ApplicationRpcLane.Control, cancellation.Token).ConfigureAwait(false));
            for (var waiting = 0; waiting < 32; waiting++)
            {
#pragma warning disable CA2025 // Finally cancels and joins every queued control acquisition before disposing the source or held leases.
                queued.Add(admission.AcquireAsync(ApplicationRpcLane.Control, cancellation.Token).AsTask());
#pragma warning restore CA2025
            }
            var rejected = await Assert.ThrowsAsync<AzureStorageException>(() =>
                admission.AcquireAsync(ApplicationRpcLane.Control, cancellation.Token).AsTask()).ConfigureAwait(false);
            Assert.Equal("ServerBusy", rejected.ErrorCode);
            var metrics = admission.RenderMetrics();
            Assert.Contains("mk8_sava_application_rpc_active_requests{lane=\"control\"} 4\n", metrics, StringComparison.Ordinal);
            Assert.Contains("mk8_sava_application_rpc_queued_requests{lane=\"control\"} 32\n", metrics, StringComparison.Ordinal);
        }
        finally
        {
            await cancellation.CancelAsync().ConfigureAwait(false);
            try
            {
                // All four permits remain held until the queued tasks have
                // canceled, so none of these acquisitions can own a lease.
                _ = await Task.WhenAll(queued).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            foreach (var lease in held)
                await lease.DisposeAsync().ConfigureAwait(false);
        }
        Assert.Contains("mk8_sava_application_rpc_active_requests{lane=\"control\"} 0\n",
            admission.RenderMetrics(), StringComparison.Ordinal);
    }

    private static SavaWebApplicationFactory CreateAdmissionFactory(int queueLimit, string timeout = "00:00:10") =>
        CreateFactory(overrides: new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["ApplicationHosting:MaximumConcurrentRpcRequests"] = "1",
            ["ApplicationHosting:MaximumQueuedRpcRequests"] = queueLimit.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["ApplicationHosting:RpcQueueTimeout"] = timeout
        });

    private static async Task AssertInvalidSessionOptionsAsync(string name, string value)
    {
        var factory = CreateFactory(overrides: new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"ApplicationHosting:{name}"] = value
        });
        await using var disposal = factory.ConfigureAwait(false);
        _ = await Assert.ThrowsAsync<OptionsValidationException>(() => factory.InitializeAsync()).ConfigureAwait(false);
    }

    private static async Task AssertReadinessFailureAsync(bool fatal, bool wrapped)
    {
        const string secret = "readiness-exception-secret";
        var factory = CreateFactory();
        await using var disposal = factory.ConfigureAwait(false);
        await factory.InitializeAsync().ConfigureAwait(false);
#pragma warning disable CA2201 // Inject the fatal exception object to test readiness propagation; this does not exhaust real process memory.
        Exception failure = fatal ? new OutOfMemoryException(secret) : new InvalidOperationException(secret);
#pragma warning restore CA2201
        if (wrapped)
            failure = new InvalidOperationException(secret, failure);
        var logger = new ProbeLogger();
        var readiness = new ApplicationReadinessService(factory.Services.GetRequiredService<MetadataStore>(),
            new FaultingTelemetry(failure), factory.Services.GetRequiredService<ChunkStore>(),
            factory.Services.GetRequiredService<ApplicationInitializationService>(),
            factory.Services.GetRequiredService<ApplicationRpcAdmission>(), logger);
        if (fatal)
        {
            var actual = await Record.ExceptionAsync(() => readiness.GetAsync(CancellationToken.None)).ConfigureAwait(false);
            Assert.Same(failure, actual);
            Assert.Empty(logger.Messages);
        }
        else
        {
            var state = await readiness.GetAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.False(state.Ready);
            Assert.False(state.MetadataReady);
            var message = Assert.Single(logger.Messages);
            Assert.DoesNotContain(secret, message, StringComparison.Ordinal);
            Assert.Contains(nameof(InvalidOperationException), message, StringComparison.Ordinal);
        }
    }

    private static SavaWebApplicationFactory CreateFactory(
        TimeProvider? time = null, IReadOnlyDictionary<string, string?>? overrides = null)
    {
        var configuration = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        };
        if (overrides is not null)
        {
            foreach (var pair in overrides)
                configuration[pair.Key] = pair.Value;
        }
        return time is null ? new SavaWebApplicationFactory(configuration) : new SavaWebApplicationFactory(time, configuration);
    }

    private static async Task<(BlobRecord Record, byte[] Bytes)> CreateBlobAsync(SavaWebApplicationFactory factory)
    {
        await factory.InitializeAsync().ConfigureAwait(false);
        var service = factory.Services.GetRequiredService<BlobService>();
        var container = $"backend-{Guid.NewGuid():N}";
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal);
        _ = await service.CreateContainerAsync(SavaWebApplicationFactory.AccountName, container,
            metadata, null, null, false, CancellationToken.None).ConfigureAwait(false);
        var bytes = RandomNumberGenerator.GetBytes(64 * 1024);
        using var source = new MemoryStream(bytes, writable: false);
        var blob = await service.PutBlockBlobAsync(SavaWebApplicationFactory.AccountName, container, "content.bin", source,
            new BlobWriteOptions(new BlobHttpProperties(), metadata), LeaseRecord.Available, null, null, CancellationToken.None)
            .ConfigureAwait(false);
        return (blob, bytes);
    }

    private static Dictionary<string, string> Metadata(string key, string value) =>
        new(StringComparer.Ordinal) { [key] = value };

    private static async Task<T> InvokeBlobAsync<T>(SavaWebApplicationFactory factory, string name, object?[] arguments)
    {
        var dispatcher = factory.Services.GetRequiredService<IApplicationRpcDispatcher>();
        var method = typeof(IBlobApplication).GetMethod(name, BindingFlags.Public | BindingFlags.Instance)
            ?? throw new InvalidOperationException("The test's domain method does not exist.");
        using var peer = ApplicationRpcIdentity.Enter("backend-test-peer");
        return (T)(await dispatcher.InvokeAsync(typeof(IBlobApplication), method, arguments, CancellationToken.None)
            .ConfigureAwait(false))!;
    }

    private sealed class AdjustableClock(DateTimeOffset now) : TimeProvider
    {
        private long _ticks = now.UtcTicks;
        public override DateTimeOffset GetUtcNow() => new(Interlocked.Read(ref _ticks), TimeSpan.Zero);
        public void Advance(TimeSpan elapsed) => Interlocked.Add(ref _ticks, elapsed.Ticks);
    }

    private sealed class ControlledOutput : MemoryStream
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task Entered => _entered.Task;
        public void Release() => _release.TrySetResult();

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            _entered.TrySetResult();
            await _release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            await base.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class FaultingTelemetry(Exception failure) : IStorageTelemetry
    {
        public StorageUsageSnapshot Usage => throw new NotSupportedException();
        public StorageIntegritySnapshot Integrity => throw failure;
        public void RecordRequest(int statusCode, long elapsedStopwatchTicks) => throw new NotSupportedException();
        public void RecordMaintenance(StorageMaintenanceResult result, StorageUsageSnapshot usage) => throw new NotSupportedException();
        public void RecordMaintenanceFailure() => throw new NotSupportedException();
        public void RecordIntegrity(StorageIntegritySnapshot integrity) => throw new NotSupportedException();
        public string RenderPrometheus() => throw new NotSupportedException();
    }

    private sealed class ProbeLogger : ILogger<ApplicationReadinessService>
    {
        public List<string> Messages { get; } = [];
        public bool IsEnabled(LogLevel logLevel) => true;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter) => Messages.Add(formatter(state, exception));
    }
}
