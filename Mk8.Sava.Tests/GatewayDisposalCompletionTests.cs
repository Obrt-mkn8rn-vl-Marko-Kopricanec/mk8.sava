using System.Runtime.ExceptionServices;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Mk8.Sava.Application;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class GatewayDisposalCompletionTests
{
    [Theory]
    [InlineData("success")]
    [InlineData("ordinary")]
    [InlineData("fatal")]
    [InlineData("wrapped")]
    public async Task RepeatedAndReentrantDisposalJoinTheActualCloseAndRetainItsOutcome(string kind)
    {
        var failure = CreateFailure(kind);
        var probe = new CloseProbe(failure);
        var provider = new ServiceCollection().AddSingleton<IApplicationReadSessions>(probe)
            .AddSingleton<ILogger<GatewayContentRead>>(NullLogger<GatewayContentRead>.Instance).BuildServiceProvider();
        await using var providerLifetime = provider.ConfigureAwait(false);
        var http = new DefaultHttpContext { RequestServices = provider };
        var read = await GatewayContentRead.OpenAsync(http, CreateBlob(), query: false, CancellationToken.None).ConfigureAwait(true);
        probe.OnClose = () => read.DisposeAsync().AsTask();
        var first = read.DisposeAsync().AsTask();
        Exception? verification;
        Exception? cleanup;
        try
        {
#pragma warning disable VSTHRD003 // This test starts/owns first above; VerifyAsync is context-free and finally joins the actual operation before provider retirement.
            verification = await Record.ExceptionAsync(() => VerifyAsync(probe, read, first, failure)).ConfigureAwait(true);
#pragma warning restore VSTHRD003
        }
        finally
        {
            probe.Release.TrySetResult();
            // Join the FIRST actual cleanup even when a broken repeat returns early.
#pragma warning disable VSTHRD003 // first is this test's owned context-free disposal, not unrelated ambient work; release precedes its guaranteed actual join.
            cleanup = await Record.ExceptionAsync(() => first).ConfigureAwait(true);
#pragma warning restore VSTHRD003
        }
        var expected = failure is not null && CatastrophicExceptionPolicy.Contains(failure) ? failure : null;
        if (cleanup is not null && !ReferenceEquals(cleanup, expected))
        {
            if (verification is not null)
                throw new AggregateException("Verification and owned read cleanup both failed.", verification, cleanup);
            ExceptionDispatchInfo.Throw(cleanup);
        }
        if (verification is not null)
            ExceptionDispatchInfo.Throw(verification);
    }

    private static async Task VerifyAsync(CloseProbe probe, GatewayContentRead read, Task first, Exception? failure)
    {
        await probe.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5), CancellationToken.None).ConfigureAwait(false);
        Assert.False(first.IsCompleted);
        var second = read.DisposeAsync().AsTask();
        Assert.False(second.IsCompleted);
        var reentrant = Assert.IsType<Task>(probe.Reentrant, exactMatch: false);
        Assert.False(reentrant.IsCompleted);
        probe.Release.TrySetResult();
        var expected = failure is not null && CatastrophicExceptionPolicy.Contains(failure) ? failure : null;
        foreach (var operation in new[] { first, second, reentrant })
        {
            var actual = await Record.ExceptionAsync(() => operation.WaitAsync(TimeSpan.FromSeconds(5), CancellationToken.None)).ConfigureAwait(false);
            Assert.Same(expected, actual);
        }
        var repeat = await Record.ExceptionAsync(() => read.DisposeAsync().AsTask()).ConfigureAwait(false);
        Assert.Same(expected, repeat);
        Assert.Equal(1, probe.Closes);
        Assert.False(probe.CloseWasCanceled);
    }

    private static Exception? CreateFailure(string kind)
    {
        if (string.Equals(kind, "success", StringComparison.Ordinal))
            return null;
        if (string.Equals(kind, "ordinary", StringComparison.Ordinal))
            return new IOException("controlled ordinary close failure");
#pragma warning disable CA2201 // Synthetic reserved failure checks propagation; it does not exhaust memory or reproduce native corruption.
        var fatal = new OutOfMemoryException("controlled close failure");
#pragma warning restore CA2201
        return string.Equals(kind, "wrapped", StringComparison.Ordinal)
            ? new IOException("controlled wrapped close failure", fatal) : fatal;
    }

    private static BlobRecord CreateBlob() => new()
    {
        Account = "account", Container = "container", Name = "blob", GenerationId = "generation", Revision = "revision",
        Kind = BlobKind.BlockBlob, Content = ContentManifest.Empty("domain"), ETag = "\"etag\"",
        CreatedAt = DateTimeOffset.UnixEpoch, LastModified = DateTimeOffset.UnixEpoch,
    };

    private sealed class CloseProbe(Exception? failure) : IApplicationReadSessions
    {
        internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal Func<Task>? OnClose { get; set; }
        internal Task? Reentrant { get; private set; }
        internal int Closes { get; private set; }
        internal bool CloseWasCanceled { get; private set; }

        public Task<ApplicationReadSession> OpenAsync(BlobRecord record, bool query, CancellationToken cancellationToken) =>
            Task.FromResult(new ApplicationReadSession("token", record, TimeSpan.FromSeconds(1)));
        public Task TouchAsync(string token, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task WriteRangeAsync(string token, BlobEncryption encryption, long offset, long length,
            Stream destination, CancellationToken cancellationToken) => Task.CompletedTask;

        public async Task CloseAsync(string token, CancellationToken cancellationToken)
        {
            Closes++;
            CloseWasCanceled = cancellationToken.IsCancellationRequested;
            Reentrant = OnClose?.Invoke();
            Entered.TrySetResult();
            await Release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            if (failure is not null)
                throw failure;
        }
    }
}
