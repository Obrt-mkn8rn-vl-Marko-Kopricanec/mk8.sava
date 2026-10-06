using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Mk8.Sava.Application;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class GatewayReadSessionTests
{
    [Fact]
    public async Task ReadLeaseRenewsDuringCpuGapsAndDisposalJoinsItBeforeClosing()
    {
        var fixture = new RenewalFixture();
        await using var fixtureLifetime = fixture.ConfigureAwait(false);
        var read = await fixture.OpenAsync().ConfigureAwait(true);
        await fixture.Sessions.Touched.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(true);
        Assert.True(fixture.Sessions.Touches > 0);
        Assert.Equal(0, fixture.Sessions.Closes);
        Assert.False(fixture.Lifetime.Aborted.Task.IsCompleted);

        await read.DisposeAsync().ConfigureAwait(true);
        await read.DisposeAsync().ConfigureAwait(true);

        Assert.Equal(1, fixture.Sessions.Closes);
        Assert.False(fixture.Sessions.CloseWasCanceled);
        Assert.Empty(fixture.Logger.Messages);
    }

    [Fact]
    public async Task LostRenewalAbortsTheRequestAndLogsOnlyTheFailureType()
    {
        const string secret = "renewal-failure-secret";
        var fixture = new RenewalFixture(new IOException(secret));
        await using var fixtureLifetime = fixture.ConfigureAwait(false);
        var read = await fixture.OpenAsync().ConfigureAwait(true);

        await fixture.Lifetime.Aborted.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(true);
        await read.DisposeAsync().ConfigureAwait(true);

        Assert.True(fixture.Lifetime.RequestAborted.IsCancellationRequested);
        Assert.Equal(1, fixture.Sessions.Closes);
        Assert.False(fixture.Sessions.CloseWasCanceled);
        var message = Assert.Single(fixture.Logger.Messages);
        Assert.Contains(nameof(IOException), message, StringComparison.Ordinal);
        Assert.DoesNotContain(secret, message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FatalRenewalAbortsAndEscapesThroughTheOwningRequest(bool wrapped)
    {
#pragma warning disable CA2201 // Synthetic reserved failure verifies the actual renewal path without exhausting memory.
        Exception failure = new OutOfMemoryException("fatal-renewal-secret");
#pragma warning restore CA2201
        if (wrapped)
            failure = new InvalidOperationException("wrapped", failure);
        var fixture = new RenewalFixture(failure);
        await using var fixtureLifetime = fixture.ConfigureAwait(false);
        var read = await fixture.OpenAsync().ConfigureAwait(true);
        await fixture.Lifetime.Aborted.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(true);

        var actual = await Record.ExceptionAsync(() => read.DisposeAsync().AsTask()).ConfigureAwait(true);

        Assert.Same(failure, actual);
        Assert.Equal(1, fixture.Sessions.Closes);
        Assert.Empty(fixture.Logger.Messages);
    }

    [Fact]
    public async Task HungRenewalIsCanceledAndAbortsBeforeTheNegotiatedLeaseExpires()
    {
        var fixture = new RenewalFixture(hangTouch: true);
        await using var fixtureLifetime = fixture.ConfigureAwait(false);
        var read = await fixture.OpenAsync().ConfigureAwait(true);

        await fixture.Sessions.Touched.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(true);
        await fixture.Lifetime.Aborted.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(true);
        await read.DisposeAsync().ConfigureAwait(true);

        Assert.True(fixture.Sessions.TouchWasCanceled);
        Assert.Equal(1, fixture.Sessions.Closes);
        Assert.False(fixture.Sessions.CloseWasCanceled);
        Assert.Single(fixture.Logger.Messages);
    }

    [Fact]
    public async Task ClientCancellationStopsRenewalButStillClosesWithAnIndependentCleanupToken()
    {
        var fixture = new RenewalFixture();
        await using var fixtureLifetime = fixture.ConfigureAwait(false);
        var read = await fixture.OpenAsync().ConfigureAwait(true);
        fixture.Lifetime.Abort();

        await read.DisposeAsync().ConfigureAwait(true);

        Assert.Equal(1, fixture.Sessions.Closes);
        Assert.False(fixture.Sessions.CloseWasCanceled);
        Assert.Empty(fixture.Logger.Messages);
    }

    private sealed class RenewalFixture : IAsyncDisposable
    {
        private readonly ServiceProvider _provider;
        private GatewayContentRead? _read;
        internal SessionProbe Sessions { get; }
        internal LifetimeProbe Lifetime { get; } = new();
        internal ProbeLogger Logger { get; } = new();

        internal RenewalFixture(Exception? failure = null, bool hangTouch = false)
        {
            Sessions = new SessionProbe(failure, hangTouch);
            _provider = new ServiceCollection().AddSingleton<IApplicationReadSessions>(Sessions)
                .AddSingleton<ILogger<GatewayContentRead>>(Logger).BuildServiceProvider();
        }

        internal async Task<GatewayContentRead> OpenAsync()
        {
            var context = new DefaultHttpContext { RequestServices = _provider };
            context.Features.Set<IHttpRequestLifetimeFeature>(Lifetime);
            _read = await GatewayContentRead.OpenAsync(context, CreateBlob(), query: true, CancellationToken.None).ConfigureAwait(false);
            return _read;
        }

        public async ValueTask DisposeAsync()
        {
            if (_read is not null)
                await _read.DisposeAsync().ConfigureAwait(false);
            await _provider.DisposeAsync().ConfigureAwait(false);
            Lifetime.Dispose();
        }
    }

    private static BlobRecord CreateBlob() => new()
    {
        Account = "account",
        Container = "container",
        Name = "blob",
        GenerationId = "generation",
        Revision = "revision",
        Kind = BlobKind.BlockBlob,
        Content = ContentManifest.Empty("domain"),
        ETag = "\"etag\"",
        CreatedAt = DateTimeOffset.UnixEpoch,
        LastModified = DateTimeOffset.UnixEpoch
    };

    private sealed class SessionProbe(Exception? failure, bool hangTouch) : IApplicationReadSessions
    {
        internal TaskCompletionSource Touched { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal int Touches { get; private set; }
        internal int Closes { get; private set; }
        internal bool CloseWasCanceled { get; private set; }
        internal bool TouchWasCanceled { get; private set; }

        public Task<ApplicationReadSession> OpenAsync(BlobRecord record, bool query, CancellationToken cancellationToken) =>
            Task.FromResult(new ApplicationReadSession("token", record, TimeSpan.FromSeconds(1)));

        public async Task TouchAsync(string token, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Touches++;
            Touched.TrySetResult();
            if (failure is not null)
                throw failure;
            if (hangTouch)
            {
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    TouchWasCanceled = cancellationToken.IsCancellationRequested;
                }
            }
        }

        public Task WriteRangeAsync(string token, BlobEncryption encryption, long offset, long length,
            Stream destination, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task CloseAsync(string token, CancellationToken cancellationToken)
        {
            Closes++;
            CloseWasCanceled = cancellationToken.IsCancellationRequested;
            return Task.CompletedTask;
        }
    }

    private sealed class LifetimeProbe : IHttpRequestLifetimeFeature, IDisposable
    {
        private readonly CancellationTokenSource _cancellation = new();
        internal TaskCompletionSource Aborted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public CancellationToken RequestAborted { get => _cancellation.Token; set => throw new NotSupportedException(); }
        public void Abort()
        {
            _cancellation.Cancel();
            Aborted.TrySetResult();
        }
        public void Dispose() => _cancellation.Dispose();
    }

    private sealed class ProbeLogger : ILogger<GatewayContentRead>
    {
        internal List<string> Messages { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Messages.Add(formatter(state, exception));
    }
}
