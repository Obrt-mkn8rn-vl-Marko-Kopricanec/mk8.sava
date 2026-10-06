using Mk8.Sava.Storage;
using Microsoft.Extensions.Logging;

namespace Mk8.Sava.Protocol;

internal sealed class GatewayContentRead : IAsyncDisposable
{
    private readonly IApplicationReadSessions _sessions;
    private readonly ApplicationReadSession _session;
    private readonly CancellationTokenSource _renewalCancellation;
    private readonly Task _renewal;
    private int _closed;
    private static readonly Action<ILogger, string, Exception?> RenewalFailed = LoggerMessage.Define<string>(
        LogLevel.Warning, new EventId(5101, "ReadSessionRenewalFailed"),
        "Application read-session renewal failed ({ExceptionType}); aborting the active Gateway request.");

    private GatewayContentRead(HttpContext http, IApplicationReadSessions sessions, ApplicationReadSession session)
    {
        _sessions = sessions;
        _session = session;
        _renewalCancellation = CancellationTokenSource.CreateLinkedTokenSource(http.RequestAborted);
        _renewal = RenewAsync(http, _renewalCancellation.Token);
    }

    public BlobRecord Record => _session.Record;

    public static async Task<GatewayContentRead> OpenAsync(
        HttpContext http, BlobRecord blob, bool query, CancellationToken cancellationToken)
    {
        var sessions = http.RequestServices.GetRequiredService<IApplicationReadSessions>();
        var session = await sessions.OpenAsync(blob, query, cancellationToken).ConfigureAwait(false);
        if (session.IdleTimeout < TimeSpan.FromSeconds(1) || session.IdleTimeout > TimeSpan.FromHours(1))
            throw new InvalidDataException("Application returned an invalid read-session lease duration.");
        return new GatewayContentRead(http, sessions, session);
    }

    public Task WriteRangeAsync(BlobEncryption encryption, long offset, long length,
        Stream destination, CancellationToken cancellationToken) =>
        _sessions.WriteRangeAsync(_session.Token, encryption, offset, length, destination, cancellationToken);

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0)
            return;
        try
        {
            await _renewalCancellation.CancelAsync().ConfigureAwait(false);
#pragma warning disable VSTHRD003 // This instance starts/owns renewal; every renewal await is context-free and DisposeAsync cancels it before joining.
            await _renewal.ConfigureAwait(false);
#pragma warning restore VSTHRD003
        }
        finally
        {
            _renewalCancellation.Dispose();
            await CloseAsync().ConfigureAwait(false);
        }
    }

    private async Task RenewAsync(HttpContext http, CancellationToken cancellationToken)
    {
        var interval = TimeSpan.FromTicks(Math.Min(TimeSpan.FromSeconds(30).Ticks, _session.IdleTimeout.Ticks / 3));
        var renewalTimeout = TimeSpan.FromTicks(_session.IdleTimeout.Ticks / 3);
        try
        {
            while (true)
            {
                await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
                using var attempt = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                attempt.CancelAfter(renewalTimeout);
                await _sessions.TouchAsync(_session.Token, attempt.Token).ConfigureAwait(false);
            }
        }
        catch (Exception exception) when (CatastrophicExceptionPolicy.Contains(exception))
        {
            http.Abort();
            throw;
        }
        catch (OperationCanceledException exception) when (cancellationToken.IsCancellationRequested &&
            !CatastrophicExceptionPolicy.Contains(exception))
        {
            // Request completion/cancellation stops renewal without claiming a backend failure.
        }
#pragma warning disable CA1031 // An expired/lost remote lease must stop the public request, never continue outside Application's admission and pin.
        catch (Exception exception) when (!CatastrophicExceptionPolicy.Contains(exception))
        {
            RenewalFailed(http.RequestServices.GetRequiredService<ILogger<GatewayContentRead>>(), exception.GetType().Name, null);
            http.Abort();
        }
#pragma warning restore CA1031
    }

    private async Task CloseAsync()
    {
        using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            await _sessions.CloseAsync(_session.Token, cleanup.Token).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // A failed remote close cannot invalidate completed content; Application independently expires abandoned sessions.
        catch (Exception exception) when (!CatastrophicExceptionPolicy.Contains(exception))
        {
        }
#pragma warning restore CA1031
    }
}
