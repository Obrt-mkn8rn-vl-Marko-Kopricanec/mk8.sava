using Mk8.Sava.Storage;
using Microsoft.Extensions.Logging;
using System.Runtime.ExceptionServices;

namespace Mk8.Sava.Protocol;

internal sealed class GatewayContentRead : IAsyncDisposable
{
    private readonly IApplicationReadSessions _sessions;
    private readonly ApplicationReadSession _session;
    private readonly CancellationTokenSource _renewalCancellation;
    private readonly Task _renewal;
    private readonly TaskCompletionSource _disposal = new(TaskCreationOptions.RunContinuationsAsynchronously);
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
        try
        {
            if (session.IdleTimeout < TimeSpan.FromSeconds(1) || session.IdleTimeout > TimeSpan.FromHours(1))
                throw new InvalidDataException("Application returned an invalid read-session lease duration.");
            return new GatewayContentRead(http, sessions, session);
        }
#pragma warning disable CA1031 // Close the acquired remote session on every failed ownership transfer; rethrow the original failure or retain it with an independent fatal cleanup failure.
        catch (Exception openingFailure)
        {
            try
            {
                await CloseAsync(sessions, session.Token).ConfigureAwait(false);
            }
            catch (Exception cleanupFailure)
            {
                throw new AggregateException("Read-session acquisition and cleanup both failed.", openingFailure, cleanupFailure);
            }
            throw;
        }
#pragma warning restore CA1031
    }

    public Task WriteRangeAsync(BlobEncryption encryption, long offset, long length,
        Stream destination, CancellationToken cancellationToken) =>
        _sessions.WriteRangeAsync(_session.Token, encryption, offset, length, destination, cancellationToken);

    public ValueTask DisposeAsync()
    {
        // Publish the shared completion before starting cleanup: synchronous
        // callbacks may reenter, and repeats must join rather than report success.
        if (Interlocked.Exchange(ref _closed, 1) == 0)
            _ = CompleteDisposalAsync();
        return new ValueTask(_disposal.Task);
    }

    private async Task CompleteDisposalAsync()
    {
        try
        {
            await DisposeCoreAsync().ConfigureAwait(false);
            _disposal.TrySetResult();
        }
#pragma warning disable CA1031 // Transfer every cleanup failure, including fatal graphs, unchanged to the one completion awaited by all owners; do not swallow or normalize it.
        catch (Exception failure)
#pragma warning restore CA1031
        {
            _disposal.TrySetException(failure);
        }
    }

    private async Task DisposeCoreAsync()
    {
        Exception? failure = null;
#pragma warning disable CA1031 // Retain every owned cleanup failure, including fatal graphs, while joining renewal and closing the remote session; rethrow after all cleanup settles.
        try
        {
            await _renewalCancellation.CancelAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failure = exception;
        }
        try
        {
#pragma warning disable VSTHRD003 // This instance starts/owns renewal; every renewal await is context-free and DisposeAsync cancels it before joining.
            await _renewal.ConfigureAwait(false);
#pragma warning restore VSTHRD003
        }
        catch (Exception exception)
        {
            failure = CombineFailures(failure, exception);
        }
        try
        {
            _renewalCancellation.Dispose();
        }
        catch (Exception exception)
        {
            failure = CombineFailures(failure, exception);
        }
        try
        {
            await CloseAsync(_sessions, _session.Token).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failure = CombineFailures(failure, exception);
        }
#pragma warning restore CA1031
        if (failure is not null)
            ExceptionDispatchInfo.Throw(failure);
    }

    private static Exception CombineFailures(Exception? first, Exception next) => first is null
        ? next : new AggregateException("Read-session cleanup had multiple failures.", first, next);

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

    private static async Task CloseAsync(IApplicationReadSessions sessions, string token)
    {
        using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            await sessions.CloseAsync(token, cleanup.Token).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // A failed remote close cannot invalidate completed content; Application independently expires abandoned sessions.
        catch (Exception exception) when (!CatastrophicExceptionPolicy.Contains(exception))
        {
        }
#pragma warning restore CA1031
    }
}
