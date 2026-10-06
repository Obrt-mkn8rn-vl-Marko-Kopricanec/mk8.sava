using System.Security.Cryptography;
using System.Threading.RateLimiting;
using Microsoft.Extensions.Options;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Application;

internal sealed class ApplicationReadSessions(
    ChunkStore chunks, BlobService blobs,
    IOptions<ApplicationHostingOptions> configuredOptions, TimeProvider time) : BackgroundService, IApplicationReadSessions
{
    private readonly Lock _gate = new();
    private readonly Dictionary<string, ReadSession> _sessions = new(StringComparer.Ordinal);
    private readonly ApplicationHostingOptions _options = configuredOptions.Value;
    private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _opening;
    private int _activeOperations;
    private bool _closed;

    public async Task<ApplicationReadSession> OpenAsync(
        BlobRecord record, bool query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(record);
        var peer = RequirePeer();
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_closed, this);
            if (_sessions.Count + _opening >= _options.MaximumReadSessions)
                throw new AzureStorageException(503, "ServerBusy", "The server has reached its read-session limit.");
            _opening++;
        }

        RateLimitLease? admission = null;
        IDisposable? pin = null;
        try
        {
            admission = query
                ? await chunks.Admission.AcquireQueryAsync(cancellationToken).ConfigureAwait(false)
                : await chunks.Admission.AcquireReadAsync(cancellationToken).ConfigureAwait(false);
            var authoritative = await ReloadBlobAsync(record, cancellationToken).ConfigureAwait(false);
            pin = chunks.Pin(authoritative.Content);
            // A delete/overwrite can race the initial row lookup before the pin
            // takes effect. Do not read that retired incarnation: after pinning,
            // confirm that the authoritative revision still names this content.
            _ = await ReloadBlobAsync(authoritative, cancellationToken).ConfigureAwait(false);
            var accessed = await blobs.RecordDataAccessAsync(authoritative, cancellationToken).ConfigureAwait(false);
            if (!string.Equals(accessed.GenerationId, authoritative.GenerationId, StringComparison.Ordinal) ||
                !ContentMatchesPinnedManifest(authoritative.Content, accessed.Content))
            {
                throw new StorageConcurrencyException("The blob changed while recording data access.");
            }
            var token = Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(32));
            lock (_gate)
            {
                ObjectDisposedException.ThrowIf(_closed, this);
                _sessions.Add(token, new ReadSession(peer, accessed, query, admission, pin, time.GetUtcNow()));
                admission = null;
                pin = null;
            }
            return new ApplicationReadSession(token, accessed, _options.ReadSessionIdleTimeout);
        }
        finally
        {
            pin?.Dispose();
            admission?.Dispose();
            lock (_gate)
            {
                _opening--;
                SignalIfDrained();
            }
        }
    }

    internal static bool ContentMatchesPinnedManifest(ContentManifest pinned, ContentManifest accessed) =>
        string.Equals(pinned.Domain, accessed.Domain, StringComparison.Ordinal) &&
        pinned.Length == accessed.Length &&
        string.Equals(pinned.Sha256, accessed.Sha256, StringComparison.Ordinal) &&
        pinned.Chunks.SequenceEqual(accessed.Chunks);

    private async Task<BlobRecord> ReloadBlobAsync(BlobRecord record, CancellationToken cancellationToken)
    {
        var authoritative = await blobs.GetBlobAsync(
            record.Account, record.Container, record.Name, record.VersionId, record.Snapshot,
            includeDeleted: false, cancellationToken).ConfigureAwait(false);
        if (!string.Equals(record.GenerationId, authoritative.GenerationId, StringComparison.Ordinal) ||
            !string.Equals(record.Revision, authoritative.Revision, StringComparison.Ordinal))
        {
            throw new StorageConcurrencyException("The blob changed before its read session opened.");
        }
        return authoritative;
    }

    public async Task WriteRangeAsync(
        string token, BlobEncryption encryption, long offset, long length,
        Stream destination, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(destination);
        var peer = RequirePeer();
        ReadSession session;
        lock (_gate)
        {
            if (!_sessions.TryGetValue(token, out session!) || session.Closing)
                throw AzureStorageException.BlobNotFound();
            EnsurePeer(session, peer);
            if (time.GetUtcNow() - session.LastUsed >= _options.ReadSessionIdleTimeout && session.Active == 0)
            {
                Retire(token, session);
                throw AzureStorageException.BlobNotFound();
            }
            session.Active++;
            _activeOperations++;
            session.LastUsed = time.GetUtcNow();
        }
        try
        {
            if (session.Query)
            {
                await blobs.WriteContentAsync(
                    session.Record, encryption, offset, length, destination, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await blobs.WriteContentUnderReadLeaseAsync(
                    session.Admission, session.Record, encryption, offset, length, destination,
                    cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            lock (_gate)
            {
                session.Active--;
                _activeOperations--;
                session.LastUsed = time.GetUtcNow();
                if (session.Closing && session.Active == 0)
                    session.Dispose();
                SignalIfDrained();
            }
        }
    }

    public Task TouchAsync(string token, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var peer = RequirePeer();
        lock (_gate)
        {
            if (!_sessions.TryGetValue(token, out var session) || session.Closing)
                throw AzureStorageException.BlobNotFound();
            EnsurePeer(session, peer);
            var now = time.GetUtcNow();
            if (now - session.LastUsed >= _options.ReadSessionIdleTimeout && session.Active == 0)
            {
                Retire(token, session);
                throw AzureStorageException.BlobNotFound();
            }
            session.LastUsed = now;
        }
        return Task.CompletedTask;
    }

    public Task CloseAsync(string token, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var peer = RequirePeer();
        lock (_gate)
        {
            if (_sessions.TryGetValue(token, out var session))
            {
                EnsurePeer(session, peer);
                Retire(token, session);
            }
        }
        return Task.CompletedTask;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(10), time);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                lock (_gate)
                {
                    var expired = _sessions.Where(pair => pair.Value.Active == 0 &&
                        time.GetUtcNow() - pair.Value.LastUsed >= _options.ReadSessionIdleTimeout).ToArray();
                    foreach (var pair in expired)
                        Retire(pair.Key, pair.Value);
                }
            }
        }
        catch (OperationCanceledException exception) when (stoppingToken.IsCancellationRequested &&
                                                           !CatastrophicExceptionPolicy.Contains(exception))
        {
            // Stopping this hosted service completes its cleaner normally; it
            // must not trigger Host's background-service failure shutdown.
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        CloseAll();
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
        await _drained.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    public override void Dispose()
    {
        CloseAll();
        base.Dispose();
    }

    private void CloseAll()
    {
        lock (_gate)
        {
            _closed = true;
            foreach (var pair in _sessions.ToArray())
                Retire(pair.Key, pair.Value);
            SignalIfDrained();
        }
    }

    private void SignalIfDrained()
    {
        if (_closed && _opening == 0 && _activeOperations == 0)
            _drained.TrySetResult();
    }

    private void Retire(string token, ReadSession session)
    {
        _sessions.Remove(token);
        session.Closing = true;
        if (session.Active == 0)
            session.Dispose();
    }

    private static string RequirePeer() => ApplicationRpcIdentity.CurrentPeerFingerprint
        ?? throw new AzureStorageException(403, "AuthorizationFailure", "An authenticated Application peer is required.");

    private static void EnsurePeer(ReadSession session, string peer)
    {
        if (!string.Equals(session.Peer, peer, StringComparison.Ordinal))
            throw new AzureStorageException(403, "AuthorizationFailure", "The read session belongs to a different Application peer.");
    }

    private sealed class ReadSession(
        string peer, BlobRecord record, bool query, RateLimitLease admission, IDisposable pin,
        DateTimeOffset lastUsed) : IDisposable
    {
        public string Peer { get; } = peer;
        public BlobRecord Record { get; } = record;
        public bool Query { get; } = query;
        public RateLimitLease Admission { get; } = admission;
        public DateTimeOffset LastUsed { get; set; } = lastUsed;
        public int Active { get; set; }
        public bool Closing { get; set; }

        public void Dispose()
        {
            pin.Dispose();
            Admission.Dispose();
        }
    }
}
