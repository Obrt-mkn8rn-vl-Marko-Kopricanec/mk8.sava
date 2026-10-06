using System.Threading.RateLimiting;

namespace Mk8.Sava.Hosting;

internal sealed class GatewayRequestAdmissionScope : IDisposable
{
    private readonly Lock _gate = new();
    private RateLimitLease? _permit;
    private bool _disposed;

    internal void Attach(RateLimitLease permit)
    {
        ArgumentNullException.ThrowIfNull(permit);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_permit is not null)
                throw new InvalidOperationException("The Gateway request already owns an admission permit.");
            _permit = permit;
        }
    }

    public void Dispose()
    {
        RateLimitLease? permit;
        lock (_gate)
        {
            if (_disposed)
                return;
            _disposed = true;
            permit = _permit;
            _permit = null;
        }
        permit?.Dispose();
        GC.SuppressFinalize(this);
    }
}
