namespace Mk8.Sava.Transport;

public interface IApplicationRpcAdmission
{
    ValueTask<IAsyncDisposable> AcquireAsync(ApplicationRpcLane lane, CancellationToken cancellationToken);
}
