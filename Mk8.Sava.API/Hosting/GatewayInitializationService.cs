namespace Mk8.Sava.Hosting;

internal sealed class GatewayInitializationService(GatewayStagingPaths staging) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _ = staging.Staging; // Validate and reclaim only Gateway-owned scratch before accepting requests.
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
